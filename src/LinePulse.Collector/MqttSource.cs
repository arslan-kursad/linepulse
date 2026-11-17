using System.Buffers;
using System.Text.Json;
using LinePulse.Contracts;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Formatter;
using MQTTnet.Protocol;

namespace LinePulse.Collector;

/// <summary>
/// The test-bench side of the gateway: an MQTT subscriber that turns per-part
/// test records into spooled <see cref="TestRecord"/> events.
/// </summary>
/// <remarks>
/// <para>
/// End-of-line benches are PCs, and what they produce is one discrete record
/// per part. MQTT is the right carrier for that and the wrong one for
/// guarantees: QoS 1 is at-least-once, so the same part arrives twice whenever
/// a bench reconnects before its PUBACK came back. That is not a fault to be
/// fixed on the bench, it is traffic this side has to absorb -- which is why
/// the idempotency key is <c>(Source, Seq)</c> and not the event id a
/// restarted bench re-mints.
/// </para>
/// <para>
/// The acknowledgement is deliberately manual. The broker is told the message
/// is handled only after it is on disk, so a collector killed mid-handler
/// leaves the record queued on the broker instead of losing it quietly.
/// </para>
/// </remarks>
public sealed class MqttSource
{
    private readonly MqttOptions _options;
    private readonly EventSpooler _spooler;
    private readonly CollectorMetrics _metrics;
    private readonly ILogger<MqttSource> _log;

    /// <summary>Set when a record could not be spooled and is still in flight on the broker.</summary>
    private volatile bool _resumeRequested;

    public MqttSource(
        IOptions<CollectorOptions> options,
        EventSpooler spooler,
        CollectorMetrics metrics,
        ILogger<MqttSource> log)
    {
        _options = options.Value.Mqtt;
        _spooler = spooler;
        _metrics = metrics;
        _log = log;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        if (!_options.Enabled)
        {
            _log.LogInformation("MQTT source disabled by configuration");
            return;
        }

        var factory = new MqttClientFactory();
        using var client = factory.CreateMqttClient();
        client.ApplicationMessageReceivedAsync += OnMessageAsync;

        var clientOptions = new MqttClientOptionsBuilder()
            .WithTcpServer(_options.Host, _options.Port)
            .WithClientId(_options.ClientId)
            .WithProtocolVersion(MqttProtocolVersion.V500)
            // Resume the existing session rather than starting a clean one, so
            // whatever the broker queued for this client id while the collector
            // was down is delivered instead of discarded.
            .WithCleanStart(false)
            .WithSessionExpiryInterval(_options.SessionExpiryIntervalSeconds)
            .WithKeepAlivePeriod(TimeSpan.FromSeconds(_options.KeepAliveSeconds))
            .Build();

        var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
            .WithTopicFilter(_options.TopicFilter, MqttQualityOfServiceLevel.AtLeastOnce)
            .Build();

        var backoff = new Backoff();

        while (!ct.IsCancellationRequested)
        {
            try
            {
                if (_resumeRequested && client.IsConnected)
                {
                    // A record we failed to spool is still unacknowledged on the
                    // broker, and MQTT only resends in-flight messages when a
                    // session is resumed. Dropping and resuming the session is
                    // what turns "left unacknowledged" into "delivered again".
                    _resumeRequested = false;
                    _log.LogWarning("resuming the MQTT session so the broker resends unspooled records");
                    await client.DisconnectAsync(cancellationToken: ct);
                }

                if (!client.IsConnected)
                {
                    var connected = await client.ConnectAsync(clientOptions, ct);
                    _log.LogInformation(
                        "MQTT connected to {Host}:{Port} as {ClientId} (session resumed: {Resumed})",
                        _options.Host,
                        _options.Port,
                        _options.ClientId,
                        connected.IsSessionPresent);

                    var result = await client.SubscribeAsync(subscribeOptions, ct);
                    foreach (var item in result.Items)
                    {
                        _log.LogInformation(
                            "MQTT subscribed {Topic} -> {Result}",
                            item.TopicFilter.Topic,
                            item.ResultCode);
                    }

                    backoff.Reset();
                }

                // Supervision only: the message flow runs on the client's own
                // callbacks. Polling the flag is enough because a dead socket
                // is detected by the keep-alive, and there is nothing useful to
                // do in between.
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = backoff.Peek();
                _log.LogWarning(
                    "MQTT connection to {Host}:{Port} failed (attempt {Attempt}): {Reason}; retrying in {Delay:0.0}s",
                    _options.Host,
                    _options.Port,
                    backoff.Attempt + 1,
                    ex.Message,
                    delay.TotalSeconds);

                try
                {
                    await backoff.DelayAsync(ct);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }

        if (client.IsConnected)
        {
            try
            {
                // A normal DISCONNECT keeps the session on the broker for the
                // configured expiry, so records published while the collector
                // is stopped are waiting for it when it starts again.
                await client.DisconnectAsync(cancellationToken: CancellationToken.None);
            }
            catch (Exception ex)
            {
                _log.LogDebug(ex, "MQTT disconnect failed during shutdown");
            }
        }

        _log.LogInformation("MQTT source stopped");
    }

    private async Task OnMessageAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        // Hold the PUBACK back until the record is on disk.
        e.AutoAcknowledge = false;

        bool acknowledge;
        try
        {
            acknowledge = await HandleAsync(e);
        }
        catch (Exception ex)
        {
            // Left unacknowledged on purpose: the failure was ours (disk full,
            // spool locked), not the message's, so the broker should keep it.
            _log.LogError(ex, "failed to spool message from {Topic}", e.ApplicationMessage.Topic);
            _resumeRequested = true;
            acknowledge = false;
        }

        if (acknowledge)
        {
            await e.AcknowledgeAsync(CancellationToken.None);
        }
    }

    /// <summary>
    /// Handles one message.
    /// </summary>
    /// <returns>
    /// <c>true</c> when the broker may forget the message: either it is on
    /// disk, or it was rubbish that redelivery cannot improve.
    /// </returns>
    private async Task<bool> HandleAsync(MqttApplicationMessageReceivedEventArgs e)
    {
        var message = e.ApplicationMessage;

        TestRecord? record;
        try
        {
            record = Parse(message.Payload);
        }
        catch (JsonException ex)
        {
            _metrics.OnDeserializationFailure();
            // Acknowledged anyway: a malformed payload will not become valid on
            // redelivery, and refusing the ack would make the broker replay it
            // forever in front of the records that are fine.
            _log.LogWarning(
                ex,
                "unparseable payload on {Topic} ({Bytes} bytes) discarded",
                message.Topic,
                message.Payload.Length);
            return true;
        }

        if (record is null
            || string.IsNullOrEmpty(record.Source)
            || record.Seq <= 0
            || string.IsNullOrEmpty(record.Site)
            || string.IsNullOrEmpty(record.Line))
        {
            _metrics.OnDeserializationFailure();
            _log.LogWarning(
                "record on {Topic} lacks source/seq/site/line; discarded",
                message.Topic);
            return true;
        }

        // Arrival time is stamped here and nowhere else. The bench's own "ts"
        // is left exactly as it sent it, which is what lets a record that shows
        // up twenty seconds late still land in the right minute.
        var stamped = record with { IngestTimeUtc = DateTimeOffset.UtcNow };

        await _spooler.SpoolAsync(
            stamped,
            Subjects.Test(stamped.Site, stamped.Line),
            CancellationToken.None);

        return true;
    }

    /// <summary>
    /// Reads the record straight out of the client's receive buffers, which may
    /// be several segments, without first copying the payload into an array.
    /// </summary>
    private static TestRecord? Parse(ReadOnlySequence<byte> payload)
    {
        var reader = new Utf8JsonReader(payload);
        return JsonSerializer.Deserialize<TestRecord>(ref reader, JsonDefaults.Options);
    }
}
