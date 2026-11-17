using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LinePulse.Core.Ingest;

/// <summary>
/// Subscribes to the plant's event bus and feeds the ingest pipeline.
/// </summary>
/// <remarks>
/// <para>
/// One wildcard subscription for the whole plant. Adding line 14 is a collector
/// configuration change, not a redeploy of the KPI service.
/// </para>
/// <para>
/// The subscription is a durable JetStream consumer, and that is a correction, not a
/// preference. An earlier version subscribed to core NATS on the reasoning that the
/// collector's spool already provides replay. It does not cover this case: the spool
/// entry is deleted once the event is safely stored, and with plain publish/subscribe
/// "safely stored" meant "the server took it", so an event published while this
/// service was down was accepted and then dropped. Measured: a 15-second outage lost
/// 23% of a run. Durable consumption puts the memory of "how far has this reader got"
/// in the server, so the service can be stopped, redeployed or crash and resume at the
/// first event it did not acknowledge.
/// </para>
/// <para>
/// Each message is acknowledged only after the pipeline has stored it. If the process
/// dies in between, the message comes back, and the pipeline's primary-key idempotency
/// makes the repeat harmless: at-least-once delivery is safe here exactly because the
/// consumer is idempotent.
/// </para>
/// <para>
/// A dropped connection is normal, so it is handled as a loop rather than an
/// exception: the broker may be restarting, and the collector will be spooling
/// while we are away.
/// </para>
/// </remarks>
public sealed class NatsIngestService(
    IngestPipeline pipeline,
    IOptions<NatsOptions> options,
    ILoggerFactory loggerFactory,
    ILogger<NatsIngestService> log) : BackgroundService
{
    private readonly NatsOptions _options = options.Value;
    private volatile bool _connected;

    public bool Connected => _connected;

    public string Url => _options.Url;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var retryDelay = TimeSpan.FromSeconds(Math.Max(1, _options.ReconnectSeconds));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ConsumeAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _connected = false;
                log.LogWarning(ex, "nats subscription dropped; retrying in {Delay}s", retryDelay.TotalSeconds);
            }

            try
            {
                await Task.Delay(retryDelay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }

        _connected = false;
    }

    private async Task ConsumeAsync(CancellationToken ct)
    {
        var opts = NatsOpts.Default with
        {
            Url = _options.Url,
            Name = _options.ClientName,
            LoggerFactory = loggerFactory,
            // Keep retrying rather than throwing on a cold start: in a plant the
            // broker and this service come up in whatever order systemd decides.
            RetryOnInitialConnect = true,
            MaxReconnectRetry = -1,
        };

        await using var connection = new NatsConnection(opts);
        await connection.ConnectAsync().ConfigureAwait(false);

        // The stream belongs to the collector, so before the first collector has run
        // there is nothing to attach to. That is an ordinary start-up order, handled by
        // the retry loop above rather than by creating a second copy of the stream here
        // that could drift from the first.
        var js = new NatsJSContext(connection);
        var consumer = await js
            .CreateOrUpdateConsumerAsync(
                _options.Stream,
                new ConsumerConfig(_options.Durable)
                {
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    FilterSubject = _options.Subject,
                    // Long enough for a slow flush, short enough that a message whose
                    // handler died comes back within the minute.
                    AckWait = TimeSpan.FromSeconds(30),
                    MaxAckPending = 2_000,
                    // A message that fails every time must not be redelivered for ever.
                    MaxDeliver = 10,
                },
                ct)
            .ConfigureAwait(false);

        _connected = true;
        log.LogInformation(
            "consuming {Subject} from stream {Stream} as durable consumer {Durable} on {Url}",
            _options.Subject, _options.Stream, _options.Durable, _options.Url);

        await foreach (var message in consumer
            .ConsumeAsync(serializer: NatsRawSerializer<byte[]>.Default, cancellationToken: ct)
            .ConfigureAwait(false))
        {
            if (message.Data is not { Length: > 0 })
            {
                await message.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            // Acknowledge only after the event has been stored. HandleAsync absorbs the
            // events it refuses (malformed, no key) and counts them, so a poison message
            // is acknowledged and not redelivered; an exception means a genuine failure,
            // and the unacknowledged message returns.
            await pipeline.HandleAsync(message.Subject, message.Data, ct).ConfigureAwait(false);
            await message.AckAsync(cancellationToken: ct).ConfigureAwait(false);
        }

        _connected = false;
    }
}
