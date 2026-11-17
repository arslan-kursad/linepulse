using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace LinePulse.Collector;

/// <summary>
/// Drains the spool into a JetStream stream: the only component here that touches
/// the plant network.
/// </summary>
/// <remarks>
/// <para>
/// The rule that makes the whole gateway trustworthy is one line long: a spooled
/// event is deleted only after it has been stored durably somewhere that does not
/// depend on the consumer being awake. Everything else -- batching, backoff, the
/// stream setup -- is in service of that rule.
/// </para>
/// <para>
/// "Durably somewhere else" is the part that changed. An earlier version published
/// to core NATS and treated a PING/PONG round trip as the confirmation. That proves
/// the server accepted the message, and nothing more: core NATS keeps nothing for a
/// subscriber that is not connected, so with the KPI service down the spool entry was
/// deleted and the event was gone. Measured, a 15-second consumer outage lost 23% of
/// a run. A JetStream publish is acknowledged only once the message is persisted in
/// the stream, and a durable consumer on the other side picks it up whenever it
/// returns -- so the acknowledgement is now a statement about the event, not about
/// the connection.
/// </para>
/// <para>
/// Each message carries <c>Nats-Msg-Id = source#seq</c>. A batch retried after a
/// timeout or a reconnect is therefore refused by the stream's duplicate window for
/// the messages that did get in, and the core's primary key catches whatever the
/// window has forgotten. Retrying is always safe and never silent.
/// </para>
/// </remarks>
public sealed class NatsForwarder
{
    private readonly NatsOptions _options;
    private readonly Spool _spool;
    private readonly CollectorMetrics _metrics;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<NatsForwarder> _log;
    private bool _streamReady;

    public NatsForwarder(
        IOptions<CollectorOptions> options,
        Spool spool,
        CollectorMetrics metrics,
        ILoggerFactory loggerFactory,
        ILogger<NatsForwarder> log)
    {
        _options = options.Value.Nats;
        _spool = spool;
        _metrics = metrics;
        _loggerFactory = loggerFactory;
        _log = log;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var opts = NatsOpts.Default with
        {
            Url = _options.Url,
            Name = _options.ConnectionName,
            LoggerFactory = _loggerFactory,
            // Fail a publish instead of buffering it in memory while the link
            // is down. Silent buffering is the dangerous option here: it would
            // let a batch be acknowledged and deleted from the spool without
            // ever reaching the server.
            PublishTimeoutOnDisconnected = true,
        };

        await using var connection = new NatsConnection(opts);
        var backoff = new Backoff();

        _log.LogInformation("forwarding spool to NATS at {Url}", _options.Url);

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var drained = await DrainBatchAsync(connection, ct);
                if (drained > 0)
                {
                    if (backoff.Attempt > 0)
                    {
                        _log.LogInformation("NATS reachable again; draining the spool");
                    }

                    backoff.Reset();
                }
                else
                {
                    await Task.Delay(_options.IdleDelayMs, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                var delay = backoff.Peek();
                _log.LogWarning(
                    "NATS unavailable ({Reason}); spool depth {Depth}, retrying in {Delay:0.0}s",
                    ex.Message,
                    await DepthSafeAsync(ct),
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

        _log.LogInformation(
            "NATS forwarder stopped; {Depth} events remain spooled and will be sent on next start",
            await DepthSafeAsync(CancellationToken.None));
    }

    /// <summary>Publishes one batch and deletes it from the spool only once the stream has stored it.</summary>
    /// <returns>How many events left the spool.</returns>
    private async Task<int> DrainBatchAsync(NatsConnection connection, CancellationToken ct)
    {
        var batch = await _spool.PeekAsync(_options.BatchSize, ct);
        if (batch.Count == 0)
        {
            return 0;
        }

        if (connection.ConnectionState == NatsConnectionState.Closed)
        {
            // First use, or the server was down at start.
            await connection.ConnectAsync();
        }

        var js = new NatsJSContext(connection);
        var published = 0;
        try
        {
            await EnsureStreamAsync(js, ct);

            // One at a time and in order. Each publish waits for its own acknowledgement,
            // so a failure part-way leaves a known prefix in the stream, and the events
            // keep the order the line produced them in -- which the core's watermark
            // would otherwise have to count as lateness.
            foreach (var entry in batch)
            {
                using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeout.CancelAfter(TimeSpan.FromMilliseconds(Math.Max(100, _options.ConfirmTimeoutMs)));

                // The payload is already the serialised event: the spool stores exactly the
                // bytes that go on the wire, so a retry cannot re-serialise them differently.
                var ack = await js.PublishAsync(
                    entry.Subject,
                    entry.Payload,
                    serializer: NatsRawSerializer<byte[]>.Default,
                    headers: new NatsHeaders { ["Nats-Msg-Id"] = $"{entry.Source}#{entry.Seq}" },
                    cancellationToken: timeout.Token);
                ack.EnsureSuccess();
                published++;
            }
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            Abandon(published, batch.Count);
            throw new TimeoutException(
                $"no acknowledgement from the stream within {_options.ConfirmTimeoutMs} ms; batch left on the spool");
        }
        catch
        {
            Abandon(published, batch.Count);
            throw;
        }

        // Not cancellable: the stream has the batch, and skipping this write during
        // shutdown would only mean sending all of it again on next start.
        await _spool.AckAsync(batch, CancellationToken.None);
        _metrics.OnForwarded(batch.Count);
        return batch.Count;
    }

    /// <summary>A batch that failed part-way goes out again from the start; the stream and the core drop the repeats.</summary>
    private void Abandon(int published, int total)
    {
        _streamReady = false;
        if (published > 0)
        {
            _metrics.OnBatchResent();
            _log.LogWarning(
                "{Published} of {Total} events were stored before the failure; the whole batch will be sent again and the repeats dropped",
                published,
                total);
        }
    }

    /// <summary>
    /// Creates the stream if it is missing, or brings it to the configured shape. Done
    /// again after any failure, because a server that came back without its data
    /// directory has no stream, and publishing into nothing would fail for ever.
    /// </summary>
    private async Task EnsureStreamAsync(NatsJSContext js, CancellationToken ct)
    {
        if (_streamReady)
        {
            return;
        }

        await js.CreateOrUpdateStreamAsync(
            new StreamConfig(_options.Stream, [_options.Subject])
            {
                Storage = StreamConfigStorage.File,
                Retention = StreamConfigRetention.Limits,
                MaxAge = TimeSpan.FromHours(_options.StreamMaxAgeHours),
                DuplicateWindow = TimeSpan.FromMinutes(_options.DuplicateWindowMinutes),
            },
            ct);
        _streamReady = true;
    }

    private async Task<long> DepthSafeAsync(CancellationToken ct)
    {
        try
        {
            return await _spool.DepthAsync(ct);
        }
        catch (Exception ex)
        {
            // A depth query that fails must not take down the forwarder: the
            // number is diagnostics, the queue is the product.
            _log.LogDebug(ex, "spool depth query failed");
            return -1;
        }
    }
}
