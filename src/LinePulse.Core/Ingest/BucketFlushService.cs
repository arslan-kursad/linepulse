namespace LinePulse.Core.Ingest;

/// <summary>
/// Rebuilds dirty minute buckets on a short timer.
/// </summary>
/// <remarks>
/// Separating this from ingest is what keeps a burst survivable: the subscriber's
/// job becomes "store the event and note which minute changed", which is bounded
/// work, and the expensive aggregation runs at its own pace. If the flush falls
/// behind, the queue coalesces and the next flush does the same amount of work --
/// it does not pile up per event.
/// There is deliberately no pruning job here. Idempotency rests on the primary key
/// of the raw tables, which lives exactly as long as the events it protects, so
/// there is no separate ledger of seen keys to age out.
/// </remarks>
public sealed class BucketFlushService(
    IngestPipeline pipeline,
    IngestOptions options,
    ILogger<BucketFlushService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await pipeline.StartupRestoreAsync(stoppingToken).ConfigureAwait(false);

        var flushInterval = TimeSpan.FromMilliseconds(Math.Max(50, options.BucketFlushMs));
        using var timer = new PeriodicTimer(flushInterval);

        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await pipeline.FlushAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                // Never let one bad flush kill the loop: the dirty queue is still
                // dirty and the next tick is a fraction of a second away.
                log.LogError(ex, "bucket flush failed");
            }
        }
    }
}
