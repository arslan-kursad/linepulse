using Microsoft.Extensions.Options;

namespace LinePulse.Collector;

/// <summary>
/// Writes the edge's health line on a fixed beat and keeps the deduplication
/// window pruned.
/// </summary>
/// <remarks>
/// <para>
/// This runs on its own timer rather than inside the forwarder's loop because
/// the forwarder is exactly what stalls during an outage -- it sits in backoff
/// waiting for the uplink -- and the outage is when the line is needed most.
/// </para>
/// <para>
/// How to read it: received climbing while forwarded is flat and depth grows
/// means the uplink is gone and nothing is being lost. Everything flat means
/// the lines are not running. Depth near zero with everything climbing is a
/// good day.
/// </para>
/// </remarks>
public sealed class SpoolMonitor
{
    private readonly SpoolOptions _options;
    private readonly Spool _spool;
    private readonly CollectorMetrics _metrics;
    private readonly ILogger<SpoolMonitor> _log;

    public SpoolMonitor(
        IOptions<CollectorOptions> options,
        Spool spool,
        CollectorMetrics metrics,
        ILogger<SpoolMonitor> log)
    {
        _options = options.Value.Spool;
        _spool = spool;
        _metrics = metrics;
        _log = log;
    }

    public async Task RunAsync(CancellationToken ct)
    {
        var reportInterval = TimeSpan.FromSeconds(Math.Max(1, _options.ReportIntervalSeconds));
        var pruneInterval = TimeSpan.FromSeconds(Math.Max(30, _options.PruneIntervalSeconds));
        var lastPrune = DateTimeOffset.UtcNow;
        using var timer = new PeriodicTimer(reportInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                await ReportAsync(ct);

                if (DateTimeOffset.UtcNow - lastPrune >= pruneInterval)
                {
                    lastPrune = DateTimeOffset.UtcNow;
                    // The deduplication window is a window, not a ledger:
                    // without pruning the table grows for as long as the box
                    // runs and the duplicate check gets slower every shift.
                    var pruned = await _spool.PruneSeenAsync(ct);
                    if (pruned > 0)
                    {
                        _log.LogInformation("pruned {Count} expired dedup keys", pruned);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal shutdown; the final line below is the one a person reads
            // after a restart to see where the previous run left off.
        }

        await ReportAsync(CancellationToken.None);
    }

    private async Task ReportAsync(CancellationToken ct)
    {
        long depth;
        try
        {
            depth = await _spool.DepthAsync(ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Diagnostics must not fail the component that produces them; -1
            // in the line says "unknown" without hiding the other numbers.
            _log.LogDebug(ex, "spool depth query failed");
            depth = -1;
        }

        _log.LogInformation(
            "edge health: spool depth={Depth} | received opcua={Machine} bench={Bench} | spooled={Spooled} duplicatesRejected={Duplicates} | forwarded={Forwarded} batchesResent={Resent} | unparseable={Unparseable} counterRestarts={Restarts}",
            depth,
            _metrics.ReceivedMachineSamples,
            _metrics.ReceivedTestRecords,
            _metrics.Spooled,
            _metrics.DuplicatesRejected,
            _metrics.Forwarded,
            _metrics.BatchesResent,
            _metrics.DeserializationFailures,
            _metrics.CounterRestarts);
    }
}
