namespace LinePulse.Collector;

/// <summary>
/// Runs the long-lived parts of the gateway and ties their lifetimes together.
/// </summary>
/// <remarks>
/// <para>
/// The two sources and the forwarder are independent on purpose: a dead PLC
/// must not stop test records arriving, and a dead uplink must not stop either
/// of them being collected. They only meet at the spool.
/// </para>
/// <para>
/// Shutdown flushes nothing. There is nothing to flush -- an event is on disk
/// before anyone is told it exists -- so stopping is just a matter of letting
/// the loops notice the token. Whatever is still spooled goes out on the next
/// start, which is the same code path as a two-hour network outage and is
/// therefore the same code path that gets tested every time.
/// </para>
/// </remarks>
public sealed class CollectorWorker : BackgroundService
{
    private readonly OpcUaSource _opcUa;
    private readonly MqttSource _mqtt;
    private readonly NatsForwarder _forwarder;
    private readonly SpoolMonitor _monitor;
    private readonly ILogger<CollectorWorker> _log;

    public CollectorWorker(
        OpcUaSource opcUa,
        MqttSource mqtt,
        NatsForwarder forwarder,
        SpoolMonitor monitor,
        ILogger<CollectorWorker> log)
    {
        _opcUa = opcUa;
        _mqtt = mqtt;
        _forwarder = forwarder;
        _monitor = monitor;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _log.LogInformation("LinePulse collector starting");

        var tasks = new[]
        {
            Supervise("opcua", _opcUa.RunAsync, stoppingToken),
            Supervise("mqtt", _mqtt.RunAsync, stoppingToken),
            Supervise("nats", _forwarder.RunAsync, stoppingToken),
            Supervise("monitor", _monitor.RunAsync, stoppingToken),
        };

        await Task.WhenAll(tasks);
        _log.LogInformation("LinePulse collector stopped");
    }

    /// <summary>
    /// Keeps one component's failure from taking down the others.
    /// </summary>
    /// <remarks>
    /// Each component already retries internally; this is the last line of
    /// defence against a bug in that retry loop. Logging the component name and
    /// carrying on beats a process that exits at 03:00 and is noticed at 06:00
    /// when the shift report is empty.
    /// </remarks>
    private async Task Supervise(string name, Func<CancellationToken, Task> run, CancellationToken ct)
    {
        try
        {
            // Yield first so a component that does synchronous work before its
            // first await cannot hold up the start of the others.
            await Task.Yield();
            await run(ct);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            _log.LogCritical(ex, "component {Component} stopped unexpectedly", name);
        }
    }
}
