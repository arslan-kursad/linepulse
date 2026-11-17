using LinePulse.Core.Kpi;
using Microsoft.Extensions.Options;

namespace LinePulse.Core.Realtime;

/// <summary>
/// Pushes a KPI tick to every connected dashboard.
/// </summary>
/// <remarks>
/// Recomputed once per tick and fanned out, rather than once per connected client:
/// five people watching the same board on five phones should cost the database one
/// query, not five. It also keeps the stream warm, which is what stops a proxy
/// from closing an idle connection and leaving a frozen board on the wall.
/// </remarks>
public sealed class KpiTickService(
    KpiService kpi,
    LiveFeed feed,
    IOptions<LiveOptions> options,
    ILogger<KpiTickService> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, options.Value.KpiTickSeconds));
        using var timer = new PeriodicTimer(interval);

        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            if (feed.ClientCount == 0)
            {
                continue;
            }

            try
            {
                feed.Publish("kpiTick", await kpi.LinesAsync(stoppingToken).ConfigureAwait(false));
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                log.LogError(ex, "kpi tick failed");
            }
        }
    }
}
