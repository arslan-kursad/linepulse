using System.Text.Json;
using LinePulse.Core.Ingest;
using LinePulse.Core.Kpi;
using LinePulse.Core.Realtime;
using LinePulse.Core.Storage;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace LinePulse.Core.Api;

/// <summary>
/// The HTTP surface. One place, so the contract the dashboard codes against is
/// readable in a single file.
/// </summary>
public static class ApiRoutes
{
    public static void MapLinePulseApi(this WebApplication app)
    {
        app.MapGet("/api/health", (
            IngestCounters counters,
            NatsIngestService nats,
            DedupGate dedup,
            MinuteBucketQueue buckets,
            WatermarkTracker watermarks,
            LiveFeed live,
            SqliteDb db,
            IOptions<IngestOptions> ingest) =>
        {
            var options = ingest.Value;
            return new HealthDto(
                nats.Connected ? "ok" : "degraded",
                new HealthCountersDto(
                    counters.Ingested,
                    counters.Deduped,
                    counters.LateApplied,
                    counters.LateBeyondWatermark,
                    Math.Round(counters.UptimeSeconds, 1),
                    counters.Received,
                    counters.Malformed,
                    counters.MissingKey,
                    counters.OutOfOrderState,
                    counters.BucketRecomputes,
                    counters.MicroStops),
                new HealthDetailDto(
                    nats.Connected,
                    nats.Url,
                    db.Path,
                    options.WatermarkSeconds,
                    options.MicroStopThresholdSeconds,
                    buckets.DirtyCount,
                    dedup.CacheCount,
                    watermarks.Streams.Select(s => (s.Site, s.Line)).Distinct().Count(),
                    live.ClientCount));
        });

        app.MapGet("/api/lines", (KpiService kpi, CancellationToken ct) => kpi.LinesAsync(ct));

        app.MapGet("/api/lines/{site}/{line}/oee", async (
            string site,
            string line,
            [FromQuery] DateTimeOffset? from,
            [FromQuery] DateTimeOffset? to,
            KpiService kpi,
            TimeProvider time,
            CancellationToken ct) =>
        {
            var (start, end) = Window(kpi, time, site, from, to);
            return await kpi.OeeSeriesAsync(site, line, start, end, ct).ConfigureAwait(false);
        });

        app.MapGet("/api/lines/{site}/{line}/downtime", async (
            string site,
            string line,
            [FromQuery] DateTimeOffset? from,
            [FromQuery] DateTimeOffset? to,
            KpiService kpi,
            TimeProvider time,
            CancellationToken ct) =>
        {
            var (start, end) = Window(kpi, time, site, from, to);
            return await kpi.DowntimeAsync(site, line, start, end, ct).ConfigureAwait(false);
        });

        app.MapGet("/api/quality/fpy", async (
            [FromQuery] DateTimeOffset? from,
            [FromQuery] DateTimeOffset? to,
            KpiService kpi,
            TimeProvider time,
            CancellationToken ct) =>
        {
            // No site in the route, so there is no single shift window to default to:
            // the two plants are in different time zones. Eight hours back is one
            // shift's worth of data without pretending to know whose shift it is.
            var end = to ?? time.GetUtcNow();
            var start = from ?? end.AddHours(-8);
            return await kpi.FpyAsync(start, end, ct).ConfigureAwait(false);
        });

        app.MapGet("/api/quality/fixtures", (KpiService kpi, CancellationToken ct) => kpi.FixturesAsync(ct));

        app.MapGet("/api/sites/rollup", (KpiService kpi, CancellationToken ct) => kpi.RollupAsync(ct));

        app.MapGet("/api/parts/{serial}", async (string serial, KpiService kpi, CancellationToken ct) =>
        {
            var part = await kpi.PartAsync(serial, ct).ConfigureAwait(false);
            return part is null
                ? Results.NotFound(new { serial, message = "no test record stored for this serial" })
                : Results.Ok(part);
        });

        app.MapGet("/api/events/stream", StreamAsync);
    }

    /// <summary>
    /// Server-sent events rather than WebSockets: the traffic is one-way, SSE
    /// reconnects by itself, and it survives the HTTP proxies a plant network puts
    /// in the way. The KPI tick doubles as the keepalive, so an idle stream is
    /// never silent long enough for a proxy to decide it is dead.
    /// </summary>
    private static async Task StreamAsync(
        HttpContext context,
        LiveFeed feed,
        KpiService kpi,
        LineSnapshotCache snapshots,
        CancellationToken ct)
    {
        context.Response.Headers.ContentType = "text/event-stream";
        context.Response.Headers.CacheControl = "no-cache";
        context.Response.Headers.Connection = "keep-alive";
        // Tells nginx not to buffer, which would otherwise hold events until the
        // response closed and make the live board look broken.
        context.Response.Headers["X-Accel-Buffering"] = "no";

        await context.Response.WriteAsync("retry: 3000\n\n", ct).ConfigureAwait(false);
        await WriteEventAsync(context, "lineState", snapshots.All, ct).ConfigureAwait(false);
        await WriteEventAsync(context, "kpiTick", await kpi.LinesAsync(ct).ConfigureAwait(false), ct)
            .ConfigureAwait(false);

        try
        {
            await foreach (var message in feed.SubscribeAsync(ct).ConfigureAwait(false))
            {
                await WriteEventAsync(context, message.Name, message.Payload, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // The dashboard navigated away. Not an error.
        }
    }

    private static async Task WriteEventAsync(HttpContext context, string name, object payload, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload, SseJson);
        await context.Response.WriteAsync($"event: {name}\ndata: {json}\n\n", ct).ConfigureAwait(false);
        await context.Response.Body.FlushAsync(ct).ConfigureAwait(false);
    }

    private static readonly JsonSerializerOptions SseJson = new(JsonSerializerDefaults.Web);

    /// <summary>Defaults an unspecified window to the current shift at that site.</summary>
    private static (DateTimeOffset From, DateTimeOffset To) Window(
        KpiService kpi, TimeProvider time, string site, DateTimeOffset? from, DateTimeOffset? to)
    {
        if (from is null && to is null)
        {
            var shift = kpi.CurrentShift(site);
            return (shift.StartUtc, shift.EndUtc);
        }

        var end = to ?? time.GetUtcNow();
        return (from ?? end.AddHours(-8), end);
    }
}
