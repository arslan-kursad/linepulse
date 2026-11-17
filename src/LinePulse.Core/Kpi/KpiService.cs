using System.Text.Json;
using LinePulse.Contracts;
using LinePulse.Core.Api;
using LinePulse.Core.Realtime;
using LinePulse.Core.Storage;
using Microsoft.Extensions.Options;

namespace LinePulse.Core.Kpi;

/// <summary>
/// Everything the API returns, computed from stored rows.
/// </summary>
/// <remarks>
/// No KPI is kept as a running total in memory. Every number here is derived from
/// the database on request, which is what makes a late event correctable: repair
/// the rows and the next read is right. A cached counter would have to be found
/// and patched, and the one nobody remembered to patch is the one that ends up on
/// a slide.
/// </remarks>
public sealed class KpiService(
    IKpiStore store,
    LineSnapshotCache snapshots,
    ShiftCalendar shifts,
    SpcAnalyzer spc,
    IOptions<SpcOptions> spcOptions,
    TimeProvider time)
{
    private readonly SpcOptions _spc = spcOptions.Value;

    public async Task<IReadOnlyList<LineDto>> LinesAsync(CancellationToken ct)
    {
        var now = time.GetUtcNow();
        var lines = await ResolveLinesAsync(ct).ConfigureAwait(false);
        var result = new List<LineDto>(lines.Count);

        // One aggregate query per site, not per line: every line at a site shares
        // the same shift window, so the grouping belongs in the database.
        foreach (var siteGroup in lines.GroupBy(l => l.Site, StringComparer.Ordinal))
        {
            var shift = shifts.Current(siteGroup.Key, now);
            var aggregates = (await store
                .AggregateLinesAsync(siteGroup.Key, shift.StartMs, shift.EndMs, ct)
                .ConfigureAwait(false))
                .ToDictionary(a => a.Line, StringComparer.Ordinal);

            foreach (var line in siteGroup.OrderBy(l => l.Line, StringComparer.Ordinal))
            {
                var snapshot = snapshots.Get(line.Site, line.Line);
                aggregates.TryGetValue(line.Line, out var aggregate);

                var oee = aggregate is null
                    ? OeeMath.Empty
                    : OeeMath.Compute(
                        aggregate.RunSeconds,
                        aggregate.DownUnplannedSeconds,
                        aggregate.DownPlannedSeconds,
                        aggregate.IdealRunSeconds,
                        aggregate.Good,
                        aggregate.Scrap);

                result.Add(new LineDto(
                    line.Site,
                    line.Line,
                    snapshot?.State.ToString() ?? LineState.Idle.ToString(),
                    snapshot?.ProgramId ?? "",
                    ToDto(oee),
                    aggregate?.Good ?? 0,
                    aggregate?.Scrap ?? 0,
                    snapshot?.LastSeenUtc,
                    shift.Label,
                    shift.StartUtc,
                    snapshot?.AlarmCode ?? 0,
                    snapshot?.AlarmText ?? "",
                    Math.Round(aggregate?.RunSeconds ?? 0, 1),
                    Math.Round(aggregate?.DownUnplannedSeconds ?? 0, 1),
                    Math.Round(aggregate?.DownPlannedSeconds ?? 0, 1),
                    aggregate?.Revisions ?? 0,
                    aggregate?.LateBeyondCount ?? 0));
            }
        }

        return result;
    }

    public async Task<IReadOnlyList<MinuteBucketDto>> OeeSeriesAsync(
        string site, string line, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var rows = await store
            .MinuteBucketsAsync(site, line, Clock.ToMs(from), Clock.ToMs(to), ct)
            .ConfigureAwait(false);

        return rows.Select(r => new MinuteBucketDto(
            Clock.FromMs(r.BucketStartMs),
            r.ProgramId,
            r.Good,
            r.Scrap,
            r.Samples,
            Math.Round(r.RunSeconds, 1),
            Math.Round(r.DownUnplannedSeconds, 1),
            Math.Round(r.DownPlannedSeconds, 1),
            Math.Round(r.AvgCycleMs, 1),
            ToDto(OeeMath.Compute(
                r.RunSeconds, r.DownUnplannedSeconds, r.DownPlannedSeconds, r.IdealRunSeconds, r.Good, r.Scrap)),
            r.Revisions,
            r.LateBeyondCount,
            r.SealedAtMs is not null)).ToList();
    }

    public async Task<IReadOnlyList<DowntimeDto>> DowntimeAsync(
        string site, string line, DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var rows = await store
            .DowntimeParetoAsync(site, line, Clock.ToMs(from), Clock.ToMs(to), ct)
            .ConfigureAwait(false);

        var total = rows.Sum(r => r.TotalSeconds);
        return rows.Select(r => new DowntimeDto(
            r.ReasonCode,
            r.ReasonText,
            r.Planned,
            r.Stops,
            r.OpenStops,
            r.MicroStops,
            Math.Round(r.TotalSeconds, 1),
            Math.Round(r.LongestSeconds, 1),
            total > 0 ? Math.Round(100 * r.TotalSeconds / total, 1) : 0)).ToList();
    }

    public async Task<IReadOnlyList<FpyDto>> FpyAsync(DateTimeOffset from, DateTimeOffset to, CancellationToken ct)
    {
        var fromMs = Clock.ToMs(from);
        var toMs = Clock.ToMs(to);

        var yields = await store.FirstPassYieldAsync(fromMs, toMs, ct).ConfigureAwait(false);
        var failures = await store.FailureBreakdownAsync(fromMs, toMs, ct).ConfigureAwait(false);

        return yields.Select(y => new FpyDto(
            y.Site,
            y.Line,
            y.Tested,
            y.Passed,
            y.Tested - y.Passed,
            y.Tested > 0 ? Math.Round((double)y.Passed / y.Tested, 4) : 0,
            y.Retests,
            failures
                .Where(f => f.Site == y.Site && f.Line == y.Line)
                .OrderByDescending(f => f.Count)
                .Select(f => new FailureDto(f.CheckName, f.Count))
                .ToList())).ToList();
    }

    public async Task<IReadOnlyList<FixtureSpc>> FixturesAsync(CancellationToken ct)
    {
        // A day of history is enough for a rolling window of a few hundred parts and
        // keeps the scan bounded; a fixture that has not run today has nothing to say.
        var sinceMs = Clock.ToMs(time.GetUtcNow().AddDays(-1));
        var samples = await store.FixtureSamplesAsync(_spc.WindowSize, sinceMs, ct).ConfigureAwait(false);

        return samples
            .GroupBy(s => (s.Site, s.Line, s.FixtureId))
            .Select(g =>
            {
                var ordered = g.OrderBy(s => s.TsMs).ToList();
                var limit = ordered.Min(s => s.LeakLimit);
                return spc.Analyse(
                    g.Key.Site,
                    g.Key.Line,
                    g.Key.FixtureId,
                    ordered.Select(s => s.LeakRate).ToList(),
                    limit);
            })
            .OrderBy(f => f.Site, StringComparer.Ordinal)
            .ThenBy(f => f.Line, StringComparer.Ordinal)
            .ThenBy(f => f.FixtureId, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>
    /// Cross-site comparison. The two plants build the same product family, so the
    /// only comparison worth putting on a screen is per product: comparing a site
    /// running vacuum pumps with a site running oil pumps says nothing about either.
    /// </summary>
    public async Task<IReadOnlyList<RollupDto>> RollupAsync(CancellationToken ct)
    {
        var now = time.GetUtcNow();
        var lines = await ResolveLinesAsync(ct).ConfigureAwait(false);
        var result = new List<RollupDto>();

        foreach (var siteGroup in lines.GroupBy(l => l.Site, StringComparer.Ordinal).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var site = siteGroup.Key;
            var shift = shifts.Current(site, now);

            var aggregates = await store.AggregateLinesAsync(site, shift.StartMs, shift.EndMs, ct).ConfigureAwait(false);
            var products = await store.AggregateProductsAsync(site, shift.StartMs, shift.EndMs, ct).ConfigureAwait(false);
            var yields = await store.FirstPassYieldAsync(shift.StartMs, shift.EndMs, ct).ConfigureAwait(false);

            var stops = 0;
            var microStops = 0;
            foreach (var line in siteGroup)
            {
                foreach (var reason in await store
                    .DowntimeParetoAsync(site, line.Line, shift.StartMs, shift.EndMs, ct).ConfigureAwait(false))
                {
                    stops += reason.Stops;
                    microStops += reason.MicroStops;
                }
            }

            var good = aggregates.Sum(a => a.Good);
            var scrap = aggregates.Sum(a => a.Scrap);
            var run = aggregates.Sum(a => a.RunSeconds);
            var downUnplanned = aggregates.Sum(a => a.DownUnplannedSeconds);
            var downPlanned = aggregates.Sum(a => a.DownPlannedSeconds);
            var idealRun = aggregates.Sum(a => a.IdealRunSeconds);

            var siteYields = yields.Where(y => y.Site == site).ToList();
            var tested = siteYields.Sum(y => y.Tested);
            var passed = siteYields.Sum(y => y.Passed);

            result.Add(new RollupDto(
                site,
                shift.UtcOffsetHours,
                shift.Label,
                shift.StartUtc,
                shift.EndUtc,
                siteGroup.Count(),
                good,
                scrap,
                Math.Round(run, 1),
                Math.Round(downUnplanned, 1),
                Math.Round(downPlanned, 1),
                ToDto(OeeMath.Compute(run, downUnplanned, downPlanned, idealRun, good, scrap)),
                tested,
                passed,
                tested > 0 ? Math.Round((double)passed / tested, 4) : 0,
                stops,
                microStops,
                products.Select(p => new RollupProductDto(
                    p.ProgramId,
                    p.Good,
                    p.Scrap,
                    ToDto(OeeMath.Compute(
                        p.RunSeconds, p.DownUnplannedSeconds, p.DownPlannedSeconds, p.IdealRunSeconds, p.Good, p.Scrap))))
                    .ToList()));
        }

        return result;
    }

    /// <summary>
    /// Full genealogy for one part. This is the query a warranty claim turns into:
    /// a serial number arrives from the field and somebody has to say which housing
    /// lot it came from, which tool cut that lot, and how the rest of the lot tested.
    /// </summary>
    public async Task<PartDto?> PartAsync(string serial, CancellationToken ct)
    {
        var attempts = await store.TestsForSerialAsync(serial, ct).ConfigureAwait(false);
        if (attempts.Count == 0)
        {
            return null;
        }

        var first = attempts[0];
        var lotQuality = await store.LotQualityAsync(first.HousingLotId, ct).ConfigureAwait(false);
        var tools = await store.ToolsForLotAsync(first.HousingLotId, ct).ConfigureAwait(false);

        return new PartDto(
            serial,
            first.Site,
            first.Line,
            first.ProgramId,
            ToDto(first),
            attempts.Select(ToDto).ToList(),
            attempts.Count > 1,
            new LotDto(
                first.HousingLotId,
                lotQuality?.Parts ?? 0,
                lotQuality?.Tested ?? 0,
                lotQuality?.Passed ?? 0,
                lotQuality is { Tested: > 0 } ? Math.Round((double)lotQuality.Passed / lotQuality.Tested, 4) : 0,
                tools.Select(t => new LotToolDto(
                    t.Site,
                    t.Line,
                    t.ToolId,
                    Clock.FromMs(t.FirstSeenMs),
                    Clock.FromMs(t.LastSeenMs),
                    t.Samples,
                    t.AvgSpindleLoadPct is { } load ? Math.Round(load, 1) : null,
                    t.MinToolLifePct is { } life ? Math.Round(life, 1) : null)).ToList()));
    }

    /// <summary>Default window when a caller gives none: the current shift at that site.</summary>
    public ShiftWindow CurrentShift(string site) => shifts.Current(site, time.GetUtcNow());

    private async Task<IReadOnlyList<LineRef>> ResolveLinesAsync(CancellationToken ct)
    {
        var fromCache = snapshots.All;
        if (fromCache.Count > 0)
        {
            return fromCache.Select(s => new LineRef { Site = s.Site, Line = s.Line }).ToList();
        }

        // Cold start with history on disk, or a plant that is not running right now.
        return await store.KnownLinesAsync(ct).ConfigureAwait(false);
    }

    private static OeeDto ToDto(OeeFactors factors) =>
        new(factors.Availability, factors.Performance, factors.Quality, factors.Oee);

    private static PartTestDto ToDto(TestRecordRow row) =>
        new(
            Clock.FromMs(row.TsMs),
            row.Station,
            row.FixtureId,
            row.Result,
            row.Passed,
            new MeasurementsDto(
                row.LeakRate,
                row.LeakLimit,
                row.FlowLpm,
                row.FlowTarget,
                row.PressureBar,
                DeserializeTorque(row.TorqueJson)),
            row.OperatorId,
            row.Source,
            row.Seq);

    private static double[] DeserializeTorque(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<double[]>(json) ?? [];
        }
        catch (JsonException)
        {
            // A malformed stored blob must not take out a traceability lookup: the
            // answer "here is the part, minus one measurement" beats a 500.
            return [];
        }
    }
}
