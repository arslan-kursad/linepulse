using LinePulse.Contracts;

namespace LinePulse.Core.Kpi;

/// <summary>One stored machine sample, as the minute arithmetic needs it.</summary>
/// <remarks>Init-only properties rather than a positional record: the storage layer
/// materialises these with Dapper, which maps a positional record only when every
/// column type matches its constructor exactly -- and SQLite returns every INTEGER
/// as a 64-bit value. Property mapping converts by name and is provider-neutral.</remarks>
public sealed record BucketSample
{
    public long TsMs { get; init; }

    public int State { get; init; }

    public int AlarmCode { get; init; }

    public long GoodCount { get; init; }

    public long ScrapCount { get; init; }

    public double CycleMs { get; init; }

    public double IdealCycleMs { get; init; }

    public string ProgramId { get; init; } = "";
}

/// <summary>
/// Everything needed to rebuild one minute: the samples inside it, the last sample
/// before it, and when the first sample after it arrived.
/// </summary>
public sealed record MinuteInputs(BucketSample? Previous, IReadOnlyList<BucketSample> Rows, long? NextTsMs);

/// <summary>One rebuilt minute of one line.</summary>
public sealed record MinuteBucket(
    string Site,
    string Line,
    long BucketStartMs,
    string ProgramId,
    long Good,
    long Scrap,
    int Samples,
    double RunSeconds,
    double DownUnplannedSeconds,
    double DownPlannedSeconds,
    double IdealRunSeconds,
    double IdealCycleMs,
    double AvgCycleMs);

/// <summary>The two rules that are configuration rather than arithmetic.</summary>
public sealed record AggregationRules(long MaxStateCarryMs, double TimeScale)
{
    public static AggregationRules From(IngestOptions options) =>
        new(Math.Max(1, options.MaxStateCarrySeconds) * 1000L, options.TimeScale > 0 ? options.TimeScale : 1.0);
}

/// <summary>
/// The arithmetic a shift report argues about: production, run time and downtime
/// for one minute, rebuilt from raw samples.
/// </summary>
/// <remarks>
/// <para>
/// A pure function -- same samples in, same minute out, no clock and no database --
/// because the number has to be reproducible when someone challenges it, and
/// because it is the one piece every storage provider would otherwise have to
/// re-implement and get subtly different.
/// </para>
/// <para>
/// State is a step function: each sample holds from its own timestamp until the
/// next sample. That is what an OPC-UA subscription means, since it reports by
/// exception -- a line faulted for five minutes sends one sample when it faults
/// and one when it restarts, and nothing in between. So a silent minute in the
/// middle of a stop is still a minute of downtime, and is billed as one.
/// </para>
/// <para>
/// Two things are deliberately not billed. Time after the newest sample has no
/// evidence behind it yet: the minute is still in progress, and it is rebuilt
/// when the next sample arrives. And a state is trusted for at most
/// <see cref="AggregationRules.MaxStateCarryMs"/> without a fresh sample, because
/// past that the silence is more likely a collector outage than an unchanged line.
/// </para>
/// </remarks>
public static class MinuteAggregator
{
    /// <returns>The rebuilt minute, or null when nothing in it was observed or
    /// carried in -- "no data for that minute" is a true statement, an invented
    /// bucket would be a guess presented as a measurement.</returns>
    public static MinuteBucket? Aggregate(
        string site,
        string line,
        long bucketStartMs,
        MinuteInputs inputs,
        AggregationRules rules)
    {
        var rows = inputs.Rows;
        var previous = inputs.Previous;
        if (rows.Count == 0 && previous is null)
        {
            return null;
        }

        var bucketEndMs = bucketStartMs + Clock.MinuteMs;
        var (good, scrap, idealRunSeconds, idealCycleMs, programId) = Production(previous, rows);
        var (run, downUnplanned, downPlanned) = StateTime(previous, rows, inputs.NextTsMs, bucketStartMs, bucketEndMs, rules);

        if (rows.Count == 0 && run + downUnplanned + downPlanned <= 0)
        {
            return null;
        }

        var cycles = rows.Where(r => r.CycleMs > 0).Select(r => r.CycleMs).ToList();

        return new MinuteBucket(
            site,
            line,
            bucketStartMs,
            programId,
            good,
            scrap,
            rows.Count,
            run,
            downUnplanned,
            downPlanned,
            idealRunSeconds,
            idealCycleMs,
            cycles.Count > 0 ? cycles.Average() : 0);
    }

    /// <summary>Parts made in the minute, from counter deltas.</summary>
    private static (long Good, long Scrap, double IdealRunSeconds, double IdealCycleMs, string ProgramId) Production(
        BucketSample? previous,
        IReadOnlyList<BucketSample> rows)
    {
        long good = 0, scrap = 0;
        double idealRunSeconds = 0;

        // The sample before the minute is the counter baseline. Without one this is
        // the line's first minute on record, and its first sample can only
        // establish a baseline: its absolute value is however many parts the PLC
        // had counted before anyone was listening.
        long? baselineGood = previous?.GoodCount;
        long? baselineScrap = previous?.ScrapCount;
        var idealCycleMs = previous?.IdealCycleMs ?? 0;
        var programId = previous?.ProgramId ?? "";

        foreach (var row in rows)
        {
            if (row.IdealCycleMs > 0)
            {
                idealCycleMs = row.IdealCycleMs;
            }

            if (!string.IsNullOrEmpty(row.ProgramId))
            {
                programId = row.ProgramId;
            }

            if (baselineGood is null || baselineScrap is null)
            {
                baselineGood = row.GoodCount;
                baselineScrap = row.ScrapCount;
                continue;
            }

            // A counter that went backwards is a restart -- the PLC was power
            // cycled or the collector reconnected to a reset tag. Emitting the
            // negative difference would show the line un-building pumps, so the
            // delta is dropped and the baseline re-anchored to the new counter.
            var deltaGood = row.GoodCount - baselineGood.Value;
            var deltaScrap = row.ScrapCount - baselineScrap.Value;
            if (deltaGood < 0 || deltaScrap < 0)
            {
                deltaGood = 0;
                deltaScrap = 0;
            }

            good += deltaGood;
            scrap += deltaScrap;
            baselineGood = row.GoodCount;
            baselineScrap = row.ScrapCount;

            // Theoretical time for the parts actually made, at the ideal cycle in
            // force when they were made. Summed per delta rather than once per
            // minute so that a changeover inside the minute stays honest.
            idealRunSeconds += (deltaGood + deltaScrap) * idealCycleMs / 1000.0;
        }

        return (good, scrap, idealRunSeconds, idealCycleMs, programId);
    }

    /// <summary>Seconds of the minute spent running, down unplanned and down planned.</summary>
    private static (double Run, double DownUnplanned, double DownPlanned) StateTime(
        BucketSample? previous,
        IReadOnlyList<BucketSample> rows,
        long? nextTsMs,
        long bucketStartMs,
        long bucketEndMs,
        AggregationRules rules)
    {
        double run = 0, downUnplanned = 0, downPlanned = 0;

        var timeline = new List<BucketSample>(rows.Count + 1);
        if (previous is not null)
        {
            timeline.Add(previous);
        }

        timeline.AddRange(rows);

        for (var i = 0; i < timeline.Count; i++)
        {
            var sample = timeline[i];
            var followingTsMs = i + 1 < timeline.Count ? timeline[i + 1].TsMs : nextTsMs;
            if (followingTsMs is not { } following)
            {
                // Newest sample on record: what happens after it is not known yet.
                continue;
            }

            var start = Math.Max(sample.TsMs, bucketStartMs);
            var end = Math.Min(Math.Min(following, sample.TsMs + rules.MaxStateCarryMs), bucketEndMs);
            if (end <= start)
            {
                continue;
            }

            var seconds = (end - start) / 1000.0 * rules.TimeScale;
            if (sample.State == (int)LineState.Running)
            {
                run += seconds;
            }
            else if (ReasonCatalog.IsPlanned(sample.AlarmCode, sample.State))
            {
                downPlanned += seconds;
            }
            else
            {
                // Idle with no alarm lands here on purpose. "The line was not
                // running and nobody said why" is the loss plants most want to
                // see, so it is counted as unplanned rather than excused.
                downUnplanned += seconds;
            }
        }

        return (run, downUnplanned, downPlanned);
    }
}
