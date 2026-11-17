using LinePulse.Contracts;
using LinePulse.Core;
using LinePulse.Core.Kpi;

namespace LinePulse.Tests;

/// <summary>
/// The minute arithmetic every KPI is built from.
/// </summary>
/// <remarks>
/// The end-to-end verifier proves that the same samples produce the same buckets whatever
/// order they arrive in. It cannot prove the buckets are <em>right</em>: a consistently wrong
/// aggregator would pass it. These tests check the arithmetic against hand-worked numbers,
/// which is the only way to catch a convention that is reproducible and still not what a
/// production engineer means.
/// </remarks>
public class MinuteAggregatorTests
{
    private const long Minute = Clock.MinuteMs;
    private const double IdealCycleMs = 7_200;

    // Real time, so a second of event time is a second of production time and the expected
    // values can be worked out by hand.
    private static readonly AggregationRules Rules = AggregationRules.From(new IngestOptions
    {
        MaxStateCarrySeconds = 3600,
        TimeScale = 1.0,
    });

    private static BucketSample Sample(
        long tsMs, LineState state = LineState.Running, long good = 1000, long scrap = 10,
        int alarm = 0, double cycleMs = IdealCycleMs, string program = "OP-4412-A") =>
        new()
        {
            TsMs = tsMs,
            State = (int)state,
            AlarmCode = alarm,
            GoodCount = good,
            ScrapCount = scrap,
            CycleMs = cycleMs,
            IdealCycleMs = IdealCycleMs,
            ProgramId = program,
        };

    private static MinuteBucket Aggregate(
        BucketSample? previous, IReadOnlyList<BucketSample> rows, long? nextTsMs, long bucketStart = 0)
    {
        var bucket = MinuteAggregator.Aggregate("IZM", "L01", bucketStart, new MinuteInputs(previous, rows, nextTsMs), Rules);
        Assert.NotNull(bucket);
        return bucket!;
    }

    [Fact]
    public void AMinuteWithNoEvidenceIsNotInvented()
    {
        // "No data for that minute" is a true statement; a bucket of zeroes is a guess
        // presented as a measurement, and it would drag every average down.
        Assert.Null(MinuteAggregator.Aggregate(
            "IZM", "L01", 0, new MinuteInputs(null, [], null), Rules));
    }

    [Fact]
    public void TheFirstSampleOnRecordOnlySetsTheCounterBaseline()
    {
        // A PLC counter starts wherever it starts. Treating 5,000 as production would
        // credit the line with every part it made before anyone was listening.
        var bucket = Aggregate(
            previous: null,
            rows: [Sample(0, good: 5_000, scrap: 40), Sample(30_000, good: 5_010, scrap: 41)],
            nextTsMs: Minute);

        Assert.Equal(10, bucket.Good);
        Assert.Equal(1, bucket.Scrap);
    }

    [Fact]
    public void ProductionIsTheDeltaAcrossTheMinuteBoundary()
    {
        // The sample before the minute is the baseline, so a part counted at 00:59 belongs
        // to the previous minute and the first delta of this one starts from its value.
        var bucket = Aggregate(
            previous: Sample(-1_000, good: 1_000, scrap: 10),
            rows: [Sample(10_000, good: 1_008, scrap: 10), Sample(50_000, good: 1_016, scrap: 11)],
            nextTsMs: Minute + 10_000);

        Assert.Equal(16, bucket.Good);
        Assert.Equal(1, bucket.Scrap);
    }

    [Fact]
    public void ACounterGoingBackwardsIsARestartAndNeverNegativeProduction()
    {
        // The PLC was power-cycled mid-minute: 1,040 -> 12. The delta across the reset is
        // unknowable, so it is dropped and the baseline re-anchored. Showing a line that
        // un-built pumps would be wrong in a way nobody notices in an average.
        var bucket = Aggregate(
            previous: Sample(-1_000, good: 1_000, scrap: 10),
            rows:
            [
                Sample(10_000, good: 1_040, scrap: 10),
                Sample(20_000, good: 12, scrap: 0),      // reset
                Sample(40_000, good: 20, scrap: 1),
            ],
            nextTsMs: Minute);

        Assert.True(bucket.Good >= 0, "production is never negative");
        Assert.Equal(48, bucket.Good);   // 40 before the reset, 8 after it
        Assert.Equal(1, bucket.Scrap);
    }

    [Fact]
    public void StateIsAStepFunctionSoASilentMinuteInsideAStopIsStillDowntime()
    {
        // An OPC-UA subscription reports by exception: a line faulted for five minutes sends
        // one sample when it faults and one when it recovers. The minutes in between contain
        // no rows at all, and they are entirely downtime.
        var bucket = Aggregate(
            previous: Sample(-30_000, LineState.Faulted, alarm: 1001),
            rows: [],
            nextTsMs: 5 * Minute);

        Assert.Equal(60.0, bucket.DownUnplannedSeconds, 3);
        Assert.Equal(0.0, bucket.RunSeconds, 3);
        Assert.Equal(0, bucket.Samples);
    }

    [Fact]
    public void PlannedAndUnplannedDowntimeAreSeparated()
    {
        // Changeover (2001) is planned, a seal fault (1001) is not. The split is what keeps
        // availability honest: a scheduled changeover is not an availability loss.
        var bucket = Aggregate(
            previous: Sample(0, LineState.Running),
            rows:
            [
                Sample(15_000, LineState.Changeover, alarm: 2001),
                Sample(35_000, LineState.Faulted, alarm: 1001),
                Sample(50_000, LineState.Running),
            ],
            nextTsMs: Minute);

        Assert.Equal(15.0 + 10.0, bucket.RunSeconds, 3);
        Assert.Equal(20.0, bucket.DownPlannedSeconds, 3);
        Assert.Equal(15.0, bucket.DownUnplannedSeconds, 3);
        Assert.Equal(60.0, bucket.RunSeconds + bucket.DownPlannedSeconds + bucket.DownUnplannedSeconds, 3);
    }

    [Fact]
    public void IdleWithNoAlarmIsCountedAsUnplannedRatherThanExcused()
    {
        var bucket = Aggregate(
            previous: Sample(0, LineState.Idle),
            rows: [Sample(30_000, LineState.Running)],
            nextTsMs: Minute);

        Assert.Equal(30.0, bucket.DownUnplannedSeconds, 3);
        Assert.Equal(0.0, bucket.DownPlannedSeconds, 3);
    }

    [Fact]
    public void TimeAfterTheNewestSampleIsNotBilled()
    {
        // The minute is still in progress. Billing the remainder would assume a state nothing
        // has confirmed, and the bucket is rebuilt anyway when the next sample arrives.
        var bucket = Aggregate(
            previous: null,
            rows: [Sample(0, LineState.Running, good: 1_000), Sample(20_000, LineState.Running, good: 1_003)],
            nextTsMs: null);

        Assert.Equal(20.0, bucket.RunSeconds, 3);
        Assert.Equal(0.0, bucket.DownUnplannedSeconds, 3);
    }

    [Fact]
    public void AStateIsNotTrustedBeyondTheCarryLimit()
    {
        // Past the limit the silence is more likely a collector outage than an unchanged
        // line, so the time is left as "no data" instead of being billed either way.
        var rules = AggregationRules.From(new IngestOptions { MaxStateCarrySeconds = 10, TimeScale = 1.0 });
        var bucket = MinuteAggregator.Aggregate(
            "IZM", "L01", 0,
            new MinuteInputs(Sample(-5_000, LineState.Running), [], 10 * Minute),
            rules);

        Assert.NotNull(bucket);
        Assert.Equal(5.0, bucket!.RunSeconds, 3);   // 10 s of carry, 5 of which fall before the minute
        Assert.Equal(0.0, bucket.DownUnplannedSeconds, 3);
    }

    [Fact]
    public void TimeScaleConvertsCompressedDurationsBackToProductionSeconds()
    {
        // The simulator runs machines 20x faster than the wall clock while stamping ordinary
        // timestamps, so a 3-second measured stop is a 60-second production stop. Without
        // this the micro-stop rule and the performance factor lose their meaning.
        var rules = AggregationRules.From(new IngestOptions { MaxStateCarrySeconds = 3600, TimeScale = 20.0 });
        var bucket = MinuteAggregator.Aggregate(
            "IZM", "L01", 0,
            new MinuteInputs(Sample(0, LineState.Faulted, alarm: 1001), [Sample(3_000, LineState.Running)], 6_000),
            rules);

        Assert.NotNull(bucket);
        Assert.Equal(60.0, bucket!.DownUnplannedSeconds, 3);
        Assert.Equal(60.0, bucket.RunSeconds, 3);
    }

    [Fact]
    public void IdealRunTimeFollowsTheProgramThatWasActuallyRunning()
    {
        // A changeover inside the minute changes the ideal cycle. Using one figure for the
        // whole minute would mis-state performance for whichever product ran less.
        var fast = Sample(0, good: 1_000, program: "OP-4412-A");
        var slow = new BucketSample
        {
            TsMs = 30_000, State = (int)LineState.Running, GoodCount = 1_005, ScrapCount = 10,
            CycleMs = 9_400, IdealCycleMs = 9_400, ProgramId = "VP-2207-A",
        };

        var bucket = Aggregate(previous: fast, rows: [slow], nextTsMs: Minute);

        // Five parts made under the 9.4 s program.
        Assert.Equal(5 * 9.4, bucket.IdealRunSeconds, 3);
        Assert.Equal("VP-2207-A", bucket.ProgramId);
    }

    [Theory]
    // run, unplanned, planned, idealRun, good, scrap -> availability, performance, quality
    [InlineData(60, 0, 0, 60, 10, 0, 1.0, 1.0, 1.0)]
    [InlineData(30, 30, 0, 30, 10, 0, 0.5, 1.0, 1.0)]
    // Planned downtime is excluded from the denominator: a changeover is not an availability loss.
    [InlineData(30, 0, 30, 30, 10, 0, 1.0, 1.0, 1.0)]
    [InlineData(60, 0, 0, 30, 10, 0, 1.0, 0.5, 1.0)]
    [InlineData(60, 0, 0, 60, 9, 1, 1.0, 1.0, 0.9)]
    // An ideal cycle longer than reality is a data-quality problem, not performance above 100%.
    [InlineData(60, 0, 0, 120, 10, 0, 1.0, 1.0, 1.0)]
    // Nothing observed: zeroes, not a division by zero.
    [InlineData(0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0)]
    public void OeeFactorsFollowTheStatedConventions(
        double run, double unplanned, double planned, double idealRun, long good, long scrap,
        double availability, double performance, double quality)
    {
        var factors = OeeMath.Compute(run, unplanned, planned, idealRun, good, scrap);

        Assert.Equal(availability, factors.Availability, 4);
        Assert.Equal(performance, factors.Performance, 4);
        Assert.Equal(quality, factors.Quality, 4);
        Assert.Equal(Math.Round(availability * performance * quality, 4), factors.Oee, 4);
    }

    [Theory]
    [InlineData(-10.0, 10.0, 0.0, 10.0, -5, 5)]
    [InlineData(double.NaN, 0.0, 0.0, 0.0, 0, 0)]
    public void OeeFactorsStayInsideZeroToOneEvenOnNonsenseInput(
        double run, double unplanned, double planned, double idealRun, long good, long scrap)
    {
        var factors = OeeMath.Compute(run, unplanned, planned, idealRun, good, scrap);

        foreach (var value in new[] { factors.Availability, factors.Performance, factors.Quality, factors.Oee })
        {
            Assert.False(double.IsNaN(value), "a factor is NaN");
            Assert.InRange(value, 0.0, 1.0);
        }
    }
}
