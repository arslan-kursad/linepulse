using LinePulse.Core;
using LinePulse.Core.Kpi;

namespace LinePulse.Tests;

/// <summary>
/// The leak-fixture early warning.
/// </summary>
/// <remarks>
/// The live run shows the drifting fixture turning amber, which proves the path works on one
/// data set. What it cannot show is the property the feature is sold on: that the warning
/// arrives <em>while every part still passes</em>, and that a fixture which is merely noisy or
/// merely new does not raise one. Those are the two ways this feature loses the plant's
/// trust -- a late alarm is useless, a false alarm stops a line for nothing -- so they are
/// checked here against constructed data rather than hoped for.
/// </remarks>
public class SpcAnalyzerTests
{
    private const double Limit = 1.0e-3;
    private const double Healthy = 2.5e-4;

    private static SpcAnalyzer Analyzer() => new(new SpcOptions
    {
        WindowSize = 200,
        MinSamples = 25,
        WarnCpk = 1.33,
        AlarmCpk = 1.00,
        ProjectionHorizonParts = 1000,
    });

    /// <summary>A deterministic, repeatable wobble: no clock, no random seed to explain.</summary>
    private static List<double> Series(int count, double start, double perPart, double noise = 0)
    {
        var values = new List<double>(count);
        for (var i = 0; i < count; i++)
        {
            var wobble = noise == 0 ? 0 : noise * Math.Sin(i * 1.7);
            values.Add(start + perPart * i + wobble);
        }

        return values;
    }

    [Fact]
    public void BelowTheMinimumSampleCountItSaysSoInsteadOfGuessing()
    {
        // A sigma from eight parts is a guess, and a guess that stops a line is worse than no
        // alarm at all.
        var spc = Analyzer().Analyse("IZM", "L01", "FX-01", Series(8, Healthy, 0, 2e-5), Limit);

        Assert.Equal(SpcAnalyzer.StatusInsufficient, spc.Status);
        Assert.Equal(8, spc.N);
        Assert.Contains("25", spc.Note);
    }

    [Fact]
    public void AStableFixtureIsLeftAlone()
    {
        var spc = Analyzer().Analyse("IZM", "L01", "FX-01", Series(150, Healthy, 0, 3e-5), Limit);

        Assert.Equal(SpcAnalyzer.StatusOk, spc.Status);
        Assert.InRange(spc.Mean, Healthy * 0.9, Healthy * 1.1);
    }

    [Fact]
    public void DriftIsFlaggedWhileEveryPartStillPasses()
    {
        // 150 parts drifting at 2e-6 per part: the last reading is 5.5e-4, well inside a
        // limit of 1.0e-3. Nothing has failed, and that is exactly when the seal should be
        // scheduled -- the claim this whole feature rests on.
        var values = Series(150, Healthy, 2e-6, 2e-5);

        var spc = Analyzer().Analyse("IZM", "L01", "FX-03", values, Limit);

        Assert.All(values, v => Assert.True(v < Limit, "no part exceeded the limit"));
        Assert.NotEqual(SpcAnalyzer.StatusOk, spc.Status);
        Assert.True(spc.Slope > 0, "the upward trend was detected");
        Assert.NotNull(spc.PartsToLimit);
        Assert.True(spc.PartsToLimit > 0, "it says how long there is left");
    }

    [Fact]
    public void AFixtureAtTheLimitIsAnAlarmNotAWarning()
    {
        // The 3-sigma band has reached the limit: parts are about to start failing, which is
        // a different message from "schedule this fixture".
        var spc = Analyzer().Analyse("IZM", "L01", "FX-03", Series(150, Limit * 0.95, 0, 3e-5), Limit);

        Assert.Equal(SpcAnalyzer.StatusAlarm, spc.Status);
        Assert.True(spc.CpkUpper < 1.0, "capability is below 1.0");
    }

    [Fact]
    public void NoiseAloneRaisesTheAlarmEvenWithoutATrend()
    {
        // A fixture that is simply erratic has no slope to project, so the trend test can
        // never fire. Capability is the other trigger, and it has to carry this case.
        var spc = Analyzer().Analyse("IZM", "L01", "FX-02", Series(150, Healthy * 1.2, 0, 3.0e-4), Limit);

        Assert.NotEqual(SpcAnalyzer.StatusOk, spc.Status);
        Assert.True(spc.Sigma > 1e-4, "the noise was measured");
    }

    [Fact]
    public void ADownwardTrendIsNotAWarning()
    {
        // A fixture that was serviced reads lower every part. Warning about an improving
        // fixture is the false alarm that teaches a plant to ignore the column.
        var spc = Analyzer().Analyse("IZM", "L01", "FX-01", Series(150, Healthy * 1.5, -1e-6, 2e-5), Limit);

        Assert.Equal(SpcAnalyzer.StatusOk, spc.Status);
        Assert.True(spc.Slope < 0, "the downward trend was measured");
        Assert.Null(spc.PartsToLimit);
    }

    [Fact]
    public void IdenticalReadingsDoNotProduceInfinityOrNaN()
    {
        // A bench reporting a constant (a stuck sensor, or a rounded zero) gives sigma = 0,
        // which is a division by zero in every capability formula.
        var spc = Analyzer().Analyse("IZM", "L01", "FX-01", Enumerable.Repeat(Healthy, 60).ToList(), Limit);

        Assert.Equal(0.0, spc.Sigma);
        Assert.False(double.IsNaN(spc.CpkUpper) || double.IsInfinity(spc.CpkUpper), "capability is a usable number");
        Assert.Equal(SpcAnalyzer.StatusOk, spc.Status);
    }

    [Fact]
    public void AReadingAlreadyOverTheLimitIsAnAlarmWithNoHeadroomLeft()
    {
        var spc = Analyzer().Analyse("IZM", "L01", "FX-03", Enumerable.Repeat(Limit * 1.2, 60).ToList(), Limit);

        Assert.Equal(SpcAnalyzer.StatusAlarm, spc.Status);
        Assert.Equal(0, spc.CpkUpper);
    }

    [Fact]
    public void TheStatusFollowsTheDocumentedPrecedenceAtEveryDriftSpeed()
    {
        // Rather than pick one slope and assert one status -- which only tests whichever
        // series the author happened to choose -- sweep the drift speed and check the full
        // documented rule against what the analyser itself reports. Both triggers are in
        // play: a fast drift widens sigma enough that capability fires before the projection
        // does, which is the behaviour the two-trigger design exists for, and which a
        // single-slope test would have hidden.
        var analyzer = Analyzer();
        const double warnCpk = 1.33, alarmCpk = 1.00;
        const int horizon = 1000;
        var seen = new List<string>();

        foreach (var perPart in new[] { 2e-7, 4e-7, 6e-7, 8e-7, 1.2e-6, 2e-6, 4e-6 })
        {
            var spc = analyzer.Analyse("IZM", "L01", "FX-03", Series(150, Healthy, perPart, 2e-5), Limit);
            seen.Add(spc.Status);

            Assert.True(spc.Slope > 0, $"upward drift of {perPart} was detected");
            Assert.NotNull(spc.PartsToLimit);

            var expected =
                spc.CpkUpper < alarmCpk ? SpcAnalyzer.StatusAlarm
                : spc.PartsToLimit <= horizon / 2.0 ? SpcAnalyzer.StatusAlarm
                : spc.CpkUpper < warnCpk ? SpcAnalyzer.StatusWarning
                : spc.PartsToLimit <= horizon ? SpcAnalyzer.StatusWarning
                : SpcAnalyzer.StatusOk;
            Assert.Equal(expected, spc.Status);
        }

        // The sweep has to actually cross both thresholds, or it proves nothing.
        Assert.Contains(SpcAnalyzer.StatusOk, seen);
        Assert.Contains(SpcAnalyzer.StatusWarning, seen);
        Assert.Contains(SpcAnalyzer.StatusAlarm, seen);
    }

    [Fact]
    public void AFasterDriftReachesTheLimitSooner()
    {
        var analyzer = Analyzer();

        var slow = analyzer.Analyse("IZM", "L01", "FX-03", Series(150, Healthy, 6e-7, 2e-5), Limit);
        var fast = analyzer.Analyse("IZM", "L01", "FX-03", Series(150, Healthy, 5e-6, 2e-5), Limit);

        Assert.True(fast.PartsToLimit < slow.PartsToLimit, "a faster drift reaches the limit sooner");
    }
}
