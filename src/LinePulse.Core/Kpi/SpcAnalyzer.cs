namespace LinePulse.Core.Kpi;

/// <summary>One leak-bench fixture's statistical state.</summary>
public sealed record FixtureSpc(
    string Site,
    string Line,
    string FixtureId,
    int N,
    double Mean,
    double Sigma,
    double Slope,
    double Limit,
    double CpkUpper,
    double? PartsToLimit,
    string Status,
    string Note);

/// <summary>
/// Statistical process control for the leak bench.
/// </summary>
/// <remarks>
/// <para>
/// The business case for this is narrow and real: a leak-test fixture's seals age,
/// its measured leak rate creeps upward, and for a long time every part still
/// passes. Then the drift crosses the limit and the line starts scrapping good
/// pumps while everyone looks for a problem in assembly. Catching the drift while
/// parts still pass is the whole point -- an alarm that fires after the scrap has
/// started is a report, not a warning.
/// </para>
/// <para>
/// Two independent triggers, because drift and noise fail differently:
/// a one-sided capability index (how many sigmas of headroom are left to the
/// limit), and a fitted trend projected forward (how many more parts until the
/// 3-sigma band reaches the limit). The trend fires first on a slow, clean drift;
/// the capability index fires first when the fixture simply gets noisy.
/// </para>
/// </remarks>
public sealed class SpcAnalyzer(SpcOptions options)
{
    public const string StatusOk = "ok";
    public const string StatusWarning = "warning";
    public const string StatusAlarm = "alarm";
    public const string StatusInsufficient = "insufficient-data";

    /// <summary>
    /// <paramref name="values"/> must be in event-time order, oldest first: the
    /// trend is a regression against part order, so reversing it reverses the sign.
    /// </summary>
    public FixtureSpc Analyse(string site, string line, string fixtureId, IReadOnlyList<double> values, double limit)
    {
        var n = values.Count;
        if (n < options.MinSamples)
        {
            return new FixtureSpc(
                site, line, fixtureId, n, 0, 0, 0, limit, 0, null,
                StatusInsufficient,
                $"{n} of {options.MinSamples} parts needed before sigma means anything");
        }

        var mean = values.Average();
        // Sample standard deviation (n-1): with a rolling window this is a sample
        // of the process, not the whole of it.
        var variance = values.Sum(v => (v - mean) * (v - mean)) / (n - 1);
        var sigma = Math.Sqrt(variance);

        // A bench that reports the same number for every part -- a stuck sensor, or a reading
        // rounded to its display precision -- has no spread at all, but the arithmetic above
        // leaves floating-point residue of around 1e-19 instead of a clean zero. Treated as
        // real variation that divides into a capability index, it produced a reported Cpk of
        // -1e15 on the dashboard. Anything this far below the mean is noise in the sum, not
        // spread in the process.
        if (sigma <= Math.Abs(mean) * 1e-9)
        {
            sigma = 0;
        }
        var slope = Slope(values);

        // One-sided upper capability: how much headroom is left, in units of 3 sigma.
        var headroom = limit - mean;
        var cpkUpper = sigma > 0 ? headroom / (3 * sigma) : (headroom > 0 ? double.PositiveInfinity : 0);

        // Parts until the 3-sigma band reaches the limit at the current trend.
        double? partsToLimit = null;
        if (slope > 0)
        {
            var distance = limit - (mean + 3 * sigma);
            partsToLimit = distance > 0 ? distance / slope : 0;
        }

        var (status, note) = Classify(cpkUpper, partsToLimit);

        return new FixtureSpc(
            site, line, fixtureId, n,
            Signif(mean), Signif(sigma), Signif(slope), limit,
            double.IsInfinity(cpkUpper) ? 99 : Math.Round(cpkUpper, 3),
            partsToLimit.HasValue ? Math.Round(partsToLimit.Value, 0) : null,
            status, note);
    }

    private (string Status, string Note) Classify(double cpkUpper, double? partsToLimit)
    {
        if (cpkUpper < options.AlarmCpk)
        {
            return (StatusAlarm, "3-sigma band has reached the leak limit; parts will start failing");
        }

        if (partsToLimit is { } parts && parts <= options.ProjectionHorizonParts / 2.0)
        {
            return (StatusAlarm, $"trending to the limit in about {parts:F0} parts; schedule the fixture now");
        }

        if (cpkUpper < options.WarnCpk)
        {
            return (StatusWarning, $"capability below {options.WarnCpk:F2}; headroom to the limit is shrinking");
        }

        if (partsToLimit is { } soon && soon <= options.ProjectionHorizonParts)
        {
            return (StatusWarning, $"upward trend reaches the limit in about {soon:F0} parts");
        }

        return (StatusOk, "stable within limit");
    }

    /// <summary>Least-squares slope against part index, in measurement units per part.</summary>
    private static double Slope(IReadOnlyList<double> values)
    {
        var n = values.Count;
        double meanX = (n - 1) / 2.0;
        var meanY = values.Average();
        double sxy = 0, sxx = 0;
        for (var i = 0; i < n; i++)
        {
            var dx = i - meanX;
            sxy += dx * (values[i] - meanY);
            sxx += dx * dx;
        }

        return sxx > 0 ? sxy / sxx : 0;
    }

    /// <summary>Leak rates live around 1e-4, so fixed-decimal rounding would erase
    /// the signal. Keep four significant figures instead.</summary>
    private static double Signif(double value)
    {
        if (value == 0 || double.IsNaN(value) || double.IsInfinity(value))
        {
            return 0;
        }

        var magnitude = (int)Math.Floor(Math.Log10(Math.Abs(value)));
        var factor = Math.Pow(10, 3 - magnitude);
        return Math.Round(value * factor) / factor;
    }
}
