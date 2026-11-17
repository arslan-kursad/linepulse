namespace LinePulse.Core.Kpi;

/// <summary>The three OEE factors and their product.</summary>
public sealed record OeeFactors(double Availability, double Performance, double Quality, double Oee);

/// <summary>
/// OEE arithmetic, kept in one place and free of any data access.
/// </summary>
/// <remarks>
/// OEE is a number people argue about, so the conventions used here are written
/// down rather than implied:
/// <list type="bullet">
/// <item>Availability uses loading time, not calendar time: planned downtime
/// (changeover, planned maintenance) is excluded from the denominator. Counting a
/// scheduled changeover as an availability loss makes the number look terrible and
/// tells the line nothing it can act on.</item>
/// <item>Performance is theoretical time for the parts produced divided by the time
/// the line actually ran. It is capped at 1.0 because a factor above 100% does not
/// mean the line beat physics, it means the ideal cycle time in the PLC is wrong --
/// which is a data-quality finding, not extra OEE.</item>
/// <item>Quality counts scrap against the line that made it, including parts
/// rejected at the end-of-line bench.</item>
/// </list>
/// </remarks>
public static class OeeMath
{
    public static OeeFactors Compute(
        double runSeconds,
        double unplannedDownSeconds,
        double plannedDownSeconds,
        double idealRunSeconds,
        long good,
        long scrap)
    {
        _ = plannedDownSeconds; // excluded by design; named so the omission is visible.

        var loadingTime = runSeconds + unplannedDownSeconds;
        var availability = loadingTime > 0 ? runSeconds / loadingTime : 0;

        var performance = runSeconds > 0 ? Math.Min(idealRunSeconds / runSeconds, 1.0) : 0;

        var total = good + scrap;
        var quality = total > 0 ? (double)good / total : 0;

        return new OeeFactors(
            Round(availability),
            Round(performance),
            Round(quality),
            Round(availability * performance * quality));
    }

    public static OeeFactors Empty { get; } = new(0, 0, 0, 0);

    private static double Round(double value) => Math.Round(Math.Clamp(value, 0, 1), 4);
}
