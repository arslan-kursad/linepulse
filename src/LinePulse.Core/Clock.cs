using System.Globalization;

namespace LinePulse.Core;

/// <summary>
/// Time conversions shared by every layer: milliseconds since epoch for machines,
/// ISO-8601 text for people.
/// </summary>
/// <remarks>
/// It lives outside the storage folder on purpose. The minute bucket is a business
/// unit -- the grain every OEE number is built from -- and the ingest path must be
/// able to name it without depending on whichever database happens to be configured.
/// </remarks>
public static class Clock
{
    /// <summary>Width of one OEE bucket. Every KPI is a sum of these.</summary>
    public const long MinuteMs = 60_000;

    public static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    public static long ToMs(DateTimeOffset value) => value.ToUnixTimeMilliseconds();

    public static DateTimeOffset FromMs(long ms) => DateTimeOffset.FromUnixTimeMilliseconds(ms);

    public static string ToIso(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture);

    public static string IsoFromMs(long ms) => ToIso(FromMs(ms));

    /// <summary>Floors an instant to the start of its one-minute bucket, correctly for
    /// instants before 1970 as well, so a bad producer clock cannot crash the flush.</summary>
    public static long BucketStartMs(long tsMs) =>
        tsMs - (((tsMs % MinuteMs) + MinuteMs) % MinuteMs);
}
