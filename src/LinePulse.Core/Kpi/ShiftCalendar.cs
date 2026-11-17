using System.Collections.Concurrent;
using Microsoft.Extensions.Options;

namespace LinePulse.Core.Kpi;

/// <summary>A production shift as a half-open UTC window.</summary>
/// <param name="UtcOffsetHours">The site's UTC offset when the shift starts, rounded to whole hours.</param>
public sealed record ShiftWindow(string Label, DateTimeOffset StartUtc, DateTimeOffset EndUtc, int UtcOffsetHours)
{
    public long StartMs => StartUtc.ToUnixTimeMilliseconds();

    public long EndMs => EndUtc.ToUnixTimeMilliseconds();
}

/// <summary>
/// Turns an instant into "which shift is this, at this site".
/// </summary>
/// <remarks>
/// <para>
/// The two sites are in different time zones, so "today's first shift" is two
/// different UTC windows. A KPI service that uses the server's local time gets one
/// of the two plants wrong, and gets it wrong in a way nobody notices until a
/// month-end report does not reconcile.
/// </para>
/// <para>
/// A fixed UTC offset is not enough either. Turkey has stayed on UTC+3 all year
/// since 2016, but Serbia moves between UTC+1 and UTC+2, so an offset that is right
/// for Nis in winter puts every shift boundary an hour out in summer. Boundaries are
/// therefore local wall-clock times resolved through the site's real time zone, and
/// the one night a year the clocks change, a shift is genuinely seven or nine hours
/// long rather than eight.
/// </para>
/// <para>
/// A mistyped zone id fails at startup. Quietly falling back to UTC would produce
/// shift reports that look fine and are wrong by hours.
/// </para>
/// </remarks>
public sealed class ShiftCalendar
{
    private readonly ShiftOptions _options;
    private readonly ConcurrentDictionary<string, TimeZoneInfo> _zones = new();
    private readonly int[] _starts;

    public ShiftCalendar(IOptions<ShiftOptions> options)
    {
        _options = options.Value;
        _starts = _options.StartHours.OrderBy(h => h).ToArray();

        // Resolve every configured zone now, so a typo is a startup failure.
        ResolveZone(_options.DefaultTimeZone);
        foreach (var zoneId in _options.SiteTimeZones.Values)
        {
            ResolveZone(zoneId);
        }
    }

    /// <summary>The shift containing <paramref name="instant"/> at the given site.</summary>
    public ShiftWindow Current(string site, DateTimeOffset instant)
    {
        var zone = ZoneFor(site);
        var local = TimeZoneInfo.ConvertTime(instant, zone);

        if (_starts.Length == 0)
        {
            // Degenerate configuration: treat the local day as one shift rather
            // than throwing in the middle of a shift report.
            return Window("DAY", local.Date, local.Date.AddDays(1), zone);
        }

        var index = Array.FindLastIndex(_starts, h => h <= local.Hour);
        DateTime startLocal;
        if (index < 0)
        {
            // Before the first start of the day: we are in the shift that began
            // last night, e.g. 03:00 belongs to the 22:00 shift.
            index = _starts.Length - 1;
            startLocal = local.Date.AddDays(-1).AddHours(_starts[index]);
        }
        else
        {
            startLocal = local.Date.AddHours(_starts[index]);
        }

        // The last shift of the day ends at the first start hour of the next day,
        // which is how a 22:00-06:00 night shift crosses midnight.
        var nextIndex = (index + 1) % _starts.Length;
        var endLocal = nextIndex == 0
            ? startLocal.Date.AddDays(1).AddHours(_starts[0])
            : startLocal.Date.AddHours(_starts[nextIndex]);

        var label = index < _options.Labels.Length ? _options.Labels[index] : $"S{index + 1}";
        return Window(label, startLocal, endLocal, zone);
    }

    private TimeZoneInfo ZoneFor(string site) =>
        _zones.GetOrAdd(
            site,
            key => ResolveZone(_options.SiteTimeZones.TryGetValue(key, out var id) ? id : _options.DefaultTimeZone));

    private static TimeZoneInfo ResolveZone(string id)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(id);
        }
        catch (TimeZoneNotFoundException ex)
        {
            throw new InvalidOperationException(
                $"Shifts: time zone '{id}' is not known on this machine. Use an IANA id such as Europe/Istanbul.", ex);
        }
    }

    private static ShiftWindow Window(string label, DateTime startLocal, DateTime endLocal, TimeZoneInfo zone)
    {
        var startUtc = ToUtc(startLocal, zone);
        var endUtc = ToUtc(endLocal, zone);
        var offsetHours = (int)Math.Round(zone.GetUtcOffset(startUtc).TotalHours);
        return new ShiftWindow(label, startUtc, endUtc, offsetHours);
    }

    private static DateTimeOffset ToUtc(DateTime local, TimeZoneInfo zone)
    {
        var unspecified = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);

        // A wall-clock time that the spring-forward change skips does not exist; a
        // shift configured to start inside that hour begins when the hour resumes.
        if (zone.IsInvalidTime(unspecified))
        {
            unspecified = unspecified.AddHours(1);
        }

        return new DateTimeOffset(TimeZoneInfo.ConvertTimeToUtc(unspecified, zone), TimeSpan.Zero);
    }
}
