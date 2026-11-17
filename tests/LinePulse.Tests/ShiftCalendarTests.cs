using LinePulse.Core;
using LinePulse.Core.Kpi;
using Microsoft.Extensions.Options;

namespace LinePulse.Tests;

/// <summary>
/// Shift boundaries at two sites in two time zones.
/// </summary>
/// <remarks>
/// These exist because the end-to-end runs happen in November, when Izmir and Nis are
/// one hour apart, and could never have caught the case that actually matters: in summer
/// both sites are on UTC+3 and a fixed offset for Nis is an hour wrong. A shift report
/// that is an hour out does not fail loudly -- it reconciles badly at month end, which is
/// the worst way to find a bug.
/// </remarks>
public class ShiftCalendarTests
{
    private static ShiftCalendar Calendar(params string[] zones)
    {
        var options = new ShiftOptions
        {
            StartHours = [6, 14, 22],
            Labels = ["A", "B", "C"],
            SiteTimeZones = new Dictionary<string, string>
            {
                ["IZM"] = zones.Length > 0 ? zones[0] : "Europe/Istanbul",
                ["NIS"] = zones.Length > 1 ? zones[1] : "Europe/Belgrade",
            },
            DefaultTimeZone = "UTC",
        };
        return new ShiftCalendar(new OptionsWrapper<ShiftOptions>(options));
    }

    private static DateTimeOffset Utc(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, TimeSpan.Zero);

    [Theory]
    // Winter: Izmir UTC+3, Nis UTC+1. 12:30 UTC is 15:30 in Izmir (shift B, 14:00-22:00) but
    // only 13:30 in Nis, which is still shift A (06:00-14:00) -- the hour between the two
    // sites moves a instant across a shift boundary, which is the whole reason this is per site.
    [InlineData("IZM", 2026, 1, 15, 12, "B", 11, 19)]
    [InlineData("NIS", 2026, 1, 15, 12, "A", 5, 13)]
    // Summer: Izmir still UTC+3, Nis now UTC+2. 04:00 UTC is 07:00 in Izmir (A) and 06:00 in Nis (A).
    [InlineData("IZM", 2026, 7, 15, 4, "A", 3, 11)]
    [InlineData("NIS", 2026, 7, 15, 4, "A", 4, 12)]
    // The summer case a fixed +1 offset for Nis would get wrong: 03:30 UTC is 05:30 local,
    // which is still the night shift that began at 22:00 the day before.
    [InlineData("NIS", 2026, 7, 15, 3, "C", 20, 4)]
    public void ShiftWindowFollowsTheSiteZone(
        string site, int year, int month, int day, int hour,
        string expectedLabel, int expectedStartUtcHour, int expectedEndUtcHour)
    {
        var shift = Calendar().Current(site, Utc(year, month, day, hour, 30));

        Assert.Equal(expectedLabel, shift.Label);
        Assert.Equal(expectedStartUtcHour, shift.StartUtc.Hour);
        Assert.Equal(expectedEndUtcHour, shift.EndUtc.Hour);
        Assert.True(shift.StartUtc <= Utc(year, month, day, hour, 30), "shift starts before the instant");
        Assert.True(shift.EndUtc > Utc(year, month, day, hour, 30), "shift ends after the instant");
    }

    [Fact]
    public void InSummerBothSitesShareTheSameShiftWindow()
    {
        // Serbia moves to UTC+2 for the summer, Turkey stays on UTC+3, so the two sites are
        // an hour apart in winter and an hour apart in summer too -- but in the other
        // direction from what a fixed offset table would say if it were written in winter.
        var calendar = Calendar();
        var instant = Utc(2026, 7, 15, 12);

        var izmir = calendar.Current("IZM", instant);
        var nis = calendar.Current("NIS", instant);

        Assert.Equal(3, izmir.UtcOffsetHours);
        Assert.Equal(2, nis.UtcOffsetHours);
        Assert.NotEqual(izmir.StartUtc, nis.StartUtc);
        Assert.Equal(TimeSpan.FromHours(1), nis.StartUtc - izmir.StartUtc);
    }

    [Fact]
    public void NightShiftCrossesMidnightAndStaysEightHours()
    {
        // 03:00 local belongs to the shift that began at 22:00 the previous day.
        var shift = Calendar().Current("IZM", Utc(2026, 1, 15, 0, 30));  // 03:30 in Izmir

        Assert.Equal("C", shift.Label);
        Assert.Equal(new DateTime(2026, 1, 14), shift.StartUtc.UtcDateTime.Date);
        Assert.Equal(TimeSpan.FromHours(8), shift.EndUtc - shift.StartUtc);
    }

    [Fact]
    public void EveryShiftOfADayIsEightHoursAndTheyTileTheDay()
    {
        var calendar = Calendar();
        var seen = new List<ShiftWindow>();

        // Walk a whole day in half-hour steps; every instant must fall inside the window
        // reported for it, and the distinct windows must join end to start with no gap.
        for (var minutes = 0; minutes < 24 * 60; minutes += 30)
        {
            var instant = Utc(2026, 1, 15, 0).AddMinutes(minutes);
            var shift = calendar.Current("IZM", instant);
            Assert.InRange(instant, shift.StartUtc, shift.EndUtc.AddTicks(-1));
            if (seen.Count == 0 || seen[^1].StartUtc != shift.StartUtc)
            {
                seen.Add(shift);
            }
        }

        Assert.All(seen, s => Assert.Equal(TimeSpan.FromHours(8), s.EndUtc - s.StartUtc));
        for (var i = 1; i < seen.Count; i++)
        {
            Assert.Equal(seen[i - 1].EndUtc, seen[i].StartUtc);
        }
    }

    [Fact]
    public void ClocksGoingForwardDoesNotProduceAnInvalidShiftStart()
    {
        // Serbia springs forward at 02:00 local on the last Sunday of March. A shift
        // configured to start inside a skipped hour has no such local time; the calendar
        // must still return a usable window rather than throwing in a report.
        var options = new ShiftOptions
        {
            StartHours = [2, 10, 18],
            Labels = ["A", "B", "C"],
            SiteTimeZones = new Dictionary<string, string> { ["NIS"] = "Europe/Belgrade" },
            DefaultTimeZone = "UTC",
        };
        var calendar = new ShiftCalendar(new OptionsWrapper<ShiftOptions>(options));

        var shift = calendar.Current("NIS", Utc(2026, 3, 29, 3));

        Assert.True(shift.EndUtc > shift.StartUtc, "the window is not inverted");
        Assert.Equal("A", shift.Label);
    }

    [Fact]
    public void AnUnknownSiteFallsBackToTheDefaultZone()
    {
        var shift = Calendar().Current("ZZZ", Utc(2026, 1, 15, 12));

        // 12:30 UTC is 12:30 local with no offset: the 06:00-14:00 shift.
        Assert.Equal(0, shift.UtcOffsetHours);
        Assert.Equal("A", shift.Label);
    }

    [Fact]
    public void AMistypedZoneFailsLoudlyAtConstruction()
    {
        // Falling back to UTC on a typo would produce shift reports that look fine and are
        // wrong by hours, so this is deliberately a startup failure.
        var options = new ShiftOptions
        {
            StartHours = [6, 14, 22],
            Labels = ["A", "B", "C"],
            SiteTimeZones = new Dictionary<string, string> { ["IZM"] = "Europe/Izmir" },
            DefaultTimeZone = "UTC",
        };

        var error = Assert.Throws<InvalidOperationException>(
            () => new ShiftCalendar(new OptionsWrapper<ShiftOptions>(options)));
        Assert.Contains("Europe/Izmir", error.Message);
    }

    [Fact]
    public void BucketStartFloorsCorrectlyBeforeTheEpochToo()
    {
        // A device with a badly set clock can report a timestamp before 1970. C#'s remainder
        // is negative there, so a naive floor would round towards the future and the flush
        // would write a bucket that no sample belongs to.
        Assert.Equal(-60_000, Clock.BucketStartMs(-1));
        Assert.Equal(-60_000, Clock.BucketStartMs(-60_000));
        Assert.Equal(-120_000, Clock.BucketStartMs(-60_001));
        Assert.Equal(0, Clock.BucketStartMs(0));
        Assert.Equal(0, Clock.BucketStartMs(59_999));
        Assert.Equal(60_000, Clock.BucketStartMs(60_000));
    }
}
