using LinePulse.Contracts;

namespace LinePulse.Core.Kpi;

/// <summary>A downtime reason as maintenance codes it.</summary>
public sealed record ReasonInfo(int Code, string Text, string Station, bool Planned);

/// <summary>
/// Downtime reason codes and whether they are planned losses.
/// </summary>
/// <remarks>
/// <para>
/// In a plant this table is master data: it lives in the MES or the maintenance
/// system and maintenance owns it, because the people who argue about OEE are the
/// people who decide what counts as planned. Hard-coding it in the KPI service
/// would mean a deployment every time a new reason is added, so this class is
/// deliberately a cache with a fallback, not the source of truth.
/// </para>
/// <para>
/// The codes below mirror sim/plant.py, which is the only producer this lab has,
/// and a test fails if the two drift apart: the planned flag decides what
/// availability excuses, so a mismatch here is a wrong OEE, not a typo. An unknown
/// code is treated as unplanned and keeps its number in the Pareto rather than
/// being lumped into "other" -- an unexplained stop you can still see is useful;
/// one that has been averaged away is not.
/// </para>
/// </remarks>
public static class ReasonCatalog
{
    private static readonly Dictionary<int, ReasonInfo> Known = new ReasonInfo[]
    {
        // Assembly lines.
        new(1001, "Leak test fixture seal fault", "ST30", Planned: false),
        new(1002, "Screwdriver torque NOK / spindle fault", "ST20", Planned: false),
        new(1003, "Rotor feeder jam", "ST20", Planned: false),
        new(1004, "DMC reader no-read", "ST50", Planned: false),
        new(1005, "Housing infeed starved", "ST10", Planned: false),
        new(1006, "Flow bench calibration due", "ST40", Planned: false),
        new(3001, "Operator stop", "ST20", Planned: false),

        // Scheduled losses: excluded from availability's loading time.
        new(2001, "Product changeover", "LINE", Planned: true),
        new(2002, "Planned maintenance", "LINE", Planned: true),

        // Machining centre. It fails in its own ways; filing its stops under
        // assembly reasons is the kind of detail a production engineer spots and
        // stops trusting the data over.
        new(4001, "Tool breakage detected", "OP10", Planned: false),
        new(4002, "Tool life expired, change required", "OP10", Planned: false),
        new(4003, "Chip conveyor jam", "OP20", Planned: false),
        new(4004, "Coolant level low", "OP20", Planned: false),
        new(4005, "Spindle overload trip", "OP10", Planned: false),
        new(4006, "Pallet changer fault", "OP20", Planned: false),
        new(4007, "Probe measurement out of tolerance", "OP20", Planned: false),
    }.ToDictionary(r => r.Code);

    /// <summary>Synthetic codes for stops the PLC reported without an alarm. A stop
    /// with no reason is still a stop, and giving it a stable code is what lets it
    /// appear in the Pareto instead of vanishing.</summary>
    public const int NoReasonIdle = 9000;

    public const int NoReasonBlocked = 9002;

    public const int NoReasonFaulted = 9003;

    public const int NoReasonChangeover = 9004;

    public static IReadOnlyCollection<ReasonInfo> All => Known.Values;

    public static bool IsPlanned(int alarmCode, int state)
    {
        if (Known.TryGetValue(alarmCode, out var reason))
        {
            return reason.Planned;
        }

        // Changeover is planned even when the PLC forgot to raise a code for it:
        // the line is down because somebody decided to change the product.
        return state == (int)LineState.Changeover || alarmCode == NoReasonChangeover;
    }

    public static string Text(int alarmCode, int state, string alarmText)
    {
        if (!string.IsNullOrWhiteSpace(alarmText))
        {
            return alarmText;
        }

        if (Known.TryGetValue(alarmCode, out var reason))
        {
            return $"{reason.Station}: {reason.Text}";
        }

        return alarmCode switch
        {
            NoReasonIdle => "Idle, no reason code",
            NoReasonBlocked => "Blocked, downstream full",
            NoReasonFaulted => "Faulted, no alarm code",
            NoReasonChangeover => "Changeover, no reason code",
            _ => $"Unmapped reason code {alarmCode}",
        };
    }

    /// <summary>Picks the code to file a stop under when the PLC raised no alarm.</summary>
    public static int CodeForState(int alarmCode, int state) =>
        alarmCode != 0
            ? alarmCode
            : (LineState)state switch
            {
                LineState.Blocked => NoReasonBlocked,
                LineState.Faulted => NoReasonFaulted,
                LineState.Changeover => NoReasonChangeover,
                _ => NoReasonIdle,
            };
}
