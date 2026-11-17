using LinePulse.Contracts;

namespace LinePulse.Collector;

/// <summary>Browse names of the tags one line publishes. Resolved by name, never by node id.</summary>
internal static class MachineTags
{
    public const string State = "State";
    public const string StateText = "StateText";
    public const string GoodCount = "GoodCount";
    public const string ScrapCount = "ScrapCount";
    public const string CycleTimeMs = "CycleTimeMs";
    public const string IdealCycleMs = "IdealCycleMs";
    public const string AlarmCode = "AlarmCode";
    public const string AlarmText = "AlarmText";
    public const string ProgramId = "ProgramId";
    public const string SpindleLoadPct = "SpindleLoadPct";
    public const string ToolId = "ToolId";
    public const string ToolLifeRemainingPct = "ToolLifeRemainingPct";
    public const string CurrentLotId = "CurrentLotId";

    /// <summary>
    /// The tags a <see cref="MachineSample"/> is built from. A line that does
    /// not expose one of them (an assembly line has no spindle) simply has no
    /// node to monitor, which is why the set is intersected with what the
    /// browse actually found instead of being demanded up front.
    /// </summary>
    public static readonly string[] Subscribed =
    [
        State, StateText, GoodCount, ScrapCount, CycleTimeMs, IdealCycleMs,
        AlarmCode, AlarmText, ProgramId,
        SpindleLoadPct, ToolId, ToolLifeRemainingPct, CurrentLotId,
    ];
}
