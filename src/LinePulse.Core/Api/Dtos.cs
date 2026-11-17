namespace LinePulse.Core.Api;

/// <summary>
/// The wire shapes. Kept separate from the storage rows on purpose: the dashboard
/// should not have to change because a column was renamed, and a database column
/// should not have to keep a bad name because a dashboard depends on it.
/// </summary>
public sealed record OeeDto(double Availability, double Performance, double Quality, double Oee);

public sealed record HealthCountersDto(
    long Ingested,
    long Deduped,
    long LateApplied,
    long LateBeyondWatermark,
    double UptimeSeconds,
    long Received,
    long Malformed,
    long MissingKey,
    long OutOfOrderState,
    long BucketRecomputes,
    long MicroStops);

public sealed record HealthDetailDto(
    bool NatsConnected,
    string NatsUrl,
    string Database,
    int WatermarkSeconds,
    int MicroStopThresholdSeconds,
    int DirtyBuckets,
    int DedupCacheKeys,
    int LinesSeen,
    int StreamClients);

public sealed record HealthDto(string Status, HealthCountersDto Counters, HealthDetailDto Detail);

public sealed record LineDto(
    string Site,
    string Line,
    string State,
    string ProgramId,
    OeeDto Oee,
    long GoodShift,
    long ScrapShift,
    DateTimeOffset? LastSeenUtc,
    string Shift,
    DateTimeOffset ShiftStartUtc,
    int AlarmCode,
    string AlarmText,
    double RunSecondsShift,
    double DownUnplannedSecondsShift,
    double DownPlannedSecondsShift,
    int BucketRevisionsShift,
    int LateBeyondWatermarkShift);

public sealed record MinuteBucketDto(
    DateTimeOffset BucketStartUtc,
    string ProgramId,
    long Good,
    long Scrap,
    int Samples,
    double RunSeconds,
    double DownUnplannedSeconds,
    double DownPlannedSeconds,
    double AvgCycleMs,
    OeeDto Oee,
    int Revisions,
    int LateBeyondCount,
    bool Sealed);

public sealed record DowntimeDto(
    int ReasonCode,
    string ReasonText,
    bool Planned,
    int Stops,
    int OpenStops,
    int MicroStops,
    double TotalSeconds,
    double LongestSeconds,
    double SharePct);

public sealed record FailureDto(string Check, int Count);

public sealed record FpyDto(
    string Site,
    string Line,
    int Tested,
    int Passed,
    int Failed,
    double Fpy,
    int Retests,
    IReadOnlyList<FailureDto> Failures);

public sealed record RollupProductDto(
    string ProgramId,
    long Good,
    long Scrap,
    OeeDto Oee);

public sealed record RollupDto(
    string Site,
    int UtcOffsetHours,
    string Shift,
    DateTimeOffset FromUtc,
    DateTimeOffset ToUtc,
    int Lines,
    long Good,
    long Scrap,
    double RunSeconds,
    double DownUnplannedSeconds,
    double DownPlannedSeconds,
    OeeDto Oee,
    int Tested,
    int PassedFirstTime,
    double Fpy,
    int Stops,
    int MicroStops,
    IReadOnlyList<RollupProductDto> Products);

public sealed record MeasurementsDto(
    double LeakRateMbarLs,
    double LeakLimitMbarLs,
    double FlowLpm,
    double FlowTargetLpm,
    double PressureBar,
    double[] TorqueNm);

public sealed record PartTestDto(
    DateTimeOffset TsUtc,
    string Station,
    string FixtureId,
    string Result,
    bool Passed,
    MeasurementsDto Measurements,
    string OperatorId,
    string Source,
    long Seq);

public sealed record LotToolDto(
    string Site,
    string Line,
    string ToolId,
    DateTimeOffset FirstSeenUtc,
    DateTimeOffset LastSeenUtc,
    int Samples,
    double? AvgSpindleLoadPct,
    double? MinToolLifePct);

public sealed record LotDto(
    string HousingLotId,
    int Parts,
    int Tested,
    int Passed,
    double PassRate,
    IReadOnlyList<LotToolDto> Tools);

public sealed record PartDto(
    string Serial,
    string Site,
    string Line,
    string ProgramId,
    PartTestDto FirstTest,
    IReadOnlyList<PartTestDto> Attempts,
    bool Reworked,
    LotDto Lot);
