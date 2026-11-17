namespace LinePulse.Core.Storage;

/// <summary>
/// The read side: every query the API needs, and nothing else.
/// </summary>
/// <remarks>
/// <para>
/// Kept separate from <see cref="IEventStore"/> because the two sides have
/// different pressures. Ingest cares about write cost and idempotency; the API
/// cares about index coverage and response time. A provider port only has to
/// satisfy these two interfaces -- and splitting them makes it obvious which
/// statements are on the hot write path.
/// </para>
/// <para>
/// The row types are init-only property records, not positional ones. Dapper maps
/// a positional record only when every column type matches its constructor
/// exactly; SQLite reports every INTEGER as 64-bit and an all-NULL aggregate as a
/// blob, MSSQL reports what the column was declared as, so positional rows break
/// on one provider or the other. Property mapping converts by name.
/// </para>
/// </remarks>
public interface IKpiStore
{
    Task<IReadOnlyList<LineRef>> KnownLinesAsync(CancellationToken ct);

    Task<IReadOnlyList<LineAggregateRow>> AggregateLinesAsync(string site, long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<ProductAggregateRow>> AggregateProductsAsync(string site, long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<MinuteBucketRow>> MinuteBucketsAsync(string site, string line, long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<DowntimeParetoRow>> DowntimeParetoAsync(string site, string line, long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<FpyRow>> FirstPassYieldAsync(long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<FailureCountRow>> FailureBreakdownAsync(long fromMs, long toMs, CancellationToken ct);

    Task<IReadOnlyList<FixtureSampleRow>> FixtureSamplesAsync(int windowSize, long sinceMs, CancellationToken ct);

    Task<IReadOnlyList<TestRecordRow>> TestsForSerialAsync(string serial, CancellationToken ct);

    Task<IReadOnlyList<LotToolRow>> ToolsForLotAsync(string housingLotId, CancellationToken ct);

    Task<LotQualityRow?> LotQualityAsync(string housingLotId, CancellationToken ct);
}

public sealed record LineRef
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";
}

/// <summary>Minute buckets summed over a window, for one line.</summary>
public sealed record LineAggregateRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public long Good { get; init; }

    public long Scrap { get; init; }

    public double RunSeconds { get; init; }

    public double DownUnplannedSeconds { get; init; }

    public double DownPlannedSeconds { get; init; }

    public double IdealRunSeconds { get; init; }

    public int Minutes { get; init; }

    public int Revisions { get; init; }

    public int LateBeyondCount { get; init; }
}

public sealed record ProductAggregateRow
{
    public string Site { get; init; } = "";

    public string ProgramId { get; init; } = "";

    public long Good { get; init; }

    public long Scrap { get; init; }

    public double RunSeconds { get; init; }

    public double DownUnplannedSeconds { get; init; }

    public double DownPlannedSeconds { get; init; }

    public double IdealRunSeconds { get; init; }
}

public sealed record MinuteBucketRow
{
    public long BucketStartMs { get; init; }

    public string ProgramId { get; init; } = "";

    public long Good { get; init; }

    public long Scrap { get; init; }

    public int Samples { get; init; }

    public double RunSeconds { get; init; }

    public double DownUnplannedSeconds { get; init; }

    public double DownPlannedSeconds { get; init; }

    public double IdealRunSeconds { get; init; }

    public double AvgCycleMs { get; init; }

    public int Revisions { get; init; }

    public int LateBeyondCount { get; init; }

    public long? SealedAtMs { get; init; }
}

public sealed record DowntimeParetoRow
{
    public int ReasonCode { get; init; }

    public string ReasonText { get; init; } = "";

    public bool Planned { get; init; }

    public int Stops { get; init; }

    public int OpenStops { get; init; }

    public int MicroStops { get; init; }

    public double TotalSeconds { get; init; }

    public double LongestSeconds { get; init; }
}

public sealed record FpyRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public int Tested { get; init; }

    public int Passed { get; init; }

    public int Retests { get; init; }
}

public sealed record FailureCountRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public string CheckName { get; init; } = "";

    public int Count { get; init; }
}

public sealed record FixtureSampleRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public string FixtureId { get; init; } = "";

    public long TsMs { get; init; }

    public double LeakRate { get; init; }

    public double LeakLimit { get; init; }
}

public sealed record TestRecordRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public long TsMs { get; init; }

    public string Station { get; init; } = "";

    public string Serial { get; init; } = "";

    public string ProgramId { get; init; } = "";

    public string HousingLotId { get; init; } = "";

    public string FixtureId { get; init; } = "";

    public string Result { get; init; } = "";

    public bool Passed { get; init; }

    public double LeakRate { get; init; }

    public double LeakLimit { get; init; }

    public double FlowLpm { get; init; }

    public double FlowTarget { get; init; }

    public double PressureBar { get; init; }

    public string TorqueJson { get; init; } = "[]";

    public string OperatorId { get; init; } = "";

    public string Source { get; init; } = "";

    public long Seq { get; init; }
}

/// <summary>The machining-centre tool that produced a housing lot.</summary>
public sealed record LotToolRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public string ToolId { get; init; } = "";

    public long FirstSeenMs { get; init; }

    public long LastSeenMs { get; init; }

    public int Samples { get; init; }

    public double? AvgSpindleLoadPct { get; init; }

    public double? MinToolLifePct { get; init; }
}

public sealed record LotQualityRow
{
    public int Parts { get; init; }

    public int Tested { get; init; }

    public int Passed { get; init; }
}
