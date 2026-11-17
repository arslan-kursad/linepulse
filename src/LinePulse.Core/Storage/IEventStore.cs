using LinePulse.Contracts;
using LinePulse.Core.Kpi;

namespace LinePulse.Core.Storage;

/// <summary>What the durable idempotency check decided about one event.</summary>
public enum StoreOutcome
{
    /// <summary>First time this (source, seq) was seen: stored.</summary>
    Stored,

    /// <summary>Same (source, seq) and same event time as a stored event: a
    /// redelivery under at-least-once, refused.</summary>
    Duplicate,

    /// <summary>
    /// Same (source, seq) but a different event time. Not a redelivery: a producer
    /// that lost its sequence counter and is reusing numbers for new parts.
    /// Refused, because nothing here can tell which of the two is right, and
    /// counted on its own because it means a device needs looking at -- quietly
    /// filing it as a duplicate would hide new parts behind old ones.
    /// </summary>
    KeyConflict,
}

/// <summary>
/// The write side of the brain: everything ingest needs from a database.
/// </summary>
/// <remarks>
/// This interface is the provider seam. The lab implementation is SQLite; the
/// plant target is MSSQL. Nothing above this line knows any SQL and nothing below
/// it knows any OEE: the minute arithmetic lives in <see cref="MinuteAggregator"/>,
/// so a second provider only moves rows and cannot compute a different number.
/// </remarks>
public interface IEventStore
{
    Task InitializeAsync(CancellationToken ct);

    /// <summary>
    /// Stores a sample unless its (source, seq) is already stored. The idempotency
    /// check and the write are one statement against the table's primary key, so
    /// there is no window in which a key is claimed but its row is missing -- a
    /// crash in between cannot turn a redelivery into a lost event -- and two Core
    /// instances behind a load balancer still agree.
    /// </summary>
    Task<StoreOutcome> StoreMachineSampleAsync(MachineSample sample, long ingestTsMs, CancellationToken ct);

    /// <summary>Same contract as <see cref="StoreMachineSampleAsync"/>, for a test
    /// record and its failed-check rows, in one transaction.</summary>
    Task<StoreOutcome> StoreTestRecordAsync(TestRecord record, long ingestTsMs, CancellationToken ct);

    /// <summary>Event time of the first stored sample after <paramref name="afterTsMs"/>,
    /// which is how far back-filled state from a late sample reaches.</summary>
    Task<long?> NextSampleTsAsync(string site, string line, long afterTsMs, CancellationToken ct);

    /// <summary>Raw material for rebuilding one minute.</summary>
    Task<MinuteInputs> LoadMinuteInputsAsync(string site, string line, long bucketStartMs, CancellationToken ct);

    /// <summary>
    /// Writes a rebuilt minute over whatever was there. Idempotent by construction:
    /// the bucket depends only on stored rows, so writing it twice is wasted work
    /// rather than double counting.
    /// </summary>
    /// <returns>How many times the minute has now been revised after it was sealed.</returns>
    Task<long> UpsertMinuteAsync(MinuteBucket bucket, int lateBeyondWatermarkCount, CancellationToken ct);

    /// <summary>Marks every bucket whose minute has fully passed the line's
    /// watermark as published, so a later change to it is visibly a revision.</summary>
    Task<int> SealBucketsAsync(string site, string line, long watermarkMs, CancellationToken ct);

    Task<long> OpenDowntimeAsync(DowntimeOpen open, CancellationToken ct);

    Task CloseDowntimeAsync(long id, long endedMs, double durationSeconds, bool microStop, CancellationToken ct);

    /// <summary>Stops that were in progress when the service last stopped.</summary>
    Task<IReadOnlyList<OpenDowntimeRow>> LoadOpenDowntimeAsync(CancellationToken ct);

    /// <summary>Event time of the first sample in <paramref name="state"/> after an
    /// instant -- how a stop left open by a crash finds out when it really ended.</summary>
    Task<long?> FirstSampleInStateAfterAsync(string site, string line, long afterTsMs, LineState state, CancellationToken ct);

    /// <summary>Latest sample per line, so a restart does not start from a blank state board.</summary>
    Task<IReadOnlyList<LineStateRow>> LoadLatestLineStatesAsync(CancellationToken ct);
}

/// <summary>A stop that has just started.</summary>
public sealed record DowntimeOpen(
    string Site,
    string Line,
    long StartedMs,
    LineState State,
    int ReasonCode,
    string ReasonText,
    bool Planned);

/// <summary>An unfinished stop, as stored.</summary>
public sealed record OpenDowntimeRow
{
    public long Id { get; init; }

    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public long StartedMs { get; init; }

    public int ReasonCode { get; init; }

    public string ReasonText { get; init; } = "";

    public bool Planned { get; init; }
}

/// <summary>The newest stored sample of one line.</summary>
public sealed record LineStateRow
{
    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    public long TsMs { get; init; }

    public int State { get; init; }

    public string StateText { get; init; } = "";

    public int AlarmCode { get; init; }

    public string AlarmText { get; init; } = "";

    public string ProgramId { get; init; } = "";
}
