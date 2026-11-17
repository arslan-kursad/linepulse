namespace LinePulse.Core.Ingest;

/// <summary>
/// Live ingest counters, exposed by GET /api/health.
/// </summary>
/// <remarks>
/// These are the numbers that answer "is the pipeline telling me the truth?" on a
/// shop floor. Duplicates and late events are normal traffic, not errors, so they
/// are counted rather than logged: a rate you can watch beats a log you have to
/// go and read. Every way an event can fail to become a stored row has its own
/// counter, so "received" always reconciles against the outcomes. All increments
/// are interlocked because ingest runs concurrently with the HTTP reads.
/// </remarks>
public sealed class IngestCounters
{
    private long _received;
    private long _ingested;
    private long _deduped;
    private long _keyConflicts;
    private long _lateApplied;
    private long _lateBeyondWatermark;
    private long _malformed;
    private long _missingKey;
    private long _failed;
    private long _droppedByClient;
    private long _outOfOrderState;
    private long _bucketRecomputes;
    private long _microStops;

    public DateTimeOffset StartedUtc { get; } = DateTimeOffset.UtcNow;

    /// <summary>Messages handed to the pipeline.</summary>
    public long Received => Interlocked.Read(ref _received);

    /// <summary>Events accepted and stored.</summary>
    public long Ingested => Interlocked.Read(ref _ingested);

    /// <summary>Refused because the same (source, seq) with the same event time was
    /// already stored: a redelivery.</summary>
    public long Deduped => Interlocked.Read(ref _deduped);

    /// <summary>Refused because the same (source, seq) was already stored with a
    /// different event time: a producer reusing sequence numbers.</summary>
    public long KeyConflicts => Interlocked.Read(ref _keyConflicts);

    /// <summary>Late, inside the watermark: applied to an earlier minute, which was
    /// then rebuilt.</summary>
    public long LateApplied => Interlocked.Read(ref _lateApplied);

    /// <summary>Later than the watermark: stored and applied anyway, but the minute
    /// had already settled, so the change is a visible revision.</summary>
    public long LateBeyondWatermark => Interlocked.Read(ref _lateBeyondWatermark);

    public long Malformed => Interlocked.Read(ref _malformed);

    /// <summary>Events with no usable idempotency key. Cannot be deduplicated, so
    /// they are refused rather than stored.</summary>
    public long MissingKey => Interlocked.Read(ref _missingKey);

    /// <summary>Events that parsed but could not be processed, e.g. a database
    /// error. Logged with the key, because core NATS will not redeliver them.</summary>
    public long Failed => Interlocked.Read(ref _failed);

    /// <summary>Messages the NATS client discarded because ingest fell behind and
    /// its pending buffer filled. Core NATS is at-most-once; this is the number that
    /// says whether that has cost anything.</summary>
    public long DroppedByClient => Interlocked.Read(ref _droppedByClient);

    /// <summary>Samples older than the newest already applied to a line's state
    /// machine. Stored, counted, but not allowed to rewind the downtime record.</summary>
    public long OutOfOrderState => Interlocked.Read(ref _outOfOrderState);

    public long BucketRecomputes => Interlocked.Read(ref _bucketRecomputes);

    public long MicroStops => Interlocked.Read(ref _microStops);

    public double UptimeSeconds => (DateTimeOffset.UtcNow - StartedUtc).TotalSeconds;

    public void MarkReceived() => Interlocked.Increment(ref _received);

    public void MarkIngested() => Interlocked.Increment(ref _ingested);

    public void MarkDeduped() => Interlocked.Increment(ref _deduped);

    public void MarkKeyConflict() => Interlocked.Increment(ref _keyConflicts);

    public void MarkLateApplied() => Interlocked.Increment(ref _lateApplied);

    public void MarkLateBeyondWatermark() => Interlocked.Increment(ref _lateBeyondWatermark);

    public void MarkMalformed() => Interlocked.Increment(ref _malformed);

    public void MarkMissingKey() => Interlocked.Increment(ref _missingKey);

    public void MarkFailed() => Interlocked.Increment(ref _failed);

    public void MarkDroppedByClient() => Interlocked.Increment(ref _droppedByClient);

    public void MarkOutOfOrderState() => Interlocked.Increment(ref _outOfOrderState);

    public void AddBucketRecomputes(int count) => Interlocked.Add(ref _bucketRecomputes, count);

    public void MarkMicroStop() => Interlocked.Increment(ref _microStops);
}
