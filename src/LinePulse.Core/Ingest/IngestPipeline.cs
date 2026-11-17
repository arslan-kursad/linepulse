using LinePulse.Contracts;
using LinePulse.Core.Kpi;
using LinePulse.Core.Realtime;
using LinePulse.Core.Storage;

namespace LinePulse.Core.Ingest;

/// <summary>
/// The ingest path: one method, five decisions, in this order.
/// </summary>
/// <remarks>
/// <para>
/// The order is the design. <b>Deduplicate</b> first, because every later step is
/// more expensive and a duplicate deserves none of it: a memory of recent keys
/// catches the common case cheaply, and the primary key of the raw table decides
/// everything else. <b>Store raw</b> second, so the event survives whatever
/// happens next -- derived numbers can always be rebuilt from raw rows, and a raw
/// row that was never written cannot be rebuilt from anything. <b>Classify by event
/// time</b> third, against the stream's watermark, never against arrival time.
/// <b>Mark the affected minutes dirty</b> fourth. <b>Advance the watermark</b> last.
/// </para>
/// <para>
/// The last two are ordered on purpose. A flush that runs between them can only
/// ever see a watermark that has not yet moved past the minutes about to be
/// rebuilt, so it cannot seal a minute that this event still has to revise.
/// Advancing first would open exactly that window.
/// </para>
/// <para>
/// Nothing in this class drops an event silently. A duplicate is rejected and
/// counted; a reused sequence number is refused and counted on its own, because it
/// means a device needs attention; a late event is applied and counted; an event
/// later than the watermark is applied, counted, and marks its bucket as revised.
/// The only other refusals are an event that does not parse and an event with no
/// idempotency key, and both are counted too. "Where did my part go" is not a
/// question a traceability system should ever answer with a shrug.
/// </para>
/// </remarks>
public sealed class IngestPipeline(
    IEventStore store,
    DedupGate dedup,
    WatermarkTracker watermarks,
    DowntimeTracker downtime,
    MinuteBucketQueue buckets,
    LineSnapshotCache snapshots,
    LiveFeed live,
    IngestCounters counters,
    IngestOptions options,
    ILogger<IngestPipeline> log)
{
    /// <summary>
    /// How far back to rebuild minute buckets after a restart.
    /// </summary>
    /// <remarks>
    /// The dirty queue lives in memory, so a process killed between storing an event
    /// and flushing its minute loses the mark but not the event. Rebuilding is
    /// idempotent -- a bucket depends only on stored rows -- so re-queueing the most
    /// recent minutes is cheap and closes that gap. Ten minutes covers the watermark
    /// with room for a flush backlog; it is not tied to the watermark because a
    /// backlog during a burst can be longer than the lateness the line tolerates.
    /// </remarks>
    private const long RestartRebuildWindowMs = 10 * Clock.MinuteMs;

    private readonly AggregationRules _rules = AggregationRules.From(options);
    private readonly long _maxCarryMs = Math.Max(1, options.MaxStateCarrySeconds) * 1000L;

    public async Task HandleAsync(string subject, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        counters.MarkReceived();

        var (decoded, failure) = EventCodec.Decode(subject, payload.Span);
        if (decoded is null)
        {
            switch (failure)
            {
                case DecodeFailure.MissingKey:
                    counters.MarkMissingKey();
                    log.LogWarning("refused event on {Subject}: no usable (source, seq) key", subject);
                    break;
                default:
                    counters.MarkMalformed();
                    log.LogWarning("refused event on {Subject}: {Failure}", subject, failure);
                    break;
            }

            return;
        }

        var key = decoded.Key;
        var eventTsMs = Clock.ToMs(decoded.EventTimeUtc);

        // 1. Idempotency, cheap half. (Source, Seq), never EventId: a bench that
        // restarts mints a new EventId for a part it already reported.
        if (dedup.CheckRecent(key, eventTsMs) is { } remembered)
        {
            CountRefusal(remembered, decoded);
            return;
        }

        // 2. Store raw, and let the database make the call. The check and the write
        // are one statement against the primary key, so this is the guarantee the
        // cache above only approximates.
        var ingestTsMs = Clock.NowMs();
        StoreOutcome outcome;
        switch (decoded)
        {
            case MachineSample sample:
                outcome = await store.StoreMachineSampleAsync(sample, ingestTsMs, ct).ConfigureAwait(false);
                break;

            case TestRecord record:
                outcome = await store.StoreTestRecordAsync(record, ingestTsMs, ct).ConfigureAwait(false);
                break;

            default:
                counters.MarkMalformed();
                return;
        }

        // Remember the ruling whichever way it went: a redelivery that arrives once
        // arrives again, because at-least-once retries on a timer.
        dedup.Remember(key, eventTsMs);

        if (outcome != StoreOutcome.Stored)
        {
            CountRefusal(outcome, decoded);
            return;
        }

        // 3. Event time, never arrival time. Classified against the stream's own
        // watermark, and before this event moves it: an event must be judged against
        // what its stream had already sent, not against itself.
        var stream = StreamKey.For(decoded);
        var lateness = watermarks.Classify(stream, eventTsMs);
        switch (lateness)
        {
            case Lateness.LateApplied:
                counters.MarkLateApplied();
                break;

            case Lateness.LateBeyondWatermark:
                counters.MarkLateBeyondWatermark();
                log.LogInformation(
                    "late beyond watermark: {Key} event {EventTime} is {Seconds:F1}s behind the {Stream} watermark; "
                    + "bucket {Bucket} will be revised",
                    key,
                    Clock.IsoFromMs(eventTsMs),
                    ((watermarks.WatermarkMs(stream) ?? eventTsMs) - eventTsMs) / 1000.0,
                    stream,
                    Clock.IsoFromMs(Clock.BucketStartMs(eventTsMs)));
                break;
        }

        // 4. Queue the minutes this event can change.
        //
        // Test records do not feed the minute buckets: OEE quality comes from the
        // PLC's good and scrap counters, which are the plant's own number. They feed
        // first-pass yield, SPC and genealogy, all of which are queried from raw rows,
        // so a late test record needs no bucket repair.
        if (decoded is MachineSample machine)
        {
            await AfterMachineSampleAsync(machine, eventTsMs, lateness, ct).ConfigureAwait(false);
        }

        // 5. Only now let the watermark move.
        watermarks.Advance(stream, eventTsMs);
        counters.MarkIngested();
    }

    private async Task AfterMachineSampleAsync(
        MachineSample sample,
        long eventTsMs,
        Lateness lateness,
        CancellationToken ct)
    {
        var site = sample.Site;
        var line = sample.Line;

        if (lateness == Lateness.OnTime)
        {
            // The newest sample. Its predecessor's state has been waiting for it:
            // time after the latest sample is not billed until something proves the
            // state still held, so the minutes between the two become billable now.
            var previousMs = snapshots.Get(site, line) is { } previous
                ? Math.Min(Clock.ToMs(previous.LastSeenUtc), eventTsMs)
                : eventTsMs;
            buckets.MarkSpanDirty(site, line, previousMs, eventTsMs, _maxCarryMs);
        }
        else
        {
            // A back-filled sample changes what holds from its own timestamp until
            // the sample that used to follow it, and that following minute's counter
            // baseline too. The span is rebuilt from rows rather than patched, so the
            // repair is exact however many late events arrive, in whatever order.
            var nextTsMs = await store.NextSampleTsAsync(site, line, eventTsMs, ct).ConfigureAwait(false);
            buckets.MarkSpanDirty(site, line, eventTsMs, nextTsMs ?? eventTsMs, _maxCarryMs);
        }

        if (lateness == Lateness.LateBeyondWatermark)
        {
            // The minute was already settled. Count the revision on the bucket itself
            // so the morning meeting can see which number moved and how often.
            buckets.MarkDirty(new BucketKey(site, line, Clock.BucketStartMs(eventTsMs)), lateBeyondWatermark: 1);
        }

        // Downtime, from the transition. Order-sensitive, so it comes last among
        // the derived state and it refuses to move backwards.
        var closed = await downtime.ApplyAsync(sample, ct).ConfigureAwait(false);
        if (closed is not null)
        {
            log.LogInformation(
                "{Site}/{Line} stop closed: {Seconds:F0}s, reason {Code} {Text}{Micro}",
                closed.Site,
                closed.Line,
                closed.DurationSeconds,
                closed.ReasonCode,
                closed.ReasonText,
                closed.MicroStop ? " [micro-stop]" : "");
            live.Publish("downtime", closed);
        }

        var changed = snapshots.Apply(sample);
        if (changed is not null)
        {
            live.Publish("lineState", changed);
        }
    }

    private void CountRefusal(StoreOutcome outcome, EventEnvelope refused)
    {
        switch (outcome)
        {
            case StoreOutcome.Duplicate:
                counters.MarkDeduped();
                break;

            case StoreOutcome.KeyConflict:
                counters.MarkKeyConflict();
                log.LogWarning(
                    "key conflict: {Key} was already stored with a different event time -- "
                    + "the producer has probably lost its sequence counter",
                    refused.Key);
                break;
        }
    }

    /// <summary>
    /// Rebuilds every dirty minute and advances the sealed watermark.
    /// Called on a timer by <see cref="BucketFlushService"/>.
    /// </summary>
    public async Task<int> FlushAsync(CancellationToken ct)
    {
        var dirty = buckets.Drain();
        var rebuilt = 0;

        foreach (var (key, lateBeyondCount) in dirty)
        {
            try
            {
                var inputs = await store
                    .LoadMinuteInputsAsync(key.Site, key.Line, key.BucketStartMs, ct)
                    .ConfigureAwait(false);

                var bucket = MinuteAggregator.Aggregate(key.Site, key.Line, key.BucketStartMs, inputs, _rules);
                if (bucket is not null)
                {
                    var revisions = await store.UpsertMinuteAsync(bucket, lateBeyondCount, ct).ConfigureAwait(false);
                    if (lateBeyondCount > 0)
                    {
                        log.LogInformation(
                            "{Site}/{Line} {Bucket} revised after sealing ({Revisions} revision(s) so far)",
                            key.Site, key.Line, Clock.IsoFromMs(key.BucketStartMs), revisions);
                    }
                }

                rebuilt++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Put it back: a bucket that failed to rebuild is still wrong, and the
                // next flush is a fraction of a second away.
                buckets.MarkDirty(key, lateBeyondCount);
                log.LogError(
                    ex,
                    "rebuild failed for {Site}/{Line} {Bucket}",
                    key.Site, key.Line, Clock.IsoFromMs(key.BucketStartMs));
            }
        }

        counters.AddBucketRecomputes(rebuilt);

        // Only a line's machine stream seals minute buckets: the bench is a different
        // producer with a different clock, and its records never feed a bucket.
        foreach (var stream in watermarks.Streams)
        {
            if (stream.Kind != StreamKind.Machine || watermarks.WatermarkMs(stream) is not { } watermarkMs)
            {
                continue;
            }

            await store.SealBucketsAsync(stream.Site, stream.Line, watermarkMs, ct).ConfigureAwait(false);
        }

        return dirty.Count;
    }

    public async Task StartupRestoreAsync(CancellationToken ct)
    {
        var states = await store.LoadLatestLineStatesAsync(ct).ConfigureAwait(false);

        snapshots.Seed(states);
        await downtime.RestoreAsync(states, ct).ConfigureAwait(false);

        foreach (var row in states)
        {
            watermarks.Advance(new StreamKey(row.Site, row.Line, StreamKind.Machine), row.TsMs);
            buckets.MarkSpanDirty(row.Site, row.Line, row.TsMs - RestartRebuildWindowMs, row.TsMs, _maxCarryMs);
        }

        if (states.Count > 0)
        {
            log.LogInformation(
                "restored state for {Count} lines; re-queued the last {Minutes} minutes of each for rebuild",
                states.Count,
                RestartRebuildWindowMs / Clock.MinuteMs);
        }
    }
}
