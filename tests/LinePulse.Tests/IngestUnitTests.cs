using System.Text;
using System.Text.Json;
using LinePulse.Contracts;
using LinePulse.Core;
using LinePulse.Core.Ingest;
using LinePulse.Core.Storage;

namespace LinePulse.Tests;

/// <summary>
/// The pieces that decide whether an event is a duplicate, late, or usable at all.
/// </summary>
/// <remarks>
/// The end-to-end verifier exercises these through a real broker, which is the right way to
/// prove they work together. It is the wrong way to prove the edges: a clock running
/// backwards, a sequence number of zero, a payload with an unexpected field. Those are
/// cheap to construct here and awkward to arrange in a live plant or a live simulator.
/// </remarks>
public class IngestUnitTests
{
    private const long Minute = Clock.MinuteMs;

    private static readonly IngestOptions Options = new()
    {
        WatermarkSeconds = 30,
        DedupCacheSize = 4,
        MicroStopThresholdSeconds = 120,
    };

    private static StreamKey Machine(string line = "L01") => new("IZM", line, StreamKind.Machine);

    // ------------------------------------------------------------------ watermark

    [Fact]
    public void TheFirstEventOfAStreamIsOnTime()
    {
        var tracker = new WatermarkTracker(Options);

        Assert.Equal(Lateness.OnTime, tracker.Classify(Machine(), 1_000_000));
        Assert.Null(tracker.HighWaterMs(Machine()));
    }

    [Fact]
    public void OutOfOrderInsideOneMinuteIsNotLateness()
    {
        // The minute is rebuilt from all of its rows, so two samples swapped inside it change
        // nothing. Counting that as a correction would make the late counter meaningless.
        var tracker = new WatermarkTracker(Options);
        tracker.Advance(Machine(), 30_000);

        Assert.Equal(Lateness.OnTime, tracker.Classify(Machine(), 10_000));
    }

    [Fact]
    public void ReachingBackIntoAnEarlierMinuteInsideTheWatermarkIsAnAppliedCorrection()
    {
        var tracker = new WatermarkTracker(Options);
        tracker.Advance(Machine(), Minute + 5_000);        // 01:05

        Assert.Equal(Lateness.LateApplied, tracker.Classify(Machine(), 55_000));   // 00:55, 10 s late
    }

    [Fact]
    public void OlderThanTheWatermarkIsReportedSeparately()
    {
        // Its minute was already settled, so applying it revises a number somebody may have
        // read. That is a different event from an ordinary correction and is counted apart.
        var tracker = new WatermarkTracker(Options);
        tracker.Advance(Machine(), 10 * Minute);

        Assert.Equal(Lateness.LateBeyondWatermark, tracker.Classify(Machine(), 10 * Minute - 31_000));
    }

    [Fact]
    public void TheWatermarkNeverMovesBackwards()
    {
        var tracker = new WatermarkTracker(Options);
        tracker.Advance(Machine(), 5 * Minute);
        tracker.Advance(Machine(), 1 * Minute);

        Assert.Equal(5 * Minute, tracker.HighWaterMs(Machine()));
    }

    [Fact]
    public void OneStreamsClockCannotSealAnothersMinutes()
    {
        // This is why the watermark is per stream. The bench is a PC and the PLC data comes
        // through an OPC-UA server; in a plant they are routinely seconds apart. A shared
        // watermark would let a fast bench mark the PLC's own samples as late.
        var tracker = new WatermarkTracker(Options);
        var bench = new StreamKey("IZM", "L01", StreamKind.Test);

        tracker.Advance(bench, 10 * Minute);

        Assert.Equal(Lateness.OnTime, tracker.Classify(Machine(), 2 * Minute));
        Assert.Null(tracker.HighWaterMs(Machine()));
    }

    [Fact]
    public void StreamKeyIsDerivedFromTheEventType()
    {
        var sample = new MachineSample { Source = "opcua:IZM/L01", Seq = 1, Site = "IZM", Line = "L01" };
        var record = new TestRecord { Source = "bench:IZM/L01", Seq = 1, Site = "IZM", Line = "L01" };

        Assert.Equal(StreamKind.Machine, StreamKey.For(sample).Kind);
        Assert.Equal(StreamKind.Test, StreamKey.For(record).Kind);
        Assert.NotEqual(StreamKey.For(sample), StreamKey.For(record));
    }

    // ---------------------------------------------------------------------- dedup

    [Fact]
    public void ARepublishWithTheSameEventTimeIsADuplicate()
    {
        var gate = new DedupGate(Options);
        var key = new EventKey("bench:IZM/L01", 42);

        Assert.Null(gate.CheckRecent(key, 1_000));
        gate.Remember(key, 1_000);

        Assert.Equal(StoreOutcome.Duplicate, gate.CheckRecent(key, 1_000));
    }

    [Fact]
    public void TheSameKeyWithADifferentEventTimeIsAConflictNotADuplicate()
    {
        // The producer lost its counter and is reusing numbers for new parts. Filing that as
        // a duplicate would hide new parts behind old ones; it needs a device looked at.
        var gate = new DedupGate(Options);
        var key = new EventKey("bench:IZM/L01", 42);
        gate.Remember(key, 1_000);

        Assert.Equal(StoreOutcome.KeyConflict, gate.CheckRecent(key, 9_000));
    }

    [Fact]
    public void EvictionFromTheCacheCanOnlyCostSpeedNeverCorrectness()
    {
        // The cache is a cost optimisation in front of the primary key. With a size of four,
        // the fifth key evicts the first, and the first then has to ask the database -- which
        // is the only thing that actually guarantees idempotency.
        var gate = new DedupGate(Options);
        for (var seq = 1; seq <= 5; seq++)
        {
            gate.Remember(new EventKey("bench:IZM/L01", seq), seq * 1_000);
        }

        Assert.True(gate.CacheCount <= Options.DedupCacheSize, "the cache stays bounded");
        Assert.Null(gate.CheckRecent(new EventKey("bench:IZM/L01", 1), 1_000));
        Assert.Equal(StoreOutcome.Duplicate, gate.CheckRecent(new EventKey("bench:IZM/L01", 5), 5_000));
    }

    [Fact]
    public void RememberingAKeyTwiceDoesNotGrowTheCache()
    {
        var gate = new DedupGate(Options);
        var key = new EventKey("bench:IZM/L01", 7);

        gate.Remember(key, 1_000);
        gate.Remember(key, 1_000);

        Assert.Equal(1, gate.CacheCount);
    }

    // ---------------------------------------------------------------------- codec

    private static byte[] Payload(object value) =>
        Encoding.UTF8.GetBytes(JsonSerializer.Serialize(value, JsonDefaults.Options));

    [Fact]
    public void AMachineSampleRoundTripsFromItsSubject()
    {
        var payload = Payload(new
        {
            eventId = Guid.NewGuid(),
            source = "opcua:IZM/L01",
            seq = 12L,
            site = "IZM",
            line = "L01",
            ts = "2026-11-17T05:30:00.000Z",
            state = 1,
            goodCount = 5_000L,
            scrapCount = 40L,
            programId = "OP-4412-A",
        });

        var (decoded, failure) = EventCodec.Decode("lp.IZM.L01.machine", payload);

        Assert.Equal(DecodeFailure.None, failure);
        var sample = Assert.IsType<MachineSample>(decoded);
        Assert.Equal(LineState.Running, sample.State);
        Assert.Equal(5_000UL, sample.GoodCount);
        Assert.Equal(new EventKey("opcua:IZM/L01", 12), sample.Key);
    }

    [Fact]
    public void AnUnexpectedFieldFromAFirmwareUpdateDoesNotStopTraceability()
    {
        var payload = Payload(new
        {
            source = "bench:IZM/L01", seq = 1L, site = "IZM", line = "L01",
            ts = "2026-11-17T05:30:00.000Z", serial = "IZM-L01-2646-000001", result = "PASS",
            someNewVendorField = "added in a service pack",
        });

        var (decoded, failure) = EventCodec.Decode("lp.IZM.L01.test", payload);

        Assert.Equal(DecodeFailure.None, failure);
        Assert.Equal("IZM-L01-2646-000001", Assert.IsType<TestRecord>(decoded).Serial);
    }

    [Theory]
    // A missing seq deserialises as 0. Accepting it would give every event from that producer
    // the same key, and all but the first would be filed as duplicates: new parts lost under
    // traffic that looks like ordinary at-least-once redelivery.
    [InlineData(@"{""source"":""bench:IZM/L01"",""site"":""IZM"",""line"":""L01"",""ts"":""2026-11-17T05:30:00Z""}", DecodeFailure.MissingKey)]
    [InlineData(@"{""source"":""bench:IZM/L01"",""seq"":0,""site"":""IZM"",""line"":""L01"",""ts"":""2026-11-17T05:30:00Z""}", DecodeFailure.MissingKey)]
    [InlineData(@"{""seq"":5,""site"":""IZM"",""line"":""L01"",""ts"":""2026-11-17T05:30:00Z""}", DecodeFailure.MissingKey)]
    [InlineData(@"{""source"":""bench:IZM/L01"",""seq"":5,""line"":""L01"",""ts"":""2026-11-17T05:30:00Z""}", DecodeFailure.MissingKey)]
    [InlineData(@"{""source"":""bench:IZM/L01"",""seq"":5,""site"":""IZM"",""line"":""L01""}", DecodeFailure.MissingKey)]
    [InlineData(@"{not json", DecodeFailure.Malformed)]
    public void AnEventWithoutAUsableKeyIsRefusedAndNamed(string json, DecodeFailure expected)
    {
        var (decoded, failure) = EventCodec.Decode("lp.IZM.L01.test", Encoding.UTF8.GetBytes(json));

        Assert.Null(decoded);
        Assert.Equal(expected, failure);
    }

    [Fact]
    public void AnUnknownSubjectIsNamedAsSuchRatherThanCalledMalformed()
    {
        // The two failures lead to different actions: a bad payload is a producer bug, an
        // unknown subject is usually a configuration mistake.
        var (decoded, failure) = EventCodec.Decode("lp.IZM.L01.something", Payload(new { source = "x", seq = 1 }));

        Assert.Null(decoded);
        Assert.Equal(DecodeFailure.UnknownSubject, failure);
    }

    // -------------------------------------------------------------- bucket queue

    [Fact]
    public void DirtyMinutesCoalesceSoABurstCostsOneRebuild()
    {
        // 13 lines sampling four times a second put thousands of events into one minute.
        // Rebuilding per event would do the same arithmetic thousands of times for the same
        // answer, and the rebuild reads every sample in the minute.
        var queue = new MinuteBucketQueue();
        var key = new BucketKey("IZM", "L01", 0);

        for (var i = 0; i < 500; i++)
        {
            queue.MarkDirty(key);
        }

        Assert.Equal(1, queue.DirtyCount);
        Assert.Single(queue.Drain());
        Assert.Equal(0, queue.DirtyCount);
    }

    [Fact]
    public void BeyondWatermarkRevisionsAccumulatePerBucket()
    {
        // Several late events can land in the same minute between flushes, and the bucket's
        // revision audit has to reflect all of them.
        var queue = new MinuteBucketQueue();
        var key = new BucketKey("IZM", "L01", 0);

        queue.MarkDirty(key, lateBeyondWatermark: 1);
        queue.MarkDirty(key, lateBeyondWatermark: 2);
        queue.MarkDirty(key);

        var (_, count) = Assert.Single(queue.Drain());
        Assert.Equal(3, count);
    }

    [Fact]
    public void ASpanMarksEveryMinuteAStateCanReach()
    {
        // A sample's state holds until the next sample, so its effect is a span. Marking only
        // its own minute would leave the minutes in the middle of a five-minute stop as they
        // were -- which is to say, empty.
        var queue = new MinuteBucketQueue();

        queue.MarkSpanDirty("IZM", "L01", 30_000, 4 * Minute + 30_000, maxCarryMs: 3_600_000);

        var minutes = queue.Drain().Select(d => d.Key.BucketStartMs).OrderBy(ms => ms).ToList();
        Assert.Equal([0, Minute, 2 * Minute, 3 * Minute, 4 * Minute], minutes);
    }

    [Fact]
    public void ASpanIsClippedToTheCarryLimitButStillMarksTheArrivalMinute()
    {
        // Beyond the carry limit the state is not billed, so those minutes need no rebuild --
        // except the minute of the new sample, whose counter baseline is the old one however
        // far back it is.
        var queue = new MinuteBucketQueue();

        queue.MarkSpanDirty("IZM", "L01", 0, 100 * Minute, maxCarryMs: 120_000);

        var minutes = queue.Drain().Select(d => d.Key.BucketStartMs).OrderBy(ms => ms).ToList();
        Assert.Equal([0, Minute, 2 * Minute, 100 * Minute], minutes);
    }
}
