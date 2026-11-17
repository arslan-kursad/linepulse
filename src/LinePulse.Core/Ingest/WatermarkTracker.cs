using System.Collections.Concurrent;
using LinePulse.Contracts;

namespace LinePulse.Core.Ingest;

/// <summary>How late an event was, relative to what its stream had already sent.</summary>
public enum Lateness
{
    /// <summary>Belongs to the newest minute we have seen for this stream.</summary>
    OnTime,

    /// <summary>Belongs to an earlier minute, but is inside the watermark: an
    /// ordinary correction, applied and counted.</summary>
    LateApplied,

    /// <summary>Older than the watermark: its minute was already settled, so
    /// applying the event revises a number somebody may have read.</summary>
    LateBeyondWatermark,
}

/// <summary>Which of a line's two producers an event came from.</summary>
public enum StreamKind
{
    Machine,
    Test,
}

/// <summary>One producer's ordered stream of events for one line.</summary>
public readonly record struct StreamKey(string Site, string Line, StreamKind Kind)
{
    public static StreamKey For(EventEnvelope e) =>
        new(e.Site, e.Line, e is TestRecord ? StreamKind.Test : StreamKind.Machine);

    public override string ToString() => $"{Kind}:{Site}/{Line}";
}

/// <summary>
/// Per-stream event-time watermark.
/// </summary>
/// <remarks>
/// <para>
/// Bucketing on arrival time is the mistake this class exists to prevent. A bench
/// that reconnects after a network blip dumps twenty minutes of records at once;
/// if those land in the minute they arrived in, the shift report shows a line that
/// made nothing for twenty minutes and then made everything in one minute. Both
/// numbers are wrong and the wrongness is invisible.
/// </para>
/// <para>
/// The watermark belongs to a stream -- one line's PLC samples, or one line's bench
/// records -- and not to the line or the plant. Each producer stamps events with its
/// own clock: the leak bench is a PC, the PLC data comes through an OPC-UA server,
/// and in a plant the two are routinely seconds apart. A shared watermark would
/// let a bench running 40 seconds fast seal the line's minutes before the PLC
/// samples for them had arrived, and a bench running slow would have every one of
/// its records counted as a revision. A stream is only ever judged against itself.
/// </para>
/// <para>
/// Classifying and advancing are separate calls on purpose. The pipeline marks the
/// affected minutes dirty between the two, so a flush running concurrently can
/// never seal a minute that the event it is about to see still has to rebuild.
/// </para>
/// </remarks>
public sealed class WatermarkTracker(IngestOptions options)
{
    private readonly ConcurrentDictionary<StreamKey, long> _highWaterMs = new();

    public long LatenessMs { get; } = options.WatermarkSeconds * 1000L;

    /// <summary>Highest event time seen on a stream, or null if it never reported.</summary>
    public long? HighWaterMs(StreamKey stream) =>
        _highWaterMs.TryGetValue(stream, out var value) ? value : null;

    /// <summary>Everything older than this is considered settled.</summary>
    public long? WatermarkMs(StreamKey stream) => HighWaterMs(stream) - LatenessMs;

    public IReadOnlyCollection<StreamKey> Streams => _highWaterMs.Keys.ToArray();

    /// <summary>Classifies an event without moving the watermark.</summary>
    public Lateness Classify(StreamKey stream, long eventTsMs)
    {
        if (HighWaterMs(stream) is not { } highWater || eventTsMs >= highWater)
        {
            return Lateness.OnTime;
        }

        if (eventTsMs < highWater - LatenessMs)
        {
            return Lateness.LateBeyondWatermark;
        }

        // Behind the high-water mark but inside the watermark. Only interesting if
        // it reaches back into an earlier minute; out-of-order inside one minute
        // changes nothing, because the minute is rebuilt from all of its rows.
        return Clock.BucketStartMs(eventTsMs) < Clock.BucketStartMs(highWater)
            ? Lateness.LateApplied
            : Lateness.OnTime;
    }

    /// <summary>Moves the stream's high-water mark forward; never backward.</summary>
    public void Advance(StreamKey stream, long eventTsMs) =>
        _highWaterMs.AddOrUpdate(stream, eventTsMs, (_, existing) => Math.Max(existing, eventTsMs));
}
