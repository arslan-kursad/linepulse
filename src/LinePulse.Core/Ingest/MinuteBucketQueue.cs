using System.Collections.Concurrent;

namespace LinePulse.Core.Ingest;

/// <summary>One minute of one line that needs rebuilding.</summary>
public readonly record struct BucketKey(string Site, string Line, long BucketStartMs);

/// <summary>
/// Coalescing work queue for minute-bucket rebuilds.
/// </summary>
/// <remarks>
/// <para>
/// Thirteen lines sampling four times a second put 3,120 events into the same
/// minute. Rebuilding that minute on every one of them would do the same arithmetic
/// thousands of times to reach the same answer -- and the rebuild reads every
/// sample in the minute, so the cost is quadratic in sample rate. Marking the
/// minute dirty and rebuilding it on a short timer turns that into one rebuild per
/// minute per flush, which is the difference between this running on a laptop and
/// not.
/// </para>
/// <para>
/// The beyond-watermark count is accumulated here rather than written per event,
/// because several late events can land in the same minute between flushes and
/// the bucket's revision audit has to reflect all of them.
/// </para>
/// </remarks>
public sealed class MinuteBucketQueue
{
    private readonly ConcurrentDictionary<BucketKey, int> _dirty = new();

    public int DirtyCount => _dirty.Count;

    public void MarkDirty(BucketKey key, int lateBeyondWatermark = 0) =>
        _dirty.AddOrUpdate(key, lateBeyondWatermark, (_, count) => count + lateBeyondWatermark);

    /// <summary>
    /// Marks every minute an event's state can reach: from the minute it starts
    /// holding to the minute the next sample takes over.
    /// </summary>
    /// <remarks>
    /// A sample's state holds until the next sample, so its effect is a span, not a
    /// point. The span is clipped to the carry limit, because state is not billed
    /// beyond it anyway -- except the minute of <paramref name="toTsMs"/> itself,
    /// whose counter baseline depends on the sample before it however far back
    /// that is.
    /// </remarks>
    public void MarkSpanDirty(string site, string line, long fromTsMs, long toTsMs, long maxCarryMs)
    {
        var last = Clock.BucketStartMs(Math.Min(toTsMs, fromTsMs + maxCarryMs));
        for (var bucket = Clock.BucketStartMs(fromTsMs); bucket <= last; bucket += Clock.MinuteMs)
        {
            MarkDirty(new BucketKey(site, line, bucket));
        }

        MarkDirty(new BucketKey(site, line, Clock.BucketStartMs(toTsMs)));
    }

    /// <summary>Takes everything currently dirty. Minutes marked while a flush is in
    /// progress stay queued for the next one, so nothing is lost and nothing is
    /// rebuilt twice for the same mark.</summary>
    public List<(BucketKey Key, int LateBeyondCount)> Drain()
    {
        var taken = new List<(BucketKey, int)>(_dirty.Count);
        foreach (var key in _dirty.Keys)
        {
            if (_dirty.TryRemove(key, out var count))
            {
                taken.Add((key, count));
            }
        }

        return taken;
    }
}
