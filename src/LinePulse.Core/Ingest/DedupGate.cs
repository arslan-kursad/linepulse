using System.Collections.Concurrent;
using LinePulse.Contracts;
using LinePulse.Core.Storage;

namespace LinePulse.Core.Ingest;

/// <summary>
/// The cheap half of duplicate rejection: a memory of recently stored keys.
/// </summary>
/// <remarks>
/// <para>
/// Two layers, and only one of them is a guarantee. The guarantee is the primary
/// key of the raw tables, checked by the same statement that stores the event, so
/// it survives a restart and still holds with a second Core instance behind a load
/// balancer. This cache is a cost optimisation: a bench republishing at QoS 1
/// usually repeats itself within seconds, and catching that without touching the
/// database keeps the hot path cheap under a retry storm.
/// </para>
/// <para>
/// The cache remembers each key's event time as well, so it can tell a redelivery
/// (same time) from a producer that reset its sequence counter (different time) --
/// the same distinction the database draws, made without a round trip.
/// </para>
/// <para>
/// Because the cache is only an optimisation, evicting from it can never produce a
/// wrong answer, only a slower one. That is what makes a fixed-size cache safe
/// here, and it is why the eviction policy is allowed to be as crude as insertion
/// order.
/// </para>
/// </remarks>
public sealed class DedupGate(IngestOptions options)
{
    private readonly ConcurrentDictionary<EventKey, long> _recent = new();
    private readonly ConcurrentQueue<EventKey> _insertionOrder = new();

    public int CacheCount => _recent.Count;

    /// <returns>The refusal if this key was stored recently, or null when only the
    /// database can decide.</returns>
    public StoreOutcome? CheckRecent(EventKey key, long eventTsMs) =>
        _recent.TryGetValue(key, out var storedTsMs)
            ? storedTsMs == eventTsMs ? StoreOutcome.Duplicate : StoreOutcome.KeyConflict
            : null;

    /// <summary>Remembers a key the database has just ruled on, whichever way. A
    /// duplicate that arrived once will arrive again: at-least-once delivery retries
    /// on a timer, not on a whim.</summary>
    public void Remember(EventKey key, long eventTsMs)
    {
        if (!_recent.TryAdd(key, eventTsMs))
        {
            return;
        }

        _insertionOrder.Enqueue(key);
        while (_insertionOrder.Count > options.DedupCacheSize && _insertionOrder.TryDequeue(out var evicted))
        {
            _recent.TryRemove(evicted, out _);
        }
    }
}
