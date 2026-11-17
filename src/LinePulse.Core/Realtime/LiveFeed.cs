using System.Collections.Concurrent;
using System.Threading.Channels;

namespace LinePulse.Core.Realtime;

/// <summary>One server-sent event: an SSE event name and its JSON payload.</summary>
public sealed record LiveEvent(string Name, object Payload);

/// <summary>
/// Fan-out for GET /api/events/stream.
/// </summary>
/// <remarks>
/// Each connected dashboard gets its own bounded queue, and a full queue drops its
/// oldest entry instead of blocking the publisher. That direction matters: a laptop
/// on the plant's guest wifi reading the board must never be able to slow down
/// ingest, and for a live KPI tick the newest value is the only one worth having.
/// </remarks>
public sealed class LiveFeed(LiveOptions options, ILogger<LiveFeed> log)
{
    private readonly ConcurrentDictionary<Guid, Channel<LiveEvent>> _clients = new();

    public int ClientCount => _clients.Count;

    public void Publish(string name, object payload)
    {
        if (_clients.IsEmpty)
        {
            return;
        }

        var message = new LiveEvent(name, payload);
        foreach (var channel in _clients.Values)
        {
            // BoundedChannelFullMode.DropOldest makes this always succeed.
            channel.Writer.TryWrite(message);
        }
    }

    public async IAsyncEnumerable<LiveEvent> SubscribeAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateBounded<LiveEvent>(new BoundedChannelOptions(options.ClientQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        _clients[id] = channel;
        log.LogDebug("sse client {Id} attached ({Count} total)", id, _clients.Count);

        try
        {
            await foreach (var message in channel.Reader.ReadAllAsync(ct).ConfigureAwait(false))
            {
                yield return message;
            }
        }
        finally
        {
            _clients.TryRemove(id, out _);
            log.LogDebug("sse client {Id} detached ({Count} left)", id, _clients.Count);
        }
    }
}
