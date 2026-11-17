using Opc.Ua;
using Opc.Ua.Client.Subscriptions;
using Opc.Ua.Client.Subscriptions.MonitoredItems;
using ISession = Opc.Ua.Client.ISession;

namespace LinePulse.Collector;

/// <summary>
/// The one OPC-UA subscription that feeds every line's <see cref="LineTagBuffer"/>,
/// built on the stack's managed (V2) subscription model.
/// </summary>
/// <remarks>
/// <para>
/// One subscription for the whole site rather than one per line: the server's
/// publish cycle is the unit of cost, and thirteen lines of tags fit in one
/// comfortably. If a server caps monitored items per subscription, the V2
/// engine splits the items across server-side subscriptions on its own.
/// </para>
/// <para>
/// The handler keeps the values it is handed after the callback returns, which
/// is why notification pooling stays at its default of off: pooled values would
/// be recycled while the buffers still hold them.
/// </para>
/// </remarks>
internal sealed class MachineSubscription : ISubscriptionNotificationHandler, IAsyncDisposable
{
    private const uint KeepAliveCount = 10;

    /// <summary>Publishing cycles that cover the session timeout with a quarter to spare.</summary>
    internal static uint LifetimeCountFor(OpcUaOptions options)
    {
        var cycles = Math.Ceiling(options.SessionTimeoutMs * 1.25 / Math.Max(1, options.PublishingIntervalMs));
        return (uint)Math.Max(3 * KeepAliveCount, cycles);
    }

    private static readonly TimeSpan CreateTimeout = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan DisposeTimeout = TimeSpan.FromSeconds(2);

    private readonly Dictionary<string, TagRoute> _routes;
    private readonly ILogger _log;
    private ISubscription? _subscription;
    private long _badQualityUpdates;
    private long _stalledSinceTicks;
    private volatile string? _lostReason;
    private volatile bool _disposing;

    private MachineSubscription(Dictionary<string, TagRoute> routes, ILogger log)
    {
        _routes = routes;
        _log = log;
    }

    /// <summary>Where one monitored item's values go.</summary>
    private readonly record struct TagRoute(LineTagBuffer Buffer, string Tag, NodeId NodeId);

    /// <summary>
    /// Subscribes to every discovered tag and waits until the server has
    /// accepted the subscription and its items.
    /// </summary>
    public static async Task<MachineSubscription> CreateAsync(
        ISession session,
        IReadOnlyList<DiscoveredLine> lines,
        IReadOnlyDictionary<string, LineTagBuffer> buffers,
        OpcUaOptions options,
        ILogger log,
        CancellationToken ct)
    {
        if (!session.TryGetSubscriptionManager(out var manager))
        {
            throw new InvalidOperationException(
                "OPC-UA session has no V2 subscription manager; it must be created with the V2 subscription engine");
        }

        var routes = new Dictionary<string, TagRoute>(StringComparer.Ordinal);
        foreach (var line in lines)
        {
            foreach (var (tag, nodeId) in line.Tags)
            {
                routes[$"{line.Key}/{tag}"] = new TagRoute(buffers[line.Key], tag, nodeId);
            }
        }

        var handler = new MachineSubscription(routes, log);
        var subscription = manager.Add(handler, new OptionsMonitor<SubscriptionOptions>(new SubscriptionOptions
        {
            PublishingEnabled = true,
            PublishingInterval = TimeSpan.FromMilliseconds(options.PublishingIntervalMs),
            // Keep-alive and lifetime in publishing cycles. A server that stops
            // publishing while the session itself stays up is a different
            // failure from the socket dying, and the keep-alive count is how
            // quickly the engine notices it.
            KeepAliveCount = KeepAliveCount,
            // The lifetime is how long the server keeps the subscription alive
            // without hearing from us. It must outlast the session timeout:
            // otherwise a link that drops for longer than the lifetime but shorter
            // than the session timeout leaves a session that survives with its
            // subscription deleted, and every reconnect has to rebuild it. (At
            // 30 cycles it was 7.5 s against a 60 s session, and the stack warned
            // about it on every connect.) The spec also wants at least three
            // keep-alives inside one lifetime.
            LifetimeCount = LifetimeCountFor(options),
            MaxNotificationsPerPublish = 0,
        }));
        handler._subscription = subscription;

        foreach (var (name, route) in routes)
        {
            var itemOptions = new MonitoredItemOptions
            {
                StartNodeId = route.NodeId,
                AttributeId = Attributes.Value,
                MonitoringMode = MonitoringMode.Reporting,
                SamplingInterval = TimeSpan.FromMilliseconds(options.SamplingIntervalMs),
                // Source and server timestamps both: the source timestamp is the
                // event time every KPI buckets on, the server one is the fallback
                // for devices that do not stamp their own values.
                TimestampsToReturn = TimestampsToReturn.Both,
                // A queue rather than the latest value, so a stop that begins
                // and ends between two publishes arrives as two changes instead
                // of being overwritten by the restart. When the queue does
                // overflow, the newest values win: the line's present state
                // matters more than an old intermediate one, and the counters
                // are monotonic, so the newest value already contains every part.
                QueueSize = 10,
                DiscardOldest = true,
            };

            if (!subscription.MonitoredItems.TryAdd(name, new OptionsMonitor<MonitoredItemOptions>(itemOptions), out _))
            {
                log.LogWarning("monitored item {Item} could not be added", name);
            }
        }

        try
        {
            await handler.WaitUntilCreatedAsync(ct);
        }
        catch
        {
            await handler.DisposeAsync();
            throw;
        }

        return handler;
    }

    /// <summary>Tag updates refused on quality since the last call.</summary>
    public long TakeBadQualityCount() => Interlocked.Exchange(ref _badQualityUpdates, 0);

    /// <summary>
    /// Why the subscription can no longer be trusted, or <c>null</c> while it is
    /// healthy. A non-null answer means the session should be rebuilt.
    /// </summary>
    public string? FailureReason(TimeSpan stallTimeout)
    {
        if (_lostReason is { } lost)
        {
            return lost;
        }

        var stalledSince = Interlocked.Read(ref _stalledSinceTicks);
        if (stalledSince != 0 && DateTime.UtcNow.Ticks - stalledSince > stallTimeout.Ticks)
        {
            return $"server stopped publishing for more than {stallTimeout.TotalSeconds:0}s";
        }

        return null;
    }

    public ValueTask OnDataChangeNotificationAsync(
        ISubscription subscription,
        uint sequenceNumber,
        DateTime publishTime,
        ReadOnlyMemory<DataValueChange> notification,
        PublishState publishStateMask,
        IReadOnlyList<string> stringTable)
    {
        MarkAlive();

        // Replayed in the order things happened on the line, not message order.
        // A notification lists each item's queued values together, so a stop
        // and its restart on State come before the alarm code that explains
        // the stop. Sorting by producer time puts every tag back into the order
        // it actually changed, which is what lets the buffer give each stop its
        // own sample with the right alarm on it. The sort is stable, so two
        // writes in the same instant keep the server's order.
        foreach (var change in notification.ToArray().OrderBy(c => ProducerTicks(c.Value)))
        {
            if (change.MonitoredItem is not { } item || !_routes.TryGetValue(item.Name, out var route))
            {
                continue;
            }

            if (!route.Buffer.Apply(route.Tag, change.Value))
            {
                Interlocked.Increment(ref _badQualityUpdates);
            }
        }

        return ValueTask.CompletedTask;
    }

    public ValueTask OnEventDataNotificationAsync(
        ISubscription subscription,
        uint sequenceNumber,
        DateTime publishTime,
        ReadOnlyMemory<EventNotification> notification,
        PublishState publishStateMask,
        IReadOnlyList<string> stringTable) => ValueTask.CompletedTask;

    public ValueTask OnKeepAliveNotificationAsync(
        ISubscription subscription,
        uint sequenceNumber,
        DateTime publishTime,
        PublishState publishStateMask)
    {
        MarkAlive();
        return ValueTask.CompletedTask;
    }

    public ValueTask OnSubscriptionStateChangedAsync(
        ISubscription subscription,
        SubscriptionState state,
        PublishState publishStateMask,
        CancellationToken ct = default)
    {
        if (publishStateMask.HasFlag(PublishState.Stopped))
        {
            Interlocked.CompareExchange(ref _stalledSinceTicks, DateTime.UtcNow.Ticks, 0);
            _log.LogWarning("OPC-UA subscription stopped receiving publish responses");
        }

        if (publishStateMask.HasFlag(PublishState.Recovered))
        {
            MarkAlive();
            _log.LogInformation("OPC-UA subscription publishing recovered");
        }

        if (publishStateMask.HasFlag(PublishState.Republish))
        {
            // A gap in the sequence numbers, and the engine has asked the
            // server for the missing message. Worth knowing about: it is the
            // difference between "a value was late" and "a value was lost".
            _log.LogInformation("OPC-UA notification gap detected; republish requested");
        }

        if (_disposing)
        {
            return ValueTask.CompletedTask;
        }

        if (publishStateMask.HasFlag(PublishState.Timeout))
        {
            _lostReason = "subscription timed out on the server";
        }
        else if (state is SubscriptionState.Deleted or SubscriptionState.Error)
        {
            _lostReason = $"subscription entered state {state}";
        }

        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _disposing = true;
        if (_subscription is null)
        {
            return;
        }

        try
        {
            // Bounded, because during an outage this tries to delete the
            // subscription on a server that is not there, and shutting down or
            // reconnecting must not wait on that.
            await _subscription.DisposeAsync().AsTask().WaitAsync(DisposeTimeout);
        }
        catch (Exception ex)
        {
            _log.LogDebug(ex, "disposing the OPC-UA subscription failed");
        }
    }

    /// <summary>
    /// Waits until the server has created the subscription and has accepted or
    /// refused every item, then says which ones it refused.
    /// </summary>
    private async Task WaitUntilCreatedAsync(CancellationToken ct)
    {
        var subscription = _subscription!;
        var deadline = DateTime.UtcNow + CreateTimeout;

        while (!subscription.Created || subscription.MonitoredItems.Items.Any(i => !IsSettled(i)))
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(
                    $"OPC-UA subscription not created within {CreateTimeout.TotalSeconds:0}s");
            }

            await Task.Delay(100, ct);
        }

        var items = subscription.MonitoredItems.Items.ToList();
        var refused = items.Where(i => !i.Created).ToList();
        foreach (var item in refused)
        {
            _log.LogWarning("server refused monitored item {Item}: {Error}", item.Name, item.Error);
        }

        _log.LogInformation(
            "subscription created: {Items} monitored items ({Refused} refused), publishing {Publishing} ms",
            items.Count - refused.Count,
            refused.Count,
            subscription.CurrentPublishingInterval.TotalMilliseconds);
    }

    private static bool IsSettled(IMonitoredItem item) =>
        item.Created || (item.Error is { } error && ServiceResult.IsBad(error));

    private void MarkAlive() => Interlocked.Exchange(ref _stalledSinceTicks, 0);

    private static long ProducerTicks(DataValue value)
    {
        if (!value.SourceTimestamp.IsNull)
        {
            return value.SourceTimestamp.ToDateTime().Ticks;
        }

        // Values without any timestamp are stamped "now" by the buffer, so they
        // sort after everything the device did stamp.
        return value.ServerTimestamp.IsNull ? long.MaxValue : value.ServerTimestamp.ToDateTime().Ticks;
    }
}
