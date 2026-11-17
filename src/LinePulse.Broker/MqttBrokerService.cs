using System.Collections.Concurrent;
using Microsoft.Extensions.Options;
using MQTTnet.Server;

namespace LinePulse.Broker;

/// <summary>
/// The lab's MQTT broker: an in-process MQTTnet server on port 1883.
/// </summary>
/// <remarks>
/// <para>
/// A plant broker is infrastructure somebody else runs -- Mosquitto, HiveMQ,
/// EMQX, or the broker built into the historian. This process exists so the
/// whole lab starts from one checkout with no external service and no
/// container runtime, which is what makes the chaos tests (kill the broker,
/// kill the uplink) cheap enough to run on every demo.
/// </para>
/// <para>
/// If <c>/usr/local/bin/mosquitto</c> is installed it is an alternative, not a
/// replacement: point the bench simulator and the collector at it instead and
/// nothing else changes, because both only speak MQTT over TCP. Keeping this
/// broker in the solution is what guarantees that stays true.
/// </para>
/// </remarks>
public sealed class MqttBrokerService : BackgroundService
{
    private readonly BrokerOptions _options;
    private readonly ILogger<MqttBrokerService> _log;

    /// <summary>
    /// Inbound message counts per topic. A broker that cannot tell you which
    /// topic is loud is useless during a flood: when a bench starts stuttering
    /// and republishing, this is the first place to look.
    /// </summary>
    private readonly ConcurrentDictionary<string, TopicCounter> _topics = new(StringComparer.Ordinal);

    private long _connects;
    private long _disconnects;

    public MqttBrokerService(IOptions<BrokerOptions> options, ILogger<MqttBrokerService> log)
    {
        _options = options.Value;
        _log = log;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new MqttServerFactory();
        var serverOptions = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(_options.Port)
            .WithDefaultEndpointBoundIPAddress(System.Net.IPAddress.Parse(_options.BindAddress))
            // Without this a restarted broker can fail to bind for as long as
            // the OS holds the old socket in TIME_WAIT, which turns a two-second
            // restart into a minute of missing test records.
            .WithDefaultEndpointReuseAddress()
            .WithPersistentSessions(_options.PersistentSessions)
            .WithMaxPendingMessagesPerClient(_options.MaxPendingMessagesPerClient)
            .WithPendingMessagesOverflowStrategy(MqttPendingMessagesOverflowStrategy.DropOldestQueuedMessage)
            .Build();

        using var server = factory.CreateMqttServer(serverOptions);

        server.ClientConnectedAsync += OnClientConnectedAsync;
        server.ClientDisconnectedAsync += OnClientDisconnectedAsync;
        server.ClientSubscribedTopicAsync += OnClientSubscribedAsync;
        server.InterceptingPublishAsync += OnPublishAsync;

        await server.StartAsync();
        _log.LogInformation(
            "MQTT broker listening on {Address}:{Port} (persistentSessions={Persistent})",
            _options.BindAddress,
            _options.Port,
            _options.PersistentSessions);

        try
        {
            await ReportLoopAsync(stoppingToken);
        }
        finally
        {
            // Tell connected clients the broker is going away rather than
            // dropping the TCP connection silently: a bench that gets a proper
            // DISCONNECT reconnects immediately instead of waiting for its
            // keep-alive to expire.
            await server.StopAsync(new MqttServerStopOptionsBuilder().Build());
            _log.LogInformation(
                "MQTT broker stopped; connects={Connects} disconnects={Disconnects} totalMessages={Messages}",
                Interlocked.Read(ref _connects),
                Interlocked.Read(ref _disconnects),
                _topics.Values.Sum(t => t.Total));
        }
    }

    private async Task ReportLoopAsync(CancellationToken ct)
    {
        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.ReportIntervalSeconds));
        using var timer = new PeriodicTimer(interval);

        while (await timer.WaitForNextTickAsync(ct))
        {
            foreach (var (topic, counter) in _topics)
            {
                var window = counter.ReadAndReset();
                if (window.Messages == 0)
                {
                    continue;
                }

                _log.LogInformation(
                    "topic {Topic}: {Delta} msg / {Bytes} B in {Window:0}s (total {Total})",
                    topic,
                    window.Messages,
                    window.Bytes,
                    interval.TotalSeconds,
                    window.Total);
            }
        }
    }

    private Task OnClientConnectedAsync(ClientConnectedEventArgs e)
    {
        Interlocked.Increment(ref _connects);
        _log.LogInformation("client connected: {ClientId} from {Endpoint}", e.ClientId, e.RemoteEndPoint);
        return Task.CompletedTask;
    }

    private Task OnClientDisconnectedAsync(ClientDisconnectedEventArgs e)
    {
        Interlocked.Increment(ref _disconnects);
        // The disconnect type is the interesting part. A clean disconnect is a
        // bench shutting down; NotClean means the network or the process died,
        // which is what the collector has to survive.
        _log.LogInformation(
            "client disconnected: {ClientId} type={Type} reason={Reason}",
            e.ClientId,
            e.DisconnectType,
            e.ReasonCode);
        return Task.CompletedTask;
    }

    private Task OnClientSubscribedAsync(ClientSubscribedTopicEventArgs e)
    {
        _log.LogInformation(
            "client {ClientId} subscribed {Topic} qos={Qos}",
            e.ClientId,
            e.TopicFilter.Topic,
            e.TopicFilter.QualityOfServiceLevel);
        return Task.CompletedTask;
    }

    private Task OnPublishAsync(InterceptingPublishEventArgs e)
    {
        _topics.GetOrAdd(e.ApplicationMessage.Topic, static _ => new TopicCounter())
            .Add(e.ApplicationMessage.Payload.Length);
        return Task.CompletedTask;
    }

    /// <summary>Message and byte counts for one topic, since the last report and in total.</summary>
    private sealed class TopicCounter
    {
        private long _windowMessages;
        private long _windowBytes;
        private long _total;

        public long Total => Interlocked.Read(ref _total);

        public void Add(long bytes)
        {
            Interlocked.Increment(ref _windowMessages);
            Interlocked.Add(ref _windowBytes, bytes);
            Interlocked.Increment(ref _total);
        }

        public (long Messages, long Bytes, long Total) ReadAndReset() =>
            (Interlocked.Exchange(ref _windowMessages, 0),
             Interlocked.Exchange(ref _windowBytes, 0),
             Interlocked.Read(ref _total));
    }
}
