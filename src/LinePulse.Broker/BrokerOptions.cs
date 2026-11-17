namespace LinePulse.Broker;

/// <summary>Settings for the lab broker, bound from the <c>Broker</c> configuration section.</summary>
public sealed class BrokerOptions
{
    public const string Section = "Broker";

    /// <summary>TCP port. 1883 is the plain-MQTT default every bench vendor ships with.</summary>
    public int Port { get; set; } = 1883;

    /// <summary>
    /// Bind address. Loopback by default: this broker is a lab stand-in, and a
    /// broker that listens on every interface by accident is how an unsecured
    /// MQTT port ends up on a plant network.
    /// </summary>
    public string BindAddress { get; set; } = "127.0.0.1";

    /// <summary>
    /// Keep sessions when a client disconnects, so a QoS 1 subscriber that
    /// drops out gets its queued messages on reconnect instead of a gap. The
    /// collector relies on this: it is the broker-side half of at-least-once.
    /// </summary>
    public bool PersistentSessions { get; set; } = true;

    /// <summary>
    /// Queue limit per client. A bounded queue is a deliberate choice: when a
    /// subscriber falls far enough behind, dropping oldest and saying so beats
    /// growing until the broker is killed by the OS.
    /// </summary>
    public int MaxPendingMessagesPerClient { get; set; } = 10_000;

    /// <summary>How often the per-topic traffic report is written.</summary>
    public int ReportIntervalSeconds { get; set; } = 10;
}
