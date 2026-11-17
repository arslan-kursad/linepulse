namespace LinePulse.Collector;

/// <summary>
/// Everything the edge gateway needs to know about its surroundings, bound from
/// the <c>Collector</c> configuration section.
/// </summary>
/// <remarks>
/// All of it is configuration and none of it is compiled in, because the same
/// binary is meant to run on a box in Izmir and a box in Nis with nothing
/// different but a file. Hardcoding an endpoint is how you end up with two
/// builds and a question about which one is deployed.
/// </remarks>
public sealed class CollectorOptions
{
    public const string Section = "Collector";

    public OpcUaOptions OpcUa { get; set; } = new();

    public MqttOptions Mqtt { get; set; } = new();

    public NatsOptions Nats { get; set; } = new();

    public SpoolOptions Spool { get; set; } = new();
}

public sealed class OpcUaOptions
{
    /// <summary>Set false when the line PLCs are not reachable, e.g. a bench-only demo.</summary>
    public bool Enabled { get; set; } = true;

    public string EndpointUrl { get; set; } = "opc.tcp://127.0.0.1:4840/linepulse/server/";

    /// <summary>Object under <c>Objects</c> that holds the sites. Browsed, never guessed.</summary>
    public string RootObjectName { get; set; } = "LinePulse";

    public string ApplicationName { get; set; } = "LinePulse Collector";

    /// <summary>
    /// Where the client's own certificate and the trust lists live. The lab
    /// accepts the simulator's self-signed certificate, so this is only here so
    /// that turning security on later is a configuration change, not a rewrite.
    /// </summary>
    public string PkiRoot { get; set; } = "data/pki";

    /// <summary>
    /// How often the server sends a notification batch. 250 ms is a compromise:
    /// fast enough that a stopped line shows up on the board before the operator
    /// looks up, slow enough that a bad day on 13 lines does not turn into tens
    /// of thousands of events a second.
    /// </summary>
    public int PublishingIntervalMs { get; set; } = 250;

    /// <summary>How often the server samples each tag. Below the fastest cycle time, well above the PLC scan.</summary>
    public int SamplingIntervalMs { get; set; } = 100;

    /// <summary>
    /// Session keep-alive. The collector treats a stalled keep-alive as a dead
    /// session and rebuilds it; this value is therefore also how long a dead
    /// switch stays invisible.
    /// </summary>
    public int KeepAliveIntervalMs { get; set; } = 5_000;

    public int SessionTimeoutMs { get; set; } = 60_000;

    /// <summary>
    /// How often a line's accumulated tag changes are turned into one
    /// <c>MachineSample</c>. Per-tag events would be mostly noise: what the
    /// plant asks about is the state of the line at a moment in time.
    /// </summary>
    public int SampleEmitIntervalMs { get; set; } = 250;

    /// <summary>
    /// How long the subscription may go without a publish response before the
    /// session is rebuilt. This catches a server that stops publishing while
    /// still answering keep-alives -- the line looks frozen on the board, and
    /// nothing else would notice.
    /// </summary>
    public int SubscriptionStallTimeoutMs { get; set; } = 15_000;
}

public sealed class MqttOptions
{
    public bool Enabled { get; set; } = true;

    public string Host { get; set; } = "127.0.0.1";

    public int Port { get; set; } = 1883;

    /// <summary>
    /// Stable client id on purpose. With a persistent session the broker keeps
    /// QoS 1 messages for exactly this id while the collector is down, so a
    /// collector restart costs latency and not test records.
    /// </summary>
    public string ClientId { get; set; } = "linepulse-collector";

    /// <summary>Wildcard subscription: every bench on every line of every site.</summary>
    public string TopicFilter { get; set; } = "linepulse/+/+/eol/test";

    public int KeepAliveSeconds { get; set; } = 30;

    /// <summary>How long the broker should hold the session open after a disconnect.</summary>
    public uint SessionExpiryIntervalSeconds { get; set; } = 3_600;
}

public sealed class NatsOptions
{
    public string Url { get; set; } = "nats://127.0.0.1:4222";

    public string ConnectionName { get; set; } = "linepulse-collector";

    /// <summary>
    /// How many spooled events are published between flushes. Larger batches
    /// drain a backlog faster; smaller batches mean less work repeated when the
    /// link dies mid-batch.
    /// </summary>
    public int BatchSize { get; set; } = 200;

    /// <summary>Pause when the spool is empty, so an idle collector is not a busy loop.</summary>
    public int IdleDelayMs { get; set; } = 100;

    /// <summary>
    /// How long to wait for the stream to acknowledge one message. Past this the
    /// batch stays on the spool and goes out again, which costs duplicates (removed
    /// by the stream's duplicate window and by the core) and never costs data.
    /// </summary>
    public int ConfirmTimeoutMs { get; set; } = 5_000;

    /// <summary>The JetStream stream the collector publishes into. The collector owns
    /// it: it is created here, so the producer never depends on a consumer having
    /// set anything up first.</summary>
    public string Stream { get; set; } = "LINEPULSE";

    /// <summary>Subjects the stream captures.</summary>
    public string Subject { get; set; } = "lp.>";

    /// <summary>
    /// How long the stream keeps a message. The stream is the buffer between the
    /// collector and a consumer that may be down, so this is how long a consumer
    /// outage can last before events start falling off the end. A day covers a
    /// weekend-night stoppage of the KPI service on one site; sizing it for a real
    /// plant is a storage question, not a design one.
    /// </summary>
    public int StreamMaxAgeHours { get; set; } = 24;

    /// <summary>
    /// Window in which the stream refuses a message whose Nats-Msg-Id it has already
    /// stored. The id is "source#seq", the same key the core deduplicates on, so this
    /// is a third layer on the same rule rather than a different rule.
    /// </summary>
    public int DuplicateWindowMinutes { get; set; } = 10;
}

public sealed class SpoolOptions
{
    /// <summary>
    /// SQLite file holding everything not yet forwarded. Relative to the working
    /// directory so a dev run and a deployed run can both be read with sqlite3.
    /// </summary>
    public string Path { get; set; } = "data/spool.db";

    /// <summary>How often the spool depth and the counters are logged.</summary>
    public int ReportIntervalSeconds { get; set; } = 10;

    /// <summary>How often expired deduplication keys are dropped.</summary>
    public int PruneIntervalSeconds { get; set; } = 300;
}

/// <summary>Bounds for the shared exponential backoff.</summary>
public static class BackoffDefaults
{
    public static readonly TimeSpan Min = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Capped at half a minute: a reconnect loop that backs off to minutes
    /// means a switch that came back at 09:00 is still not collecting at 09:05,
    /// and nobody on a shop floor accepts that.
    /// </summary>
    public static readonly TimeSpan Max = TimeSpan.FromSeconds(30);
}
