namespace LinePulse.Core;

/// <summary>Where the event bus lives and what we listen to.</summary>
public sealed class NatsOptions
{
    public const string Section = "Nats";

    public string Url { get; set; } = "nats://127.0.0.1:4222";

    /// <summary>Wildcard subject. One subscription for the whole plant keeps the
    /// consumer count flat as lines are added -- adding line 14 must not mean a
    /// code change and a redeploy.</summary>
    public string Subject { get; set; } = "lp.>";

    public string ClientName { get; set; } = "linepulse-core";

    /// <summary>Seconds to wait before re-attaching to the stream. The stream holds events
    /// while we are away, so being slow costs latency and never data; one second keeps the
    /// catch-up after a broker restart short without hammering a server that is really down.
    /// (At five seconds, events produced just after an outage waited five seconds for no
    /// reason, which showed up as a seven-second 99th-percentile latency.)</summary>
    public int ReconnectSeconds { get; set; } = 5;

    /// <summary>Name of the JetStream stream the collector publishes into.</summary>
    public string Stream { get; set; } = "LINEPULSE";

    /// <summary>
    /// Durable consumer name. Durable means the server remembers how far this consumer
    /// has read: stop the service for an hour and it resumes at the first event it did
    /// not acknowledge, instead of at "now". That is the whole difference from a plain
    /// subscription, which keeps nothing for a subscriber that is not there.
    /// </summary>
    public string Durable { get; set; } = "linepulse-core";
}

/// <summary>
/// Which database the service talks to.
/// </summary>
/// <remarks>
/// The provider is a config switch rather than a compile-time choice because the
/// lab runs on SQLite (no server, no container, survives a laptop) while the plant
/// runs MSSQL. Every statement lives behind <c>IEventStore</c> / <c>IKpiStore</c>,
/// so swapping the provider is an implementation of two interfaces and not a
/// rewrite of the ingest path.
/// </remarks>
public sealed class StorageOptions
{
    public const string Section = "Storage";

    /// <summary>"Sqlite" or "SqlServer".</summary>
    public string Provider { get; set; } = "Sqlite";

    public string SqlitePath { get; set; } = "data/linepulse.db";

    public string SqlServerConnectionString { get; set; } = "";
}

/// <summary>Knobs for the part of the system the demo is actually about.</summary>
public sealed class IngestOptions
{
    public const string Section = "Ingest";

    /// <summary>
    /// How late an event may be and still be treated as an ordinary correction.
    /// Beyond this its minute is considered published, so a change to it is a
    /// revision somebody may have to explain at the morning meeting -- which is
    /// why it is counted separately instead of being dropped.
    /// </summary>
    public int WatermarkSeconds { get; set; } = 30;

    /// <summary>Unplanned stops shorter than this are micro-stops: real losses that
    /// never reach a shift report because nobody writes down a 40-second jam.
    /// Matches MICRO_STOP_THRESHOLD_S in sim/plant.py.</summary>
    public int MicroStopThresholdSeconds { get; set; } = 120;

    /// <summary>Hot-path duplicate cache size. Purely a cost optimisation: the
    /// primary key of the raw tables is what actually guarantees idempotency.</summary>
    public int DedupCacheSize { get; set; } = 50_000;

    /// <summary>How often dirty minute buckets are recomputed. Coalescing matters:
    /// 13 lines sampling every second would otherwise recompute the same minute
    /// 780 times for no new information.</summary>
    public int BucketFlushMs { get; set; } = 500;

    /// <summary>
    /// How long a line's last reported state is trusted without a new sample.
    /// </summary>
    /// <remarks>
    /// An OPC-UA subscription reports by exception: a line that has been faulted
    /// for twenty minutes sends nothing, because nothing changed. Carrying the last
    /// state forward is therefore what a silent minute means while the session is
    /// alive. Core cannot see the session, though -- a collector outage looks the
    /// same -- so the carry is bounded, and time beyond the bound is left as "no
    /// data" rather than billed as running or down. One hour covers every stop the
    /// simulator produces at any speed. Measured in event-time seconds.
    /// </remarks>
    public int MaxStateCarrySeconds { get; set; } = 3600;

    /// <summary>
    /// Production seconds per event-time second. 1 in a plant.
    /// </summary>
    /// <remarks>
    /// Lab only. The simulator compresses a shift by running its machines
    /// <c>LP_SPEED</c> times faster than the wall clock, while every timestamp it
    /// emits is ordinary wall-clock time. Durations measured from those timestamps
    /// are therefore compressed too, and a 300-second seal fault arrives as 2.5
    /// seconds. Setting this to the simulator's speed converts measured durations
    /// back into production seconds, so the two-minute micro-stop rule and the
    /// performance factor (which compares against the PLC's ideal cycle time, a
    /// production-time figure) keep their meaning. Timestamps, the watermark and
    /// the shift calendar stay in wall-clock time: they are about when things
    /// happened, not how long they took.
    /// </remarks>
    public double TimeScale { get; set; } = 1.0;
}

/// <summary>Statistical process control on the leak bench.</summary>
public sealed class SpcOptions
{
    public const string Section = "Spc";

    /// <summary>Rolling window in parts. Short enough to react to a seal that is
    /// going off, long enough that sigma is not noise.</summary>
    public int WindowSize { get; set; } = 200;

    /// <summary>Below this we report "insufficient data" rather than a status, because
    /// a sigma from eight parts is a guess and a guess that stops a line is worse
    /// than no alarm at all.</summary>
    public int MinSamples { get; set; } = 25;

    /// <summary>One-sided capability index thresholds. 1.33 is the usual automotive
    /// customer requirement; below 1.00 the 3-sigma band has reached the limit.</summary>
    public double WarnCpk { get; set; } = 1.33;

    public double AlarmCpk { get; set; } = 1.00;

    /// <summary>If the fitted trend puts the 3-sigma band over the limit within this
    /// many parts, warn now.</summary>
    public int ProjectionHorizonParts { get; set; } = 1000;

    /// <summary>
    /// Warn when a fixture reads this many sigma above its sister fixtures on the
    /// same bench. This is the early-warning path: a seal that is going off makes
    /// one fixture read high while the others, testing the same pumps, do not --
    /// and it shows long before any part fails.
    /// </summary>
    public double PeerShiftWarnSigma { get; set; } = 1.0;

    /// <summary>The shift must also be statistically real, not a small-sample
    /// wobble: a z-score at least this large on the difference of the two means.</summary>
    public double PeerShiftMinZ { get; set; } = 3.0;
}

/// <summary>
/// Shift calendar. Two sites in two time zones is not a detail: a shift report
/// that silently uses the server's clock is wrong at one of the two plants, and
/// one that uses a fixed UTC offset is wrong at Nis for half of every year.
/// </summary>
public sealed class ShiftOptions
{
    public const string Section = "Shifts";

    /// <summary>Local shift start hours. Three eight-hour shifts.</summary>
    /// <remarks>
    /// No default in code, on purpose. The configuration binder appends a configured
    /// array to a default one instead of replacing it, so a default of [6, 14, 22]
    /// plus the same values in appsettings.json becomes [6, 14, 22, 6, 14, 22] and
    /// every shift label after the first lands on the wrong shift. The values live
    /// in appsettings.json, where an operator can see and change them.
    /// </remarks>
    public int[] StartHours { get; set; } = [];

    public string[] Labels { get; set; } = [];

    /// <summary>IANA time zone per site. Turkey has stayed on UTC+3 all year since
    /// 2016; Serbia moves between UTC+1 and UTC+2, so only a real zone gets the
    /// Nis shift boundaries right in summer.</summary>
    public Dictionary<string, string> SiteTimeZones { get; set; } = new()
    {
        ["IZM"] = "Europe/Istanbul",
        ["NIS"] = "Europe/Belgrade",
    };

    public string DefaultTimeZone { get; set; } = "UTC";
}

/// <summary>Server-sent events feed.</summary>
public sealed class LiveOptions
{
    public const string Section = "Live";

    /// <summary>How often the KPI tick is pushed. The tick also keeps an idle stream
    /// warm, which stops a proxy from deciding the connection is dead and leaving a
    /// frozen board on the wall.</summary>
    public int KpiTickSeconds { get; set; } = 5;

    public int ClientQueueCapacity { get; set; } = 64;
}
