using System.Text.Json;
using System.Text.Json.Serialization;

namespace LinePulse.Contracts;

/// <summary>Line state as the PLC reports it.</summary>
public enum LineState
{
    Idle = 0,
    Running = 1,
    Blocked = 2,
    Faulted = 3,
    Changeover = 4,
}

/// <summary>
/// Identity of a single event, and the only thing the ingest side trusts for
/// deduplication.
/// </summary>
/// <remarks>
/// The key is <c>(Source, Seq)</c>, deliberately not <see cref="EventId"/>.
/// A test bench that restarts mid-shift will mint a fresh <see cref="EventId"/>
/// for a record it already published, so an EventId-only check would let the
/// duplicate through. The sequence number is the bench's own counter and stays
/// stable for a given part. <see cref="EventId"/> is kept because it is useful
/// when tracing one physical message through the logs.
/// </remarks>
public readonly record struct EventKey(string Source, long Seq)
{
    public override string ToString() => $"{Source}#{Seq}";
}

/// <summary>Fields every event carries, whichever protocol it arrived on.</summary>
public abstract record EventEnvelope
{
    public Guid EventId { get; init; } = Guid.NewGuid();

    /// <summary>Who produced this, e.g. <c>bench:IZM/L01</c> or <c>opcua:IZM/L01</c>.</summary>
    public string Source { get; init; } = "";

    /// <summary>Producer-side monotonic counter. Half of the idempotency key.</summary>
    public long Seq { get; init; }

    public string Site { get; init; } = "";

    public string Line { get; init; } = "";

    /// <summary>
    /// When the event happened, stamped by the producer. Every KPI buckets on
    /// this and never on arrival time -- which is what makes a late record
    /// correctable rather than simply wrong.
    /// </summary>
    [JsonPropertyName("ts")]
    public DateTimeOffset EventTimeUtc { get; init; }

    /// <summary>When the pipeline first saw it. Set by the collector, not the producer.</summary>
    public DateTimeOffset? IngestTimeUtc { get; init; }

    [JsonIgnore]
    public EventKey Key => new(Source, Seq);

    [JsonIgnore]
    public string LineKey => $"{Site}/{Line}";
}

/// <summary>
/// One observation of a line's machine tags, read from OPC-UA.
/// </summary>
/// <remarks>
/// Counters are reported as the PLC holds them: monotonic and arbitrary at
/// start. Downstream code must work in deltas, because a PLC restart or a
/// collector restart can both make the absolute value meaningless.
/// </remarks>
public sealed record MachineSample : EventEnvelope
{
    public LineState State { get; init; }

    public string StateText { get; init; } = "";

    public ulong GoodCount { get; init; }

    public ulong ScrapCount { get; init; }

    public double CycleTimeMs { get; init; }

    public double IdealCycleMs { get; init; }

    public int AlarmCode { get; init; }

    public string AlarmText { get; init; } = "";

    public string ProgramId { get; init; } = "";

    /// <summary>Machining centre only; null on assembly lines.</summary>
    public double? SpindleLoadPct { get; init; }

    public string? ToolId { get; init; }

    public double? ToolLifeRemainingPct { get; init; }

    /// <summary>Housing lot currently being produced. The genealogy link.</summary>
    public string? CurrentLotId { get; init; }
}

/// <summary>Measured values from the end-of-line benches for one part.</summary>
public sealed record TestMeasurements
{
    public double LeakRateMbarLs { get; init; }

    public double LeakLimitMbarLs { get; init; }

    public double FlowLpm { get; init; }

    public double FlowTargetLpm { get; init; }

    public double PressureBar { get; init; }

    public double[] TorqueNm { get; init; } = [];
}

/// <summary>
/// One part's end-of-line test result, published by a bench over MQTT.
/// </summary>
public sealed record TestRecord : EventEnvelope
{
    public string Station { get; init; } = "";

    /// <summary>Data-matrix serial lasered onto the part. The traceability anchor.</summary>
    public string Serial { get; init; } = "";

    public string ProgramId { get; init; } = "";

    /// <summary>
    /// Housing lot the part was built from, so a tool problem at the machining
    /// centre can be traced forward to the finished pumps that received those
    /// housings. This is the join that makes a warranty claim answerable.
    /// </summary>
    public string HousingLotId { get; init; } = "";

    public string FixtureId { get; init; } = "";

    public string Result { get; init; } = "";

    public string[] FailedChecks { get; init; } = [];

    public TestMeasurements Measurements { get; init; } = new();

    public string OperatorId { get; init; } = "";

    [JsonIgnore]
    public bool Passed => string.Equals(Result, "PASS", StringComparison.OrdinalIgnoreCase);
}

/// <summary>NATS subject layout, kept in one place so publisher and consumer cannot drift.</summary>
public static class Subjects
{
    public const string Prefix = "lp";

    public static string Machine(string site, string line) => $"{Prefix}.{site}.{line}.machine";

    public static string Test(string site, string line) => $"{Prefix}.{site}.{line}.test";

    /// <summary>Everything, for the ingest service.</summary>
    public const string All = $"{Prefix}.>";

    public const string AllMachine = $"{Prefix}.*.*.machine";

    public const string AllTest = $"{Prefix}.*.*.test";
}

/// <summary>
/// Serialisation settings shared by every process.
/// </summary>
/// <remarks>
/// camelCase because the producers are Python simulators standing in for real
/// benches, and real plant equipment emits whatever it emits. Being strict here
/// and lenient about unknown members is the right way round: added fields from
/// a firmware update must not break ingest.
/// </remarks>
public static class JsonDefaults
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        NumberHandling = JsonNumberHandling.AllowReadingFromString,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new JsonStringEnumConverter() },
    };
}
