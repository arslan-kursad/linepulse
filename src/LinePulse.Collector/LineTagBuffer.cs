using LinePulse.Contracts;
using Opc.Ua;

namespace LinePulse.Collector;

/// <summary>One emitted sample plus what the counters did since the previous one.</summary>
internal readonly record struct SampleEmission(
    MachineSample Sample,
    long GoodDelta,
    long ScrapDelta,
    bool CounterRestart);

/// <summary>What one line did since the previous periodic report.</summary>
internal readonly record struct LineReport(
    LineState State,
    long Good,
    long Scrap,
    long StateChanges);

/// <summary>
/// The collector's shadow copy of one line's tags, and the only place a
/// <see cref="MachineSample"/> is assembled.
/// </summary>
/// <remarks>
/// <para>
/// OPC-UA delivers one notification per tag. Turning each of those into an
/// event would mean thirteen lines times a dozen tags of mostly unchanged
/// values, and a consumer that has to reassemble the line's state anyway.
/// So changes land here and the source emits about one sample per line per
/// publishing cycle -- the state of the line at a moment in time, which is
/// what the question "why did L01 stop at 14:12" is actually about.
/// </para>
/// <para>
/// A change of line state is the one thing that is never merged. An operator
/// clears a rotor feeder jam in seconds, so a stop can begin and end inside a
/// single publishing interval; folding it into the next sample would report a
/// line that never stopped, and the micro-stop would vanish from exactly the
/// view that exists to find it. A sample that carries a state nobody has been
/// told about is therefore sealed the moment the next state arrives, and goes
/// out as its own event with its own timestamp.
/// </para>
/// <para>
/// Counters are the delicate part. The PLC reports them monotonic and
/// arbitrary at start, so production is the delta between two samples and
/// nothing else. When a counter jumps backwards the device restarted: the
/// baseline is re-seeded and the delta for that sample is zero, because
/// "minus four thousand parts" would otherwise travel all the way into an OEE
/// figure and make the shift look impossible.
/// </para>
/// </remarks>
internal sealed class LineTagBuffer
{
    private readonly object _gate = new();
    private readonly Dictionary<string, DataValue> _values = new(StringComparer.Ordinal);
    private readonly List<SampleEmission> _sealed = [];

    private bool _dirty;
    private DateTimeOffset _eventTime;
    private long _seq;
    private ulong? _lastGood;
    private ulong? _lastScrap;
    private LineState? _lastBuiltState;
    private long _goodSinceReport;
    private long _scrapSinceReport;
    private long _stateChangesSinceReport;

    public LineTagBuffer(string site, string line, long seqSeed)
    {
        Site = site;
        Line = line;
        _seq = seqSeed;
    }

    public string Site { get; }

    public string Line { get; }

    public string Key => $"{Site}/{Line}";

    /// <summary>
    /// Producer identity for the idempotency key. The collector is the producer
    /// of machine samples -- the PLC has no event identity of its own -- so the
    /// prefix records which protocol the data came in on.
    /// </summary>
    public string SourceId => $"opcua:{Site}/{Line}";

    /// <summary>
    /// Applies one tag change. Changes must arrive in the order they happened
    /// on the line, which is the subscription handler's job.
    /// </summary>
    /// <returns><c>false</c> when the value was rejected on quality grounds.</returns>
    public bool Apply(string tag, DataValue value)
    {
        // A tag whose status is not Good carries no usable number. Keeping the
        // last good value and counting the rejection is right: a rack fault
        // that makes GoodCount Bad must not be read as "production stopped",
        // and it must not be silent either.
        if (value.IsNotGood)
        {
            return false;
        }

        lock (_gate)
        {
            if (tag == MachineTags.State && WouldOverwriteUnreportedState(value))
            {
                _sealed.Add(Build());
            }

            _values[tag] = value;
            _dirty = true;

            // Event time is the newest producer timestamp in the sample, never
            // the collector's clock: every KPI downstream buckets on it.
            var stamp = value.SourceTimestamp.IsNull
                ? value.ServerTimestamp
                : value.SourceTimestamp;
            var stampUtc = stamp.IsNull ? DateTimeOffset.UtcNow : stamp.ToDateTimeOffset();
            if (stampUtc > _eventTime)
            {
                _eventTime = stampUtc;
            }
        }

        return true;
    }

    /// <summary>
    /// Moves every sample built since the last call into <paramref name="into"/>,
    /// oldest first.
    /// </summary>
    /// <returns>How many samples were added.</returns>
    public int TakeSamples(List<SampleEmission> into)
    {
        lock (_gate)
        {
            var count = _sealed.Count;
            into.AddRange(_sealed);
            _sealed.Clear();

            if (_dirty && _values.Count > 0)
            {
                into.Add(Build());
                count++;
            }

            return count;
        }
    }

    /// <summary>Production and state changes since the last call, for the periodic log line.</summary>
    public LineReport TakeReport()
    {
        lock (_gate)
        {
            var report = new LineReport(
                _lastBuiltState ?? LineState.Idle,
                _goodSinceReport,
                _scrapSinceReport,
                _stateChangesSinceReport);
            _goodSinceReport = 0;
            _scrapSinceReport = 0;
            _stateChangesSinceReport = 0;
            return report;
        }
    }

    /// <summary>
    /// True when <paramref name="incoming"/> is a new state and the pending
    /// sample still holds a state that has not gone out yet. Overwriting it
    /// would erase that state from the record entirely.
    /// </summary>
    private bool WouldOverwriteUnreportedState(DataValue incoming)
    {
        if (!_dirty || !_values.TryGetValue(MachineTags.State, out var pending))
        {
            return false;
        }

        var pendingState = (LineState)pending.GetValue(0);
        var incomingState = (LineState)incoming.GetValue(0);
        return incomingState != pendingState && pendingState != _lastBuiltState;
    }

    /// <summary>Turns the current tag values into a sample. Caller holds the lock.</summary>
    private SampleEmission Build()
    {
        _dirty = false;

        var good = Get(MachineTags.GoodCount, 0UL);
        var scrap = Get(MachineTags.ScrapCount, 0UL);
        var (goodDelta, goodRestart) = Delta(ref _lastGood, good);
        var (scrapDelta, scrapRestart) = Delta(ref _lastScrap, scrap);
        _goodSinceReport += goodDelta;
        _scrapSinceReport += scrapDelta;

        var state = (LineState)Get(MachineTags.State, 0);
        if (_lastBuiltState is { } previous && previous != state)
        {
            _stateChangesSinceReport++;
        }

        _lastBuiltState = state;

        var sample = new MachineSample
        {
            Source = SourceId,
            Seq = ++_seq,
            Site = Site,
            Line = Line,
            EventTimeUtc = _eventTime == default ? DateTimeOffset.UtcNow : _eventTime,
            IngestTimeUtc = DateTimeOffset.UtcNow,
            State = state,
            StateText = Get(MachineTags.StateText, string.Empty),
            GoodCount = good,
            ScrapCount = scrap,
            CycleTimeMs = Get(MachineTags.CycleTimeMs, 0.0),
            IdealCycleMs = Get(MachineTags.IdealCycleMs, 0.0),
            AlarmCode = Get(MachineTags.AlarmCode, 0),
            AlarmText = Get(MachineTags.AlarmText, string.Empty),
            ProgramId = Get(MachineTags.ProgramId, string.Empty),
            SpindleLoadPct = GetNullable<double>(MachineTags.SpindleLoadPct),
            ToolId = _values.ContainsKey(MachineTags.ToolId)
                ? Get(MachineTags.ToolId, string.Empty)
                : null,
            ToolLifeRemainingPct = GetNullable<double>(MachineTags.ToolLifeRemainingPct),
            CurrentLotId = _values.ContainsKey(MachineTags.CurrentLotId)
                ? Get(MachineTags.CurrentLotId, string.Empty)
                : null,
        };

        return new SampleEmission(sample, goodDelta, scrapDelta, goodRestart || scrapRestart);
    }

    private static (long Delta, bool Restart) Delta(ref ulong? previous, ulong current)
    {
        if (previous is null)
        {
            // First sample after a connect: the absolute value is meaningless,
            // so it establishes the baseline and produces nothing. Counting it
            // as production would invent however many parts the line had made
            // before the collector was started.
            previous = current;
            return (0, false);
        }

        if (current < previous.Value)
        {
            previous = current;
            return (0, true);
        }

        var delta = current - previous.Value;
        previous = current;
        return ((long)delta, false);
    }

    private T Get<T>(string tag, T fallback) =>
        _values.TryGetValue(tag, out var value) ? value.GetValue(fallback) : fallback;

    private T? GetNullable<T>(string tag) where T : struct =>
        _values.TryGetValue(tag, out var value) ? value.GetValue<T>(default) : null;
}
