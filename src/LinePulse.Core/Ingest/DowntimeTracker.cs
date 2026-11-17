using System.Collections.Concurrent;
using LinePulse.Contracts;
using LinePulse.Core.Kpi;
using LinePulse.Core.Storage;

namespace LinePulse.Core.Ingest;

/// <summary>A stop that has just been closed, for the live feed and logs.</summary>
public sealed record ClosedStop(
    string Site,
    string Line,
    int ReasonCode,
    string ReasonText,
    DateTimeOffset StartedUtc,
    DateTimeOffset EndedUtc,
    double DurationSeconds,
    bool MicroStop,
    bool Planned);

/// <summary>
/// Turns line-state transitions into downtime records.
/// </summary>
/// <remarks>
/// <para>
/// A PLC does not report downtime; it reports state. The stop exists only in the
/// transition out of Running and the transition back into it, which means nothing
/// downstream can reconstruct it unless somebody tracks the edges. That is this
/// class.
/// </para>
/// <para>
/// The micro-stop classification is the business point of the whole demo. Stops
/// under two minutes never reach a shift report -- the operator clears a feeder jam
/// in forty seconds and nobody writes it down -- but forty of them in a shift is
/// half an hour of lost production that no one can explain. Deriving them from
/// state transitions makes losses visible that the paperwork cannot see. A planned
/// stop is never a micro-stop, however short: a changeover is on the schedule, so
/// it is by definition not a loss nobody wrote down.
/// </para>
/// <para>
/// Out-of-order samples are refused by the state machine rather than applied: an
/// older sample arriving after a newer one must not rewind the line's state and
/// invent a stop that never happened. The sample is still stored and still feeds
/// the minute buckets, which are order-independent because they are rebuilt from
/// all of their rows -- so the correction is not lost, it just does not corrupt
/// the stop timeline.
/// </para>
/// </remarks>
public sealed class DowntimeTracker(IEventStore store, IngestCounters counters, IngestOptions options, ILogger<DowntimeTracker> log)
{
    private sealed class LineFsm
    {
        public long LastTsMs;
        public int State = -1;
        public long? OpenDowntimeId;
        public long OpenStartedMs;
        public int OpenReasonCode;
        public string OpenReasonText = "";
        public bool OpenPlanned;
    }

    private readonly ConcurrentDictionary<string, LineFsm> _lines = new();
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly double _timeScale = options.TimeScale > 0 ? options.TimeScale : 1.0;

    public TimeSpan MicroStopThreshold { get; } = TimeSpan.FromSeconds(options.MicroStopThresholdSeconds);

    /// <summary>
    /// Rebuilds the state machines after a restart, so a mid-shift redeploy neither
    /// loses the stop that is happening right now nor leaves one open for ever.
    /// </summary>
    public async Task RestoreAsync(IReadOnlyList<LineStateRow> latestStates, CancellationToken ct)
    {
        foreach (var row in latestStates)
        {
            var fsm = _lines.GetOrAdd(Key(row.Site, row.Line), _ => new LineFsm());
            fsm.LastTsMs = row.TsMs;
            fsm.State = row.State;
        }

        foreach (var open in await store.LoadOpenDowntimeAsync(ct).ConfigureAwait(false))
        {
            // A service killed between storing the Running sample and closing the
            // stop leaves the row open although the line is long back. The stored
            // samples know when it really ended, so close it there instead of
            // letting the next transition orphan it.
            var endedMs = await store
                .FirstSampleInStateAfterAsync(open.Site, open.Line, open.StartedMs, LineState.Running, ct)
                .ConfigureAwait(false);

            if (endedMs is { } ended)
            {
                var (duration, micro) = Measure(open.StartedMs, ended, open.Planned);
                await store.CloseDowntimeAsync(open.Id, ended, duration, micro, ct).ConfigureAwait(false);
                log.LogWarning(
                    "closed stop {Id} on {Site}/{Line} left open by a previous run: ended {Ended}, {Seconds:F0}s",
                    open.Id, open.Site, open.Line, Clock.IsoFromMs(ended), duration);
                continue;
            }

            var lineFsm = _lines.GetOrAdd(Key(open.Site, open.Line), _ => new LineFsm());
            lineFsm.OpenDowntimeId = open.Id;
            lineFsm.OpenStartedMs = open.StartedMs;
            lineFsm.OpenReasonCode = open.ReasonCode;
            lineFsm.OpenReasonText = open.ReasonText;
            lineFsm.OpenPlanned = open.Planned;
            log.LogInformation(
                "resumed open stop {Id} on {Site}/{Line} started {Started}",
                open.Id, open.Site, open.Line, Clock.IsoFromMs(open.StartedMs));
        }
    }

    public async Task<ClosedStop?> ApplyAsync(MachineSample sample, CancellationToken ct)
    {
        var tsMs = Clock.ToMs(sample.EventTimeUtc);

        await _mutex.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var fsm = _lines.GetOrAdd(sample.LineKey, _ => new LineFsm());

            if (fsm.State >= 0 && tsMs <= fsm.LastTsMs)
            {
                counters.MarkOutOfOrderState();
                return null;
            }

            var previousState = fsm.State;
            var newState = (int)sample.State;
            fsm.LastTsMs = tsMs;
            fsm.State = newState;

            if (previousState < 0)
            {
                // First sample for this line in this process. We know the state but
                // not when it started, so no stop is opened: a fabricated start time
                // would put a made-up duration on a real report.
                return null;
            }

            var wasRunning = previousState == (int)LineState.Running;
            var isRunning = newState == (int)LineState.Running;

            if (wasRunning && !isRunning)
            {
                var reasonCode = ReasonCatalog.CodeForState(sample.AlarmCode, newState);
                var reasonText = ReasonCatalog.Text(reasonCode, newState, sample.AlarmText);
                var planned = ReasonCatalog.IsPlanned(reasonCode, newState);

                fsm.OpenDowntimeId = await store.OpenDowntimeAsync(
                    new DowntimeOpen(sample.Site, sample.Line, tsMs, sample.State, reasonCode, reasonText, planned),
                    ct).ConfigureAwait(false);
                fsm.OpenStartedMs = tsMs;
                fsm.OpenReasonCode = reasonCode;
                fsm.OpenReasonText = reasonText;
                fsm.OpenPlanned = planned;
                return null;
            }

            if (!wasRunning && isRunning && fsm.OpenDowntimeId is { } openId)
            {
                var (durationSeconds, microStop) = Measure(fsm.OpenStartedMs, tsMs, fsm.OpenPlanned);
                await store.CloseDowntimeAsync(openId, tsMs, durationSeconds, microStop, ct).ConfigureAwait(false);
                fsm.OpenDowntimeId = null;

                return new ClosedStop(
                    sample.Site, sample.Line, fsm.OpenReasonCode, fsm.OpenReasonText,
                    Clock.FromMs(fsm.OpenStartedMs), sample.EventTimeUtc,
                    Math.Round(durationSeconds, 1), microStop, fsm.OpenPlanned);
            }

            return null;
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Duration in production seconds and the micro-stop verdict. The
    /// verdict is stored with the stop rather than computed on read: the threshold
    /// is a business rule that can change, and last month's shift report must keep
    /// saying what it said.</summary>
    private (double DurationSeconds, bool MicroStop) Measure(long startedMs, long endedMs, bool planned)
    {
        var durationSeconds = (endedMs - startedMs) / 1000.0 * _timeScale;
        var microStop = !planned && durationSeconds < MicroStopThreshold.TotalSeconds;
        if (microStop)
        {
            counters.MarkMicroStop();
        }

        return (durationSeconds, microStop);
    }

    private static string Key(string site, string line) => $"{site}/{line}";
}
