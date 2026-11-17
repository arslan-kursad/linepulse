using System.Collections.Concurrent;
using LinePulse.Contracts;
using LinePulse.Core.Storage;

namespace LinePulse.Core.Realtime;

/// <summary>What a line is doing right now.</summary>
public sealed record LineSnapshot(
    string Site,
    string Line,
    LineState State,
    string StateText,
    string ProgramId,
    int AlarmCode,
    string AlarmText,
    DateTimeOffset LastSeenUtc);

/// <summary>
/// In-memory "now" for every line.
/// </summary>
/// <remarks>
/// Current state is read far more often than it changes -- every dashboard tile,
/// every poll, every SSE tick -- and it is always the newest sample, which ingest
/// has in its hand anyway. Keeping it in memory means the state board does not
/// issue one "latest row per line" query per refresh. The cache is rebuilt from
/// the database at startup so a restart does not show a blank board, and it is
/// only ever advanced by a newer sample, so an out-of-order arrival cannot make a
/// running line look faulted.
/// </remarks>
public sealed class LineSnapshotCache
{
    private readonly ConcurrentDictionary<string, LineSnapshot> _snapshots = new();

    public IReadOnlyCollection<LineSnapshot> All => _snapshots.Values.ToArray();

    public LineSnapshot? Get(string site, string line) =>
        _snapshots.TryGetValue($"{site}/{line}", out var snapshot) ? snapshot : null;

    public void Seed(IReadOnlyList<LineStateRow> rows)
    {
        foreach (var row in rows)
        {
            _snapshots[$"{row.Site}/{row.Line}"] = new LineSnapshot(
                row.Site, row.Line, (LineState)row.State, row.StateText, row.ProgramId,
                row.AlarmCode, row.AlarmText, Clock.FromMs(row.TsMs));
        }
    }

    /// <returns>The snapshot if the state or program changed, otherwise null --
    /// so the live feed only pushes transitions, not every sample.</returns>
    public LineSnapshot? Apply(MachineSample sample)
    {
        var key = sample.LineKey;
        var next = new LineSnapshot(
            sample.Site, sample.Line, sample.State, sample.StateText, sample.ProgramId,
            sample.AlarmCode, sample.AlarmText, sample.EventTimeUtc);

        var changed = false;
        _snapshots.AddOrUpdate(
            key,
            _ =>
            {
                changed = true;
                return next;
            },
            (_, existing) =>
            {
                if (existing.LastSeenUtc > next.LastSeenUtc)
                {
                    // Older than what we already have: the minute buckets will still
                    // absorb it, but the state board shows the newest truth only.
                    return existing;
                }

                changed = existing.State != next.State
                    || !string.Equals(existing.ProgramId, next.ProgramId, StringComparison.Ordinal)
                    || existing.AlarmCode != next.AlarmCode;
                return next;
            });

        return changed ? next : null;
    }
}
