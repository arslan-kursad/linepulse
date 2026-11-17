using System.Globalization;
using Microsoft.Data.Sqlite;

namespace LinePulse.Collector;

/// <summary>
/// Durable on-disk queue between the shop floor and the plant network.
/// </summary>
/// <remarks>
/// <para>
/// This is the piece that makes the collector an edge gateway rather than a
/// relay. A shop-floor box loses its uplink regularly -- a switch reboots, a
/// firewall rule changes, the WAN link to another site flaps -- and a plant
/// cannot accept "we lost two hours of traceability because the network
/// blinked". So nothing is forwarded from memory: every event is written here
/// first, forwarded second, and deleted only once the broker has acknowledged
/// it.
/// </para>
/// <para>
/// The <c>seen</c> table is a deduplication window, kept separately from the
/// pending queue on purpose. Once a row is drained it leaves <c>spool</c>, so
/// the queue's own unique index can no longer reject a late duplicate; the
/// window remembers keys for <see cref="SeenRetention"/> and catches republishes
/// that arrive after the original was already forwarded. It is a window and not
/// a permanent set because permanent is not affordable, and a duplicate that
/// turns up a day late is a different problem.
/// </para>
/// <para>
/// Edge-side deduplication does not replace the check at ingest. A duplicate
/// that arrives after the retention window, or after this box is reimaged, will
/// still reach NATS -- which is exactly why ingest dedups again. Two cheap
/// checks in different places beat one expensive guarantee.
/// </para>
/// </remarks>
public sealed class Spool : IAsyncDisposable
{
    /// <summary>How long a forwarded key is remembered for duplicate rejection.</summary>
    public static readonly TimeSpan SeenRetention = TimeSpan.FromHours(6);

    private readonly SqliteConnection _connection;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly ILogger<Spool> _log;

    public Spool(string path, ILogger<Spool> log)
    {
        _log = log;
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(path))!);

        _connection = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Private,
        }.ToString());
        _connection.Open();

        // WAL so a reader never blocks the writer, and NORMAL rather than FULL
        // because a lost tail on power failure costs one sample, while fsync on
        // every insert costs throughput all day.
        Execute("PRAGMA journal_mode=WAL;");
        Execute("PRAGMA synchronous=NORMAL;");
        Execute("""
            CREATE TABLE IF NOT EXISTS spool (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                subject     TEXT    NOT NULL,
                payload     BLOB    NOT NULL,
                source      TEXT    NOT NULL,
                seq         INTEGER NOT NULL,
                event_time  TEXT    NOT NULL,
                enqueued_at TEXT    NOT NULL
            );
            """);
        Execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_spool_key ON spool(source, seq);");
        Execute("""
            CREATE TABLE IF NOT EXISTS seen (
                source     TEXT    NOT NULL,
                seq        INTEGER NOT NULL,
                first_seen TEXT    NOT NULL,
                PRIMARY KEY (source, seq)
            );
            """);
        Execute("CREATE INDEX IF NOT EXISTS ix_seen_first_seen ON seen(first_seen);");
    }

    private void Execute(string sql)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Writes one event to disk unless its key has already been handled.
    /// </summary>
    /// <returns><c>true</c> when stored, <c>false</c> when rejected as a duplicate.</returns>
    public async Task<bool> EnqueueAsync(
        string subject,
        byte[] payload,
        string source,
        long seq,
        DateTimeOffset eventTime,
        CancellationToken ct)
    {
        await _mutex.WaitAsync(ct);
        try
        {
            await using var tx = (SqliteTransaction)await _connection.BeginTransactionAsync(ct);

            await using (var seenCmd = _connection.CreateCommand())
            {
                seenCmd.Transaction = tx;
                seenCmd.CommandText = "SELECT 1 FROM seen WHERE source = $s AND seq = $q LIMIT 1;";
                seenCmd.Parameters.AddWithValue("$s", source);
                seenCmd.Parameters.AddWithValue("$q", seq);
                if (await seenCmd.ExecuteScalarAsync(ct) is not null)
                {
                    await tx.RollbackAsync(ct);
                    return false;
                }
            }

            await using (var insert = _connection.CreateCommand())
            {
                insert.Transaction = tx;
                // OR IGNORE covers the case where the same key is still pending:
                // the unique index rejects it without an exception round-trip.
                insert.CommandText = """
                    INSERT OR IGNORE INTO spool (subject, payload, source, seq, event_time, enqueued_at)
                    VALUES ($subject, $payload, $source, $seq, $event_time, $enqueued_at);
                    """;
                insert.Parameters.AddWithValue("$subject", subject);
                insert.Parameters.AddWithValue("$payload", payload);
                insert.Parameters.AddWithValue("$source", source);
                insert.Parameters.AddWithValue("$seq", seq);
                insert.Parameters.AddWithValue("$event_time", Iso(eventTime));
                insert.Parameters.AddWithValue("$enqueued_at", Iso(DateTimeOffset.UtcNow));

                var rows = await insert.ExecuteNonQueryAsync(ct);
                await tx.CommitAsync(ct);
                return rows > 0;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Reads the oldest pending events without removing them.</summary>
    public async Task<List<SpoolEntry>> PeekAsync(int limit, CancellationToken ct)
    {
        await _mutex.WaitAsync(ct);
        try
        {
            var result = new List<SpoolEntry>(limit);
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = """
                SELECT id, subject, payload, source, seq, event_time
                FROM spool ORDER BY id LIMIT $limit;
                """;
            cmd.Parameters.AddWithValue("$limit", limit);

            await using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                result.Add(new SpoolEntry(
                    Id: reader.GetInt64(0),
                    Subject: reader.GetString(1),
                    Payload: (byte[])reader[2],
                    Source: reader.GetString(3),
                    Seq: reader.GetInt64(4),
                    EventTime: DateTimeOffset.Parse(
                        reader.GetString(5), CultureInfo.InvariantCulture)));
            }

            return result;
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>
    /// Removes entries the broker has accepted and records their keys in the
    /// deduplication window.
    /// </summary>
    public async Task AckAsync(IReadOnlyList<SpoolEntry> entries, CancellationToken ct)
    {
        if (entries.Count == 0)
        {
            return;
        }

        await _mutex.WaitAsync(ct);
        try
        {
            await using var tx = (SqliteTransaction)await _connection.BeginTransactionAsync(ct);

            await using var delete = _connection.CreateCommand();
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM spool WHERE id = $id;";
            var idParam = delete.CreateParameter();
            idParam.ParameterName = "$id";
            delete.Parameters.Add(idParam);

            await using var remember = _connection.CreateCommand();
            remember.Transaction = tx;
            remember.CommandText = """
                INSERT OR IGNORE INTO seen (source, seq, first_seen) VALUES ($s, $q, $t);
                """;
            var sourceParam = remember.CreateParameter();
            sourceParam.ParameterName = "$s";
            var seqParam = remember.CreateParameter();
            seqParam.ParameterName = "$q";
            var timeParam = remember.CreateParameter();
            timeParam.ParameterName = "$t";
            remember.Parameters.AddRange([sourceParam, seqParam, timeParam]);

            foreach (var entry in entries)
            {
                idParam.Value = entry.Id;
                await delete.ExecuteNonQueryAsync(ct);

                sourceParam.Value = entry.Source;
                seqParam.Value = entry.Seq;
                timeParam.Value = Iso(DateTimeOffset.UtcNow);
                await remember.ExecuteNonQueryAsync(ct);
            }

            await tx.CommitAsync(ct);
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Number of events waiting to be forwarded. The headline health metric.</summary>
    public async Task<long> DepthAsync(CancellationToken ct)
    {
        await _mutex.WaitAsync(ct);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM spool;";
            return Convert.ToInt64(await cmd.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
        }
        finally
        {
            _mutex.Release();
        }
    }

    /// <summary>Drops deduplication keys older than <see cref="SeenRetention"/>.</summary>
    public async Task<int> PruneSeenAsync(CancellationToken ct)
    {
        await _mutex.WaitAsync(ct);
        try
        {
            await using var cmd = _connection.CreateCommand();
            cmd.CommandText = "DELETE FROM seen WHERE first_seen < $cutoff;";
            cmd.Parameters.AddWithValue("$cutoff", Iso(DateTimeOffset.UtcNow - SeenRetention));
            var removed = await cmd.ExecuteNonQueryAsync(ct);
            if (removed > 0)
            {
                _log.LogDebug("pruned {Count} expired dedup keys", removed);
            }

            return removed;
        }
        finally
        {
            _mutex.Release();
        }
    }

    private static string Iso(DateTimeOffset value) =>
        value.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture);

    public async ValueTask DisposeAsync()
    {
        await _connection.DisposeAsync();
        _mutex.Dispose();
    }
}

/// <summary>One row of the spool, on its way to the broker.</summary>
public readonly record struct SpoolEntry(
    long Id,
    string Subject,
    byte[] Payload,
    string Source,
    long Seq,
    DateTimeOffset EventTime);
