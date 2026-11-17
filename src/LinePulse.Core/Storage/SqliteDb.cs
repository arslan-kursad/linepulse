using Microsoft.Data.Sqlite;

namespace LinePulse.Core.Storage;

/// <summary>
/// Connection management for the SQLite file.
/// </summary>
/// <remarks>
/// <para>
/// SQLite allows many readers and exactly one writer. Rather than discover that
/// under load as a stream of SQLITE_BUSY errors, writes are funnelled through one
/// long-lived connection behind a semaphore and reads get their own short-lived
/// pooled connections. In WAL mode a reader never blocks the writer, so the API
/// stays responsive while ingest is hammering the same file.
/// </para>
/// <para>
/// All timestamps are stored twice: <c>*_ms</c> as epoch milliseconds for every
/// index and predicate, and <c>*_iso</c> as text for anyone opening the file in a
/// SQL client. Comparing ISO strings looks fine until one producer sends
/// <c>+03:00</c> and another sends <c>Z</c> for the same instant and the BETWEEN
/// stops matching; integers do not have opinions about offsets.
/// </para>
/// </remarks>
public sealed class SqliteDb : IAsyncDisposable
{
    private readonly string _connectionString;
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly SqliteConnection _writer;

    public SqliteDb(string path)
    {
        Path = System.IO.Path.GetFullPath(path);
        var directory = System.IO.Path.GetDirectoryName(Path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        _connectionString = new SqliteConnectionStringBuilder
        {
            DataSource = Path,
            Mode = SqliteOpenMode.ReadWriteCreate,
            // Private cache: with WAL, independent connections read concurrently without
            // the shared-cache table locks that turn a busy writer into an error.
            Cache = SqliteCacheMode.Default,
            Pooling = true,
        }.ToString();

        _writer = new SqliteConnection(_connectionString);
        _writer.Open();
        ApplyPragmas(_writer, writer: true);
    }

    public string Path { get; }

    private static void ApplyPragmas(SqliteConnection connection, bool writer)
    {
        using var cmd = connection.CreateCommand();
        // WAL: readers never see a half-written transaction and never block it either.
        // synchronous=NORMAL: a power cut can cost the last fsync window. One lost
        // second of samples is acceptable; fsync per insert is not, at 13 lines.
        // busy_timeout: wait instead of throwing when the writer holds the lock.
        cmd.CommandText = writer
            ? "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;"
            : "PRAGMA busy_timeout=5000; PRAGMA foreign_keys=ON;";
        cmd.ExecuteNonQuery();
    }

    /// <summary>A fresh read connection. Cheap: the pool keeps the handles.</summary>
    public SqliteConnection OpenRead()
    {
        var connection = new SqliteConnection(_connectionString);
        connection.Open();
        ApplyPragmas(connection, writer: false);
        return connection;
    }

    /// <summary>Runs one unit of write work with exclusive access to the writer.</summary>
    public async Task<T> WriteAsync<T>(Func<SqliteConnection, Task<T>> work, CancellationToken ct)
    {
        await _writeLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await work(_writer).ConfigureAwait(false);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    public async Task WriteAsync(Func<SqliteConnection, Task> work, CancellationToken ct) =>
        await WriteAsync<object?>(async c => { await work(c).ConfigureAwait(false); return null; }, ct)
            .ConfigureAwait(false);

    public async ValueTask DisposeAsync()
    {
        await _writer.DisposeAsync().ConfigureAwait(false);
        _writeLock.Dispose();
        SqliteConnection.ClearAllPools();
    }
}
