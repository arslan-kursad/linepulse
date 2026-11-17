namespace LinePulse.Core.Storage;

/// <summary>
/// The SQLite schema, with one comment per index saying which query pays for it.
/// </summary>
/// <remarks>
/// An index nobody can name a query for is a write-amplification tax, and ingest
/// is the write-heavy side of this system. Each one below earns its place.
/// The MSSQL dialect of the same model is in db/mssql/schema.sql.
/// </remarks>
public static class SqliteSchema
{
    public const string Ddl = """
        -- -------------------------------------------------------- machine samples
        -- One OPC-UA observation. Counters arrive monotonic and arbitrary at start,
        -- so they are stored raw and production is derived in deltas downstream.
        -- The primary key is the idempotency key, (source, seq) and not event_id:
        -- the insert that stores a sample is the same statement that refuses its
        -- duplicate, so no key can be claimed without its row being written.
        CREATE TABLE IF NOT EXISTS machine_samples (
            source          TEXT    NOT NULL,
            seq             INTEGER NOT NULL,
            event_id        TEXT    NOT NULL,
            site            TEXT    NOT NULL,
            line            TEXT    NOT NULL,
            ts_ms           INTEGER NOT NULL,
            ts_iso          TEXT    NOT NULL,
            ingest_ts_ms    INTEGER NOT NULL,
            state           INTEGER NOT NULL,
            state_text      TEXT    NOT NULL,
            good_count      INTEGER NOT NULL,
            scrap_count     INTEGER NOT NULL,
            cycle_ms        REAL    NOT NULL,
            ideal_cycle_ms  REAL    NOT NULL,
            alarm_code      INTEGER NOT NULL,
            alarm_text      TEXT    NOT NULL,
            program_id      TEXT    NOT NULL,
            spindle_load_pct      REAL,
            tool_id               TEXT,
            tool_life_pct         REAL,
            current_lot_id        TEXT,
            PRIMARY KEY (source, seq)
        );
        -- Serves: the minute-bucket rebuild, which reads one line's samples in
        -- event-time order plus its neighbours either side of the minute. This is
        -- the hottest read in the system and the only reason the rebuild is
        -- affordable on every late event.
        CREATE INDEX IF NOT EXISTS ix_samples_line_ts ON machine_samples(site, line, ts_ms);
        -- Serves: part genealogy -- find the machining-centre samples that were
        -- producing a given housing lot, to name the tool that cut it.
        CREATE INDEX IF NOT EXISTS ix_samples_lot ON machine_samples(current_lot_id)
            WHERE current_lot_id IS NOT NULL;

        -- ---------------------------------------------------------- test records
        -- Same idempotency rule as the samples: (source, seq) is the primary key.
        CREATE TABLE IF NOT EXISTS test_records (
            source        TEXT    NOT NULL,
            seq           INTEGER NOT NULL,
            event_id      TEXT    NOT NULL,
            site          TEXT    NOT NULL,
            line          TEXT    NOT NULL,
            ts_ms         INTEGER NOT NULL,
            ts_iso        TEXT    NOT NULL,
            ingest_ts_ms  INTEGER NOT NULL,
            station       TEXT    NOT NULL,
            serial        TEXT    NOT NULL,
            program_id    TEXT    NOT NULL,
            housing_lot_id TEXT   NOT NULL,
            fixture_id    TEXT    NOT NULL,
            result        TEXT    NOT NULL,
            passed        INTEGER NOT NULL,
            leak_rate     REAL    NOT NULL,
            leak_limit    REAL    NOT NULL,
            flow_lpm      REAL    NOT NULL,
            flow_target   REAL    NOT NULL,
            pressure_bar  REAL    NOT NULL,
            torque_json   TEXT    NOT NULL,
            operator_id   TEXT    NOT NULL,
            PRIMARY KEY (source, seq)
        );
        -- Serves: GET /api/parts/{serial}. A warranty claim arrives as a serial
        -- number and nothing else, so this lookup has to be a seek, not a scan.
        CREATE INDEX IF NOT EXISTS ix_tests_serial ON test_records(serial);
        -- Serves: FPY per line over a window, and the first-test-per-serial subquery.
        CREATE INDEX IF NOT EXISTS ix_tests_line_ts ON test_records(site, line, ts_ms);
        -- Serves: SPC. The rolling window is "last N parts on this fixture", which
        -- is a backwards scan of exactly this index.
        CREATE INDEX IF NOT EXISTS ix_tests_fixture_ts ON test_records(site, line, fixture_id, ts_ms);
        -- Serves: lot-level containment -- every finished pump built from one
        -- suspect housing lot.
        CREATE INDEX IF NOT EXISTS ix_tests_lot ON test_records(housing_lot_id);

        -- Failed checks, normalised. A JSON blob would make "which check fails most
        -- on line 2 this week" a table scan plus string matching; a child table
        -- makes it a GROUP BY.
        CREATE TABLE IF NOT EXISTS test_failures (
            source     TEXT    NOT NULL,
            seq        INTEGER NOT NULL,
            check_name TEXT    NOT NULL,
            site       TEXT    NOT NULL,
            line       TEXT    NOT NULL,
            ts_ms      INTEGER NOT NULL,
            PRIMARY KEY (source, seq, check_name)
        );
        -- Serves: the failure-reason breakdown inside GET /api/quality/fpy.
        CREATE INDEX IF NOT EXISTS ix_failures_line_ts ON test_failures(site, line, ts_ms);

        -- -------------------------------------------------------------- downtime
        -- One row per stop, opened when the line leaves Running and closed when it
        -- comes back. Kept open-ended on purpose: a stop in progress is the single
        -- most useful row on the board, and it does not have a duration yet.
        -- duration_s is in production seconds: the event-time difference times
        -- Ingest:TimeScale, which is 1 everywhere except the time-compressed lab.
        CREATE TABLE IF NOT EXISTS downtime (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            site          TEXT    NOT NULL,
            line          TEXT    NOT NULL,
            started_ms    INTEGER NOT NULL,
            started_iso   TEXT    NOT NULL,
            ended_ms      INTEGER,
            duration_s    REAL,
            state         INTEGER NOT NULL,
            reason_code   INTEGER NOT NULL,
            reason_text   TEXT    NOT NULL,
            planned       INTEGER NOT NULL,
            micro_stop    INTEGER,
            is_open       INTEGER NOT NULL
        );
        -- Serves: the downtime Pareto for a line over a window, and the shift
        -- stop counts in the cross-site rollup.
        CREATE INDEX IF NOT EXISTS ix_downtime_line_started ON downtime(site, line, started_ms);
        -- Serves: reopening the in-progress stops after a service restart, so a
        -- redeploy mid-shift does not lose the stop that is happening right now.
        CREATE INDEX IF NOT EXISTS ix_downtime_open ON downtime(is_open) WHERE is_open = 1;

        -- ------------------------------------------------------------ oee_minute
        -- Recomputable one-minute buckets, keyed by EVENT time. This table is a
        -- materialised view, never an append-only log: when a sample lands, the
        -- minutes it touches are rebuilt from machine_samples and overwritten.
        -- samples = 0 marks a minute inside a stop that sent no data of its own and
        -- was billed from the state carried in. revisions counts rebuilds after the
        -- minute was sealed -- after someone may have read it -- and
        -- late_beyond_count the beyond-watermark events that caused them.
        CREATE TABLE IF NOT EXISTS oee_minute (
            site                   TEXT    NOT NULL,
            line                   TEXT    NOT NULL,
            bucket_start_ms        INTEGER NOT NULL,
            bucket_start_iso       TEXT    NOT NULL,
            program_id             TEXT    NOT NULL,
            good                   INTEGER NOT NULL,
            scrap                  INTEGER NOT NULL,
            samples                INTEGER NOT NULL,
            run_seconds            REAL    NOT NULL,
            down_unplanned_seconds REAL    NOT NULL,
            down_planned_seconds   REAL    NOT NULL,
            ideal_run_seconds      REAL    NOT NULL,
            ideal_cycle_ms         REAL    NOT NULL,
            avg_cycle_ms           REAL    NOT NULL,
            revisions              INTEGER NOT NULL,
            late_beyond_count      INTEGER NOT NULL,
            sealed_at_ms           INTEGER,
            updated_at_ms          INTEGER NOT NULL,
            PRIMARY KEY (site, line, bucket_start_ms)
        );
        -- Serves: the cross-site rollup and the shift aggregate, which slice by
        -- time first and site second. The primary key is line-first, so a
        -- time-first index is a different access path and worth its cost.
        CREATE INDEX IF NOT EXISTS ix_oee_minute_ts ON oee_minute(bucket_start_ms, site);
        -- Serves: the sealing sweep -- buckets whose watermark has passed.
        CREATE INDEX IF NOT EXISTS ix_oee_minute_unsealed ON oee_minute(site, line, bucket_start_ms)
            WHERE sealed_at_ms IS NULL;
        """;
}
