/* ============================================================================
   LinePulse -- MSSQL schema
   ----------------------------------------------------------------------------
   HONEST STATUS: WRITTEN, NOT EXECUTED, NOT REVIEWED by anyone who works with SQL Server.

   This file has never been run. The machine this project was built on cannot
   host SQL Server: Docker is switched off and an 8 GB laptop will not spare the
   memory for a database server VM. So every statement below is written from the
   MSSQL manual and from the SQLite schema it mirrors, and none of it has been
   validated by a server. Treat it as a design document that happens to be
   executable, and expect a first run to turn up at least a typo.

   Where it is used: the running lab stores to SQLite (src/LinePulse.Core/
   Storage/SqliteSchema.cs). The provider is a configuration switch behind
   IEventStore / IKpiStore, so this file is the other half of a seam that already
   exists in the code -- not a plan for one.

   Target: SQL Server 2019 or later (needs OPENJSON, filtered indexes,
   ROW_NUMBER, and the MERGE ... OUTPUT behaviour used below).

   Differences from the SQLite schema, and why:
     * Event times are datetime2(3), not epoch milliseconds. SQLite has no date
       type, so the lab stores ms for predicates and ISO text for humans. MSSQL
       has a real type with a real index order, so storing two representations of
       the same fact would just be two ways to disagree.
     * Identity columns are IDENTITY rather than AUTOINCREMENT.
     * Clustered index choices are explicit. SQLite only has rowid ordering, so
       this is the one place where the MSSQL version is genuinely a design and
       not a translation -- the reasoning is in the comment above each table.
   ========================================================================== */

IF SCHEMA_ID(N'lp') IS NULL
    EXEC(N'CREATE SCHEMA lp');
GO

/* ---------------------------------------------------------------- idempotency
   There is no separate ledger of seen keys. The primary key (source, seq) of
   machine_samples and test_records is the guarantee: the key is the producer's own
   sequence number and deliberately not event_id, because a test bench that restarts
   mid-shift re-mints event_id for a part it has already published. Checking and
   writing are one statement against that key, so a crash cannot leave a key claimed
   with its row missing. */

/* ------------------------------------------------------------ machine_samples
   One OPC-UA observation of a line. Counters arrive monotonic and arbitrary at
   start, so they are stored exactly as the PLC reported them and production is
   derived in deltas downstream -- a collector restart or a PLC power cycle
   makes the absolute value meaningless, never the difference.

   Clustered on (site, line, event_time_utc) because the dominant read is "all
   of one line's samples in event-time order", which the minute-bucket recompute
   issues on every late event. Clustering makes that a range scan with no
   lookups. Inserts stay roughly ascending inside each line's key range, so the
   page-split cost is bounded and predictable -- and 13 lines is 13 hot ranges,
   not 13 random writers.

   good_count / scrap_count are bigint rather than an unsigned type: MSSQL has
   none, the PLC counters will not reach 2^63 in the life of a machine, and
   decimal(20,0) would cost arithmetic on every delta for range nobody needs. */
IF OBJECT_ID(N'lp.machine_samples', N'U') IS NULL
BEGIN
    CREATE TABLE lp.machine_samples
    (
        source                  varchar(64)   NOT NULL,
        seq                     bigint        NOT NULL,
        event_id                uniqueidentifier NOT NULL,
        site                    varchar(8)    NOT NULL,
        line                    varchar(16)   NOT NULL,
        event_time_utc          datetime2(3)  NOT NULL,
        ingest_time_utc         datetime2(3)  NOT NULL,
        state                   tinyint       NOT NULL,
        state_text              varchar(32)   NOT NULL,
        good_count              bigint        NOT NULL,
        scrap_count             bigint        NOT NULL,
        cycle_ms                float         NOT NULL,
        ideal_cycle_ms          float         NOT NULL,
        alarm_code              int           NOT NULL,
        alarm_text              nvarchar(256) NOT NULL,
        program_id              varchar(32)   NOT NULL,
        spindle_load_pct        float         NULL,
        tool_id                 varchar(32)   NULL,
        tool_life_remaining_pct float         NULL,
        current_lot_id          varchar(32)   NULL,
        CONSTRAINT PK_machine_samples PRIMARY KEY NONCLUSTERED (source, seq)
    );

    CREATE CLUSTERED INDEX CIX_machine_samples_line_time
        ON lp.machine_samples (site, line, event_time_utc);

    /* Serves: part genealogy -- which machining-centre samples were producing a
       given housing lot, so the tool that cut it can be named. Filtered because
       only the machining centre ever sets a lot id; on the assembly lines the
       column is null and an unfiltered index would be mostly empty pages. */
    CREATE NONCLUSTERED INDEX IX_machine_samples_lot
        ON lp.machine_samples (current_lot_id, event_time_utc)
        INCLUDE (tool_id, tool_life_remaining_pct, spindle_load_pct, site, line)
        WHERE current_lot_id IS NOT NULL;
END
GO

/* --------------------------------------------------------------- test_records
   One part's end-of-line result. Clustered on (site, line, event_time_utc) for
   the same reason as the samples: every quality query is a line over a window. */
IF OBJECT_ID(N'lp.test_records', N'U') IS NULL
BEGIN
    CREATE TABLE lp.test_records
    (
        source            varchar(64)   NOT NULL,
        seq               bigint        NOT NULL,
        event_id          uniqueidentifier NOT NULL,
        site              varchar(8)    NOT NULL,
        line              varchar(16)   NOT NULL,
        event_time_utc    datetime2(3)  NOT NULL,
        ingest_time_utc   datetime2(3)  NOT NULL,
        station           varchar(32)   NOT NULL,
        serial            varchar(48)   NOT NULL,
        program_id        varchar(32)   NOT NULL,
        housing_lot_id    varchar(32)   NOT NULL,
        fixture_id        varchar(16)   NOT NULL,
        result            varchar(8)    NOT NULL,
        passed            bit           NOT NULL,
        leak_rate_mbar_ls float         NOT NULL,
        leak_limit_mbar_ls float        NOT NULL,
        flow_lpm          float         NOT NULL,
        flow_target_lpm   float         NOT NULL,
        pressure_bar      float         NOT NULL,
        torque_nm_json    nvarchar(256) NOT NULL,
        operator_id       varchar(16)   NOT NULL,
        CONSTRAINT PK_test_records PRIMARY KEY NONCLUSTERED (source, seq)
    );

    CREATE CLUSTERED INDEX CIX_test_records_line_time
        ON lp.test_records (site, line, event_time_utc);

    /* Serves: GET /api/parts/{serial}. A warranty claim arrives as a serial and
       nothing else. INCLUDE keeps the genealogy answer inside the index, so the
       lookup never touches the clustered index at all. */
    CREATE NONCLUSTERED INDEX IX_test_records_serial
        ON lp.test_records (serial, event_time_utc)
        INCLUDE (site, line, housing_lot_id, program_id, fixture_id, result, passed);

    /* Serves: SPC. The rolling window is "last N parts on this fixture", which
       is a backwards scan of exactly this key. */
    CREATE NONCLUSTERED INDEX IX_test_records_fixture
        ON lp.test_records (site, line, fixture_id, event_time_utc)
        INCLUDE (leak_rate_mbar_ls, leak_limit_mbar_ls);

    /* Serves: lot containment -- every finished pump built from one suspect
       housing lot. */
    CREATE NONCLUSTERED INDEX IX_test_records_lot
        ON lp.test_records (housing_lot_id)
        INCLUDE (serial, passed);
END
GO

/* -------------------------------------------------------------- test_failures
   Failed checks, normalised out of the JSON array the bench publishes. A JSON
   column would make "which check fails most on line 2 this week" a scan plus
   string matching; a child table makes it a GROUP BY on an index. */
IF OBJECT_ID(N'lp.test_failures', N'U') IS NULL
BEGIN
    CREATE TABLE lp.test_failures
    (
        source         varchar(64)  NOT NULL,
        seq            bigint       NOT NULL,
        check_name     varchar(32)  NOT NULL,
        site           varchar(8)   NOT NULL,
        line           varchar(16)  NOT NULL,
        event_time_utc datetime2(3) NOT NULL,
        CONSTRAINT PK_test_failures PRIMARY KEY NONCLUSTERED (source, seq, check_name),
        CONSTRAINT FK_test_failures_record FOREIGN KEY (source, seq)
            REFERENCES lp.test_records (source, seq)
    );

    /* Serves: the failure-reason breakdown inside GET /api/quality/fpy. */
    CREATE CLUSTERED INDEX CIX_test_failures_line_time
        ON lp.test_failures (site, line, event_time_utc, check_name);
END
GO

/* ------------------------------------------------------------------- downtime
   One row per stop: opened when the line leaves Running, closed when it comes
   back. Rows stay open-ended on purpose -- the stop in progress is the most
   useful row on the board and it does not have a duration yet.

   micro_stop is persisted rather than computed on read. The threshold is a
   business rule that can change, and a shift report printed last month must
   keep saying what it said: recomputing history when the rule changes is how
   numbers stop being auditable. */
IF OBJECT_ID(N'lp.downtime', N'U') IS NULL
BEGIN
    CREATE TABLE lp.downtime
    (
        downtime_id      bigint IDENTITY(1,1) NOT NULL,
        site             varchar(8)    NOT NULL,
        line             varchar(16)   NOT NULL,
        started_utc      datetime2(3)  NOT NULL,
        ended_utc        datetime2(3)  NULL,
        duration_seconds float         NULL,
        state            tinyint       NOT NULL,
        reason_code      int           NOT NULL,
        reason_text      nvarchar(256) NOT NULL,
        planned          bit           NOT NULL,
        micro_stop       bit           NULL,
        is_open          bit           NOT NULL,
        CONSTRAINT PK_downtime PRIMARY KEY CLUSTERED (downtime_id)
    );

    /* Serves: the downtime Pareto for a line over a window, and the availability
       split in the shift OEE. */
    CREATE NONCLUSTERED INDEX IX_downtime_line_started
        ON lp.downtime (site, line, started_utc)
        INCLUDE (reason_code, reason_text, planned, micro_stop, duration_seconds, is_open);

    /* Serves: reopening in-progress stops after a service restart, so a redeploy
       mid-shift does not lose the stop that is happening right now. Filtered,
       because at any moment a handful of rows out of millions are open. */
    CREATE NONCLUSTERED INDEX IX_downtime_open
        ON lp.downtime (site, line, started_utc)
        WHERE is_open = 1;
END
GO

/* ----------------------------------------------------------------- oee_minute
   Recomputable one-minute buckets, keyed by EVENT time.

   This table is a materialised view, never an append-only log. When a late
   sample lands, its bucket is rebuilt from lp.machine_samples and overwritten;
   revisions and late_beyond_count are the audit trail of that having happened,
   so a number that changed after it was published can be explained rather than
   just noticed.

   Clustered on (site, line, bucket_start_utc): the primary key, the MERGE
   target, and the shape of every time-series read for one line. */
IF OBJECT_ID(N'lp.oee_minute', N'U') IS NULL
BEGIN
    CREATE TABLE lp.oee_minute
    (
        site                   varchar(8)    NOT NULL,
        line                   varchar(16)   NOT NULL,
        bucket_start_utc       datetime2(0)  NOT NULL,
        program_id             varchar(32)   NOT NULL,
        good                   bigint        NOT NULL,
        scrap                  bigint        NOT NULL,
        samples                int           NOT NULL,
        run_seconds            float         NOT NULL,
        down_unplanned_seconds float         NOT NULL,
        down_planned_seconds   float         NOT NULL,
        ideal_run_seconds      float         NOT NULL,
        ideal_cycle_ms         float         NOT NULL,
        avg_cycle_ms           float         NOT NULL,
        revisions              int           NOT NULL CONSTRAINT DF_oee_minute_revisions DEFAULT (0),
        late_beyond_count      int           NOT NULL CONSTRAINT DF_oee_minute_late DEFAULT (0),
        sealed_at_utc          datetime2(3)  NULL,
        updated_at_utc         datetime2(3)  NOT NULL CONSTRAINT DF_oee_minute_updated DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_oee_minute PRIMARY KEY CLUSTERED (site, line, bucket_start_utc)
    );

    /* Serves: the cross-site rollup, which slices by time first and site second.
       The clustered key is line-first, so this is a genuinely different access
       path and not a duplicate. */
    CREATE NONCLUSTERED INDEX IX_oee_minute_time
        ON lp.oee_minute (bucket_start_utc, site)
        INCLUDE (line, good, scrap, run_seconds, down_unplanned_seconds,
                 down_planned_seconds, ideal_run_seconds, program_id);

    /* Serves: the sealing sweep -- buckets whose line watermark has passed.
       Filtered, because a sealed bucket is never sealed again. */
    CREATE NONCLUSTERED INDEX IX_oee_minute_unsealed
        ON lp.oee_minute (site, line, bucket_start_utc)
        WHERE sealed_at_utc IS NULL;
END
GO

/* ============================================================================
   Idempotent bucket upsert.

   MERGE, because the recompute has to be safe to run again: the same late event
   replayed twice must produce the same bucket, and two ingest instances
   recomputing the same minute must not insert it twice.

   WITH (HOLDLOCK) is not decoration. A bare MERGE takes an update lock that is
   released between the read and the write, so two sessions can both miss the
   row and both insert it -- which surfaces as a primary key violation under
   exactly the load that makes it hardest to reproduce. HOLDLOCK takes a range
   lock on the key for the duration and turns the whole statement atomic.

   revisions counts how many times a published minute has been rebuilt;
   late_beyond_count accumulates rather than overwrites, because several
   beyond-watermark events can land in one bucket between flushes and all of
   them belong in the audit trail.
   ========================================================================== */
CREATE OR ALTER PROCEDURE lp.usp_oee_minute_upsert
    @site                   varchar(8),
    @line                   varchar(16),
    @bucket_start_utc       datetime2(0),
    @program_id             varchar(32),
    @good                   bigint,
    @scrap                  bigint,
    @samples                int,
    @run_seconds            float,
    @down_unplanned_seconds float,
    @down_planned_seconds   float,
    @ideal_run_seconds      float,
    @ideal_cycle_ms         float,
    @avg_cycle_ms           float,
    @late_beyond_count      int = 0
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    MERGE lp.oee_minute WITH (HOLDLOCK) AS target
    USING (SELECT @site AS site, @line AS line, @bucket_start_utc AS bucket_start_utc) AS src
        ON  target.site = src.site
        AND target.line = src.line
        AND target.bucket_start_utc = src.bucket_start_utc
    WHEN MATCHED THEN UPDATE SET
            program_id             = @program_id,
            good                   = @good,
            scrap                  = @scrap,
            samples                = @samples,
            run_seconds            = @run_seconds,
            down_unplanned_seconds = @down_unplanned_seconds,
            down_planned_seconds   = @down_planned_seconds,
            ideal_run_seconds      = @ideal_run_seconds,
            ideal_cycle_ms         = @ideal_cycle_ms,
            avg_cycle_ms           = @avg_cycle_ms,
            revisions              = target.revisions + 1,
            late_beyond_count      = target.late_beyond_count + @late_beyond_count,
            updated_at_utc         = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN INSERT
        (site, line, bucket_start_utc, program_id, good, scrap, samples,
         run_seconds, down_unplanned_seconds, down_planned_seconds,
         ideal_run_seconds, ideal_cycle_ms, avg_cycle_ms,
         revisions, late_beyond_count, sealed_at_utc, updated_at_utc)
        VALUES
        (@site, @line, @bucket_start_utc, @program_id, @good, @scrap, @samples,
         @run_seconds, @down_unplanned_seconds, @down_planned_seconds,
         @ideal_run_seconds, @ideal_cycle_ms, @avg_cycle_ms,
         0, @late_beyond_count, NULL, SYSUTCDATETIME())
    OUTPUT inserted.revisions AS revisions;
END
GO

/* ============================================================================
   Storing an event -- NOT YET WRITTEN for SQL Server.

   The SQLite provider inserts into the raw table and, when the key already exists,
   reads the stored event time to tell a redelivery (same time: Duplicate) from a
   producer that lost its sequence counter (different time: KeyConflict). The two
   outcomes are counted separately because only the second means a device needs
   attention.

   MSSQL has no INSERT ... ON CONFLICT, and IF NOT EXISTS ... INSERT is a race between
   two consumers. The atomic version inserts and catches the primary-key violation
   (error 2627): the unique index makes the decision, and losing the race is a normal
   outcome. A provider needs two small procedures, one per raw table, doing exactly that
   in one transaction together with the test_failures rows for a test record.
   ========================================================================== */

/* ============================================================================
   Read-side example: the SPC window, in MSSQL dialect.

   Included because it is the one query where the two dialects really differ in
   shape -- SQLite has no TOP ... WITH TIES and no CROSS APPLY, so the lab
   version uses ROW_NUMBER in a subquery. Here the fixture list drives an APPLY,
   which gives the optimiser a seek per fixture instead of a window over the
   whole window of records.
   ========================================================================== */
CREATE OR ALTER PROCEDURE lp.usp_fixture_spc_window
    @since_utc   datetime2(3),
    @window_size int = 200
AS
BEGIN
    SET NOCOUNT ON;

    WITH fixtures AS
    (
        SELECT DISTINCT site, line, fixture_id
        FROM lp.test_records
        WHERE event_time_utc >= @since_utc
    )
    SELECT f.site, f.line, f.fixture_id, r.event_time_utc,
           r.leak_rate_mbar_ls, r.leak_limit_mbar_ls
    FROM fixtures AS f
    CROSS APPLY
    (
        SELECT TOP (@window_size) t.event_time_utc, t.leak_rate_mbar_ls, t.leak_limit_mbar_ls
        FROM lp.test_records AS t
        WHERE t.site = f.site AND t.line = f.line AND t.fixture_id = f.fixture_id
          AND t.event_time_utc >= @since_utc
        ORDER BY t.event_time_utc DESC
    ) AS r
    ORDER BY f.site, f.line, f.fixture_id, r.event_time_utc;
END
GO
