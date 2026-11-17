using System.Text.Json;
using Dapper;
using LinePulse.Contracts;
using LinePulse.Core.Kpi;
using Microsoft.Data.Sqlite;

namespace LinePulse.Core.Storage;

/// <summary>SQLite implementation of the ingest write path.</summary>
public sealed class SqliteEventStore(SqliteDb db, ILogger<SqliteEventStore> log) : IEventStore
{
    private const string SampleColumns =
        "ts_ms AS TsMs, state AS State, alarm_code AS AlarmCode, good_count AS GoodCount, "
        + "scrap_count AS ScrapCount, cycle_ms AS CycleMs, ideal_cycle_ms AS IdealCycleMs, program_id AS ProgramId";

    public async Task InitializeAsync(CancellationToken ct)
    {
        await db.WriteAsync(c => c.ExecuteAsync(SqliteSchema.Ddl), ct).ConfigureAwait(false);
        log.LogInformation("sqlite ready at {Path}", db.Path);
    }

    public Task<StoreOutcome> StoreMachineSampleAsync(MachineSample s, long ingestTsMs, CancellationToken ct) =>
        db.WriteAsync(async c =>
        {
            var tsMs = Clock.ToMs(s.EventTimeUtc);
            // One statement, and the primary key is the arbiter. A SELECT-then-INSERT
            // would be a race between two consumers; a separate ledger written
            // before this row would leave a claimed key behind if this failed.
            var inserted = await c.ExecuteAsync(
                """
                INSERT INTO machine_samples (
                    source, seq, event_id, site, line, ts_ms, ts_iso, ingest_ts_ms,
                    state, state_text, good_count, scrap_count, cycle_ms, ideal_cycle_ms,
                    alarm_code, alarm_text, program_id,
                    spindle_load_pct, tool_id, tool_life_pct, current_lot_id)
                VALUES (
                    @source, @seq, @eventId, @site, @line, @tsMs, @tsIso, @ingestTsMs,
                    @state, @stateText, @good, @scrap, @cycleMs, @idealCycleMs,
                    @alarmCode, @alarmText, @programId,
                    @spindle, @toolId, @toolLife, @lotId)
                ON CONFLICT (source, seq) DO NOTHING;
                """,
                new
                {
                    source = s.Source,
                    seq = s.Seq,
                    eventId = s.EventId.ToString(),
                    site = s.Site,
                    line = s.Line,
                    tsMs,
                    tsIso = Clock.ToIso(s.EventTimeUtc),
                    ingestTsMs,
                    state = (int)s.State,
                    stateText = s.StateText,
                    // The PLC counters are unsigned and monotonic; they will not pass
                    // long.MaxValue in the lifetime of a machine, and signed integers are
                    // what SQLite and MSSQL both actually store.
                    good = (long)s.GoodCount,
                    scrap = (long)s.ScrapCount,
                    cycleMs = s.CycleTimeMs,
                    idealCycleMs = s.IdealCycleMs,
                    alarmCode = s.AlarmCode,
                    alarmText = s.AlarmText,
                    programId = s.ProgramId,
                    spindle = s.SpindleLoadPct,
                    toolId = s.ToolId,
                    toolLife = s.ToolLifeRemainingPct,
                    lotId = s.CurrentLotId,
                }).ConfigureAwait(false);

            return inserted > 0
                ? StoreOutcome.Stored
                : await ClassifyRefusalAsync(c, null, "machine_samples", s.Source, s.Seq, tsMs).ConfigureAwait(false);
        }, ct);

    public Task<StoreOutcome> StoreTestRecordAsync(TestRecord r, long ingestTsMs, CancellationToken ct) =>
        db.WriteAsync(async c =>
        {
            var tsMs = Clock.ToMs(r.EventTimeUtc);
            await using var tx = (SqliteTransaction)await c.BeginTransactionAsync(ct).ConfigureAwait(false);

            var inserted = await c.ExecuteAsync(
                """
                INSERT INTO test_records (
                    source, seq, event_id, site, line, ts_ms, ts_iso, ingest_ts_ms,
                    station, serial, program_id, housing_lot_id, fixture_id, result, passed,
                    leak_rate, leak_limit, flow_lpm, flow_target, pressure_bar, torque_json, operator_id)
                VALUES (
                    @source, @seq, @eventId, @site, @line, @tsMs, @tsIso, @ingestTsMs,
                    @station, @serial, @programId, @lotId, @fixtureId, @result, @passed,
                    @leakRate, @leakLimit, @flowLpm, @flowTarget, @pressureBar, @torqueJson, @operatorId)
                ON CONFLICT (source, seq) DO NOTHING;
                """,
                new
                {
                    source = r.Source,
                    seq = r.Seq,
                    eventId = r.EventId.ToString(),
                    site = r.Site,
                    line = r.Line,
                    tsMs,
                    tsIso = Clock.ToIso(r.EventTimeUtc),
                    ingestTsMs,
                    station = r.Station,
                    serial = r.Serial,
                    programId = r.ProgramId,
                    lotId = r.HousingLotId,
                    fixtureId = r.FixtureId,
                    result = r.Result,
                    passed = r.Passed ? 1 : 0,
                    leakRate = r.Measurements.LeakRateMbarLs,
                    leakLimit = r.Measurements.LeakLimitMbarLs,
                    flowLpm = r.Measurements.FlowLpm,
                    flowTarget = r.Measurements.FlowTargetLpm,
                    pressureBar = r.Measurements.PressureBar,
                    torqueJson = JsonSerializer.Serialize(r.Measurements.TorqueNm),
                    operatorId = r.OperatorId,
                },
                tx).ConfigureAwait(false);

            if (inserted == 0)
            {
                var refusal = await ClassifyRefusalAsync(c, tx, "test_records", r.Source, r.Seq, tsMs).ConfigureAwait(false);
                await tx.RollbackAsync(ct).ConfigureAwait(false);
                return refusal;
            }

            foreach (var check in r.FailedChecks.Distinct(StringComparer.OrdinalIgnoreCase))
            {
                await c.ExecuteAsync(
                    """
                    INSERT INTO test_failures (source, seq, check_name, site, line, ts_ms)
                    VALUES (@source, @seq, @check, @site, @line, @tsMs)
                    ON CONFLICT (source, seq, check_name) DO NOTHING;
                    """,
                    new { source = r.Source, seq = r.Seq, check, site = r.Site, line = r.Line, tsMs },
                    tx).ConfigureAwait(false);
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
            return StoreOutcome.Stored;
        }, ct);

    /// <summary>
    /// A refused insert is either a redelivery (same event time) or a producer that
    /// reset its sequence counter (different event time). Only the refusal path pays
    /// for this lookup, so the hot path stays one statement.
    /// </summary>
    private static async Task<StoreOutcome> ClassifyRefusalAsync(
        SqliteConnection c, SqliteTransaction? tx, string table, string source, long seq, long tsMs)
    {
        var storedTsMs = await c.ExecuteScalarAsync<long?>(
            $"SELECT ts_ms FROM {table} WHERE source = @source AND seq = @seq;",
            new { source, seq },
            tx).ConfigureAwait(false);

        return storedTsMs == tsMs ? StoreOutcome.Duplicate : StoreOutcome.KeyConflict;
    }

    public async Task<long?> NextSampleTsAsync(string site, string line, long afterTsMs, CancellationToken ct)
    {
        await using var read = db.OpenRead();
        return await read.ExecuteScalarAsync<long?>(
            """
            SELECT ts_ms FROM machine_samples
            WHERE site = @site AND line = @line AND ts_ms > @after
            ORDER BY ts_ms LIMIT 1;
            """,
            new { site, line, after = afterTsMs }).ConfigureAwait(false);
    }

    /// <remarks>
    /// The neighbours matter as much as the minute itself. The sample before it
    /// supplies the counter baseline and the state the line was already in -- a line
    /// faulted for five minutes sends no transition, so without the carried-in state
    /// the whole minute would look idle. The first sample after it is the evidence
    /// that the line was still reporting, which is what entitles the tail of the
    /// minute to be billed at all.
    /// </remarks>
    public async Task<MinuteInputs> LoadMinuteInputsAsync(string site, string line, long bucketStartMs, CancellationToken ct)
    {
        await using var read = db.OpenRead();
        var args = new { site, line, start = bucketStartMs, end = bucketStartMs + Clock.MinuteMs };

        var previous = await read.QuerySingleOrDefaultAsync<BucketSample>(
            $"""
             SELECT {SampleColumns} FROM machine_samples
             WHERE site = @site AND line = @line AND ts_ms < @start
             ORDER BY ts_ms DESC, seq DESC LIMIT 1;
             """, args).ConfigureAwait(false);

        var rows = (await read.QueryAsync<BucketSample>(
            $"""
             SELECT {SampleColumns} FROM machine_samples
             WHERE site = @site AND line = @line AND ts_ms >= @start AND ts_ms < @end
             ORDER BY ts_ms, seq;
             """, args).ConfigureAwait(false)).AsList();

        var nextTsMs = await read.ExecuteScalarAsync<long?>(
            """
            SELECT ts_ms FROM machine_samples
            WHERE site = @site AND line = @line AND ts_ms >= @end
            ORDER BY ts_ms LIMIT 1;
            """, args).ConfigureAwait(false);

        return new MinuteInputs(previous, rows, nextTsMs);
    }

    public Task<long> UpsertMinuteAsync(MinuteBucket bucket, int lateBeyondWatermarkCount, CancellationToken ct) =>
        db.WriteAsync(c => c.ExecuteScalarAsync<long>(
            """
            INSERT INTO oee_minute (
                site, line, bucket_start_ms, bucket_start_iso, program_id,
                good, scrap, samples, run_seconds, down_unplanned_seconds, down_planned_seconds,
                ideal_run_seconds, ideal_cycle_ms, avg_cycle_ms,
                revisions, late_beyond_count, sealed_at_ms, updated_at_ms)
            VALUES (
                @site, @line, @start, @startIso, @programId,
                @good, @scrap, @samples, @run, @downUnplanned, @downPlanned,
                @idealRun, @idealCycleMs, @avgCycleMs,
                0, @lateBeyond, NULL, @now)
            ON CONFLICT (site, line, bucket_start_ms) DO UPDATE SET
                program_id             = excluded.program_id,
                good                   = excluded.good,
                scrap                  = excluded.scrap,
                samples                = excluded.samples,
                run_seconds            = excluded.run_seconds,
                down_unplanned_seconds = excluded.down_unplanned_seconds,
                down_planned_seconds   = excluded.down_planned_seconds,
                ideal_run_seconds      = excluded.ideal_run_seconds,
                ideal_cycle_ms         = excluded.ideal_cycle_ms,
                avg_cycle_ms           = excluded.avg_cycle_ms,
                -- A minute still in progress is rebuilt every flush; that is how it
                -- fills up, not a revision. Only a rebuild after the minute was
                -- sealed changes a number somebody may already have read.
                revisions              = oee_minute.revisions
                                         + CASE WHEN oee_minute.sealed_at_ms IS NULL THEN 0 ELSE 1 END,
                late_beyond_count      = oee_minute.late_beyond_count + excluded.late_beyond_count,
                updated_at_ms          = excluded.updated_at_ms
            RETURNING revisions;
            """,
            new
            {
                site = bucket.Site,
                line = bucket.Line,
                start = bucket.BucketStartMs,
                startIso = Clock.IsoFromMs(bucket.BucketStartMs),
                programId = bucket.ProgramId,
                good = bucket.Good,
                scrap = bucket.Scrap,
                samples = bucket.Samples,
                run = bucket.RunSeconds,
                downUnplanned = bucket.DownUnplannedSeconds,
                downPlanned = bucket.DownPlannedSeconds,
                idealRun = bucket.IdealRunSeconds,
                idealCycleMs = bucket.IdealCycleMs,
                avgCycleMs = bucket.AvgCycleMs,
                lateBeyond = lateBeyondWatermarkCount,
                now = Clock.NowMs(),
            }), ct);

    public Task<int> SealBucketsAsync(string site, string line, long watermarkMs, CancellationToken ct) =>
        db.WriteAsync(c => c.ExecuteAsync(
            """
            UPDATE oee_minute SET sealed_at_ms = @now
            WHERE site = @site AND line = @line
              AND sealed_at_ms IS NULL
              AND bucket_start_ms + @bucket <= @watermark;
            """,
            new { site, line, watermark = watermarkMs, bucket = Clock.MinuteMs, now = Clock.NowMs() }), ct);

    public Task<long> OpenDowntimeAsync(DowntimeOpen open, CancellationToken ct) =>
        db.WriteAsync(c => c.ExecuteScalarAsync<long>(
            """
            INSERT INTO downtime (site, line, started_ms, started_iso, state, reason_code, reason_text, planned, is_open)
            VALUES (@site, @line, @started, @startedIso, @state, @reasonCode, @reasonText, @planned, 1)
            RETURNING id;
            """,
            new
            {
                site = open.Site,
                line = open.Line,
                started = open.StartedMs,
                startedIso = Clock.IsoFromMs(open.StartedMs),
                state = (int)open.State,
                reasonCode = open.ReasonCode,
                reasonText = open.ReasonText,
                planned = open.Planned ? 1 : 0,
            }), ct);

    public Task CloseDowntimeAsync(long id, long endedMs, double durationSeconds, bool microStop, CancellationToken ct) =>
        db.WriteAsync(c => c.ExecuteAsync(
            """
            UPDATE downtime
            SET ended_ms = @ended, duration_s = @duration, micro_stop = @micro, is_open = 0
            WHERE id = @id;
            """,
            new { id, ended = endedMs, duration = durationSeconds, micro = microStop ? 1 : 0 }), ct);

    public async Task<IReadOnlyList<OpenDowntimeRow>> LoadOpenDowntimeAsync(CancellationToken ct)
    {
        await using var read = db.OpenRead();
        return (await read.QueryAsync<OpenDowntimeRow>(
            """
            SELECT id AS Id, site AS Site, line AS Line, started_ms AS StartedMs,
                   reason_code AS ReasonCode, reason_text AS ReasonText, planned AS Planned
            FROM downtime WHERE is_open = 1
            ORDER BY started_ms;
            """).ConfigureAwait(false)).AsList();
    }

    public async Task<long?> FirstSampleInStateAfterAsync(
        string site, string line, long afterTsMs, LineState state, CancellationToken ct)
    {
        await using var read = db.OpenRead();
        return await read.ExecuteScalarAsync<long?>(
            """
            SELECT ts_ms FROM machine_samples
            WHERE site = @site AND line = @line AND ts_ms > @after AND state = @state
            ORDER BY ts_ms LIMIT 1;
            """,
            new { site, line, after = afterTsMs, state = (int)state }).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<LineStateRow>> LoadLatestLineStatesAsync(CancellationToken ct)
    {
        await using var read = db.OpenRead();
        // One row per line even when two samples share the newest timestamp: the
        // higher seq is the later observation from the same collector.
        return (await read.QueryAsync<LineStateRow>(
            """
            SELECT Site, Line, TsMs, State, StateText, AlarmCode, AlarmText, ProgramId FROM (
                SELECT site AS Site, line AS Line, ts_ms AS TsMs, state AS State,
                       state_text AS StateText, alarm_code AS AlarmCode,
                       alarm_text AS AlarmText, program_id AS ProgramId,
                       ROW_NUMBER() OVER (PARTITION BY site, line ORDER BY ts_ms DESC, seq DESC) AS rn
                FROM machine_samples
            )
            WHERE rn = 1;
            """).ConfigureAwait(false)).AsList();
    }
}
