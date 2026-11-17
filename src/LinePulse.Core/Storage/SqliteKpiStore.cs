using Dapper;

namespace LinePulse.Core.Storage;

/// <summary>SQLite implementation of the API's read path.</summary>
public sealed class SqliteKpiStore(SqliteDb db) : IKpiStore
{
    public async Task<IReadOnlyList<LineRef>> KnownLinesAsync(CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<LineRef>(
            "SELECT DISTINCT site AS Site, line AS Line FROM machine_samples ORDER BY site, line;")
            .ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<LineAggregateRow>> AggregateLinesAsync(
        string site, long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<LineAggregateRow>(
            """
            SELECT site                          AS Site,
                   line                          AS Line,
                   COALESCE(SUM(good), 0)        AS Good,
                   COALESCE(SUM(scrap), 0)       AS Scrap,
                   COALESCE(SUM(run_seconds), 0.0) AS RunSeconds,
                   COALESCE(SUM(down_unplanned_seconds), 0.0) AS DownUnplannedSeconds,
                   COALESCE(SUM(down_planned_seconds), 0.0)   AS DownPlannedSeconds,
                   COALESCE(SUM(ideal_run_seconds), 0.0)      AS IdealRunSeconds,
                   COUNT(*)                      AS Minutes,
                   COALESCE(SUM(revisions), 0)   AS Revisions,
                   COALESCE(SUM(late_beyond_count), 0) AS LateBeyondCount
            FROM oee_minute
            WHERE site = @site AND bucket_start_ms >= @from AND bucket_start_ms < @to
            GROUP BY site, line
            ORDER BY line;
            """,
            new { site, from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<ProductAggregateRow>> AggregateProductsAsync(
        string site, long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<ProductAggregateRow>(
            """
            SELECT site                          AS Site,
                   program_id                    AS ProgramId,
                   COALESCE(SUM(good), 0)        AS Good,
                   COALESCE(SUM(scrap), 0)       AS Scrap,
                   COALESCE(SUM(run_seconds), 0.0) AS RunSeconds,
                   COALESCE(SUM(down_unplanned_seconds), 0.0) AS DownUnplannedSeconds,
                   COALESCE(SUM(down_planned_seconds), 0.0)   AS DownPlannedSeconds,
                   COALESCE(SUM(ideal_run_seconds), 0.0)      AS IdealRunSeconds
            FROM oee_minute
            WHERE site = @site AND bucket_start_ms >= @from AND bucket_start_ms < @to AND program_id <> ''
            GROUP BY site, program_id
            ORDER BY program_id;
            """,
            new { site, from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<MinuteBucketRow>> MinuteBucketsAsync(
        string site, string line, long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<MinuteBucketRow>(
            """
            SELECT bucket_start_ms        AS BucketStartMs,
                   program_id             AS ProgramId,
                   good                   AS Good,
                   scrap                  AS Scrap,
                   samples                AS Samples,
                   run_seconds            AS RunSeconds,
                   down_unplanned_seconds AS DownUnplannedSeconds,
                   down_planned_seconds   AS DownPlannedSeconds,
                   ideal_run_seconds      AS IdealRunSeconds,
                   avg_cycle_ms           AS AvgCycleMs,
                   revisions              AS Revisions,
                   late_beyond_count      AS LateBeyondCount,
                   sealed_at_ms           AS SealedAtMs
            FROM oee_minute
            WHERE site = @site AND line = @line AND bucket_start_ms >= @from AND bucket_start_ms < @to
            ORDER BY bucket_start_ms;
            """,
            new { site, line, from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<DowntimeParetoRow>> DowntimeParetoAsync(
        string site, string line, long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        // An open stop counts in Stops and OpenStops but contributes no seconds: its
        // duration is not a fact yet. Hiding it entirely would be worse -- the stop
        // happening right now is the one the shift leader cares about.
        return (await c.QueryAsync<DowntimeParetoRow>(
            """
            SELECT reason_code                                   AS ReasonCode,
                   MIN(reason_text)                              AS ReasonText,
                   MAX(planned)                                  AS Planned,
                   COUNT(*)                                      AS Stops,
                   SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END)  AS OpenStops,
                   SUM(CASE WHEN micro_stop = 1 THEN 1 ELSE 0 END) AS MicroStops,
                   COALESCE(SUM(duration_s), 0.0)                  AS TotalSeconds,
                   COALESCE(MAX(duration_s), 0.0)                  AS LongestSeconds
            FROM downtime
            WHERE site = @site AND line = @line AND started_ms >= @from AND started_ms < @to
            GROUP BY reason_code
            ORDER BY TotalSeconds DESC, Stops DESC;
            """,
            new { site, line, from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<FpyRow>> FirstPassYieldAsync(long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        // First pass yield means the first attempt, not every attempt. A part that
        // fails, gets reworked and passes on retest is a yield loss -- counting the
        // retest as a pass is how FPY quietly becomes final yield and stops being
        // useful. Hence the join to each serial's earliest record in the window.
        return (await c.QueryAsync<FpyRow>(
            """
            SELECT t.site                                    AS Site,
                   t.line                                    AS Line,
                   COUNT(DISTINCT t.serial)                  AS Tested,
                   SUM(CASE WHEN t.ts_ms = f.first_ms AND t.passed = 1 THEN 1 ELSE 0 END) AS Passed,
                   COUNT(*) - COUNT(DISTINCT t.serial)       AS Retests
            FROM test_records t
            JOIN (
                SELECT serial, MIN(ts_ms) AS first_ms
                FROM test_records
                WHERE ts_ms >= @from AND ts_ms < @to
                GROUP BY serial
            ) f ON f.serial = t.serial
            WHERE t.ts_ms >= @from AND t.ts_ms < @to
            GROUP BY t.site, t.line
            ORDER BY t.site, t.line;
            """,
            new { from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<FailureCountRow>> FailureBreakdownAsync(long fromMs, long toMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<FailureCountRow>(
            """
            SELECT site AS Site, line AS Line, check_name AS CheckName, COUNT(*) AS Count
            FROM test_failures
            WHERE ts_ms >= @from AND ts_ms < @to
            GROUP BY site, line, check_name
            ORDER BY site, line, Count DESC;
            """,
            new { from = fromMs, to = toMs }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<FixtureSampleRow>> FixtureSamplesAsync(
        int windowSize, long sinceMs, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        // Last N parts per fixture. The window function does the "per fixture" part
        // in the database rather than pulling a day of records into memory and
        // slicing them here, which is the difference between one index scan and one
        // table scan per refresh.
        return (await c.QueryAsync<FixtureSampleRow>(
            """
            SELECT Site, Line, FixtureId, TsMs, LeakRate, LeakLimit FROM (
                SELECT site AS Site, line AS Line, fixture_id AS FixtureId, ts_ms AS TsMs,
                       leak_rate AS LeakRate, leak_limit AS LeakLimit,
                       ROW_NUMBER() OVER (PARTITION BY site, line, fixture_id ORDER BY ts_ms DESC) AS rn
                FROM test_records
                WHERE ts_ms >= @since
            )
            WHERE rn <= @window
            ORDER BY Site, Line, FixtureId, TsMs;
            """,
            new { since = sinceMs, window = windowSize }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<TestRecordRow>> TestsForSerialAsync(string serial, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return (await c.QueryAsync<TestRecordRow>(
            """
            SELECT site AS Site, line AS Line, ts_ms AS TsMs, station AS Station, serial AS Serial,
                   program_id AS ProgramId, housing_lot_id AS HousingLotId, fixture_id AS FixtureId,
                   result AS Result, passed AS Passed, leak_rate AS LeakRate, leak_limit AS LeakLimit,
                   flow_lpm AS FlowLpm, flow_target AS FlowTarget, pressure_bar AS PressureBar,
                   torque_json AS TorqueJson, operator_id AS OperatorId, source AS Source, seq AS Seq
            FROM test_records
            WHERE serial = @serial
            ORDER BY ts_ms;
            """,
            new { serial }).ConfigureAwait(false)).AsList();
    }

    public async Task<IReadOnlyList<LotToolRow>> ToolsForLotAsync(string housingLotId, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        // The genealogy join: the machining centre stamped the lot id onto its own
        // samples, so the tool that was in the spindle while that lot was running is
        // recoverable without any extra paperwork on the shop floor.
        return (await c.QueryAsync<LotToolRow>(
            """
            SELECT site AS Site, line AS Line, tool_id AS ToolId,
                   MIN(ts_ms) AS FirstSeenMs, MAX(ts_ms) AS LastSeenMs, COUNT(*) AS Samples,
                   AVG(spindle_load_pct) AS AvgSpindleLoadPct, MIN(tool_life_pct) AS MinToolLifePct
            FROM machine_samples
            WHERE current_lot_id = @lot AND tool_id IS NOT NULL
            GROUP BY site, line, tool_id
            ORDER BY FirstSeenMs;
            """,
            new { lot = housingLotId }).ConfigureAwait(false)).AsList();
    }

    public async Task<LotQualityRow?> LotQualityAsync(string housingLotId, CancellationToken ct)
    {
        await using var c = db.OpenRead();
        return await c.QuerySingleOrDefaultAsync<LotQualityRow>(
            """
            SELECT COUNT(DISTINCT serial) AS Parts, COUNT(*) AS Tested, COALESCE(SUM(passed), 0) AS Passed
            FROM test_records
            WHERE housing_lot_id = @lot;
            """,
            new { lot = housingLotId }).ConfigureAwait(false);
    }
}
