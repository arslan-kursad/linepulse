"""Turns the reliability claims into measurements.

Everything this lab says about surviving "duplicate messages, out-of-order events
and connectivity issues" is checked here against ground truth the simulators write
down themselves, and the verdict is a table of observed numbers -- not a statement.

Scenarios
---------
chaos   The whole real pipeline, end to end: OPC-UA and MQTT -> collector -> NATS ->
        core -> SQLite. The benches republish 10% of their records (half of them with
        a freshly minted eventId) and hold back 5% to release them out of order, and
        the event bus is stopped for a while mid-run. Afterwards:
          * every distinct part the benches minted is stored exactly once,
          * every injected duplicate is accounted for, at the edge or in the core,
          * nothing was lost across the outage and the spool drained to zero,
          * every machine sample the edge received reached the database.

consumer The core service is stopped for a while mid-run while data keeps flowing. The
        collector deletes a batch from its spool once the stream has stored it, and a
        durable consumer resumes where it left off, so every event the collector handed
        over must end up stored. (With plain publish/subscribe this scenario lost 23% of
        a run: the server accepted events for a subscriber that was not there.)

collector  kill -9 on the collector mid-run: nothing is stored twice, the spool is empty at
        the end, and records published while it was away are not lost.

broker  The MQTT broker is stopped for a while. This scenario MEASURES what survives: the
        benches use QoS 1 on a persistent session, so their client holds the records it
        cannot send and delivers them when the broker returns, and the collector resubscribes
        without being restarted.

plc     The OPC-UA server is power-cycled, so every counter restarts from zero: the
        collector must reconnect by itself and no minute may show negative production.

order   Out-of-order delivery of machine samples, fed straight to the core's input
        through NATS so the arrival order is exactly what the test says it is. The
        same stream of samples is delivered in order to one line and shuffled to
        another -- with a late event inside the watermark, one beyond it, and
        duplicates -- and the minute buckets of the two lines must be identical, and
        must also equal what the samples add up to. A third line has its PLC counter
        reset mid-stream and must never show negative production.

Usage (from the repository root):
    .venv/bin/python3 tools/verify.py                 # both scenarios, then stop the stack
    .venv/bin/python3 tools/verify.py --scenario order
    .venv/bin/python3 tools/verify.py --duration 180 --outage 30 --burst 40 --keep

Exit status is non-zero if any check fails. No number in the output is invented: each
is read from the database, the API, the collector log or the simulators' truth file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import sqlite3
import subprocess
import sys
import threading
import uuid
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RUN = DATA / "run"
DB_PATH = DATA / "linepulse.db"
SPOOL_PATH = DATA / "spool.db"
TRUTH_PATH = DATA / "bench_truth.json"
CORE = "http://127.0.0.1:5080"
NATS_URL = "nats://127.0.0.1:4222"

RECOVERY_GRACE_S = 15

EDGE_LINE = re.compile(
    r"edge health: spool depth=(\d+) \| received opcua=(\d+) bench=(\d+) \| "
    r"spooled=(\d+) duplicatesRejected=(\d+) \| forwarded=(\d+) batchesResent=(\d+) \| "
    r"unparseable=(\d+) counterRestarts=(\d+)"
)


# --------------------------------------------------------------------------- results


@dataclass
class Check:
    name: str
    expected: str
    observed: str
    passed: bool


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)
    facts: list[tuple[str, str]] = field(default_factory=list)

    def check(self, name: str, expected: object, observed: object, passed: bool | None = None) -> bool:
        ok = (expected == observed) if passed is None else passed
        self.checks.append(Check(name, str(expected), str(observed), ok))
        return ok

    def fact(self, name: str, value: object) -> None:
        self.facts.append((name, str(value)))

    def print(self, title: str) -> None:
        print(f"\n=== {title} ===")
        width = max((len(c.name) for c in self.checks), default=10)
        for c in self.checks:
            verdict = "PASS" if c.passed else "FAIL"
            print(f"  {verdict}  {c.name:<{width}}  expected {c.expected:<14} observed {c.observed}")
        if self.facts:
            print("  measured:")
            fwidth = max(len(n) for n, _ in self.facts)
            for name, value in self.facts:
                print(f"    {name:<{fwidth}}  {value}")

    @property
    def failed(self) -> int:
        return sum(1 for c in self.checks if not c.passed)


# ----------------------------------------------------------------------------- plumbing


def sh(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args), cwd=ROOT, env={**os.environ, **(env or {})}, capture_output=True, text=True
    )


def api(path: str) -> dict:
    with urllib.request.urlopen(CORE + path, timeout=10) as response:
        return json.loads(response.read())


def health() -> dict:
    return api("/api/health")


def pid_of(name: str) -> int | None:
    try:
        return int((RUN / f"{name}.pid").read_text())
    except (OSError, ValueError):
        return None


def spool_depth() -> int | None:
    """Rows waiting in the collector's spool, read without disturbing the writer."""

    if not SPOOL_PATH.exists():
        return None
    try:
        with sqlite3.connect(f"file:{SPOOL_PATH}?mode=ro", uri=True, timeout=1) as db:
            return int(db.execute("SELECT COUNT(*) FROM spool").fetchone()[0])
    except sqlite3.Error:
        return None


def db() -> sqlite3.Connection:
    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)


def percentile(sorted_values: list[float], fraction: float) -> float:
    if not sorted_values:
        return float("nan")
    index = min(len(sorted_values) - 1, max(0, round(fraction * (len(sorted_values) - 1))))
    return sorted_values[index]


def wipe_state() -> None:
    sh("scripts/stop-all.sh")
    for name in ("linepulse.db", "linepulse.db-wal", "linepulse.db-shm", "spool.db", "spool.db-wal",
                 "spool.db-shm", "sim_state.json", "bench_truth.json"):
        (DATA / name).unlink(missing_ok=True)
    for pidfile in RUN.glob("*"):
        pidfile.unlink(missing_ok=True)
    # Events left in the stream by an earlier run would be delivered to the fresh database.
    shutil.rmtree(DATA / "jetstream", ignore_errors=True)


def start_stack(env: dict[str, str]) -> None:
    started = sh("scripts/run-all.sh", "--chaos", env=env)
    if started.returncode != 0:
        print(started.stdout, started.stderr)
        raise SystemExit("could not start the stack")


class DepthSampler(threading.Thread):
    """Samples the spool depth once a second so the outage peak is measured, not guessed."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.samples: list[tuple[float, int]] = []
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.is_set():
            depth = spool_depth()
            if depth is not None:
                self.samples.append((time.time(), depth))
            self._halt.wait(0.5)

    def stop(self) -> None:
        self._halt.set()

    @property
    def peak(self) -> int:
        return max((d for _, d in self.samples), default=0)


def latest_edge_line() -> tuple[int, ...] | None:
    log = RUN / "collector.log"
    if not log.exists():
        return None
    last = None
    for line in log.read_text(errors="replace").splitlines():
        match = EDGE_LINE.search(line)
        if match:
            last = tuple(int(g) for g in match.groups())
    return last


def wait_for(predicate, timeout: float, interval: float = 0.25) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def terminate(name: str) -> None:
    pid = pid_of(name)
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        wait_for(lambda: not _alive(pid), 15)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


# ------------------------------------------------------------------------------ chaos


def scenario_chaos(duration: int, outage_start: int, outage: int, burst: int, speed: int) -> Report:
    report = Report()
    env = {
        "LP_SPEED": str(speed),
        "LP_DUP_RATE": "0.10",
        "LP_LATE_RATE": "0.05",
        "LP_BURST_HZ": str(burst),
        # The collector reports every ten seconds by default; a verification run wants
        # the final figures promptly after the system has gone quiet.
        "Collector__Spool__ReportIntervalSeconds": "2",
    }
    wipe_state()
    start_stack(env)

    sampler = DepthSampler()
    sampler.start()

    t0 = time.time()
    down_at = up_at = None
    print(f"running {duration}s: NATS down at {outage_start}s for {outage}s "
          f"(dup 10%, late 5%, burst {burst}/s/line, speed {speed}x)")
    while time.time() - t0 < duration:
        elapsed = time.time() - t0
        if down_at is None and elapsed >= outage_start:
            sh("tools/chaos.sh", "nats-down")
            down_at = time.time()
        if down_at is not None and up_at is None and time.time() - down_at >= outage:
            sh("tools/chaos.sh", "nats-up")
            up_at = time.time()
        time.sleep(0.2)
    if down_at is not None and up_at is None:
        sh("tools/chaos.sh", "nats-up")
        up_at = time.time()

    # Stop producing, then let everything in flight land. The benches release their
    # held-back records on the way out so that no minted part is waiting in memory.
    terminate("bench")
    wait_for(lambda: TRUTH_PATH.exists() and json.loads(TRUTH_PATH.read_text()).get("finished"), 30)
    terminate("opcua")

    def quiescent() -> bool:
        before = health()["counters"]["received"]
        time.sleep(1.0)
        return (spool_depth() == 0
                and health()["counters"]["received"] == before
                and health()["detail"]["dirtyBuckets"] == 0)

    drained = wait_for(quiescent, 90, interval=0.5)
    drained_at = time.time()
    time.sleep(3.0)  # let the collector print one last report and the flush settle
    sampler.stop()

    truth = json.loads(TRUTH_PATH.read_text())
    benches = truth["benches"]
    minted = sum(b["minted"] for b in benches.values())
    published = sum(b["published"] for b in benches.values())
    duplicated = sum(b["duplicated"] for b in benches.values())
    dup_new_id = sum(b["duplicatedNewEventId"] for b in benches.values())
    delayed = sum(b["delayed"] for b in benches.values())

    edge = latest_edge_line()
    counters = health()["counters"]

    with db() as conn:
        stored_tests = conn.execute("SELECT COUNT(*) FROM test_records").fetchone()[0]
        distinct_tests = conn.execute(
            "SELECT COUNT(*) FROM (SELECT DISTINCT source, seq FROM test_records)").fetchone()[0]
        stored_samples = conn.execute("SELECT COUNT(*) FROM machine_samples").fetchone()[0]
        test_ts = [r for r in conn.execute("SELECT ts_ms, ingest_ts_ms FROM test_records")]
        sample_ts = [r for r in conn.execute("SELECT ts_ms, ingest_ts_ms FROM machine_samples")]
        in_outage = conn.execute(
            "SELECT COUNT(*) FROM test_records WHERE ts_ms BETWEEN ? AND ?",
            (int(down_at * 1000), int(up_at * 1000))).fetchone()[0] if down_at and up_at else 0

    report.check("pipeline drained to quiescence", True, drained)
    report.check("distinct parts stored == parts the benches minted", minted, stored_tests)
    report.check("no (source, seq) stored twice", stored_tests, distinct_tests)
    report.check("spool is empty at the end", 0, spool_depth())

    if edge is None:
        report.check("collector reported its counters", True, False)
    else:
        (depth, rx_opcua, rx_bench, spooled, rejected, forwarded, _resent, unparseable, _restarts) = edge
        report.check("collector forwarded everything it spooled", spooled, forwarded)
        report.check("collector received every message the benches published", published, rx_bench)
        report.check("every machine sample the edge received is stored", rx_opcua, stored_samples)
        # Two exact identities rather than one loose sum. Every message the benches republish
        # passes through the collector first, so the edge must reject exactly as many as were
        # injected. The core can legitimately receive more than the collector forwarded: the
        # stream delivers at least once, and an acknowledgement lost while the server restarts
        # makes it deliver a stored message again. Those repeats are the core's to reject, and
        # the surplus must be exactly the number it rejected.
        report.check("duplicates injected == rejected at the edge", duplicated, rejected)
        report.check("messages the core received beyond what the collector forwarded == redeliveries it rejected",
                     counters["received"] - forwarded, counters["deduped"])
        report.check("nothing unparseable", 0, unparseable + counters["malformed"] + counters["missingKey"])

    report.check("some duplicates carried a new eventId (the dedup key is not eventId)",
                 ">0", dup_new_id, passed=dup_new_id > 0)
    report.check("the outage actually spooled something", ">0", sampler.peak, passed=sampler.peak > 0)
    report.check("parts minted during the outage window were stored", ">0", in_outage, passed=in_outage > 0)

    # When did the spool actually reach zero after the bus came back? Read from the
    # once-a-second samples; the end of the shutdown sequence is not a drain time.
    emptied_at = next((t for t, depth in sampler.samples if up_at and t >= up_at and depth == 0), None)

    # Latency: event time stamped at the source -> time the core stored it. Events
    # produced while the bus was down, up to the moment the spool emptied, are
    # reported separately: their latency is the outage, not the pipeline.
    outage_lo = int(down_at * 1000) if down_at else 0
    # The recovery window runs from the moment the bus stopped to a fixed grace after it
    # returned. The grace covers the core re-attaching its consumer and draining the
    # stream; it is a constant chosen before the run, not tuned to the result.
    outage_hi = int(((up_at or drained_at) + RECOVERY_GRACE_S) * 1000)

    def latencies(rows, steady_only: bool) -> list[float]:
        out = [float(i - t) for t, i in rows if i >= t]
        if steady_only:
            out = [float(i - t) for t, i in rows if i >= t and not (outage_lo <= t <= outage_hi)]
        return sorted(out)

    steady_samples = latencies(sample_ts, True)
    steady_tests = latencies(test_ts, True)
    affected = [float(i - t) for t, i in test_ts + sample_ts if outage_lo <= t <= outage_hi and i >= t]

    ingest_seconds = [i // 1000 for _, i in test_ts + sample_ts]
    span = (max(ingest_seconds) - min(ingest_seconds) + 1) if ingest_seconds else 1
    per_second: dict[int, int] = {}
    for s in ingest_seconds:
        per_second[s] = per_second.get(s, 0) + 1

    total_events = len(test_ts) + len(sample_ts)
    report.fact("events stored (machine samples + test records)", f"{total_events} ({len(sample_ts)} + {len(test_ts)})")
    report.fact("bench messages published / minted / duplicated / delayed",
                f"{published} / {minted} / {duplicated} / {delayed}")
    report.fact("duplicates with a fresh eventId", dup_new_id)
    report.fact("redeliveries by the stream, rejected by the core", counters["deduped"])
    report.fact("core counters",
                f"received={counters['received']} ingested={counters['ingested']} deduped={counters['deduped']} "
                f"lateApplied={counters['lateApplied']} lateBeyondWatermark={counters['lateBeyondWatermark']} "
                f"outOfOrderState={counters['outOfOrderState']} microStops={counters['microStops']}")
    report.fact("spool peak depth during the outage", f"{sampler.peak} events")
    if up_at and emptied_at:
        report.fact("outage", f"{outage} s with NATS stopped; the spool was empty again {emptied_at - up_at:.1f} s after it came back")
    else:
        report.fact("outage", "spool did not empty after the bus came back" if up_at else "none")
    report.fact("average throughput", f"{total_events / span:.1f} events/s over {span}s")
    report.fact("peak throughput (1 s)", f"{max(per_second.values(), default=0)} events/s")
    report.fact("latency, machine samples, outside the outage and its recovery (p50 / p99)",
                f"{percentile(steady_samples, 0.50):.0f} ms / {percentile(steady_samples, 0.99):.0f} ms")
    report.fact("latency, test records, outside the outage and its recovery (p50 / p99)",
                f"{percentile(steady_tests, 0.50):.0f} ms / {percentile(steady_tests, 0.99):.0f} ms "
                f"(includes the 5% held back on purpose, up to {20 / speed:.1f}s)")
    if affected:
        report.fact("worst latency of an event produced during the outage or its recovery",
                    f"{max(affected) / 1000:.1f} s (the outage lasted {outage} s)")
    report.fact("machine: the shortest stop is shorter than the OPC-UA sampling interval above this speed",
                f"5 production s / {speed}x = {5000 / speed:.0f} ms vs 100 ms sampling")
    return report


# --------------------------------------------------------------------- outages


def _core_ready() -> bool:
    try:
        health()
        return True
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        return False


def _run_outage(env: dict[str, str], duration: int, down_at_s: int, down_for_s: int,
                down: str, up: str) -> tuple[float, float]:
    """Runs the lab, takes one component away for a while, then lets everything settle."""

    wipe_state()
    start_stack(env)
    t0 = time.time()
    went_down = came_back = None
    while time.time() - t0 < duration:
        elapsed = time.time() - t0
        if went_down is None and elapsed >= down_at_s:
            sh("tools/chaos.sh", down, env=env)
            went_down = time.time()
        if went_down is not None and came_back is None and time.time() - went_down >= down_for_s:
            sh("tools/chaos.sh", up, env=env)
            wait_for(_core_ready, 60)
            came_back = time.time()
        time.sleep(0.2)

    terminate("bench")
    wait_for(lambda: TRUTH_PATH.exists() and json.loads(TRUTH_PATH.read_text()).get("finished"), 30)
    terminate("opcua")

    def quiescent() -> bool:
        before = health()["counters"]["received"]
        time.sleep(1.0)
        return spool_depth() == 0 and health()["counters"]["received"] == before

    wait_for(quiescent, 60, interval=0.5)
    time.sleep(3.0)
    return went_down or t0, came_back or time.time()


def _gaps(conn: sqlite3.Connection, truth: dict) -> dict[str, int]:
    out = {}
    for key, bench in truth.items():
        seqs = {r[0] for r in conn.execute("SELECT seq FROM test_records WHERE source = ?", (f"bench:{key}",))}
        gaps, inside = 0, False
        for seq in range(1, bench["minted"] + 1):
            if seq not in seqs and not inside:
                gaps, inside = gaps + 1, True
            elif seq in seqs:
                inside = False
        out[key] = gaps
    return out


def scenario_consumer(duration: int, down_at_s: int, down_for_s: int, speed: int) -> Report:
    report = Report()
    env = {"LP_SPEED": str(speed), "LP_DUP_RATE": "0", "LP_LATE_RATE": "0",
           "Collector__Spool__ReportIntervalSeconds": "2"}
    print(f"running {duration}s: core stopped at {down_at_s}s for {down_for_s}s (no other faults)")
    _run_outage(env, duration, down_at_s, down_for_s, "core-down", "core-up")

    edge = latest_edge_line()
    truth = json.loads(TRUTH_PATH.read_text())["benches"]
    minted = sum(b["minted"] for b in truth.values())
    with db() as conn:
        stored_tests = conn.execute("SELECT COUNT(*) FROM test_records").fetchone()[0]
        stored_samples = conn.execute("SELECT COUNT(*) FROM machine_samples").fetchone()[0]
        gaps = _gaps(conn, truth)

    if edge is None:
        report.check("collector reported its counters", True, False)
        return report
    (_depth, _rx_opcua, _rx_bench, spooled, _rej, forwarded, _resent, _unp, _rst) = edge
    stored = stored_tests + stored_samples
    lost = forwarded - stored

    report.check("the collector spooled and forwarded everything it received", spooled, forwarded)
    report.check("every event the collector handed over was stored by the core", forwarded, stored)
    report.check("the spool is empty at the end", 0, spool_depth())
    report.check("no bench lost a single record", {k: 0 for k in gaps}, gaps)
    report.fact("outage", f"core stopped for {down_for_s} s while the collector, NATS and the benches kept running")
    report.fact("events handed to NATS by the collector", forwarded)
    report.fact("events stored by the core", f"{stored} ({stored_samples} machine samples + {stored_tests} test records)")
    report.fact("lost between the stream and the database", f"{lost} events ({100 * lost / max(forwarded, 1):.1f}% of the run)")
    report.fact("test records lost", f"{minted - stored_tests} of {minted} minted")
    return report


def scenario_collector_kill(duration: int, down_at_s: int, down_for_s: int, speed: int) -> Report:
    """kill -9 on the collector: what the spool protects, and what nothing can."""

    report = Report()
    env = {"LP_SPEED": str(speed), "LP_DUP_RATE": "0", "LP_LATE_RATE": "0",
           "Collector__Spool__ReportIntervalSeconds": "2"}
    print(f"running {duration}s: collector killed (SIGKILL) at {down_at_s}s, restarted {down_for_s}s later")
    went_down, came_back = _run_outage(env, duration, down_at_s, down_for_s, "collector-kill9", "collector-up")

    truth = json.loads(TRUTH_PATH.read_text())["benches"]
    minted = sum(b["minted"] for b in truth.values())
    with db() as conn:
        stored_tests = conn.execute("SELECT COUNT(*) FROM test_records").fetchone()[0]
        stored_samples = conn.execute("SELECT COUNT(*) FROM machine_samples").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT source, seq FROM test_records)").fetchone()[0]
        gaps = _gaps(conn, truth)
        first_after = conn.execute("SELECT MIN(ts_ms) FROM machine_samples WHERE ts_ms > ?",
                                   (int(went_down * 1000) + 1000,)).fetchone()[0]

    report.check("no record stored twice", stored_tests, distinct)
    report.check("the spool is empty at the end", 0, spool_depth())
    report.check("the collector resumed delivering machine samples after the restart",
                 "yes", "yes" if first_after else "no", passed=bool(first_after))
    lost_tests = minted - stored_tests
    report.fact("outage", f"collector killed with SIGKILL, back after {came_back - went_down:.0f} s; "
                          "benches, NATS and the core kept running")
    report.fact("test records minted / stored / missing", f"{minted} / {stored_tests} / {lost_tests}")
    report.fact("contiguous gaps per bench", gaps)
    report.fact("machine samples stored", stored_samples)
    report.fact("reading this",
                "records published to the broker while the collector was away are held for it only if the broker keeps "
                "the session of a client that was already subscribed; a PLC does not buffer, so machine state changes "
                "during the outage are simply not observed.")
    return report


def scenario_broker(duration: int, down_at_s: int, down_for_s: int, speed: int) -> Report:
    """The MQTT broker is taken away: what survives, and what cannot."""

    report = Report()
    env = {"LP_SPEED": str(speed), "LP_DUP_RATE": "0", "LP_LATE_RATE": "0",
           "Collector__Spool__ReportIntervalSeconds": "2"}
    print(f"running {duration}s: MQTT broker stopped at {down_at_s}s for {down_for_s}s")
    went_down, came_back = _run_outage(env, duration, down_at_s, down_for_s, "broker-down", "broker-up")

    truth = json.loads(TRUTH_PATH.read_text())["benches"]
    minted = sum(b["minted"] for b in truth.values())
    published = sum(b["published"] for b in truth.values())
    with db() as conn:
        stored_tests = conn.execute("SELECT COUNT(*) FROM test_records").fetchone()[0]
        distinct = conn.execute("SELECT COUNT(*) FROM (SELECT DISTINCT source, seq FROM test_records)").fetchone()[0]
        stored_samples = conn.execute("SELECT COUNT(*) FROM machine_samples").fetchone()[0]
        gaps = _gaps(conn, truth)
        after = conn.execute("SELECT COUNT(*) FROM test_records WHERE ts_ms > ?",
                             (int(came_back * 1000),)).fetchone()[0]

    log = (RUN / "collector.log").read_text(errors="replace")
    resubscribed = log.count("MQTT subscribed")

    report.check("no test record was stored twice", stored_tests, distinct)
    report.check("the collector resubscribed by itself after the broker returned",
                 ">=2", resubscribed, passed=resubscribed >= 2)
    report.check("bench records flowed again once the broker was back", ">0", after, passed=after > 0)
    report.check("machine samples were unaffected by the broker outage", ">0", stored_samples, passed=stored_samples > 0)
    report.check("the spool is empty at the end", 0, spool_depth())
    report.fact("outage", f"MQTT broker stopped for {came_back - went_down:.0f} s; OPC-UA, NATS and the core kept running")
    report.fact("bench records minted / published / stored", f"{minted} / {published} / {stored_tests}")
    report.fact("records lost", f"{minted - stored_tests} ({100 * (minted - stored_tests) / max(minted, 1):.1f}% of the run)")
    report.fact("contiguous gaps per bench", gaps)
    report.fact("what this means",
                "the benches publish at QoS 1 on a persistent session, so their client queued the records "
                "it could not send and delivered them when the broker came back. That queue is in memory: "
                "it does not survive a bench restart, and a long enough outage would exhaust it. Neither "
                "of those is tested here.")
    return report


def scenario_plc(speed: int) -> Report:
    """The PLC (here the OPC-UA simulator) is power-cycled: counters restart from zero."""

    report = Report()
    env = {"LP_SPEED": str(speed), "LP_DUP_RATE": "0", "LP_LATE_RATE": "0",
           "Collector__Spool__ReportIntervalSeconds": "2"}
    print("running 70s: the OPC-UA server is stopped at 25s and started again 8s later")
    wipe_state()
    start_stack(env)
    time.sleep(25)
    sh("tools/chaos.sh", "opcua-down", env=env)
    stopped_at = time.time()
    time.sleep(8)
    sh("tools/chaos.sh", "opcua-up", env=env)
    restarted_at = time.time()
    time.sleep(37)

    terminate("bench")
    time.sleep(2)
    edge = latest_edge_line()
    log = (RUN / "collector.log").read_text(errors="replace")
    reconnect = [line for line in log.splitlines() if "reconnect" in line.lower() or "session" in line.lower()]

    with db() as conn:
        resumed = {r[0]: r[1] for r in conn.execute(
            "SELECT site || '/' || line, MAX(ts_ms) FROM machine_samples GROUP BY site, line")}
        negative = conn.execute("SELECT COUNT(*) FROM oee_minute WHERE good < 0 OR scrap < 0").fetchone()[0]
        buckets = conn.execute("SELECT COUNT(*) FROM oee_minute").fetchone()[0]

    all_resumed = len(resumed) == 4 and all(ts > restarted_at * 1000 for ts in resumed.values())
    report.check("the collector reconnected on its own, without being restarted", "yes",
                 "yes" if all_resumed else "no", passed=all_resumed)
    report.check("no minute bucket shows negative production across the counter reset", 0, negative)
    # Did the reset really reach the stored data? Without this, "no negative production" would
    # pass trivially on a run where nothing was reset.
    with db() as conn:
        resets = conn.execute(
            "SELECT COUNT(DISTINCT line) FROM (SELECT site, line, good_count - LAG(good_count) OVER "
            "(PARTITION BY site, line ORDER BY ts_ms) AS d FROM machine_samples) WHERE d < 0").fetchone()[0]
    report.check("the counter reset is present in the stored samples, on every line", 4, resets)
    report.fact("outage", f"OPC-UA server down for {restarted_at - stopped_at:.0f} s, counters restarted from zero")
    if edge:
        report.fact("collector counterRestarts", f"{edge[8]} (counts a reset seen within one session; "
                    "a restarted server means a new session, whose first value only sets the baseline)")
    report.fact("minute buckets stored", buckets)
    report.fact("collector log lines about the session", len(reconnect))
    for line in reconnect[-4:]:
        report.fact("log", line[:150])
    return report


# ------------------------------------------------------------------------------ order

BASE = datetime(2026, 11, 17, 0, 0, 0, tzinfo=timezone.utc)  # minute-aligned
PROGRAM = "OP-4412-A"


def build_stream(restart: bool, speed: int) -> list[dict]:
    """A deterministic six-minute history of one line, in event-time order.

    A tick every 15 s; two stops (a short one that is a micro-stop once converted to
    production time, a long one that is not); one scrap part. The counters are
    monotonic and arbitrary at the start, like a real PLC's. When `restart` is set the
    PLC counter drops back to zero mid-stream.
    """

    # (start offset, duration in event-time seconds, alarm code). Converted to production
    # time by the core, 60 s becomes a micro-stop and 240 s a breakdown. The micro-stop
    # starts off the 15 s tick grid on purpose: two samples with the same timestamp cannot
    # be ordered, so the core refuses the second, and that is not what this test is about.
    stops = [] if restart else [(137.0, 60.0 / speed, 1003), (250.0, 240.0 / speed, 1001)]

    def state_at(offset: float) -> tuple[str, int]:
        # A controller keeps reporting FAULTED, with its alarm, for the whole stop.
        for start, duration, alarm in stops:
            if start <= offset < start + duration:
                return "Faulted", alarm
        return "Running", 0

    offsets = {float(t) for t in range(0, 360, 15)}
    for start, duration, _alarm in stops:
        offsets.update({start, start + duration})
    points = [(offset, *state_at(offset)) for offset in sorted(offsets)]

    good, scrap = 5000, 40
    out: list[dict] = []
    for index, (offset, state, alarm) in enumerate(points):
        if index > 0:
            good += 2
        if offset == 90.0:
            scrap += 1
        if restart and offset == 180.0:
            good = 0  # the PLC was power-cycled: its counter starts again from zero
        out.append({"seq": index + 1, "offset": offset, "state": state, "alarm": alarm,
                    "good": good, "scrap": scrap})
    return out


def payload(site: str, line: str, sample: dict) -> dict:
    moment = BASE + timedelta(seconds=sample["offset"])
    return {
        # The contract says eventId is a GUID. Deterministic, so a redelivery of the same
        # sample carries the same id and two runs produce identical events.
        "eventId": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{site}/{line}/{sample['seq']}")),
        "source": f"opcua:{site}/{line}",
        "seq": sample["seq"],
        "site": site,
        "line": line,
        "ts": moment.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "state": sample["state"],
        "stateText": sample["state"],
        "goodCount": sample["good"],
        "scrapCount": sample["scrap"],
        "cycleTimeMs": 7200.0,
        "idealCycleMs": 7200.0,
        "alarmCode": sample["alarm"],
        "alarmText": "synthetic",
        "programId": PROGRAM,
    }


def shuffled_delivery(stream: list[dict]) -> list[dict]:
    """The same samples with a late event inside the watermark, one beyond it, and duplicates."""

    by_offset = {s["offset"]: s for s in stream}
    held_within = by_offset[105.0]    # delivered after t=120: 15 s late, inside a 30 s watermark
    held_beyond = by_offset[45.0]     # delivered after t=150: 105 s late, far beyond it
    order: list[dict] = []
    for sample in stream:
        if sample is held_within or sample is held_beyond:
            continue
        order.append(sample)
        if sample["offset"] == 120.0:
            order.append(held_within)
        if sample["offset"] == 150.0:
            order.append(held_beyond)
    # At-least-once delivery: three samples arrive a second time, later on.
    order += [by_offset[30.0], by_offset[165.0], by_offset[300.0]]
    return order


BUCKET_FIELDS = ("good", "scrap", "samples", "run_seconds", "down_unplanned_seconds",
                 "down_planned_seconds", "ideal_run_seconds")


def read_buckets(site: str, line: str) -> dict[int, dict]:
    with db() as conn:
        rows = conn.execute(
            f"SELECT bucket_start_ms, {', '.join(BUCKET_FIELDS)}, revisions, late_beyond_count "
            "FROM oee_minute WHERE site = ? AND line = ? ORDER BY bucket_start_ms", (site, line)).fetchall()
    base_ms = int(BASE.timestamp() * 1000)
    out: dict[int, dict] = {}
    for row in rows:
        minute = (row[0] - base_ms) // 60_000
        out[minute] = dict(zip((*BUCKET_FIELDS, "revisions", "late_beyond_count"), row[1:]))
    return out


async def publish(deliveries: list[tuple[str, dict]]) -> None:
    import nats  # imported late so the chaos scenario does not need it

    client = await nats.connect(NATS_URL)
    for subject, body in deliveries:
        await client.publish(subject, json.dumps(body).encode())
        await asyncio.sleep(0.002)
    await client.flush()
    await client.close()


def scenario_order(speed: int, stack_is_up: bool) -> Report:
    report = Report()
    if not stack_is_up:
        wipe_state()
        start_stack({"LP_SPEED": str(speed)})

    # Real traffic would move the very counters this scenario compares.
    terminate("bench")
    terminate("opcua")
    time.sleep(2.0)

    in_order = build_stream(restart=False, speed=speed)
    restart_stream = build_stream(restart=True, speed=speed)
    shuffled = shuffled_delivery(in_order)

    before = health()["counters"]
    deliveries: list[tuple[str, dict]] = []
    deliveries += [("lp.TST.I1.machine", payload("TST", "I1", s)) for s in in_order]
    deliveries += [("lp.TST.S1.machine", payload("TST", "S1", s)) for s in shuffled]
    deliveries += [("lp.TST.R1.machine", payload("TST", "R1", s)) for s in restart_stream]
    expected_new = len(in_order) + len(in_order) + len(restart_stream)
    expected_dups = 3

    asyncio.run(publish(deliveries))

    def landed() -> bool:
        c = health()["counters"]
        return c["ingested"] - before["ingested"] >= expected_new and health()["detail"]["dirtyBuckets"] == 0

    report.check("every synthetic event was processed", True, wait_for(landed, 30))
    time.sleep(1.5)  # one more flush interval, so the last rebuild has been written
    after = health()["counters"]

    report.check("new events ingested", expected_new, after["ingested"] - before["ingested"])
    report.check("redelivered samples rejected", expected_dups, after["deduped"] - before["deduped"])
    report.check("late events seen inside the watermark (>=1)", ">=1", after["lateApplied"] - before["lateApplied"],
                 passed=after["lateApplied"] - before["lateApplied"] >= 1)
    report.check("late events seen beyond the watermark", 1,
                 after["lateBeyondWatermark"] - before["lateBeyondWatermark"])
    report.check("late samples that could not move the stop list backwards", 2,
                 after["outOfOrderState"] - before["outOfOrderState"])

    i1, s1, r1 = read_buckets("TST", "I1"), read_buckets("TST", "S1"), read_buckets("TST", "R1")

    # The whole point: arrival order must not change a single minute of the result.
    differences = []
    for minute in sorted(set(i1) | set(s1)):
        a, b = i1.get(minute), s1.get(minute)
        if a is None or b is None:
            differences.append(f"minute {minute}: present in only one line")
            continue
        for fieldname in BUCKET_FIELDS:
            if abs(float(a[fieldname]) - float(b[fieldname])) > 1e-6:
                differences.append(f"minute {minute} {fieldname}: in-order={a[fieldname]} shuffled={b[fieldname]}")
    report.check("shuffled delivery produces the same minute buckets as in-order", "identical",
                 "identical" if not differences else f"{len(differences)} differences", passed=not differences)
    for line in differences[:6]:
        report.fact("difference", line)

    # And both must equal what the samples add up to: counters telescope.
    total_good = sum(b["good"] for b in i1.values())
    total_scrap = sum(b["scrap"] for b in i1.values())
    expected_good = in_order[-1]["good"] - in_order[0]["good"]
    expected_scrap = in_order[-1]["scrap"] - in_order[0]["scrap"]
    report.check("in-order: total good == last counter - first counter", expected_good, total_good)
    report.check("in-order: total scrap == last counter - first counter", expected_scrap, total_scrap)
    report.check("shuffled: total good", expected_good, sum(b["good"] for b in s1.values()))

    # The sample that arrived beyond the watermark is visible as a revision.
    revised = sum(b["late_beyond_count"] for b in s1.values())
    report.check("the beyond-watermark event is recorded on its bucket", ">=1", revised, passed=revised >= 1)
    report.check("in-order line has no revisions", 0, sum(b["late_beyond_count"] for b in i1.values()))

    # Production is not negative across a PLC restart: the reset drops one delta.
    negative = [m for m, b in r1.items() if b["good"] < 0 or b["scrap"] < 0]
    expected_restart_good = 2 * (len(restart_stream) - 2)
    report.check("counter reset: no bucket shows negative production", 0, len(negative))
    report.check("counter reset: only the reset interval is dropped", expected_restart_good,
                 sum(b["good"] for b in r1.values()))

    # Stops are order-sensitive state; the same two stops must be found on both lines.
    with db() as conn:
        def stops(line: str) -> list[tuple]:
            return conn.execute(
                "SELECT reason_code, ROUND(duration_s, 3), micro_stop FROM downtime "
                "WHERE site = 'TST' AND line = ? ORDER BY started_ms", (line,)).fetchall()
        s_i1, s_s1 = stops("I1"), stops("S1")
    expected_stops = [(1003, 60.0, 1), (1001, 240.0, 0)]
    report.check("in-order stops: 60 s is a micro-stop, 240 s is not", expected_stops, s_i1)
    report.check("shuffled stops match the in-order stops", s_i1, s_s1)
    return report


# ------------------------------------------------------------------------------- main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=("chaos", "order", "consumer", "collector", "broker", "plc", "all"), default="all")
    parser.add_argument("--duration", type=int, default=120, help="seconds of chaos run (default 120)")
    parser.add_argument("--outage-start", type=int, default=40, help="when NATS is stopped (default 40)")
    parser.add_argument("--outage", type=int, default=20, help="how long NATS stays down (default 20)")
    parser.add_argument("--burst", type=int, default=0, help="extra records/s per line (default 0)")
    parser.add_argument("--speed", type=int, default=20, help="simulator speed, LP_SPEED (default 20)")
    parser.add_argument("--keep", action="store_true", help="leave the stack running afterwards")
    args = parser.parse_args()

    reports: list[tuple[str, Report]] = []
    try:
        if args.scenario in ("chaos", "all"):
            reports.append(("chaos: duplicates, late records and a bus outage",
                            scenario_chaos(args.duration, args.outage_start, args.outage, args.burst, args.speed)))
        if args.scenario in ("consumer", "all"):
            reports.append(("consumer: the core is down while data keeps flowing",
                            scenario_consumer(60, 20, 15, args.speed)))
        if args.scenario in ("collector", "all"):
            reports.append(("collector: killed with SIGKILL while data keeps flowing",
                            scenario_collector_kill(60, 20, 10, args.speed)))
        if args.scenario in ("broker", "all"):
            reports.append(("broker: the MQTT broker is stopped (a measured loss)",
                            scenario_broker(60, 20, 15, args.speed)))
        if args.scenario in ("plc", "all"):
            reports.append(("plc: the OPC-UA server is power-cycled and its counters restart",
                            scenario_plc(args.speed)))
        if args.scenario in ("order", "all"):
            # The chaos run stopped the simulators; the stack itself is still up.
            reports.append(("order: shuffled delivery equals in-order delivery",
                            scenario_order(args.speed, stack_is_up=args.scenario == "all")))
    finally:
        if not args.keep:
            sh("scripts/stop-all.sh")

    failed = 0
    for title, report in reports:
        report.print(title)
        failed += report.failed
    total = sum(len(r.checks) for _, r in reports)
    print(f"\n{total - failed}/{total} checks passed" + ("" if not failed else f"  --  {failed} FAILED"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
