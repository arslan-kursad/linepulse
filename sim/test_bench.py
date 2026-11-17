"""Checks on the simulated data itself, before any of it reaches the pipeline.

The simulators are the only source of truth this lab has, so a quiet mistake in
them does not produce a failing test somewhere else -- it produces a dashboard
full of numbers that look fine and are wrong. These checks pin the properties
every downstream component depends on:

  * the JSON shape matches LinePulse.Contracts, including the (source, seq)
    idempotency key the ingest side deduplicates on,
  * serials are unique and well formed, since they are the traceability anchor,
  * first-pass yield sits where an automotive assembly line actually sits, and
  * the drifting leak fixture produces a signal statistical process control can
    catch WHILE EVERY PART STILL PASSES.

That last one is the point of the whole quality view and it is easy to get
wrong in the flattering direction. A fixture that drifts fast enough to start
failing parts within a shift needs no statistics at all -- the operator notices
the scrap. So the check is not "does the fixture eventually fail", it is "is the
mean shift large enough for the standard run and trend rules to fire while the
limit is still untouched". Detectability is measured in sigma of the healthy
distribution rather than as a ratio, because sigma is what the control rules
are actually defined against.

Run:
    .venv/bin/python3 sim/test_bench.py
"""

from __future__ import annotations

import collections
import json
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import plant
from mqtt_bench import BenchPublisher

SAMPLE_SIZE = 3000

# A sustained shift of this many sigma is caught decisively by the Western
# Electric run rule (nine consecutive points one side of centre) and by any
# trend test, while remaining far inside a limit that sits at ten sigma.
MIN_DETECTABLE_SHIFT_SIGMA = 1.5
MIN_TREND_SIGMA = 1.0

# Automotive assembly first-pass yield. Below this band the line would be in
# customer-escalation territory; above it there is nothing for a quality view
# to show.
FPY_RANGE = (0.90, 0.999)


class _CollectingClient:
    """Stands in for the MQTT client so records can be inspected directly."""

    def publish(self, topic: str, payload: str, qos: int = 0) -> None:  # noqa: D102
        pass


class Checks:
    def __init__(self) -> None:
        self.failures: list[str] = []

    def expect(self, name: str, ok: bool, detail: str = "") -> None:
        print(f"  {'pass' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
        if not ok:
            self.failures.append(name)


def main() -> int:
    checks = Checks()
    line = plant.LINES_BY_KEY["IZM/L01"]
    publisher = BenchPublisher(line, _CollectingClient())
    records = [
        publisher._record("OP-4412-A", "LOT-26320-001")  # noqa: SLF001 - unit under test
        for _ in range(SAMPLE_SIZE)
    ]

    print("contract shape")
    first = records[0]
    required = {
        "eventId", "seq", "source", "site", "line", "station", "serial",
        "programId", "housingLotId", "fixtureId", "ts", "result",
        "failedChecks", "measurements", "operatorId",
    }
    checks.expect("top-level fields match the contract", required <= set(first),
                  f"missing={sorted(required - set(first))}")
    measurement_fields = {
        "leakRateMbarLs", "leakLimitMbarLs", "flowLpm",
        "flowTargetLpm", "pressureBar", "torqueNm",
    }
    checks.expect("measurement fields present", measurement_fields <= set(first["measurements"]))
    checks.expect("serialises as JSON", bool(json.dumps(first)))
    checks.expect("four monitored screws", len(first["measurements"]["torqueNm"]) == 4)
    checks.expect("source identifies the bench", first["source"] == "bench:IZM/L01",
                  first["source"])

    print("idempotency key")
    keys = [(r["source"], r["seq"]) for r in records]
    checks.expect("(source, seq) is unique and starts at 1",
                  len(set(keys)) == len(keys) and keys[0][1] == 1)

    print("traceability anchor")
    parts = first["serial"].split("-")
    checks.expect("serial is SITE-LINE-YYWW-NNNNNN",
                  len(parts) == 4 and parts[0] == "IZM" and parts[1] == "L01"
                  and len(parts[3]) == 6, first["serial"])
    checks.expect("serials are unique", len({r["serial"] for r in records}) == len(records))

    print("first-pass yield")
    fpy = sum(1 for r in records if r["result"] == "PASS") / len(records)
    low, high = FPY_RANGE
    checks.expect(f"FPY within {low:.0%}-{high:.1%}", low <= fpy <= high, f"FPY={fpy:.3%}")
    reasons = collections.Counter(c for r in records for c in r["failedChecks"])
    print(f"        failure mix: {dict(reasons)}")

    print("leak fixture drift")
    by_fixture: dict[str, list[float]] = collections.defaultdict(list)
    for record in records:
        by_fixture[record["fixtureId"]].append(record["measurements"]["leakRateMbarLs"])
    checks.expect("every fixture was exercised",
                  len(by_fixture) == len(line.fixtures), str(sorted(by_fixture)))

    drifting = line.drifting_fixture
    healthy_values = [v for fx, vals in by_fixture.items() if fx != drifting for v in vals]
    baseline = statistics.mean(healthy_values)
    sigma = statistics.pstdev(healthy_values)

    drift_values = by_fixture[drifting]
    quarter = len(drift_values) // 4
    early = statistics.mean(drift_values[:quarter])
    late = statistics.mean(drift_values[-quarter:])

    shift_sigma = (late - baseline) / sigma
    checks.expect(f"drift is detectable (>={MIN_DETECTABLE_SHIFT_SIGMA} sigma shift)",
                  shift_sigma >= MIN_DETECTABLE_SHIFT_SIGMA,
                  f"{shift_sigma:.2f} sigma; baseline={baseline:.2e} sigma={sigma:.2e} late={late:.2e}")
    trend_sigma = (late - early) / sigma
    checks.expect(f"trend rises monotonically (>={MIN_TREND_SIGMA} sigma)",
                  trend_sigma >= MIN_TREND_SIGMA,
                  f"early={early:.2e} late={late:.2e} delta={trend_sigma:.2f} sigma")

    limit = first["measurements"]["leakLimitMbarLs"]
    exceedances = sum(1 for v in drift_values if v > limit)
    checks.expect("no part exceeded the leak limit, so the warning is genuinely early",
                  exceedances == 0, f"{exceedances}/{len(drift_values)} over {limit:.1e}")

    print()
    if checks.failures:
        print(f"FAILED: {len(checks.failures)} check(s): {checks.failures}")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
