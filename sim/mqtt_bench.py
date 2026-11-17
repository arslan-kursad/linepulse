"""MQTT publishers standing in for the end-of-line test benches.

Leak and flow benches are PCs, not PLCs. They produce one discrete record per
part -- serial, limits, measured values, verdict -- and in most plants those
records end up in a CSV on the bench or in a vendor database nobody can query.
Publishing them over MQTT is what a digitalisation project actually does first,
and it is why this lab carries two protocols instead of one: counters and state
belong on OPC-UA, per-part records belong on a broker.

Topics:
    linepulse/<site>/<line>/<station>/test

Chaos knobs, because the job description asks for systems that survive
"duplicate messages, out-of-order events" and connectivity loss. Rather than
assert that the pipeline handles those, the simulator can inject them and the
verifier can prove the stored result is still exact:

    LP_DUP_RATE=0.10    republish 10% of records (at-least-once delivery)
    LP_LATE_RATE=0.05   hold 5% of records and release them out of order
    LP_LATE_MAX_S=20    how late a late record may be
    LP_BURST_HZ=0       extra synthetic load, records per second per line
    LP_DUP_NEW_EVENTID=0.5  share of duplicates that carry a freshly minted eventId

The simulator also writes data/bench_truth.json: what it actually minted, republished
and delayed. A verification run compares the pipeline against that file rather than
against its own bookkeeping, because "nothing was lost" can only be claimed relative
to what was sent.

Run:
    LP_SPEED=60 python sim/mqtt_bench.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import uuid
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

from plant import PLANT, PRODUCTS, Line, LineState, speed_factor

LOG = logging.getLogger("sim.bench")

BROKER_HOST = os.environ.get("LP_MQTT_HOST", "127.0.0.1")
BROKER_PORT = int(os.environ.get("LP_MQTT_PORT", "1883"))
STATE_FILE = Path(__file__).resolve().parent.parent / "data" / "sim_state.json"

DUP_RATE = float(os.environ.get("LP_DUP_RATE", "0"))
LATE_RATE = float(os.environ.get("LP_LATE_RATE", "0"))
LATE_MAX_S = float(os.environ.get("LP_LATE_MAX_S", "20"))
BURST_HZ = float(os.environ.get("LP_BURST_HZ", "0"))

# A bench that restarts mid-shift mints a new eventId for a record it already sent,
# so half of the injected duplicates do the same. That makes the (source, seq) key,
# and not the eventId, the thing a deduplicator has to get right.
DUP_NEW_EVENT_ID = float(os.environ.get("LP_DUP_NEW_EVENTID", "0.5"))

TRUTH_FILE = STATE_FILE.parent / "bench_truth.json"

# The most recent publish on the shared client. Waiting for it at shutdown means
# every earlier QoS 1 message on the same connection has been acknowledged too.
_last_publish = None

# Leak-test fixtures drift as their seals age. One fixture per line is allowed
# to drift upward so that statistical process control has a real signal to find.
#
# The rate matters more than it looks. The entire value of the SPC view is
# catching the fixture WHILE PARTS STILL PASS -- nobody needs statistics to
# notice a fixture that is already making scrap, the operator sees that within
# an hour. So the default is tuned so that across a demo run the drifting
# fixture's mean roughly doubles (a signal no trend test can miss) while staying
# several sigma inside the limit and leaving first-pass yield where an
# automotive line actually sits, around 99%.
#
# Wear tracks how often THAT fixture is used, not how many parts the line
# built: a seal ages per test it performs. Raise LP_DRIFT_PER_USE to force the
# fixture over its limit on purpose when demonstrating the alarm state.
DRIFT_PER_USE = float(os.environ.get("LP_DRIFT_PER_USE", "2.0e-7"))


class BenchPublisher:
    """Publishes leak and flow test records for one assembly line."""

    def __init__(self, line: Line, client: mqtt.Client) -> None:
        self.line = line
        self.client = client
        self.seq = 0
        self.part_no = 0
        # How many tests each fixture has performed. Seal wear tracks this, not
        # the line's part count, because fixtures are used in rotation.
        self.fixture_uses: dict[str, int] = {fx: 0 for fx in line.fixtures}
        self.published = 0
        self.duplicated = 0
        self.delayed = 0
        # Late records waiting to be released. Kept on the instance so that shutdown
        # can let them out instead of silently losing parts it had already counted.
        self.pending: set[asyncio.Task] = set()
        self.duplicated_new_event_id = 0

    # -- measurements ----------------------------------------------------

    def _leak_rate(self, program_id: str, fixture: str) -> float:
        """Pressure-decay leak rate in mbar*l/s."""

        product = PRODUCTS[program_id]
        healthy = product.leak_limit_mbar_ls * 0.25
        value = abs(random.gauss(healthy, healthy * 0.30))
        if fixture == self.line.drifting_fixture:
            value += DRIFT_PER_USE * self.fixture_uses[fixture]
        return value

    def _flow(self, program_id: str) -> tuple[float, float]:
        product = PRODUCTS[program_id]
        flow = random.gauss(product.flow_target_lpm, product.flow_tolerance_lpm * 0.35)
        pressure = random.gauss(product.pressure_target_bar, 0.06)
        return flow, pressure

    def _torques(self, program_id: str) -> list[float]:
        product = PRODUCTS[program_id]
        return [
            round(random.gauss(product.torque_target_nm, product.torque_tolerance_nm * 0.30), 2)
            for _ in range(4)
        ]

    # -- records ---------------------------------------------------------

    def _record(self, program_id: str, lot_id: str) -> dict:
        self.seq += 1
        self.part_no += 1

        product = PRODUCTS[program_id]
        fixture = random.choice(self.line.fixtures)
        leak = self._leak_rate(program_id, fixture)
        self.fixture_uses[fixture] += 1
        flow, pressure = self._flow(program_id)
        torques = self._torques(program_id)

        leak_ok = leak <= product.leak_limit_mbar_ls
        flow_ok = abs(flow - product.flow_target_lpm) <= product.flow_tolerance_lpm
        torque_ok = all(
            abs(t - product.torque_target_nm) <= product.torque_tolerance_nm for t in torques
        )
        passed = leak_ok and flow_ok and torque_ok

        week = datetime.now(timezone.utc).strftime("%y%W")
        serial = f"{self.line.site}-{self.line.line_id}-{week}-{self.part_no:06d}"

        failed_checks = [
            name
            for name, ok in (("leak", leak_ok), ("flow", flow_ok), ("torque", torque_ok))
            if not ok
        ]

        return {
            # eventId makes a republish identifiable; (source, seq) is the
            # idempotency key the ingest side actually dedups on, because a
            # bench that restarts may well mint a new eventId for the same part.
            "eventId": str(uuid.uuid4()),
            "seq": self.seq,
            "source": f"bench:{self.line.site}/{self.line.line_id}",
            "site": self.line.site,
            "line": self.line.line_id,
            "station": "ST30/ST40",
            "serial": serial,
            "programId": program_id,
            "housingLotId": lot_id,
            "fixtureId": fixture,
            # Event time, set by the bench. Everything downstream buckets on
            # this, never on arrival time -- that is what makes late records
            # correctable instead of wrong.
            "ts": datetime.now(timezone.utc).isoformat(),
            "result": "PASS" if passed else "FAIL",
            "failedChecks": failed_checks,
            "measurements": {
                "leakRateMbarLs": round(leak, 9),
                "leakLimitMbarLs": product.leak_limit_mbar_ls,
                "flowLpm": round(flow, 2),
                "flowTargetLpm": product.flow_target_lpm,
                "pressureBar": round(pressure, 3),
                "torqueNm": torques,
            },
            "operatorId": f"OP-{random.randint(101, 140)}",
        }

    def _topic(self) -> str:
        return f"linepulse/{self.line.site}/{self.line.line_id}/eol/test"

    def _send(self, record: dict) -> None:
        global _last_publish
        _last_publish = self.client.publish(self._topic(), json.dumps(record), qos=1)
        self.published += 1

    async def _send_late(self, record: dict) -> None:
        """Hold a record back, then release it with its original event time."""

        await asyncio.sleep(random.uniform(2.0, LATE_MAX_S) / speed_factor())
        self._send(record)
        self.delayed += 1

    # -- loop ------------------------------------------------------------

    def _line_context(self) -> tuple[str, str, int] | None:
        """Read the running program and housing lot from the OPC-UA simulator."""

        try:
            state = json.loads(STATE_FILE.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        entry = state.get("lines", {}).get(self.line.key)
        if not entry:
            return None
        lot = state.get("lots", {}).get(self.line.site, "LOT-UNKNOWN")
        return entry.get("programId", self.line.products[0]), lot, entry.get("state", 0)

    async def run(self) -> None:
        speed = speed_factor()

        while True:
            context = self._line_context()
            if context is None:
                # The OPC-UA simulator is not up yet. Benches only produce when
                # the line produces, so wait rather than inventing parts.
                await asyncio.sleep(1.0)
                continue

            program_id, lot_id, state = context
            if state != int(LineState.RUNNING):
                await asyncio.sleep(0.5)
                continue

            product = PRODUCTS[program_id]
            await asyncio.sleep(product.ideal_cycle_s / speed)

            record = self._record(program_id, lot_id)

            if random.random() < LATE_RATE:
                task = asyncio.create_task(self._send_late(record))
                self.pending.add(task)
                task.add_done_callback(self.pending.discard)
            else:
                self._send(record)

            # At-least-once delivery means the same record can arrive twice.
            if random.random() < DUP_RATE:
                copy = dict(record)
                if random.random() < DUP_NEW_EVENT_ID:
                    copy["eventId"] = str(uuid.uuid4())
                    self.duplicated_new_event_id += 1
                self._send(copy)
                self.duplicated += 1

            if BURST_HZ > 0:
                for _ in range(int(BURST_HZ)):
                    self._send(self._record(program_id, lot_id))


def write_truth(publishers: list[BenchPublisher], finished: bool) -> None:
    """Records what was really sent, atomically, so a reader never sees half a file."""

    payload = {
        "updatedUtc": datetime.now(timezone.utc).isoformat(),
        "finished": finished,
        "benches": {
            p.line.key: {
                # Distinct records minted: the number the pipeline must store exactly once.
                "minted": p.seq,
                # Messages put on the wire, duplicates included.
                "published": p.published,
                "duplicated": p.duplicated,
                # Duplicates that also carried a new eventId: the ones an EventId-based
                # deduplicator would have let through.
                "duplicatedNewEventId": p.duplicated_new_event_id,
                "delayed": p.delayed,
            }
            for p in publishers
        },
    }
    tmp = TRUTH_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(TRUTH_FILE)


async def truth_loop(publishers: list[BenchPublisher]) -> None:
    while True:
        write_truth(publishers, finished=False)
        await asyncio.sleep(1.0)


async def report(publishers: list[BenchPublisher]) -> None:
    while True:
        await asyncio.sleep(10.0)
        total = sum(p.published for p in publishers)
        dup = sum(p.duplicated for p in publishers)
        late = sum(p.delayed for p in publishers)
        LOG.info("published=%d duplicated=%d delayed=%d", total, dup, late)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="linepulse-bench-sim")
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=30)
    client.loop_start()
    LOG.info(
        "MQTT %s:%d  speed=%.0fx  dup=%.0f%% late=%.0f%% burst=%.0f/s",
        BROKER_HOST,
        BROKER_PORT,
        speed_factor(),
        DUP_RATE * 100,
        LATE_RATE * 100,
        BURST_HZ,
    )

    publishers = [BenchPublisher(line, client) for line in PLANT if not line.is_machining]
    LOG.info("benches: %s", ", ".join(p.line.key for p in publishers))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    tasks = [asyncio.create_task(p.run()) for p in publishers]
    tasks.append(asyncio.create_task(report(publishers)))
    tasks.append(asyncio.create_task(truth_loop(publishers)))
    await stop.wait()

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

    # Release the records that were being held back. They were minted and counted,
    # so dropping them here would make the pipeline look like it had lost parts.
    held = [task for p in publishers for task in p.pending]
    if held:
        await asyncio.gather(*held, return_exceptions=True)

    if _last_publish is not None:
        _last_publish.wait_for_publish(timeout=5.0)
    write_truth(publishers, finished=True)

    client.loop_stop()
    client.disconnect()
    LOG.info("stopped")


if __name__ == "__main__":
    asyncio.run(main())
