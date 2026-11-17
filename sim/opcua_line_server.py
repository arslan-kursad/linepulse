"""OPC-UA server standing in for the line PLCs.

This is the machine side of the lab. In a real plant these tags come from the
line PLC, usually aggregated by a connectivity layer (Kepware or similar) that
speaks the native PLC protocols and republishes everything as OPC-UA. The
collector does not care which it is talking to -- that is the point of OPC-UA --
so this server occupies exactly the position Kepware would.

Address space:

    Objects/LinePulse/<SITE>/<LINE>/
        State                Int32   0 Idle 1 Running 2 Blocked 3 Faulted 4 Changeover
        StateText            String
        GoodCount            UInt64  monotonic, never resets while the line lives
        ScrapCount           UInt64  monotonic
        CycleTimeMs          Double  last completed cycle
        IdealCycleMs         Double  from the running product
        AlarmCode            Int32   0 when healthy
        AlarmText            String
        ProgramId            String  running product
        ShiftTargetQty       UInt32

    machining centre adds:
        PartCount            UInt64
        SpindleLoadPct       Double
        ToolId               String
        ToolLifeRemainingPct Double
        CurrentLotId         String  housing lot -- the genealogy link

Counters are monotonic on purpose. A collector that reads a counter cannot
assume it starts at zero or that it will not jump: it has to derive production
from deltas and cope with restarts. Handing out monotonic counters keeps that
honest.

Run:
    LP_SPEED=60 python sim/opcua_line_server.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import signal
from datetime import datetime, timezone
from pathlib import Path

from asyncua import Server, ua

from plant import (
    MACHINING_REASONS,
    PLANNED_REASONS,
    PLANT,
    PRODUCTS,
    STATE_TEXT,
    UNPLANNED_REASONS,
    Line,
    LineState,
    speed_factor,
)

LOG = logging.getLogger("sim.opcua")

ENDPOINT = "opc.tcp://0.0.0.0:4840/linepulse/server/"
NAMESPACE = "http://linepulse.local/plant"

# Where the OPC-UA side publishes the little bit of context the MQTT bench
# simulator needs (running product, current housing lot). Real plants solve this
# with a line controller or an MES call; a file keeps the two simulators
# independent without inventing a protocol for the lab.
STATE_FILE = Path(__file__).resolve().parent.parent / "data" / "sim_state.json"

# Per-cycle probabilities, applied once per completed cycle while running.
#
# These are tuned to land where a reasonably well-run plant actually sits rather
# than wherever the dice fell. Expected lost seconds per 7.2 s cycle are
# 0.002*115 + 0.018*49 + 0.008*25 = 1.31, which puts availability near 85%;
# with ~97% performance from cycle-time scatter and ~98.8% quality from scrap,
# OEE lands around 81%. That leaves visible losses to investigate -- the whole
# point of the dashboard -- without portraying a line in crisis. A micro-stop
# every ~15 production minutes is the frequency these losses really occur at.
P_SCRAP = 0.012
P_MICRO_STOP = 0.018        # short jams -- the invisible losses
P_BREAKDOWN = 0.002         # real faults
P_BLOCKED = 0.008           # downstream full
CHANGEOVER_EVERY_PARTS = 900


class LineSimulator:
    """Drives one line's state machine and writes it into the address space."""

    def __init__(self, line: Line, nodes: dict[str, object]) -> None:
        self.line = line
        self.nodes = nodes
        self.state = LineState.IDLE
        self.good = 0
        self.scrap = 0
        self.alarm_code = 0
        self.alarm_text = ""
        self.program_id = line.products[0]
        self.parts_since_changeover = 0
        # Machining centre only.
        self.tool_id = "T-0412"
        self.tool_life_pct = 100.0
        self.lot_seq = 0
        self.current_lot = self._new_lot()

    # -- helpers ---------------------------------------------------------

    @property
    def product(self):
        return PRODUCTS[self.program_id]

    def _new_lot(self) -> str:
        self.lot_seq += 1
        week = datetime.now(timezone.utc).strftime("%y%W")
        return f"LOT-{week}-{self.lot_seq:03d}"

    async def _write(self, name: str, value) -> None:
        node = self.nodes.get(name)
        if node is not None:
            await node.write_value(value)

    async def _publish_state(self) -> None:
        await self._write("State", ua.Variant(int(self.state), ua.VariantType.Int32))
        await self._write("StateText", STATE_TEXT[self.state])
        await self._write("AlarmCode", ua.Variant(self.alarm_code, ua.VariantType.Int32))
        await self._write("AlarmText", self.alarm_text)

    # -- state machine ---------------------------------------------------

    async def _run_cycle(self, speed: float) -> None:
        """Produce one part, with realistic cycle-time scatter."""

        product = self.product
        # Cycle time drifts a little and occasionally runs long; performance
        # loss in OEE comes out of exactly this scatter.
        jitter = random.gauss(1.03, 0.05)
        cycle_s = max(product.ideal_cycle_s * 0.95, product.ideal_cycle_s * jitter)

        await asyncio.sleep(cycle_s / speed)

        if random.random() < P_SCRAP:
            self.scrap += 1
            await self._write("ScrapCount", ua.Variant(self.scrap, ua.VariantType.UInt64))
        else:
            self.good += 1
            await self._write("GoodCount", ua.Variant(self.good, ua.VariantType.UInt64))

        await self._write("CycleTimeMs", round(cycle_s * 1000.0, 1))
        self.parts_since_changeover += 1

        if self.line.is_machining:
            await self._write("PartCount", ua.Variant(self.good, ua.VariantType.UInt64))
            await self._write("SpindleLoadPct", round(random.gauss(68.0, 6.0), 1))
            # Tool wears down and gets swapped, which starts a new housing lot.
            self.tool_life_pct = max(0.0, self.tool_life_pct - random.uniform(0.05, 0.15))
            if self.tool_life_pct < 8.0:
                self.tool_id = f"T-{random.randint(400, 499):04d}"
                self.tool_life_pct = 100.0
                self.current_lot = self._new_lot()
                LOG.info("%s tool change -> %s, lot %s", self.line.key, self.tool_id, self.current_lot)
            await self._write("ToolId", self.tool_id)
            await self._write("ToolLifeRemainingPct", round(self.tool_life_pct, 2))
            await self._write("CurrentLotId", self.current_lot)

    async def _stop(self, speed: float, reasons, state: LineState) -> None:
        reason = random.choice(reasons)
        duration = max(5.0, random.gauss(reason.mean_s, reason.spread_s))
        self.state = state
        self.alarm_code = reason.code
        self.alarm_text = f"{reason.station}: {reason.text}"
        await self._publish_state()
        LOG.info(
            "%s %s %ds (%s)",
            self.line.key,
            STATE_TEXT[state].lower(),
            int(duration),
            reason.text,
        )
        await asyncio.sleep(duration / speed)
        self.alarm_code = 0
        self.alarm_text = ""
        self.state = LineState.RUNNING
        await self._publish_state()

    async def _changeover(self, speed: float) -> None:
        if len(self.line.products) > 1:
            others = [p for p in self.line.products if p != self.program_id]
            self.program_id = random.choice(others)
        reason = PLANNED_REASONS[0]
        duration = max(60.0, random.gauss(reason.mean_s, reason.spread_s))
        self.state = LineState.CHANGEOVER
        self.alarm_code = reason.code
        self.alarm_text = reason.text
        await self._publish_state()
        LOG.info("%s changeover %ds -> %s", self.line.key, int(duration), self.program_id)
        await asyncio.sleep(duration / speed)
        await self._write("ProgramId", self.program_id)
        await self._write("IdealCycleMs", round(self.product.ideal_cycle_s * 1000.0, 1))
        self.parts_since_changeover = 0
        self.alarm_code = 0
        self.alarm_text = ""
        self.state = LineState.RUNNING
        await self._publish_state()

    async def run(self) -> None:
        speed = speed_factor()
        # A machining centre breaks in its own ways: tools, coolant, chips,
        # spindle. Assembly breaks at its stations: feeders, screwdrivers, leak
        # and flow benches, code readers.
        reason_pool = MACHINING_REASONS if self.line.is_machining else UNPLANNED_REASONS
        short_reasons = [r for r in reason_pool if r.mean_s <= 90] or list(reason_pool)

        # Lines do not all start together; stagger them so the data looks real.
        await asyncio.sleep(random.uniform(0.0, 2.0))
        self.state = LineState.RUNNING
        await self._publish_state()

        while True:
            try:
                await self._run_cycle(speed)

                if self.parts_since_changeover >= CHANGEOVER_EVERY_PARTS:
                    await self._changeover(speed)
                elif random.random() < P_BREAKDOWN:
                    await self._stop(speed, reason_pool, LineState.FAULTED)
                elif random.random() < P_MICRO_STOP:
                    await self._stop(speed, short_reasons, LineState.FAULTED)
                elif random.random() < P_BLOCKED:
                    self.state = LineState.BLOCKED
                    await self._publish_state()
                    await asyncio.sleep(random.uniform(10, 40) / speed)
                    self.state = LineState.RUNNING
                    await self._publish_state()
            except asyncio.CancelledError:
                raise
            except Exception:  # keep the simulator alive; a PLC does not crash
                LOG.exception("%s cycle failed", self.line.key)
                await asyncio.sleep(1.0)


async def build_line_nodes(server: Server, idx: int, parent, line: Line) -> dict[str, object]:
    """Create the tag set for one line and return it by name."""

    obj = await parent.add_object(idx, line.line_id)
    product = PRODUCTS[line.products[0]]

    async def var(name: str, value, writable: bool = False):
        node = await obj.add_variable(idx, name, value)
        if writable:
            await node.set_writable()
        return node

    nodes: dict[str, object] = {
        "State": await var("State", ua.Variant(0, ua.VariantType.Int32)),
        "StateText": await var("StateText", "Idle"),
        "GoodCount": await var("GoodCount", ua.Variant(0, ua.VariantType.UInt64)),
        "ScrapCount": await var("ScrapCount", ua.Variant(0, ua.VariantType.UInt64)),
        "CycleTimeMs": await var("CycleTimeMs", 0.0),
        "IdealCycleMs": await var("IdealCycleMs", product.ideal_cycle_s * 1000.0),
        "AlarmCode": await var("AlarmCode", ua.Variant(0, ua.VariantType.Int32)),
        "AlarmText": await var("AlarmText", ""),
        "ProgramId": await var("ProgramId", line.products[0]),
        "ShiftTargetQty": await var(
            "ShiftTargetQty", ua.Variant(line.shift_target_qty, ua.VariantType.UInt32)
        ),
    }

    if line.is_machining:
        nodes.update(
            {
                "PartCount": await var("PartCount", ua.Variant(0, ua.VariantType.UInt64)),
                "SpindleLoadPct": await var("SpindleLoadPct", 0.0),
                "ToolId": await var("ToolId", "T-0412"),
                "ToolLifeRemainingPct": await var("ToolLifeRemainingPct", 100.0),
                "CurrentLotId": await var("CurrentLotId", "LOT-000"),
            }
        )

    return nodes


async def write_shared_state(sims: list[LineSimulator]) -> None:
    """Publish the context the MQTT bench simulator needs, once a second."""

    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    while True:
        payload = {
            "updatedUtc": datetime.now(timezone.utc).isoformat(),
            "lines": {
                sim.line.key: {
                    "programId": sim.program_id,
                    "state": int(sim.state),
                    "good": sim.good,
                    "scrap": sim.scrap,
                }
                for sim in sims
            },
            "lots": {
                sim.line.site: sim.current_lot
                for sim in sims
                if sim.line.is_machining
            },
        }
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.replace(STATE_FILE)  # atomic, so a reader never sees half a file
        await asyncio.sleep(1.0)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(name)s  %(message)s",
        datefmt="%H:%M:%S",
    )
    # The asyncua internals are chatty at INFO.
    logging.getLogger("asyncua").setLevel(logging.WARNING)

    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)
    server.set_server_name("LinePulse Line Simulator")
    # Anonymous, unencrypted: this is a lab. A real deployment uses certificates
    # and a username policy, and the collector already has the config hooks.
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    idx = await server.register_namespace(NAMESPACE)
    root = await server.nodes.objects.add_object(idx, "LinePulse")

    sims: list[LineSimulator] = []
    sites: dict[str, object] = {}
    for line in PLANT:
        if line.site not in sites:
            sites[line.site] = await root.add_object(idx, line.site)
        nodes = await build_line_nodes(server, idx, sites[line.site], line)
        sims.append(LineSimulator(line, nodes))

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    async with server:
        LOG.info("OPC-UA server on %s  ns=%d  speed=%.0fx", ENDPOINT, idx, speed_factor())
        LOG.info("lines: %s", ", ".join(s.line.key for s in sims))
        tasks = [asyncio.create_task(s.run()) for s in sims]
        tasks.append(asyncio.create_task(write_shared_state(sims)))
        await stop.wait()
        LOG.info("shutting down")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
