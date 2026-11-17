"""Plant model for the LinePulse simulator.

Scaled model of an automotive pump plant: oil and vacuum pumps for vehicles.
The real site described in the role (Izmir, Aegean Free Zone) runs 13 assembly
lines plus a machining centre; a second site (Nis, Serbia) runs the same product
family. The simulator models three assembly lines and one machining centre across
two sites -- enough to exercise every code path (per-line KPIs, cross-site
rollup) without pretending to be the real plant.

Station layout per assembly line follows standard pump assembly practice:

    ST10  housing infeed      (fed by the machining centre)
    ST20  rotor / vane assembly, screw-driving with torque monitoring
    ST30  leak test           (pressure-decay, end-of-line)
    ST40  flow / pressure performance test (end-of-line)
    ST50  DMC laser marking and unload

Machine state and counters are exposed over OPC-UA (the PLC's natural protocol).
Discrete per-part test records are published over MQTT (what test benches
actually do -- they are PCs, not PLCs). That split is the point: each protocol
carries the traffic it is good at.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from enum import IntEnum


class LineState(IntEnum):
    """Line state as a PLC would expose it: a small integer plus a text tag."""

    IDLE = 0
    RUNNING = 1
    BLOCKED = 2       # downstream full -- a loss, but not a fault
    FAULTED = 3       # alarm active, needs intervention
    CHANGEOVER = 4    # planned product change


STATE_TEXT = {
    LineState.IDLE: "Idle",
    LineState.RUNNING: "Running",
    LineState.BLOCKED: "Blocked",
    LineState.FAULTED: "Faulted",
    LineState.CHANGEOVER: "Changeover",
}


@dataclass(frozen=True)
class ReasonCode:
    """A downtime reason as maintenance would code it.

    `planned` separates scheduled losses (changeover, maintenance) from
    unplanned ones, because OEE availability treats them differently.
    """

    code: int
    text: str
    station: str
    planned: bool = False
    # Mean duration in production seconds, and the spread around it.
    mean_s: float = 60.0
    spread_s: float = 30.0


# Unplanned stops. The short ones are the interesting part: stops under two
# minutes are the losses most plants never see, because nobody writes them on
# the shift sheet and the MES only logs declared breakdowns.
UNPLANNED_REASONS: tuple[ReasonCode, ...] = (
    ReasonCode(1001, "Leak test fixture seal fault", "ST30", mean_s=240, spread_s=120),
    ReasonCode(1002, "Screwdriver torque NOK / spindle fault", "ST20", mean_s=90, spread_s=45),
    ReasonCode(1003, "Rotor feeder jam", "ST20", mean_s=45, spread_s=25),
    ReasonCode(1004, "DMC reader no-read", "ST50", mean_s=25, spread_s=15),
    ReasonCode(1005, "Housing infeed starved", "ST10", mean_s=70, spread_s=40),
    ReasonCode(1006, "Flow bench calibration due", "ST40", mean_s=300, spread_s=120),
    ReasonCode(3001, "Operator stop", "ST20", mean_s=35, spread_s=20),
)

PLANNED_REASONS: tuple[ReasonCode, ...] = (
    ReasonCode(2001, "Product changeover", "LINE", planned=True, mean_s=1500, spread_s=300),
    ReasonCode(2002, "Planned maintenance", "LINE", planned=True, mean_s=900, spread_s=180),
)

# A machining centre fails in its own ways. Drawing its stops from the assembly
# pool would have it reporting leak-test fixture faults, which is the kind of
# detail a production engineer spots instantly and stops trusting the data over.
MACHINING_REASONS: tuple[ReasonCode, ...] = (
    ReasonCode(4001, "Tool breakage detected", "OP10", mean_s=420, spread_s=180),
    ReasonCode(4002, "Tool life expired, change required", "OP10", mean_s=200, spread_s=80),
    ReasonCode(4003, "Chip conveyor jam", "OP20", mean_s=130, spread_s=60),
    ReasonCode(4004, "Coolant level low", "OP20", mean_s=95, spread_s=45),
    ReasonCode(4005, "Spindle overload trip", "OP10", mean_s=260, spread_s=110),
    ReasonCode(4006, "Pallet changer fault", "OP20", mean_s=60, spread_s=30),
    ReasonCode(4007, "Probe measurement out of tolerance", "OP20", mean_s=40, spread_s=20),
)

REASONS_BY_CODE = {
    r.code: r for r in UNPLANNED_REASONS + PLANNED_REASONS + MACHINING_REASONS
}

# A micro-stop is a stop short enough to be invisible on a shift report.
MICRO_STOP_THRESHOLD_S = 120


@dataclass(frozen=True)
class Product:
    """A pump variant. Ideal cycle time drives the performance side of OEE."""

    program_id: str
    name: str
    ideal_cycle_s: float
    # Leak test limit in mbar*l/s -- pressure-decay benches report leak rate.
    leak_limit_mbar_ls: float
    # Performance test window at the flow bench.
    flow_target_lpm: float
    flow_tolerance_lpm: float
    pressure_target_bar: float
    # Four screws per pump cover, monitored torque.
    torque_target_nm: float = 8.3
    torque_tolerance_nm: float = 0.6


PRODUCTS: dict[str, Product] = {
    "OP-4412-A": Product(
        program_id="OP-4412-A",
        name="Oil pump, gerotor, 12 V",
        ideal_cycle_s=7.2,
        leak_limit_mbar_ls=1.0e-3,
        flow_target_lpm=12.8,
        flow_tolerance_lpm=0.9,
        pressure_target_bar=4.1,
    ),
    "OP-4418-B": Product(
        program_id="OP-4418-B",
        name="Oil pump, gerotor, high flow",
        ideal_cycle_s=8.6,
        leak_limit_mbar_ls=1.0e-3,
        flow_target_lpm=18.4,
        flow_tolerance_lpm=1.2,
        pressure_target_bar=4.4,
    ),
    "VP-2207-A": Product(
        program_id="VP-2207-A",
        name="Vacuum pump, vane, brake booster",
        ideal_cycle_s=9.4,
        leak_limit_mbar_ls=5.0e-4,
        flow_target_lpm=7.6,
        flow_tolerance_lpm=0.6,
        pressure_target_bar=0.9,
    ),
}


@dataclass
class Line:
    """One assembly line, or the machining centre when `is_machining` is set."""

    site: str
    line_id: str
    products: tuple[str, ...]
    shift_target_qty: int
    is_machining: bool = False
    # Test fixtures at the leak bench. One of them is allowed to drift so the
    # statistical-process-control path has something real to catch.
    fixtures: tuple[str, ...] = ("FX-01", "FX-02", "FX-03")
    drifting_fixture: str | None = None
    stations: tuple[str, ...] = field(
        default=("ST10", "ST20", "ST30", "ST40", "ST50")
    )

    @property
    def key(self) -> str:
        return f"{self.site}/{self.line_id}"


# Two sites, because the role explicitly involves supporting another plant
# (colleagues spend one to two weeks in Nis). The same software has to deploy
# per site and roll up to a group view, which is what makes the cross-site
# comparison in the dashboard worth building.
PLANT: tuple[Line, ...] = (
    Line(
        site="IZM",
        line_id="L01",
        products=("OP-4412-A", "OP-4418-B"),
        shift_target_qty=3200,
        drifting_fixture="FX-03",
    ),
    Line(
        site="IZM",
        line_id="L02",
        products=("VP-2207-A",),
        shift_target_qty=2400,
    ),
    Line(
        site="IZM",
        line_id="MC01",
        products=("OP-4412-A",),
        shift_target_qty=4000,
        is_machining=True,
        stations=("OP10", "OP20"),
    ),
    Line(
        site="NIS",
        line_id="L11",
        products=("OP-4412-A",),
        shift_target_qty=2800,
    ),
)

LINES_BY_KEY = {line.key: line for line in PLANT}

SHIFT_DURATION_S = 8 * 3600


def speed_factor() -> float:
    """Simulation speed: how many production seconds pass per wall-clock second.

    A demo cannot wait eight hours for a shift. At the default of 60 an entire
    shift plays out in eight minutes, which is short enough to show live and
    long enough to accumulate statistically meaningful downtime.
    """

    try:
        value = float(os.environ.get("LP_SPEED", "60"))
    except ValueError:
        return 60.0
    return max(1.0, min(value, 600.0))
