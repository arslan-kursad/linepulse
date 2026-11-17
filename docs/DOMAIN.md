# The plant problem

LinePulse models the software side of an automotive pump plant: oil pumps and vacuum
pumps for vehicles, assembled on lines fed by a machining centre, at two sites. It is a
laboratory, not a product, and this document says what is modelled, why, and how much of
it is known versus assumed.

## What is known and what is assumed

**Came from a recruiter conversation** (a screening call for a software engineering role at
a pump plant in the Aegean Free Zone, Izmir):

- the plant makes oil pumps and vacuum pumps for vehicles;
- about 400 people work there; there are 13 production lines and a machining centre;
- a four-person software team serves production, planning and risk teams;
- there is a sister plant in Nis, Serbia, and engineers travel between the two sites for
  one-to-two-week support stints.

**Everything else is a modelling assumption** drawn from general automotive assembly
practice: the station layout, cycle times, reason codes, leak-test limits, shift pattern,
and every number the simulators produce. No one at the plant provided data, and the lab
makes no claim about how that plant actually runs. If a detail here looks wrong to someone
who works on such a line, it is the model that is wrong, and the model is meant to be
corrected.

## The line being modelled

```
 machining centre            assembly line                              end of line
 +-------------+    +------+------+------+------+------+
 | OP10  OP20  | -> | ST10 | ST20 | ST30 | ST40 | ST50 |
 | housings    |    | infeed rotor/ leak  flow/  DMC    |
 +-------------+    |      | vane+ test  press. mark    |
   tool wear        |      | screws      test           |
   housing lots     +------+------+------+------+------+
```

- **Machining centre (OP10, OP20).** Cuts the pump housing. Tools wear and are changed;
  every tool change starts a new **housing lot**, which is the unit of traceability for
  everything downstream.
- **ST10 infeed.** Housings arrive from the machining centre.
- **ST20 rotor and vane assembly.** Screws are driven with monitored torque: four screws per
  cover, each with a target and a tolerance.
- **ST30 leak test.** Pressure decay, reported as a leak rate against a limit. Each bench has
  several test fixtures whose seals age.
- **ST40 flow and pressure test.** The pump is run on a bench and must deliver a flow and
  pressure inside a window.
- **ST50 laser marking.** A data-matrix serial goes onto the part. From here on, the serial
  is how the part is found.

Products: two gerotor oil pumps and one vane vacuum pump (brake booster), with different
ideal cycle times and limits (`sim/plant.py`).

## Two ordinary questions that are expensive

### 1. Where did the line's availability go?

OEE is availability times performance times quality. Quality comes from the parts counters
and performance from cycle times; both are visible on any shift report. Availability is
where the quiet losses are.

A shift report records the stops someone wrote down. A stop of forty seconds -- a rotor
feeder jam cleared by an operator, a code reader that failed to read once -- is not written
down by anybody, and a maintenance system that logs only declared breakdowns never sees it.
But they happen every fifteen minutes or so, and their total can exceed the long breakdowns
that everyone remembers.

LinePulse takes machine state straight from the controller and measures every transition,
so a stop is a stop whatever its length. Stops shorter than two minutes are classified as
**micro-stops** and counted separately (`MicroStopThresholdSeconds`, 120 s). The dashboard's
downtime chart shows how many of a shift's stops were under two minutes.

The two-minute threshold is a convention, not a law; the useful property is that it is one
configuration value and the number it produces is reproducible from raw events.

### 2. What was this part, and where did it come from?

A warranty claim arrives months or years after the part was built, naming a serial number.
The questions are: what did the end-of-line tests measure, which fixture and operator, which
housing lot did it come from, and which machining tool cut that lot? And the question behind
it: if that tool was worn, which other pumps received housings from it?

That needs a join across systems that rarely share a key: the test bench (serial, values),
the machining centre (lot, tool) and the line (which lot was feeding at the time). LinePulse
carries the housing lot on every test record and the current lot on every machining sample,
so `GET /api/parts/{serial}` answers in one request: the measurements, the lot, the lot's
pass rate, and the tool that made it with its spindle load and remaining life.

## Catching a fixture while every part still passes

A leak-test fixture's seals wear. A worn seal makes the fixture read slightly high, gradually,
and for a long time every part it tests is still inside the limit. Waiting for a part to fail
is waiting for scrap.

LinePulse watches each fixture two ways: against its own recent history (drift and projected
time to the limit) and against its sister fixtures testing the same pumps. A fixture that reads
a statistically real amount higher than its peers, while the peers show nothing, is flagged
before it produces a single failure. The simulator drifts one fixture on purpose and the check
in `sim/test_bench.py` asserts that the drift is detectable while no part exceeds the limit.

## Two sites

Both sites run the same product family under the same software. Shift windows follow each
site's own time zone (Turkey stays on UTC+3 all year; Serbia moves between UTC+1 and UTC+2),
so "this shift" is two different UTC windows and a comparison of the two plants is a
comparison of like with like. The dashboard's site comparison and `GET /api/sites/rollup`
exist because supporting the other plant is part of the work, and doing it blind is slow.

## What this is not

It is not an MES and does not schedule anything. It does not drive a machine or write to a
controller. It reads state and measurements, keeps them exactly once, and turns them into
numbers a production engineer can argue with.
