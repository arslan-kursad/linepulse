# A ten-minute walkthrough

The lab is a simulation, and the walkthrough says so in the first minute. What it shows is
real: the pipeline, the arithmetic, and what happens when the plant network misbehaves.

## Before anyone arrives

Start the lab at least ten minutes before you need it, with the drift sped up so the
fixture warning appears inside the walkthrough, and keep the machine awake:

```bash
LP_DRIFT_PER_USE=1e-6 caffeinate -i scripts/run-all.sh
open http://localhost:5080
```

`LP_DRIFT_PER_USE=1e-6` makes the simulated seal wear five times faster than the default.
Measured on a live run with this setting, the drifting fixture (IZM line L01, FX-03) went
from `ok` to `warning` after about 100 seconds and 83 parts, and to `alarm` after about 120
seconds, while every one of its tests still passed and the highest reading was 45% of the
limit. At the default setting it takes roughly five times as long; that was not timed.

If anything goes wrong, `scripts/stop-all.sh` then `scripts/run-all.sh` brings it back in a
minute, and the measured results in the README stand on their own.

## 1. Right now (0:00)

*The line cards.* Each card is a line at a site. Point at one.

> "This is simulated: the controllers and the test benches are programs. Everything after
> them is real. OEE is availability times performance times quality, and I show the three
> separately, because they are three different problems for three different people."

## 2. Where did the time go (1:30)

*The downtime chart and the sentence above it.*

> "Every stop is measured from the controller's own state, whatever its length. The sentence
> above the chart counts the ones under two minutes. A shift sheet records none of those,
> because nobody writes down a forty-second jam, and together they can cost more than the
> breakdowns everyone remembers."

## 3. A fixture drifting (3:00)

*The leak-test fixtures table. FX-03 on IZM/L01 is flagged; the others are `ok`.*

> "A leak fixture's seal wears, and for a long time every part it tests still passes. This
> compares each fixture with its own history and with its sister fixtures. This one is
> flagged, and look at the 'share of limit' column: it is nowhere near the limit. Nothing has
> failed. That is the point: you replace the seal now instead of finding out from scrap."

If asked how sure it is: it needs 25 parts before it says anything, and reports
"insufficient data" before that rather than guessing.

## 4. A part's history (4:30)

*Click one of the "try" buttons under Part genealogy.*

> "A warranty claim names a serial number, years later. This is what the part measured at the
> end of the line, which fixture and operator, which housing lot it was cut from, that lot's
> pass rate, and the machining tool that made it, with its spindle load and remaining life.
> If that tool was worn, the same lot id finds every other pump that got housings from it."

## 5. Two plants (5:30)

*Izmir and Nis, same shift.*

> "Both sites run the same product family, with shift windows in each site's own time zone.
> Turkey stays on UTC+3; Serbia moves between +1 and +2, so a fixed offset would be wrong
> at Nis for half the year. This is what I would look at before travelling to the other plant."

## 6. It keeps working when the network does not (6:30)

The part the numbers on screen cannot show. In a terminal, from the repository root:

```bash
# Watch the collector's spool while you break things
tail -f data/run/collector.log | grep --line-buffered "edge health"
```

In a second terminal:

```bash
tools/chaos.sh nats-down        # the event bus disappears
sleep 20
tools/chaos.sh nats-up          # and comes back
```

> "The collector writes every event to disk before it does anything else. With the bus gone,
> the spool depth climbs; when it returns, the depth drains to zero. The dashboard stops
> updating and then catches up."

```bash
tools/chaos.sh core-down        # now the consumer disappears instead
sleep 15
tools/chaos.sh core-up
```

> "This one I got wrong the first time. My first design published straight to the server and
> treated its acknowledgement as proof the event was safe. I measured it: with this service
> down for 15 seconds, 201 of 861 events, 23%, were accepted and then lost, because a plain
> subscription keeps nothing for a subscriber that is not there. I moved to a persisted
> stream and a durable consumer, and measured the same experiment again: zero lost."

If they want the proof run live (it restarts the lab and takes about a minute and a half):

```bash
.venv/bin/python3 tools/verify.py --scenario order
```

> "Same samples delivered in order to one line and shuffled to another, with a late event
> inside the watermark, one beyond it, and duplicates. The minute buckets must be identical.
> They are."

The full suite (`tools/verify.py` with no arguments) takes about seven minutes; its results
are in the README.

## If you are asked

- *"Did you use Kepware?"* No. The OPC-UA simulator stands where it would, and
  [KEPWARE.md](KEPWARE.md) says what would change. That is the whole answer.
- *"Is this production-ready?"* No: single-process everything, no authentication, SQLite in the
  lab, and the SQL Server schema has not been run. The README lists what it does not claim.
- *"Why not Angular?"* The machine could not comfortably carry the toolchain, and a dashboard
  that works beats one that is planned. It talks to the same API an Angular front end would.
