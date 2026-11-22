# Decisions

Short records of choices that were not obvious, with the reason. Newest concerns first within
each group; each says what would make it wrong.

## Reliability

**The idempotency key is `(source, seq)`, never `eventId`.**
A bench that restarts re-mints `eventId` for a part it already reported, so an `eventId`
check lets that duplicate through. The simulator makes this happen on purpose (half of the
injected duplicates carry a fresh `eventId`), and the verification run counts them.
Wrong if a producer cannot keep a stable per-source counter; then there is no safe key, and
`KeyConflict` (same key, different event time) is the symptom worth alerting on.

**Idempotency is guaranteed by the primary key of the raw tables. The in-memory cache is only a
speed-up.**
The check and the write are one statement, so a crash between them cannot turn a redelivery
into a lost event, and two core instances would still agree. The 50,000-entry cache exists so
a retry storm does not reach the database; evicting from it can only make things slower,
never wrong, which is why it can be crude.

**Event time, not arrival time; one watermark per stream, not per plant.**
A bench that reconnects after a blip delivers twenty minutes at once, and bucketing by arrival
would show a dead line then a miracle minute. Each producer stamps events with its own clock,
and a bench and a PLC are routinely seconds apart, so a shared watermark would either seal
minutes before the PLC data arrived or count every record from a slow clock as a revision.
Wrong if producer clocks are wildly off; then the watermark needs a per-source offset.

**Minute buckets are rebuilt from raw rows, never patched.**
A late event can change the minute it belongs to and the one after it (that minute's counter
baseline is the last sample before it). Rebuilding from rows makes the repair exact whatever
order events arrive in, and makes a bucket reproducible when someone challenges it. The cost
is a quadratic rebuild, which is why dirty minutes are coalesced and flushed every 500 ms.

**On restart the core re-queues the last ten minutes of every line.**
The dirty queue is in memory. A process killed between storing an event and flushing its
minute keeps the event and loses the mark. Rebuilding is idempotent, so re-queuing is cheap.

**The collector starts before the benches.**
An MQTT broker keeps messages only for a client that has already subscribed. The first
verification run found the first two records of every bench missing for exactly this reason.
The fix is ordering, not a weaker test.

## Transport

**OPC-UA for machine state and counters; MQTT for per-part test records.**
Each carries what it is good at. A controller exposes values that change and are sampled and
subscribed to; a test bench is a PC that produces one discrete record per part and, in most
plants, leaves it in a file. Swapping them has costs: sending parts as OPC-UA value changes
loses records that arrive between samples, and sending counters as messages invents a stream
of events no controller emits.

**Core NATS, not JetStream, in the lab. This is a known limit, and it is measured.**
The collector deletes a batch from its spool once NATS has acknowledged it. Core NATS keeps
nothing for a subscriber that is not connected, so events published while the core service is
down are accepted and then lost. `tools/verify.py --scenario consumer` measures how many. The
production answer is a persisted stream with a durable consumer (JetStream), which gives a
per-message acknowledgement and replay. The lab stays on core NATS to keep the machine small
and the moving parts few, and says so.

**An in-process MQTT broker (MQTTnet), not Mosquitto.**
Installing Mosquitto through the package manager on this machine started building OpenSSL
from source and installing a second Python, and was still going after many minutes, for what
is, here, a message relay. The collector
speaks standard MQTT 5 and does not care; Mosquitto, EMQX or a plant broker would drop in.

## Storage

**SQLite in the lab, SQL Server in production, behind two interfaces.**
`IEventStore` and `IKpiStore` hold every statement; the minute arithmetic is a pure function
(`MinuteAggregator`) so a second provider only moves rows and cannot compute a different
number. `db/mssql/schema.sql` is written for SQL Server but **has not been executed**: this
machine cannot host it (see below). Its status header says so. Expect a first run to find at
least a typo.

**No Docker.**
An 8 GB laptop with Docker Desktop's VM leaves too little for the rest, and the daemon was
off. Everything runs as native processes started by `scripts/run-all.sh`.

**`scripts/run-all.sh` retries a failed build once.**
A Release build run straight after a Debug build was seen once to resolve a project reference
to the other configuration's output and fail. It never reproduced -- not in the same directory,
not in a fresh checkout, and the disk was not full -- so the mechanism is unproven and no fix
is claimed. A failing build still fails twice and stops the script; the retry exists because
this is started minutes before a demo and six seconds is cheaper than a diagnosis.

## Dashboard

**A single self-contained HTML file, not an Angular application.**
The job description lists Angular. The dashboard is plain JavaScript with hand-written SVG
because the machine cannot comfortably carry an Angular toolchain build, because nothing at
demo time may need the network, and because a dashboard that works beats one that is
planned. It talks to the same HTTP API an Angular front end would, so the swap is a
front-end task and touches nothing else. Wrong if the point of the demonstration is Angular
itself; then this is the least convincing part and should be rebuilt first.

## The laboratory itself

**Simulation speed is a variable, defaulting to 20x.**
A shift cannot be waited for. The simulators run their machines `LP_SPEED` times faster than
the wall clock while stamping events with ordinary time, and the core converts measured
durations back with `Ingest__TimeScale`. At 20x the shortest stop (five production seconds)
lasts 250 ms of wall clock, longer than the 100 ms OPC-UA sampling interval. At 120x it would
last 42 ms and fall between two samples: a subscription cannot see an event shorter than its
sampling interval, in a laboratory or on a real line.

**Configuration arrays have no defaults in code.**
The configuration binder appends a configured array to a default one instead of replacing it.
A default shift list plus the same list in `appsettings.json` became six entries and moved
every shift label after the first onto the wrong shift. The values live in `appsettings.json`.

## Live demo

**Page loads are logged to stdout, nothing else is.**
The owner of a demo sent to a reviewer wants to know whether it was opened. One line per page
load (`visit region=... ip=... path=... ref=... ua=...`) answers that from the platform's log
search, which keeps about seven days. Not the database, which every start wipes, and not a
volume, which would pin the machine and cost money for a few lines a week. API polling and
health checks are left out, or the handful of lines that matter would drown. The referrer is
reduced to its host so a query string never reaches the log. An IP address says roughly where,
not who: two visitors on the same mobile network in the same city look alike, and the time of
the visit is usually the better clue.
