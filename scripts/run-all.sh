#!/usr/bin/env bash
# Starts the whole LinePulse lab on one machine, in dependency order, waiting for
# each service to be genuinely ready before starting the next.
#
#   scripts/run-all.sh                 normal run
#   scripts/run-all.sh --chaos         10% duplicate and 5% late records from the benches
#
# Environment:
#   LP_SPEED       production seconds per wall-clock second in the simulators (default 20)
#   LP_DUP_RATE    fraction of bench records republished (default 0, --chaos sets 0.10)
#   LP_LATE_RATE   fraction of bench records released out of order (default 0, --chaos sets 0.05)
#
# Idempotent: a service that is already running is left alone. Everything this
# script starts is recorded under data/run/, and scripts/stop-all.sh stops exactly
# those processes and nothing else.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/data/run"
mkdir -p "$RUN"

export PATH="$HOME/.dotnet:$PATH"
export DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

if [[ "${1:-}" == "--chaos" ]]; then
  export LP_DUP_RATE="${LP_DUP_RATE:-0.10}"
  export LP_LATE_RATE="${LP_LATE_RATE:-0.05}"
fi

# The simulators compress a shift by running their machines LP_SPEED times faster
# than the wall clock while stamping events with ordinary time. Core has to know the
# factor to turn measured durations back into production seconds -- otherwise a
# 30-second jam arrives as 1.5 seconds and is never classified as a stop. One
# variable drives both so they cannot disagree.
#
# 20 is deliberate. The shortest simulated stop is 5 production seconds; at 20x that
# is 250 ms of wall clock, comfortably longer than the collector's 100 ms OPC-UA
# sampling interval. Much faster and short stops fall between two samples, which is
# exactly what a real subscription does to an event shorter than its sampling rate.
export LP_SPEED="${LP_SPEED:-20}"
export Ingest__TimeScale="$LP_SPEED"

PY="$ROOT/.venv/bin/python3"
CORE_DLL="$ROOT/src/LinePulse.Core/bin/Release/net8.0/LinePulse.Core.dll"
BROKER_DLL="$ROOT/src/LinePulse.Broker/bin/Release/net8.0/LinePulse.Broker.dll"
COLLECTOR_DLL="$ROOT/src/LinePulse.Collector/bin/Release/net8.0/LinePulse.Collector.dll"

port_open() { nc -z 127.0.0.1 "$1" >/dev/null 2>&1; }

alive() {
  [[ -f "$RUN/$1.pid" ]] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null
}

# wait_for <what> <timeout-seconds> <command...>
wait_for() {
  local what="$1" timeout="$2"
  shift 2
  local tries=$((timeout * 2)) i=0
  until "$@" >/dev/null 2>&1; do
    i=$((i + 1))
    if (( i >= tries )); then
      echo "  FAILED: $what not ready after ${timeout}s -- see data/run/*.log" >&2
      return 1
    fi
    sleep 0.5
  done
  echo "  ready: $what"
}

# start <name> <cwd> <command...>
start() {
  local name="$1" cwd="$2"
  shift 2
  if alive "$name"; then
    echo "  already running: $name (pid $(cat "$RUN/$name.pid"))"
    return 0
  fi
  ( cd "$cwd" && exec nohup "$@" ) >"$RUN/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
}

health_ok() { curl -sf --max-time 2 http://127.0.0.1:5080/api/health; }

command -v nats-server >/dev/null || { echo "nats-server not found (brew install nats-server)" >&2; exit 1; }
[[ -x "$PY" ]] || { echo "Python venv missing: python3 -m venv .venv && .venv/bin/pip install asyncua paho-mqtt" >&2; exit 1; }

# Build, and retry once if it fails.
#
# Not a mask for broken code: a failing build fails twice and stops the script. It is there
# because a Release build immediately after a Debug one was seen, once, to resolve a project
# reference to the other configuration's output and fail -- never reproduced afterwards, in
# the same directory or a fresh one, so the mechanism is unproven. A demo minutes away is not
# the moment to debug an MSBuild race, and a second attempt costs six seconds.
echo "building (Release)..."
if ! dotnet build "$ROOT/LinePulse.sln" -c Release -nologo -v q > "$RUN/build.log" 2>&1; then
  echo "  build failed; retrying once"
  if ! dotnet build "$ROOT/LinePulse.sln" -c Release -nologo -v q --no-incremental > "$RUN/build.log" 2>&1; then
    echo "  BUILD FAILED twice. The errors are in data/run/build.log:" >&2
    grep -E "error [A-Z]+[0-9]+" "$RUN/build.log" | head -5 >&2
    exit 1
  fi
fi
grep -E "Warning\(s\)|Error\(s\)" "$RUN/build.log" | tail -2

echo "starting (LP_SPEED=$LP_SPEED, dup=${LP_DUP_RATE:-0}, late=${LP_LATE_RATE:-0})"

# 1. Event bus, with JetStream on. The stream is where an event waits for the core
# service when it is down, so its storage directory is part of the lab's state and is
# cleared with the rest of data/ between verification runs.
if port_open 4222 && ! alive nats; then
  echo "  port 4222 is already served by a process this script did not start; using it"
  echo "  (it must have been started with -js, or the collector cannot create its stream)"
else
  start nats "$ROOT" nats-server -a 127.0.0.1 -p 4222 -js -sd "$ROOT/data/jetstream"
fi
wait_for "NATS :4222" 15 port_open 4222

# 2. MQTT broker for the end-of-line benches.
if port_open 1883 && ! alive broker; then
  echo "  port 1883 is already served by a process this script did not start; using it"
else
  start broker "$ROOT" dotnet "$BROKER_DLL"
fi
wait_for "MQTT broker :1883" 20 port_open 1883

# 3. OPC-UA line simulator (stands in for the PLCs, or for Kepware in front of them).
if port_open 4840 && ! alive opcua; then
  echo "  port 4840 is already served by a process this script did not start; using it"
  echo "  (if it was started with a different LP_SPEED, times will not line up -- restart it)"
else
  start opcua "$ROOT/sim" "$PY" opcua_line_server.py
fi
wait_for "OPC-UA server :4840" 60 port_open 4840
wait_for "simulator state file" 30 test -s "$ROOT/data/sim_state.json"

# 4. Core: ingest, KPIs, API, dashboard.
start core "$ROOT" dotnet "$CORE_DLL" \
  --contentRoot "$ROOT/src/LinePulse.Core" \
  --Storage:SqlitePath="$ROOT/data/linepulse.db"
wait_for "Core API :5080" 60 health_ok

# 5. The collector, then the benches -- in that order, and the order matters.
#
# An MQTT broker keeps messages only for a client that has already subscribed. A
# bench that starts publishing before the collector has subscribed has its first
# records dropped by the broker, and no layer downstream can recover what it was
# never given. (The first verification run found exactly this: the first two records
# of every bench were missing.) In a plant the same rule applies to a collector
# that is installed after the benches are already running; a persistent session
# protects against restarts from then on, not against a subscription that never
# existed.
start collector "$ROOT" dotnet "$COLLECTOR_DLL"
wait_for "collector running" 10 alive collector
wait_for "collector subscribed to the benches" 60 grep -q "MQTT subscribed" "$RUN/collector.log"
start bench "$ROOT/sim" "$PY" mqtt_bench.py

echo
echo "up. dashboard: http://localhost:5080   logs: data/run/*.log   stop: scripts/stop-all.sh"
