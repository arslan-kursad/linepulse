#!/usr/bin/env bash
# Container entrypoint: the whole lab on one machine, started in dependency order.
#
# One process per component, all children of this script. If any of them exits, the
# script exits non-zero and the platform restarts the machine: a half-running pipeline
# that looks healthy from outside is worse than a restart, because the dashboard would
# keep serving numbers that have stopped moving.
#
# The machine also restarts itself every LP_MAX_UPTIME_HOURS (default 12). It normally
# runs only while someone is looking and is stopped when idle, but a tab left open keeps
# it alive, and the lab keeps every event it simulates; the bound stops a forgotten tab
# from growing the database by hundreds of megabytes a day.
set -uo pipefail

APP="${LP_APP_ROOT:-/app}"
DATA="$APP/data"
PY="$APP/.venv/bin/python3"
PORT="${PORT:-8080}"
MAX_UPTIME_HOURS="${LP_MAX_UPTIME_HOURS:-12}"
export LP_SPEED="${LP_SPEED:-20}"
export Ingest__TimeScale="${Ingest__TimeScale:-$LP_SPEED}"

# Every start is a clean lab. The machine is stopped whenever nobody is looking, and if
# the old database survived, the time it was stopped would be read as minutes in which the
# line stood in its last state making nothing: the state-carry rule bills up to an hour of
# a silent line. A fresh start shows a few empty minutes instead of a wrong hour.
rm -rf "$DATA"
mkdir -p "$DATA"
pids=()

log() { echo "[start] $*"; }

# start <name> <cwd> <command...>: runs it in the background, its output both to the
# platform log (prefixed) and to a file the readiness checks can read.
start() {
  local name="$1" cwd="$2"
  shift 2
  ( cd "$cwd" && exec "$@" ) > >(tee "$DATA/$name.log" | sed -u "s/^/[$name] /") 2>&1 &
  pids+=("$!")
  log "started $name (pid $!)"
}

wait_for() { # <what> <seconds> <command...>
  local what="$1" seconds="$2"
  shift 2
  for _ in $(seq 1 "$seconds"); do
    if "$@" >/dev/null 2>&1; then log "ready: $what"; return 0; fi
    sleep 1
  done
  log "FAILED: $what not ready after ${seconds}s"
  exit 1
}

port_open() { nc -z 127.0.0.1 "$1"; }
health_ok() { curl -sf --max-time 2 "http://127.0.0.1:$PORT/api/health"; }

stop_all() {
  log "stopping ${#pids[@]} processes"
  for pid in "${pids[@]}"; do kill -TERM "$pid" 2>/dev/null || true; done
  # Wait for the components only. A bare `wait` would also wait for anything else this
  # shell started, and an earlier version did exactly that on a twelve-hour timer: every
  # component was stopped, the script never exited, and the platform never restarted the
  # machine. A site that is down while its process looks alive is the worst failure mode.
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap 'stop_all; exit 0' TERM INT

log "LinePulse starting: LP_SPEED=$LP_SPEED, port $PORT, planned restart every ${MAX_UPTIME_HOURS}h"

# Event bus with JetStream. Bound to loopback: nothing outside the machine talks to it.
start nats "$APP" nats-server -a 127.0.0.1 -p 4222 -js -sd "$DATA/jetstream"
wait_for "NATS" 30 port_open 4222

# The core goes first, straight after the bus, because it is the only thing a visitor
# reaches. The machine is started by the first request and stopped when idle, so this is
# the cold-start path: the platform's proxy holds that request until something answers on
# the port, and the simulators take the better part of a minute to come up. With the core
# first, the page answers in seconds and the numbers arrive behind it. It needs nothing
# else to serve: its consumer keeps retrying until the collector has created the stream.
start core "$APP" dotnet "$APP/core/LinePulse.Core.dll" \
  --contentRoot "$APP/core" --urls "http://0.0.0.0:$PORT" \
  --Storage:SqlitePath="$DATA/linepulse.db"
wait_for "core API" 120 health_ok

start broker "$APP" dotnet "$APP/broker/LinePulse.Broker.dll"
wait_for "MQTT broker" 60 port_open 1883

start opcua "$APP/sim" "$PY" opcua_line_server.py
wait_for "OPC-UA server" 180 port_open 4840
wait_for "simulator state" 60 test -s "$DATA/sim_state.json"

# Collector before benches: a broker keeps messages only for a client that has already
# subscribed, so a bench that starts first loses its first records.
start collector "$APP" dotnet "$APP/collector/LinePulse.Collector.dll"
wait_for "collector subscribed" 120 grep -q "MQTT subscribed" "$DATA/collector.log"

start bench "$APP/sim" "$PY" mqtt_bench.py

log "all components up"

# Any component exiting ends the machine; the platform's restart policy brings it back.
# A polling loop rather than `wait -n`, which needs bash 4.3: the same script then runs
# unchanged on the Debian image and on a stock macOS shell for local testing.
max_seconds=$((MAX_UPTIME_HOURS * 3600))
while true; do
  # The planned restart is measured here with bash's own clock rather than by a
  # background timer, so there is no extra child to forget to stop.
  if (( SECONDS >= max_seconds )); then
    log "planned restart after ${MAX_UPTIME_HOURS}h"
    stop_all
    exit 3
  fi
  for pid in "${pids[@]}"; do
    if ! kill -0 "$pid" 2>/dev/null; then
      wait "$pid"
      status=$?
      log "component pid $pid exited with status $status; stopping the rest so the machine restarts clean"
      stop_all
      exit 1
    fi
  done
  sleep 2
done
