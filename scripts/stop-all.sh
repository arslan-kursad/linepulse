#!/usr/bin/env bash
# Stops what scripts/run-all.sh started -- by pid file, and nothing else. A process
# that was already running before run-all.sh is not in data/run/ and is left alone.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/data/run"

# Reverse dependency order: stop producers before the things they feed, so the
# collector can close its connections cleanly and leave the spool consistent.
for name in collector bench core opcua broker nats; do
  pidfile="$RUN/$name.pid"
  [[ -f "$pidfile" ]] || continue
  pid="$(cat "$pidfile")"

  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    for _ in $(seq 1 40); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "  $name (pid $pid) ignored SIGTERM for 10 s; killing"
      kill -KILL "$pid" 2>/dev/null || true
    else
      echo "  stopped: $name (pid $pid)"
    fi
  else
    echo "  not running: $name"
  fi
  rm -f "$pidfile"
done
