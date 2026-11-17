#!/usr/bin/env bash
# Fault injection for the live demo and for tools/verify.py.
#
#   tools/chaos.sh nats-down        stop the event bus (the collector starts spooling)
#   tools/chaos.sh nats-up          bring it back (the spool drains)
#   tools/chaos.sh broker-down      stop the MQTT broker (the benches cannot publish)
#   tools/chaos.sh broker-up
#   tools/chaos.sh core-down        stop the core service (nobody is subscribed to NATS)
#   tools/chaos.sh core-up          start it again
#   tools/chaos.sh opcua-down       stop the OPC-UA line simulator (a PLC power cycle)
#   tools/chaos.sh opcua-up         start it again, counters back at zero
#   tools/chaos.sh collector-kill9  kill the collector without letting it clean up
#   tools/chaos.sh collector-up     start it again
#   tools/chaos.sh status           which of them is running
#
# Works on the processes scripts/run-all.sh started, through their pid files, and
# touches nothing else.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN="$ROOT/data/run"
export PATH="$HOME/.dotnet:$PATH" DOTNET_CLI_TELEMETRY_OPTOUT=1 DOTNET_NOLOGO=1

alive() { [[ -f "$RUN/$1.pid" ]] && kill -0 "$(cat "$RUN/$1.pid")" 2>/dev/null; }

stop_one() { # name signal
  local name="$1" sig="$2"
  alive "$name" || { echo "$name: not running"; return 0; }
  local pid; pid="$(cat "$RUN/$name.pid")"
  kill "-$sig" "$pid" 2>/dev/null || true
  for _ in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
  rm -f "$RUN/$name.pid"
  echo "$name: stopped (pid $pid, SIG$sig)"
}

start_one() { # name command...
  local name="$1"; shift
  alive "$name" && { echo "$name: already running"; return 0; }
  ( cd "$ROOT" && exec nohup "$@" ) >>"$RUN/$name.log" 2>&1 &
  echo $! >"$RUN/$name.pid"
  echo "$name: started (pid $!)"
}

case "${1:-}" in
  nats-down)        stop_one nats TERM ;;
  nats-up)          start_one nats nats-server -a 127.0.0.1 -p 4222 -js -sd "$ROOT/data/jetstream" ;;
  broker-down)      stop_one broker TERM ;;
  broker-up)        start_one broker dotnet "$ROOT/src/LinePulse.Broker/bin/Release/net8.0/LinePulse.Broker.dll" ;;
  core-down)        stop_one core TERM ;;
  core-up)
    export Ingest__TimeScale="${LP_SPEED:-20}"
    start_one core dotnet "$ROOT/src/LinePulse.Core/bin/Release/net8.0/LinePulse.Core.dll" \
      --contentRoot "$ROOT/src/LinePulse.Core" --Storage:SqlitePath="$ROOT/data/linepulse.db" ;;
  opcua-down)       stop_one opcua TERM ;;
  opcua-up)
    export LP_SPEED="${LP_SPEED:-20}"
    start_one opcua "$ROOT/.venv/bin/python3" "$ROOT/sim/opcua_line_server.py" ;;
  collector-kill9)  stop_one collector KILL ;;
  collector-up)     start_one collector dotnet "$ROOT/src/LinePulse.Collector/bin/Release/net8.0/LinePulse.Collector.dll" ;;
  status)
    for n in nats broker opcua core bench collector; do
      if alive "$n"; then echo "$n: running (pid $(cat "$RUN/$n.pid"))"; else echo "$n: not running"; fi
    done ;;
  *) sed -n '2,19p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
esac
