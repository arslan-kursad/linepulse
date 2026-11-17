"""Smoke test for every route of the Core API.

Each route is called with three kinds of window -- the default, one that is in the
future (so it contains no data at all) and one that reaches far into the past -- and
must answer 200 with valid JSON each time.

The empty window matters most. Aggregate queries over no rows are where a typed
mapper meets a database that answers with a different numeric type, and that class
of failure only appears when a line has been quiet, a stop is still open, or the
database is new: exactly the conditions a demo hits and a developer's warm database
never does.

Run (with the stack up):
    .venv/bin/python3 tools/api_smoke.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE = "http://127.0.0.1:5080"
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "linepulse.db"


def iso(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%dT%H:%M:%SZ")


def get(path: str, **query: str) -> tuple[int, object]:
    url = BASE + path + ("?" + urllib.parse.urlencode(query) if query else "")
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            body = response.read()
            return response.status, json.loads(body) if body else None
    except urllib.error.HTTPError as error:
        return error.code, None
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as error:
        return -1, str(error)


SHIFT_STARTS = (6, 14, 22)
SHIFT_LABELS = ("A", "B", "C")
SITE_ZONES = {"IZM": "Europe/Istanbul", "NIS": "Europe/Belgrade"}


def expected_shift(site: str, now: datetime) -> tuple[str, datetime, datetime]:
    """The shift containing `now`, worked out independently of the service.

    A second implementation on purpose: it uses the platform's own time-zone
    database, so it agrees with the service only if the service handled the zone, the
    midnight crossing and daylight saving correctly, and it is what caught a
    configuration array being appended to its own default.
    """

    zone = ZoneInfo(SITE_ZONES[site])
    local = now.astimezone(zone)
    index = max((i for i, hour in enumerate(SHIFT_STARTS) if hour <= local.hour), default=len(SHIFT_STARTS) - 1)
    day = local.date() - timedelta(days=1) if local.hour < SHIFT_STARTS[0] else local.date()
    start = datetime(day.year, day.month, day.day, SHIFT_STARTS[index], tzinfo=zone)
    next_index = (index + 1) % len(SHIFT_STARTS)
    end_day = start.date() + timedelta(days=1) if next_index == 0 else start.date()
    end = datetime(end_day.year, end_day.month, end_day.day, SHIFT_STARTS[next_index], tzinfo=zone)
    return SHIFT_LABELS[index], start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def main() -> int:
    now = datetime.now(timezone.utc)
    windows = {
        "default": {},
        "empty (future)": {"from": iso(now + timedelta(days=30)), "to": iso(now + timedelta(days=31))},
        "wide (past)": {"from": iso(now - timedelta(days=365)), "to": iso(now + timedelta(hours=1))},
    }
    lines = [("IZM", "L01"), ("IZM", "L02"), ("IZM", "MC01"), ("NIS", "L11")]

    routes: list[tuple[str, str, dict[str, str]]] = []
    for label, window in windows.items():
        routes.append((f"health [{label}]", "/api/health", {}))
        routes.append((f"lines [{label}]", "/api/lines", {}))
        routes.append((f"fpy [{label}]", "/api/quality/fpy", window))
        routes.append((f"fixtures [{label}]", "/api/quality/fixtures", {}))
        routes.append((f"rollup [{label}]", "/api/sites/rollup", window))
        for site, line in lines:
            routes.append((f"oee {site}/{line} [{label}]", f"/api/lines/{site}/{line}/oee", window))
            routes.append((f"downtime {site}/{line} [{label}]", f"/api/lines/{site}/{line}/downtime", window))

    failures: list[str] = []
    for name, path, query in routes:
        status, body = get(path, **query)
        if status != 200 or body is None:
            failures.append(f"{name}: HTTP {status} {body if status == -1 else ''}".strip())

    # Shift calendar: label and boundaries per site, against an independent calculation.
    status, rollup = get("/api/sites/rollup")
    if status == 200 and isinstance(rollup, list):
        for entry in rollup:
            site = entry.get("site")
            if site not in SITE_ZONES:
                continue
            label, start, end = expected_shift(site, datetime.now(timezone.utc))
            got_start = datetime.fromisoformat(entry["fromUtc"])
            got_end = datetime.fromisoformat(entry["toUtc"])
            if entry.get("shift") != label or got_start != start or got_end != end:
                failures.append(
                    f"shift at {site}: service says {entry.get('shift')} {got_start:%H:%M}-{got_end:%H:%M} UTC, "
                    f"expected {label} {start:%H:%M}-{end:%H:%M} UTC")

    # Unknown part: a clean 404, not a 500.
    status, _ = get("/api/parts/NO-SUCH-PART-000000")
    if status != 404:
        failures.append(f"unknown part: expected 404, got {status}")

    # Genealogy from real data: a part's test values, its housing lot and the
    # machining tool that produced the lot.
    genealogy_note = "skipped (no database yet)"
    if DB_PATH.exists():
        with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as db:
            row = db.execute("SELECT serial FROM test_records ORDER BY ts_ms DESC LIMIT 1").fetchone()
        if row:
            status, part = get(f"/api/parts/{row[0]}")
            if status != 200 or not isinstance(part, dict):
                failures.append(f"genealogy for {row[0]}: HTTP {status}")
            else:
                text = json.dumps(part)
                genealogy_note = f"{row[0]} -> keys {sorted(part)[:6]}"
                for needle in ("housingLot", "leak"):
                    if needle.lower() not in text.lower():
                        failures.append(f"genealogy for {row[0]} lacks '{needle}'")

    print(f"{len(routes) + 2} requests, {len(failures)} failure(s)")
    print(f"genealogy: {genealogy_note}")
    for failure in failures:
        print(f"  FAIL  {failure}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
