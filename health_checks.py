"""
Sensor health checks for the London air quality pipeline.

Usage:
    python health_checks.py

For every sensor, runs four checks against the readings already in Postgres:
  freshness     how long since the last reading?
  coverage      how many of the last 24 hours have data?
  flatline      is the sensor stuck repeating the same value?
  plausibility  are values physically sensible?

Each check returns pass | warn | fail | unknown. Results are upserted into
sensor_health_current, and any status CHANGE is logged to sensor_health_events.
All thresholds are constants below, so they are easy to tune.
"""
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

# --- thresholds (tune these) -------------------------------------------------
FRESH_PASS_H = 3        # reading no older than this = pass
FRESH_WARN_H = 24       # older than this = fail, in between = warn
COVER_PASS = 0.8        # share of last 24 hours with data
COVER_WARN = 0.5
FLAT_WARN = 6           # identical readings in a row
FLAT_FAIL = 12
NEG_FAIL = -10          # below this is impossible; between this and 0 = warn
UPPER_FAIL = 1000       # ug/m3 ceiling (only applied to ug/m3 units)

log = logging.getLogger("health")


# --- pure check functions (no database, easy to test) ------------------------
def check_freshness(last_seen, now):
    if last_seen is None:
        return "fail", None, "sensor has never reported"
    hours = (now - last_seen).total_seconds() / 3600
    if hours <= FRESH_PASS_H:
        status = "pass"
    elif hours <= FRESH_WARN_H:
        status = "warn"
    else:
        status = "fail"
    return status, round(hours, 1), f"last reading {hours:.1f}h ago"


def check_coverage(hours_with_data):
    ratio = hours_with_data / 24
    if ratio >= COVER_PASS:
        status = "pass"
    elif ratio >= COVER_WARN:
        status = "warn"
    else:
        status = "fail"
    return status, round(ratio, 2), f"{hours_with_data} of last 24 hours have data"


def trailing_run(values):
    """Length of the run of identical values at the front (values are newest first)."""
    if not values:
        return 0
    run = 1
    for v in values[1:]:
        if v != values[0]:
            break
        run += 1
    return run


def check_flatline(values):
    if len(values) < FLAT_WARN:
        return "unknown", None, "not enough recent readings to judge"
    run = trailing_run(values)
    if run >= FLAT_FAIL:
        status = "fail"
    elif run >= FLAT_WARN:
        status = "warn"
    else:
        status = "pass"
    return status, run, f"{run} identical reading(s) in a row (value {values[0]})"


def check_plausibility(min_v, max_v, units):
    if min_v is None:
        return "unknown", None, "no readings in the last 24h"
    upper_applies = bool(units) and "g/m" in units   # ug/m3 only
    if min_v < NEG_FAIL or (upper_applies and max_v > UPPER_FAIL):
        status = "fail"
    elif min_v < 0:
        status = "warn"
    else:
        status = "pass"
    observed = min_v if min_v < 0 else max_v
    return status, observed, f"min {min_v}, max {max_v} {units or ''}".strip()


# --- database work -----------------------------------------------------------
SUMMARY_SQL = """
    SELECT s.sensor_id, s.units,
           MAX(r.measured_at) AS last_seen,
           COUNT(DISTINCT date_trunc('hour', r.measured_at))
               FILTER (WHERE r.measured_at >= %(now)s - interval '24 hours') AS hours_24,
           MIN(r.value) FILTER (WHERE r.measured_at >= %(now)s - interval '24 hours') AS min_24,
           MAX(r.value) FILTER (WHERE r.measured_at >= %(now)s - interval '24 hours') AS max_24
    FROM sensors s LEFT JOIN readings r USING (sensor_id)
    GROUP BY s.sensor_id, s.units
"""

RECENT_SQL = """
    SELECT sensor_id, value FROM (
        SELECT sensor_id, value,
               ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY measured_at DESC) AS rn
        FROM readings
        WHERE measured_at >= %(now)s - interval '48 hours'
    ) t
    WHERE rn <= %(n)s
    ORDER BY sensor_id, rn
"""


def run_checks(conn):
    now = datetime.now(timezone.utc)

    with conn.cursor() as cur:
        cur.execute(SUMMARY_SQL, {"now": now})
        summaries = cur.fetchall()
        cur.execute(RECENT_SQL, {"now": now, "n": FLAT_FAIL})
        recent = defaultdict(list)           # sensor_id -> values, newest first
        for sensor_id, value in cur.fetchall():
            recent[sensor_id].append(value)
        cur.execute("SELECT sensor_id, check_name, status FROM sensor_health_current")
        previous = {(sid, name): st for sid, name, st in cur.fetchall()}

    results = []   # (sensor_id, check_name, status, observed, detail)
    for sensor_id, units, last_seen, hours_24, min_24, max_24 in summaries:
        results.append((sensor_id, "freshness", *check_freshness(last_seen, now)))
        results.append((sensor_id, "coverage", *check_coverage(hours_24 or 0)))
        results.append((sensor_id, "flatline", *check_flatline(recent.get(sensor_id, []))))
        results.append((sensor_id, "plausibility", *check_plausibility(min_24, max_24, units)))

    events = [
        (sid, name, previous.get((sid, name)), status, observed, detail, now)
        for sid, name, status, observed, detail in results
        if previous.get((sid, name)) != status
    ]

    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO sensor_health_current
                (sensor_id, check_name, status, observed, detail, checked_at)
            VALUES %s
            ON CONFLICT (sensor_id, check_name) DO UPDATE SET
                status = EXCLUDED.status, observed = EXCLUDED.observed,
                detail = EXCLUDED.detail, checked_at = EXCLUDED.checked_at
        """, [(sid, name, st, obs, det, now) for sid, name, st, obs, det in results])
        if events:
            execute_values(cur, """
                INSERT INTO sensor_health_events
                    (sensor_id, check_name, old_status, new_status, observed, detail, changed_at)
                VALUES %s
            """, events)

    tally = Counter((name, status) for _, name, status, _, _ in results)
    for name in ("freshness", "coverage", "flatline", "plausibility"):
        parts = ", ".join(f"{st}={tally[(name, st)]}"
                          for st in ("pass", "warn", "fail", "unknown") if tally[(name, st)])
        log.info("%-12s %s", name, parts)
    log.info("Checked %d sensors, %d status change(s) logged", len(summaries), len(events))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not os.environ.get("DATABASE_URL"):
        sys.exit("Set DATABASE_URL first.")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        run_checks(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
