"""
Sensor health checks for the London air quality pipeline.

Usage:
    python health_checks.py

For every sensor, runs four checks against the readings already in Postgres:
  freshness     how far behind its provider's newest reading is the sensor?
  coverage      how many of the 24 hours up to its provider's newest reading have data?
  flatline      is the sensor stuck repeating the same value?
  plausibility  are values physically sensible?
plus a 'lifecycle' classification (active | quiet | retired). Retired sensors
(no reports for RETIRED_DAYS, or never) are labelled 'retired' and not judged,
so the report highlights sensors that SHOULD report and don't.

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
FRESH_PASS_H = 3        # hours behind the provider's newest reading: <= this = pass
FRESH_WARN_H = 24       # <= this = warn, beyond = fail
PIPE_PASS_H = 3         # pipeline: hours since last successful ingest
PIPE_WARN_H = 8
COVER_PASS = 0.8        # share of last 24 hours with data
COVER_WARN = 0.5
FLAT_WARN = 6           # identical readings in a row
FLAT_FAIL = 12
NEG_FAIL = -10          # below this is impossible; between this and 0 = warn
UPPER_FAIL = 1000       # ug/m3 ceiling (only applied to ug/m3 units)
ACTIVE_DAYS = 3          # reported within this many days = active (matches live polling window)
RETIRED_DAYS = 60       # silent longer than this (or never) = retired
CHECKS = ("freshness", "coverage", "flatline", "plausibility")

log = logging.getLogger("health")


# --- pure check functions (no database, easy to test) ------------------------
def classify_lifecycle(last_reported, last_seen, now):
    """last_reported: OpenAQ's latest reading time for this SENSOR; last_seen: our newest reading."""
    candidates = [t for t in (last_reported, last_seen) if t is not None]
    if not candidates:
        return "retired", None, "never reported"
    days = (now - max(candidates)).total_seconds() / 86400
    if days <= ACTIVE_DAYS:
        status = "active"
    elif days <= RETIRED_DAYS:
        status = "quiet"
    else:
        status = "retired"
    return status, round(days, 1), f"last reported {days:.1f} days ago"


def check_freshness(last_seen, reference):
    """How far behind is this sensor? Measured against the newest reading from the
    same provider (`reference`), not against the clock. That makes the check
    independent of publishing lag and of irregular pipeline runs: a stalled
    pipeline delays every sensor equally, so it does not blame the sensors."""
    if last_seen is None:
        return "fail", None, "sensor has never reported"
    hours = max((reference - last_seen).total_seconds() / 3600, 0)
    if hours <= FRESH_PASS_H:
        status = "pass"
    elif hours <= FRESH_WARN_H:
        status = "warn"
    else:
        status = "fail"
    return status, round(hours, 1), f"{hours:.1f}h behind the newest reading from its provider"


def check_pipeline(last_ok_finished, now):
    """Is the PIPELINE itself current? Based on the last successful ingest run."""
    if last_ok_finished is None:
        return "fail", None, "no successful ingest run recorded"
    hours = (now - last_ok_finished).total_seconds() / 3600
    status = "pass" if hours <= PIPE_PASS_H else "warn" if hours <= PIPE_WARN_H else "fail"
    return status, round(hours, 1), f"last successful ingest {hours:.1f}h ago"


def check_coverage(hours_with_data):
    ratio = min(hours_with_data / 24, 1.0)
    if ratio >= COVER_PASS:
        status = "pass"
    elif ratio >= COVER_WARN:
        status = "warn"
    else:
        status = "fail"
    return status, round(ratio, 2), f"{hours_with_data} of 24 hours have data (up to its provider's newest reading)"


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
    WITH ref AS (   -- newest reading per provider: the yardstick for "the last 24 hours"
        SELECT st.provider, LEAST(MAX(r.measured_at), %(now)s) AS ref_time
        FROM readings r
        JOIN sensors s ON s.sensor_id = r.sensor_id
        JOIN stations st ON st.location_id = s.location_id
        GROUP BY st.provider
    )
    SELECT s.sensor_id, s.units, st.provider, s.last_reported,
           MAX(r.measured_at) AS last_seen,
           COUNT(DISTINCT date_trunc('hour', r.measured_at))
               FILTER (WHERE r.measured_at > ref.ref_time - interval '24 hours'
                         AND r.measured_at <= ref.ref_time) AS hours_24,
           MIN(r.value) FILTER (WHERE r.measured_at > ref.ref_time - interval '24 hours'
                                  AND r.measured_at <= ref.ref_time) AS min_24,
           MAX(r.value) FILTER (WHERE r.measured_at > ref.ref_time - interval '24 hours'
                                  AND r.measured_at <= ref.ref_time) AS max_24
    FROM sensors s
    JOIN stations st ON st.location_id = s.location_id
    LEFT JOIN ref ON ref.provider IS NOT DISTINCT FROM st.provider
    LEFT JOIN readings r ON r.sensor_id = s.sensor_id
    GROUP BY s.sensor_id, s.units, st.provider, s.last_reported
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
        cur.execute("SELECT MAX(finished_at) FROM ingest_runs WHERE status IN ('ok', 'partial')")
        last_ok_ingest = cur.fetchone()[0]
        cur.execute("SELECT sensor_id, check_name, status FROM sensor_health_current")
        previous = {(sid, name): st for sid, name, st in cur.fetchall()}

    # newest reading per provider (capped at now) = the yardstick for freshness
    reference = {}
    for _, _, provider, _, last_seen, *_ in summaries:
        if last_seen is not None:
            reference[provider] = min(max(last_seen, reference.get(provider, last_seen)), now)

    results = []   # (sensor_id, check_name, status, observed, detail)
    for sensor_id, units, provider, last_reported, last_seen, hours_24, min_24, max_24 in summaries:
        lifecycle = classify_lifecycle(last_reported, last_seen, now)
        results.append((sensor_id, "lifecycle", *lifecycle))
        if lifecycle[0] == "retired":
            for name in CHECKS:       # not judged: retired sensors are expected to be silent
                results.append((sensor_id, name, "retired", None, lifecycle[2]))
            continue
        results.append((sensor_id, "freshness", *check_freshness(last_seen, reference.get(provider, now))))
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
    order = ("active", "quiet", "retired", "pass", "warn", "fail", "unknown")
    for name in ("lifecycle",) + CHECKS:
        parts = ", ".join(f"{st}={tally[(name, st)]}" for st in order if tally[(name, st)])
        log.info("%-12s %s", name, parts)
    pipe_status, _, pipe_detail = check_pipeline(last_ok_ingest, now)
    log.info("pipeline     %s: %s", pipe_status, pipe_detail)
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