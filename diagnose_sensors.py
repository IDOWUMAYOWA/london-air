"""
Diagnose sensors that OpenAQ's station date calls "active" but for which we hold no readings.

Usage:
    python diagnose_sensors.py                 # random sample of 6 such sensors
    python diagnose_sensors.py --sample 10
    python diagnose_sensors.py 12345 67890     # specific sensor ids

For each sensor it prints what OUR database holds and what OPENAQ says directly,
so you can tell apart: a bug in our fetching, a misleading station date, or a
sensor that really is silent.
"""
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import psycopg2

import ingest  # reuses the rate-limited api_get and your API key

PICK_SQL = """
    SELECT s.sensor_id, s.location_id, st.name, st.provider, s.parameter
    FROM sensors s
    JOIN stations st ON st.location_id = s.location_id
    JOIN sensor_health_current l ON l.sensor_id = s.sensor_id
         AND l.check_name = 'lifecycle' AND l.status = 'active'
    JOIN sensor_health_current f ON f.sensor_id = s.sensor_id
         AND f.check_name = 'freshness' AND f.status = 'fail' AND f.observed IS NULL
    ORDER BY random() LIMIT %s
"""
INFO_SQL = """
    SELECT s.sensor_id, s.location_id, st.name, st.provider, s.parameter
    FROM sensors s JOIN stations st ON st.location_id = s.location_id
    WHERE s.sensor_id = ANY(%s)
"""


def utc(d):
    return (d or {}).get("utc") if isinstance(d, dict) else None


def diagnose(cur, sensor_id, location_id, name, provider, parameter):
    print(f"\nsensor {sensor_id} | {name} ({provider}) {parameter} | location {location_id}")

    cur.execute("SELECT COUNT(*), MAX(measured_at) FROM readings WHERE sensor_id = %s", (sensor_id,))
    n, newest = cur.fetchone()
    print(f"  our DB:                 {n} readings, newest {newest}")

    try:
        rec = (ingest.api_get(f"/sensors/{sensor_id}", None, 2).get("results") or [{}])[0]
        print(f"  OpenAQ sensor record:   datetimeLast = {utc(rec.get('datetimeLast'))}, "
              f"datetimeFirst = {utc(rec.get('datetimeFirst'))}")
        print(f"                          fields available: {sorted(rec.keys())}")
    except Exception as exc:
        print(f"  OpenAQ sensor record:   ERROR {exc}")

    since = datetime.now(timezone.utc) - timedelta(hours=72)
    try:
        rows = ingest.api_get(f"/sensors/{sensor_id}/measurements",
                              {"datetime_from": since.isoformat(), "limit": 1000}, 2).get("results", [])
        stamps = sorted(
            t for t in (utc((r.get("period") or {}).get("datetimeFrom")) for r in rows) if t
        )
        if stamps:
            print(f"  OpenAQ measurements:    {len(rows)} rows in last 72h "
                  f"({stamps[0]} to {stamps[-1]})")
        else:
            print("  OpenAQ measurements:    0 rows in last 72h")
    except Exception as exc:
        print(f"  OpenAQ measurements:    ERROR {exc}")
    # Is the STATION alive only because of other sensors?
    try:
        latest = ingest.api_get(f"/locations/{location_id}/latest", {"limit": 100}, 2).get("results", [])
        mine = [utc(r.get("datetime")) for r in latest if r.get("sensorsId") == sensor_id]
        others = [utc(r.get("datetime")) for r in latest if r.get("sensorsId") != sensor_id]
        others = [t for t in others if t]
        print(f"  station latest values:  this sensor = {mine[0] if mine and mine[0] else 'none'}; "
              f"{len(others)} other sensor(s) at station, newest = {max(others) if others else 'none'}")
    except Exception as exc:
        print(f"  station latest values:  ERROR {exc}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("ids", nargs="*", type=int)
    parser.add_argument("--sample", type=int, default=6)
    args = parser.parse_args()
    if not os.environ.get("OPENAQ_API_KEY") or not os.environ.get("DATABASE_URL"):
        sys.exit("Set OPENAQ_API_KEY and DATABASE_URL first.")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        with conn.cursor() as cur:
            if args.ids:
                cur.execute(INFO_SQL, (args.ids,))
            else:
                cur.execute(PICK_SQL, (args.sample,))
            sensors = cur.fetchall()
            if not sensors:
                sys.exit("No matching sensors found.")
            for sensor in sensors:
                diagnose(cur, *sensor)
    finally:
        conn.close()


if __name__ == "__main__":
    main()