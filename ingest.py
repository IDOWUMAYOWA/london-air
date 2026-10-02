"""
Ingest London air quality readings from OpenAQ v3 into Postgres.

Usage:
    # put OPENAQ_API_KEY and DATABASE_URL in a .env file (see .env.example)
    python ingest.py --discover   # first run: find London stations/sensors
    python ingest.py              # hourly run: poll LIVE sensors only (fast)
    python ingest.py --full       # daily sweep: poll ALL sensors (slow)

Design notes:
  * Incremental: per sensor, fetch from the newest stored reading onward.
  * Idempotent: (sensor_id, measured_at) is the primary key, so re-runs are safe.
  * Polite: stays under the free rate limit (60/min, 2,000/hr) and backs off on 429.
  * Fault tolerant: one failing sensor does not stop the run; failures are recorded.
  * Live/stale split: a sensor is "live" if it reported within LIVE_WINDOW_DAYS.
    Normal runs poll live sensors only; --full also polls stale and never-seen
    sensors, so any sensor that comes back online is picked up within a day.
"""
import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()  # reads OPENAQ_API_KEY and DATABASE_URL from .env

API = "https://api.openaq.org/v3"
LONDON_BBOX = "-0.51,51.28,0.33,51.70"   # min_lon,min_lat,max_lon,max_lat
WANTED = {"pm25", "pm10", "no2"}
BACKFILL_HOURS = 48                       # how far back on a sensor's first fetch
LIVE_WINDOW_DAYS = 3                      # reported within this window = "live"
MIN_GAP_SECONDS = 1.1                     # ~54 requests/min, under the 60/min limit
PAGE_SIZE = 1000

log = logging.getLogger("ingest")
session = requests.Session()
session.headers["X-API-Key"] = os.environ.get("OPENAQ_API_KEY", "")


def api_get(path, params=None, retries=5):
    """GET with rate-limit spacing and 429 backoff. Returns parsed JSON."""
    for attempt in range(1, retries + 1):
        resp = session.get(f"{API}{path}", params=params, timeout=30)
        time.sleep(MIN_GAP_SECONDS)
        if resp.status_code == 429:
            wait = int(resp.headers.get("x-ratelimit-reset", 60)) + 1
            log.warning("429 rate limited, sleeping %ss (attempt %s)", wait, attempt)
            time.sleep(wait)
            continue
        if resp.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Gave up on {path} after {retries} attempts")


def discover(conn):
    """Find London locations and their pm25/pm10/no2 sensors; upsert them."""
    stations, sensors = [], []
    page = 1
    while True:
        data = api_get("/locations", {"bbox": LONDON_BBOX, "limit": PAGE_SIZE, "page": page})
        results = data.get("results", [])
        for loc in results:
            coords = loc.get("coordinates") or {}
            if coords.get("latitude") is None:
                continue
            loc_sensors = [
                s for s in loc.get("sensors", [])
                if (s.get("parameter") or {}).get("name") in WANTED
            ]
            if not loc_sensors:
                continue
            stations.append((
                loc["id"], loc.get("name"),
                (loc.get("provider") or {}).get("name"),
                coords["latitude"], coords["longitude"],
                loc.get("isMonitor"),
            ))
            for s in loc_sensors:
                sensors.append((s["id"], loc["id"], s["parameter"]["name"],
                                s["parameter"].get("units")))
        if len(results) < PAGE_SIZE:
            break
        page += 1

    with conn, conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO stations (location_id, name, provider, latitude, longitude, is_monitor)
            VALUES %s
            ON CONFLICT (location_id) DO UPDATE SET
              name = EXCLUDED.name, provider = EXCLUDED.provider,
              latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
              is_monitor = EXCLUDED.is_monitor, updated_at = now()
        """, stations)
        execute_values(cur, """
            INSERT INTO sensors (sensor_id, location_id, parameter, units)
            VALUES %s
            ON CONFLICT (sensor_id) DO UPDATE SET
              parameter = EXCLUDED.parameter, units = EXCLUDED.units
        """, sensors)
    log.info("Discovered %d stations, %d sensors", len(stations), len(sensors))


def fetch_sensor(sensor_id, since):
    """Fetch all measurements for one sensor since a timestamp. Returns rows."""
    rows, page = [], 1
    while True:
        data = api_get(f"/sensors/{sensor_id}/measurements", {
            "datetime_from": since.isoformat(),
            "limit": PAGE_SIZE,
            "page": page,
        })
        results = data.get("results", [])
        for r in results:
            value = r.get("value")
            ts = ((r.get("period") or {}).get("datetimeFrom") or {}).get("utc")
            if value is None or ts is None:
                continue  # skipped here; a null-rate check in week 2 should count these
            rows.append((sensor_id, ts, value))
        if len(results) < PAGE_SIZE:
            break
        page += 1
    return rows


def run(conn, full=False):
    started = datetime.now(timezone.utc)
    with conn, conn.cursor() as cur:
        cur.execute("""
            SELECT s.sensor_id, MAX(r.measured_at)
            FROM sensors s LEFT JOIN readings r USING (sensor_id)
            GROUP BY s.sensor_id
        """)
        watermarks = cur.fetchall()

    total_sensors = len(watermarks)
    if not full:
        live_cutoff = started - timedelta(days=LIVE_WINDOW_DAYS)
        watermarks = [(sid, seen) for sid, seen in watermarks
                      if seen is not None and seen >= live_cutoff]
    mode = "full" if full else "live"
    log.info("Mode=%s: polling %d of %d sensors", mode, len(watermarks), total_sensors)

    default_since = started - timedelta(hours=BACKFILL_HOURS)
    polled = failed = fetched = inserted = 0

    for sensor_id, last_seen in watermarks:
        polled += 1
        since = last_seen if last_seen else default_since
        try:
            rows = fetch_sensor(sensor_id, since)
        except Exception as exc:  # keep going; record the failure
            failed += 1
            log.error("sensor %s failed: %s", sensor_id, exc)
            continue
        fetched += len(rows)
        if not rows:
            continue
        with conn, conn.cursor() as cur:
            # rowcount only reflects the last batch of 100, so count RETURNING rows
            new_rows = execute_values(cur, """
                INSERT INTO readings (sensor_id, measured_at, value)
                VALUES %s ON CONFLICT (sensor_id, measured_at) DO NOTHING
                RETURNING 1
            """, rows, fetch=True)
            inserted += len(new_rows)

    status = "ok" if failed == 0 else ("failed" if failed == polled else "partial")
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ingest_runs (started_at, finished_at, sensors_polled,
                sensors_failed, rows_fetched, rows_inserted, status, mode)
            VALUES (%s, now(), %s, %s, %s, %s, %s, %s)
        """, (started, polled, failed, fetched, inserted, status, mode))
    log.info("Run %s (%s): polled=%d failed=%d fetched=%d inserted=%d",
             status, mode, polled, failed, fetched, inserted)
    return status


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--discover", action="store_true",
                        help="refresh the London stations/sensors list first")
    parser.add_argument("--full", action="store_true",
                        help="poll ALL sensors, including stale ones (daily sweep)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not os.environ.get("OPENAQ_API_KEY") or not os.environ.get("DATABASE_URL"):
        sys.exit("Set OPENAQ_API_KEY and DATABASE_URL first.")

    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    try:
        if args.discover:
            discover(conn)
        # after discovery, new sensors have no readings yet, so force a full poll
        full = args.full or args.discover
        sys.exit(0 if run(conn, full=full) != "failed" else 1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()