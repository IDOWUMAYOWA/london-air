"""
London air quality: which sensors can you trust?

Run locally:   streamlit run dashboard/app.py
Reads the tables built by ingest.py and health_checks.py (read-only).
"""
import os
from contextlib import closing

import pandas as pd
import psycopg2
import streamlit as st
from dotenv import load_dotenv

load_dotenv()
st.set_page_config(page_title="London air quality: sensor trust", layout="wide")

CHECKS = ["freshness", "coverage", "flatline", "plausibility"]
COLORS = {"healthy": "#2e9e5b", "watch": "#e0a800", "problem": "#d64545"}
SEVERITY = {"healthy": 0, "watch": 1, "problem": 2}


def db_url():
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    try:
        return st.secrets["DATABASE_URL"]
    except Exception:
        st.error("DATABASE_URL is not set (use a .env file locally, or Streamlit secrets).")
        st.stop()


@st.cache_data(ttl=300)
def query(sql: str) -> pd.DataFrame:
    with closing(psycopg2.connect(db_url())) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


SENSORS_SQL = """
    SELECT st.name AS station, st.provider, s.parameter, st.latitude, st.longitude,
           MAX(CASE WHEN c.check_name = 'lifecycle'    THEN c.status END) AS lifecycle,
           MAX(CASE WHEN c.check_name = 'freshness'    THEN c.status END) AS freshness,
           MAX(CASE WHEN c.check_name = 'coverage'     THEN c.status END) AS coverage,
           MAX(CASE WHEN c.check_name = 'flatline'     THEN c.status END) AS flatline,
           MAX(CASE WHEN c.check_name = 'plausibility' THEN c.status END) AS plausibility,
           MAX(CASE WHEN c.check_name = 'freshness'    THEN c.detail END) AS detail
    FROM sensors s
    JOIN stations st ON st.location_id = s.location_id
    JOIN sensor_health_current c ON c.sensor_id = s.sensor_id
    GROUP BY s.sensor_id, st.name, st.provider, s.parameter, st.latitude, st.longitude
"""
RUNS_SQL = """
    SELECT started_at, finished_at, mode, sensors_polled, sensors_failed,
           rows_inserted, status
    FROM ingest_runs ORDER BY run_id DESC LIMIT 10
"""
EVENTS_SQL = """
    SELECT e.changed_at, st.name AS station, s.parameter, e.check_name,
           e.old_status, e.new_status, e.detail
    FROM sensor_health_events e
    JOIN sensors s ON s.sensor_id = e.sensor_id
    JOIN stations st ON st.location_id = s.location_id
    WHERE e.old_status IS NOT NULL AND e.new_status <> 'retired'
    ORDER BY e.changed_at DESC LIMIT 25
"""
NEWEST_SQL = "SELECT MAX(measured_at) AS newest FROM readings"


def classify(row):
    if row["lifecycle"] == "retired":
        return "retired"
    statuses = [row[c] for c in CHECKS]
    if row["lifecycle"] == "quiet" or "fail" in statuses:
        return "problem"
    return "watch" if "warn" in statuses else "healthy"


st.title("London air quality: which sensors can you trust?")
st.caption("Built on OpenAQ data. Times are UTC. Refreshes every 5 minutes.")

df = query(SENSORS_SQL)
if df.empty:
    st.warning("No health data yet. Run ingest.py and health_checks.py first.")
    st.stop()
df["health"] = df.apply(classify, axis=1)

# ---- pipeline status ---------------------------------------------------------
runs = query(RUNS_SQL)
newest = query(NEWEST_SQL)["newest"].iloc[0]
ok = runs[runs["status"].isin(["ok", "partial"])]
if ok.empty:
    pipe = "no successful run yet"
else:
    last_ok = pd.to_datetime(ok["finished_at"], utc=True).max()
    hrs = (pd.Timestamp.now(tz="UTC") - last_ok).total_seconds() / 3600
    pipe = f"{hrs:.1f}h ago"
c1, c2 = st.columns(2)
c1.metric("Last successful data load", pipe)
c2.metric("Newest reading in database",
          "none" if pd.isna(newest) else pd.Timestamp(newest).strftime("%d %b %H:%M"))

# ---- headline ----------------------------------------------------------------
counts = df["lifecycle"].value_counts()
total, active, quiet, retired = len(df), counts.get("active", 0), counts.get("quiet", 0), counts.get("retired", 0)
expected = df[df["health"] != "retired"]
n_problem = int((expected["health"] == "problem").sum())
st.info(
    f"OpenAQ lists **{total}** London sensors, but only **{active + quiet}** are still expected "
    f"to report. **{n_problem}** of those currently have a problem, and **{quiet}** of the "
    f"problems are sensors that stopped reporting."
)
m = st.columns(4)
m[0].metric("Registered", total)
m[1].metric("Active (reported in 3 days)", active)
m[2].metric("Gone quiet", quiet)
m[3].metric("Retired / never active", retired)

# ---- map and provider chart --------------------------------------------------
left, right = st.columns([3, 2])
with left:
    st.subheader("Station health")
    live = expected.assign(sev=expected["health"].map(SEVERITY))
    stations = (live.sort_values("sev")
                    .groupby(["station", "latitude", "longitude"], as_index=False).last())
    stations["color"] = stations["health"].map(COLORS)
    st.map(stations, latitude="latitude", longitude="longitude", color="color", size=250)
    st.caption("Green = healthy, amber = watch (a warning), red = problem or gone silent. "
               "Each dot shows the worst sensor at that station. Retired stations are hidden.")
with right:
    st.subheader("Sensors by provider")
    chart = df.pivot_table(index="provider", columns="lifecycle", values="station",
                           aggfunc="count", fill_value=0)
    st.bar_chart(chart)

# ---- problem sensors ---------------------------------------------------------
st.subheader("Sensors that should be reporting but have a problem")
problems = expected[expected["health"] == "problem"][
    ["station", "provider", "parameter", "lifecycle"] + CHECKS + ["detail"]
].sort_values(["lifecycle", "provider", "station"])
st.dataframe(problems, hide_index=True)

# ---- recent changes ----------------------------------------------------------
st.subheader("Recent status changes")
events = query(EVENTS_SQL)
if events.empty:
    st.write("No status changes recorded yet.")
else:
    st.dataframe(events, hide_index=True)

# ---- method ------------------------------------------------------------------
with st.expander("How sensors are judged"):
    st.markdown("""
- **Lifecycle:** *active* = reported in the last 3 days, *quiet* = silent for 3 to 60 days,
  *retired* = silent longer or never. Retired sensors are not judged.
- **Freshness:** how far behind the newest reading from the same provider (pass up to 3h,
  warn up to 24h, fail beyond). Measuring against the provider, not the clock, keeps
  publishing delays and pipeline hiccups from blaming sensors.
- **Coverage:** share of the 24 hours up to the provider's newest reading that have data (80%+ pass, 50%+ warn).
- **Flatline:** same value repeated 6+ times in a row (warn) or 12+ (fail).
- **Plausibility:** impossible values, such as strongly negative readings.
- Lifecycle uses each sensor's own last reading, because a station can look alive only
  because other sensors there still report. Thresholds are judgement calls and are easy
  to change in `health_checks.py`.
""")