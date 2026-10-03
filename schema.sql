-- London air quality monitor: week 1 schema (Postgres / Neon / Supabase)

CREATE TABLE IF NOT EXISTS stations (
    location_id  BIGINT PRIMARY KEY,          -- OpenAQ location id
    name         TEXT,
    provider     TEXT,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    is_monitor   BOOLEAN,                     -- reference-grade vs low-cost sensor
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id    BIGINT PRIMARY KEY,          -- OpenAQ sensor id
    location_id  BIGINT NOT NULL REFERENCES stations(location_id),
    parameter    TEXT NOT NULL,               -- pm25 | pm10 | no2
    units        TEXT
);
CREATE INDEX IF NOT EXISTS idx_sensors_location ON sensors(location_id);

CREATE TABLE IF NOT EXISTS readings (
    sensor_id    BIGINT NOT NULL REFERENCES sensors(sensor_id),
    measured_at  TIMESTAMPTZ NOT NULL,        -- event time (start of the period)
    value        DOUBLE PRECISION NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- processing time
    PRIMARY KEY (sensor_id, measured_at)      -- makes re-runs idempotent
);
CREATE INDEX IF NOT EXISTS idx_readings_time ON readings(measured_at DESC);

-- One row per pipeline run: feeds the freshness and coverage checks in week 2
CREATE TABLE IF NOT EXISTS ingest_runs (
    run_id            BIGSERIAL PRIMARY KEY,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    sensors_polled    INT,
    sensors_failed    INT,
    rows_fetched      INT,
    rows_inserted     INT,
    status            TEXT                    -- ok | partial | failed
);
