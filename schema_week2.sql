-- Week 2: sensor health tables. Run once in the Neon SQL Editor.
--
-- Storage-conscious design: 689 sensors x 4 checks x hourly would be ~24M rows
-- a year, far too big for the 0.5 GB free tier. Instead:
--   * sensor_health_current  = latest result per sensor per check (~2,800 rows, upserted)
--   * sensor_health_events   = one row only when a status CHANGES (small)

CREATE TABLE IF NOT EXISTS sensor_health_current (
    sensor_id   BIGINT NOT NULL REFERENCES sensors(sensor_id),
    check_name  TEXT   NOT NULL,       -- freshness | coverage | flatline | plausibility
    status      TEXT   NOT NULL,       -- pass | warn | fail | unknown
    observed    DOUBLE PRECISION,      -- the measured number behind the status
    detail      TEXT,                  -- human-readable explanation
    checked_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (sensor_id, check_name)
);

CREATE TABLE IF NOT EXISTS sensor_health_events (
    event_id    BIGSERIAL PRIMARY KEY,
    sensor_id   BIGINT NOT NULL REFERENCES sensors(sensor_id),
    check_name  TEXT   NOT NULL,
    old_status  TEXT,                  -- NULL the first time a check is seen
    new_status  TEXT   NOT NULL,
    observed    DOUBLE PRECISION,
    detail      TEXT,
    changed_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_health_events_time
    ON sensor_health_events(changed_at DESC);
