CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS gpus (
    uuid        TEXT PRIMARY KEY,
    gpu_id      INT NOT NULL,
    device      TEXT NOT NULL,
    model_name  TEXT NOT NULL,
    hostname    TEXT NOT NULL,
    first_seen  TIMESTAMPTZ NOT NULL,
    last_seen   TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS telemetry (
    id               UUID PRIMARY KEY,
    processed_at     TIMESTAMPTZ NOT NULL,
    source_timestamp TEXT,
    metric_name      TEXT NOT NULL,
    gpu_uuid         TEXT NOT NULL REFERENCES gpus(uuid),
    gpu_id           INT NOT NULL,
    device           TEXT NOT NULL,
    hostname         TEXT NOT NULL,
    value            DOUBLE PRECISION NOT NULL,
    container        TEXT,
    pod              TEXT,
    namespace        TEXT,
    labels_raw       TEXT,
    streamer_id      TEXT
);

SELECT create_hypertable('telemetry', 'processed_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS telemetry_gpu_uuid_processed_at ON telemetry (gpu_uuid, processed_at DESC);
CREATE INDEX IF NOT EXISTS telemetry_metric_name_processed_at ON telemetry (metric_name, processed_at DESC);
