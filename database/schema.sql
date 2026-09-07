-- High-Density Storage Optimization Schema

-- 1. Create a standard PostgreSQL table
-- defines table grid_telemetry to hold electrical grid metrics
-- REALs are 4-byte floating point numbers 
CREATE TABLE IF NOT EXISTS grid_telemetry (
    timestamp TIMESTAMPTZ NOT NULL,
    device_id INT NOT NULL,
    voltage_v REAL NOT NULL,
    current_a REAL NOT NULL,
    power_kw REAL NOT NULL,
    security_flag INT NOT NULL
);

-- 2. Convert the table into a TimescaleDB Hypertable partitioned by 1-hour chunks
-- A hypertable is an advanced PostgreSQL database table designed to automatically
--   partition time-series and event data into smaller, manageable pieces called chunks.
-- (Choosing 1 hour balances RAM footprint for active indices against streaming volume)
SELECT create_hypertable('grid_telemetry', 'timestamp', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);

-- 3. Create a composite index to rapidly query individual devices over specific windows 
CREATE INDEX IF NOT EXISTS idx_device_time ON grid_telemetry (device_id, timestamp DESC);

-- 4. Enable compression to save up to 90% disk space (Essential for IoT data)
-- Prepares the hypertable to use TimescaleDB's columnar compression engine.
-- tells the database to group rows together by their specific device ID before compressing them.
-- triggers a fundamental architectural shift in how PostgreSQL stores data: it transforms
--     historical, time-series data from a traditional row-oriented store into a highly 
--     optimized column-oriented store. (Data grouping with arrary-based storage)
ALTER TABLE grid_telemetry SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id'
);

-- 5. Add a policy to compress data older than 2 hours automatically
SELECT add_compression_policy('grid_telemetry', INTERVAL '2 hours', if_not_exists => TRUE);
