-- 1. Create a Materialized View for 5-Minute Grid Statistics
-- (Uses 'with_no_data' so the background worker initializes without locking the table)
CREATE MATERIALIZED VIEW IF NOT EXISTS grid_telemetry_5min
WITH (timescaledb.continuous) AS
SELECT 
    time_bucket('5 minutes', timestamp) AS bucket,
    device_id,
    AVG(voltage_v) AS avg_voltage,
    MAX(voltage_v) AS max_voltage,
    AVG(current_a) AS avg_current,
    SUM(power_kw) AS total_power_kw
FROM grid_telemetry
GROUP BY bucket, device_id
WITH NO DATA;

-- 2. Add an automatic background Refresh Policy
-- Automatically re-computes missing or newly arrived stream metrics every 5 minutes
SELECT add_continuous_aggregate_policy('grid_telemetry_5min',
    start_offset => INTERVAL '1 hour',  -- Look back 1 hour to catch delayed consumer batches
    end_offset   => INTERVAL '5 minutes',-- Wait for the active 5-minute bucket to close
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);
