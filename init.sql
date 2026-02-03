CREATE TABLE IF NOT EXISTS co2_measurements (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    co2_level DOUBLE PRECISION NOT NULL,
    measurement_time TIMESTAMP NOT NULL,
    location VARCHAR(100)
);

ALTER TABLE co2_measurements REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS co2_aggregated (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    window_end TIMESTAMP NOT NULL,
    avg_co2 DOUBLE PRECISION NOT NULL
);

-- 1. Real-Time Monitoring
-- latest reading & status per sensor (Upsert target)
CREATE TABLE IF NOT EXISTS current_co2_status (
    sensor_id VARCHAR(50) PRIMARY KEY,
    co2_level DOUBLE PRECISION NOT NULL,
    status VARCHAR(20) NOT NULL,
    last_updated TIMESTAMP NOT NULL,
    location VARCHAR(100)
);

-- [NEW] For Bar Charts & Historical Comparison (efficient querying)
CREATE TABLE IF NOT EXISTS co2_hourly_aggregates (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    window_end TIMESTAMP NOT NULL, -- e.g. 10:00, 11:00
    avg_co2 DOUBLE PRECISION NOT NULL,
    min_co2 DOUBLE PRECISION,
    max_co2 DOUBLE PRECISION
);

-- 3. Alerts & Anomalies
CREATE TABLE IF NOT EXISTS co2_alerts (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    location VARCHAR(100),
    co2_level DOUBLE PRECISION NOT NULL,
    alert_time TIMESTAMP NOT NULL,
    message VARCHAR(255)
);


-- VIEWS

-- 2. Historical Trends
-- avg CO2 by hour of day (0-23)
CREATE OR REPLACE VIEW daily_patterns AS
SELECT 
    EXTRACT(HOUR FROM measurement_time) as hour_of_day,
    location,
    AVG(co2_level) as avg_co2
FROM co2_measurements
GROUP BY 1, 2;

-- daily rollups
CREATE OR REPLACE VIEW daily_summary AS
SELECT 
    DATE(measurement_time) as measurement_date,
    location,
    AVG(co2_level) as avg_co2,
    MAX(co2_level) as max_co2,
    COUNT(*) FILTER (WHERE co2_level > 400) as alerts_count
FROM co2_measurements
GROUP BY 1, 2;

-- 3. Alerts & Anomalies
-- alert counts by location/hour
CREATE MATERIALIZED VIEW IF NOT EXISTS alert_summary_hourly AS
SELECT 
    DATE_TRUNC('hour', alert_time) as alert_hour,
    location,
    COUNT(*) as alert_count
FROM co2_alerts
GROUP BY 1, 2;

CREATE OR REPLACE VIEW sensor_stats_24h AS
SELECT 
    sensor_id,
    location,
    window_end as time_bucket,
    avg_co2 as hourly_avg,
    AVG(avg_co2) OVER (
        PARTITION BY sensor_id 
        ORDER BY window_end 
        RANGE BETWEEN INTERVAL '24 HOURS' PRECEDING AND CURRENT ROW
    ) as rolling_24h_avg
FROM co2_hourly_aggregates;
