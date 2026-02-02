CREATE TABLE IF NOT EXISTS co2_measurements (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    co2_level DOUBLE PRECISION NOT NULL,
    measurement_time TIMESTAMP NOT NULL,
    location VARCHAR(100)
);

ALTER TABLE co2_measurements REPLICA IDENTITY FULL;
