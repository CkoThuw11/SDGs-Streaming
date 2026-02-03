from pyflink.table import TableEnvironment, EnvironmentSettings

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    # Enable checkpointing for JDBC sink commit
    t_env.get_config().set("execution.checkpointing.interval", "10s")
    t_env.get_config().set("parallelism.default", "2")

    # Using ROW format - more type-safe
    t_env.execute_sql("""
        CREATE TABLE co2_source (
            payload ROW<
                `before` ROW<
                    id INT,
                    sensor_id STRING,
                    co2_level DOUBLE,
                    measurement_time BIGINT,
                    location STRING
                >,
                `after` ROW<
                    id INT,
                    sensor_id STRING,
                    co2_level DOUBLE,
                    measurement_time BIGINT,
                    location STRING
                >,
                op STRING,
                ts_ms BIGINT
            >,
            proc_time AS CAST(TO_TIMESTAMP_LTZ(CAST(payload.after.measurement_time / 1000 AS BIGINT), 3) AS TIMESTAMP(3)),
            WATERMARK FOR proc_time AS proc_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sdg.public.co2_measurements',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'earliest-offset',
            'scan.watermark.idle-timeout' = '10s',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Using CAST and FROM_UNIXTIME for proper timestamp conversion
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW co2_processed AS
        SELECT 
            payload.`after`.id as id,
            payload.`after`.sensor_id as sensor_id,
            payload.`after`.co2_level as co2_level,
            proc_time as measurement_time,
            payload.`after`.location as location,
            payload.op
        FROM co2_source
        WHERE payload.`after`.id IS NOT NULL
    """)

    t_env.execute_sql("""
        CREATE TABLE print_sink (
            sensor_id STRING,
            co2_level DOUBLE,
            location STRING,
            measurement_time TIMESTAMP(3),
            status STRING
        ) WITH (
            'connector' = 'print'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE co2_aggregated_sink (
            sensor_id STRING,
            location STRING,
            window_end TIMESTAMP(3),
            avg_co2 DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/sdg_streaming',
            'table-name' = 'co2_aggregated',
            'username' = 'user',
            'password' = 'password'
        )
    """)

    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql("""
        INSERT INTO print_sink
        SELECT 
            sensor_id,
            co2_level,
            location,
            measurement_time,
            CASE 
                WHEN co2_level > 420.0 THEN 'High Warning'
                WHEN co2_level > 400.0 THEN 'Warning'
                ELSE 'Normal'
            END as status
        FROM co2_processed
        WHERE op IN ('r', 'c', 'u')
    """)

    # --- NEW SINKS ---

    # 1. Real-Time Status Sink (Upsert via JDBC - simplified as insert/update logic inside PG if needed, but Standard JDBC sink in Flink does Insert/Upsert depending on config. Here we use standard INSERT but typically for "current_co2_status" we want valid upsert. Note: Flink JDBC sink in 'upsert' mode needs a primary key defined in Flink DDL.
    t_env.execute_sql("""
        CREATE TABLE current_status_sink (
            sensor_id STRING,
            co2_level DOUBLE,
            status STRING,
            last_updated TIMESTAMP(3),
            location STRING,
            PRIMARY KEY (sensor_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/sdg_streaming',
            'table-name' = 'current_co2_status',
            'username' = 'user',
            'password' = 'password'
        )
    """)

    # 2. Alerts Sink
    t_env.execute_sql("""
        CREATE TABLE alerts_sink (
            sensor_id STRING,
            location STRING,
            co2_level DOUBLE,
            alert_time TIMESTAMP(3),
            message STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/sdg_streaming',
            'table-name' = 'co2_alerts',
            'username' = 'user',
            'password' = 'password'
        )
    """)

    # 4. Hourly Aggregates Sink
    t_env.execute_sql("""
        CREATE TABLE co2_hourly_sink (
            sensor_id STRING,
            location STRING,
            window_end TIMESTAMP(3),
            avg_co2 DOUBLE,
            min_co2 DOUBLE,
            max_co2 DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/sdg_streaming',
            'table-name' = 'co2_hourly_aggregates',
            'username' = 'user',
            'password' = 'password'
        )
    """)

    stmt_set = t_env.create_statement_set()

    # Original Print Logic


    # Original 1-min Aggregation Logic
    stmt_set.add_insert_sql("""
        INSERT INTO co2_aggregated_sink
        SELECT 
            sensor_id,
            location,
            TUMBLE_END(measurement_time, INTERVAL '1' MINUTE) as window_end,
            AVG(co2_level) as avg_co2
        FROM co2_processed
        GROUP BY 
            sensor_id, 
            location, 
            TUMBLE(measurement_time, INTERVAL '1' MINUTE)
    """)

    # --- NEW PROCESSING LOGIC ---

    # 1. Alerting Logic (> 400 ppm)
    stmt_set.add_insert_sql("""
        INSERT INTO alerts_sink
        SELECT 
            sensor_id,
            location,
            co2_level,
            measurement_time as alert_time,
            CASE 
                WHEN co2_level > 420.0 THEN 'High CO2 Level Detected'
                ELSE 'Elevated CO2 Level Detected'
            END as message
        FROM co2_processed
        WHERE co2_level > 400.0
    """)



    # 3. Current Status Update (Latest Value per sensor)
    # Using Global Top-N (Deduplication) instead of Window Top-N.
    # This produces a Retract/Upsert stream that Flink JDBC Sink can handle correctly with Primary Keys.
    stmt_set.add_insert_sql("""
        INSERT INTO current_status_sink
        SELECT 
            sensor_id,
            co2_level,
            CASE 
                WHEN co2_level > 420.0 THEN 'CRITICAL'
                WHEN co2_level > 400.0 THEN 'WARNING'
                ELSE 'NORMAL'
            END as status,
            measurement_time as last_updated,
            location
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY measurement_time DESC) as row_num
            FROM co2_processed
        ) WHERE row_num = 1
    """)

    # 4. Hourly Aggregates (New)
    stmt_set.add_insert_sql("""
        INSERT INTO co2_hourly_sink
        SELECT 
            sensor_id,
            location,
            TUMBLE_END(measurement_time, INTERVAL '1' HOUR) as window_end,
            AVG(co2_level) as avg_co2,
            MIN(co2_level) as min_co2,
            MAX(co2_level) as max_co2
        FROM co2_processed
        GROUP BY 
            sensor_id, 
            location, 
            TUMBLE(measurement_time, INTERVAL '1' HOUR)
    """)

    stmt_set.execute()

if __name__ == '__main__':
    main()