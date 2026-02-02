from pyflink.table import TableEnvironment, EnvironmentSettings

def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set("parallelism.default", "1")

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
            >
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'sdg.public.co2_measurements',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'flink-consumer-group',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Using CAST and FROM_UNIXTIME for proper timestamp conversion
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW co2_processed AS
        SELECT 
            `after`.id as id,
            `after`.sensor_id as sensor_id,
            `after`.co2_level as co2_level,
            -- Convert microseconds to timestamp
            CAST(FROM_UNIXTIME(`after`.measurement_time / 1000000) AS TIMESTAMP(3)) as measurement_time,
            `after`.location as location,
            op
        FROM co2_source
        WHERE `after`.id IS NOT NULL
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

if __name__ == '__main__':
    main()