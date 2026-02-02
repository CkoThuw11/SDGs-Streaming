# SDGStreaming Test Plan

This guide details how to build, run, and verify the entire data pipeline:
`Postgres -> Debezium -> Kafka -> Flink`

## 1. Build Services
```bash
docker-compose up
```
*Wait ~60 seconds for all services to initialize.*

## 2. Register Debezium Connector
```bash
bash register_connector.sh
```

## 3. Verify Kafka Data Flow
```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sdg.public.co2_measurements --max-messages 3
```

## 4. Submit Flink Job
```bash
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /opt/flink/usrlib/processor.py
```

## 5. Check Job Status
```bash
docker exec flink-jobmanager /opt/flink/bin/flink list
```

## 6. View Output Stream
```bash
docker logs flink-taskmanager -f | grep -E "\+I\[|status"
```

## 7. Health Check
```bash
docker compose ps
```

## 8. Troubleshooting
```bash
# Check Kafka data
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sdg.public.co2_measurements --from-beginning --max-messages 5

# View JobManager logs
docker logs flink-jobmanager

# Restart generator
docker restart data-generator
```

## 9. Stop and Restart System
```bash
docker compose down -v
docker compose up -d --build
```
