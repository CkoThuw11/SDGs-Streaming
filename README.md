# SDGStreaming

Real-time CO₂ anomaly detection pipeline using Change Data Capture (CDC).

## Architecture

```
PostgreSQL → Debezium → Kafka → Apache Flink → Print Sink
```

| Component | Purpose |
|-----------|---------|
| **PostgreSQL** | Source database with CO₂ measurements |
| **Debezium** | Captures database changes via CDC |
| **Kafka** | Event streaming platform |
| **Flink** | Real-time stream processing |
| **Data Generator** | Simulates sensor readings |

## Quick Start

```bash
# Start all services
docker-compose up -d --build

# Wait ~60 seconds, then register Debezium connector
bash register_connector.sh

# Submit Flink job
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /opt/flink/usrlib/processor.py

# View real-time output
docker logs flink-taskmanager -f | grep -E "\+I\[|status"
```

## Project Structure

```
SDGStreaming/
├── docker-compose.yml      # Service orchestration
├── init.sql                # Database schema
├── debezium-connector.json # CDC configuration
├── register_connector.sh   # Connector setup script
├── testing_guide.md        # Detailed testing instructions
├── flink_job/
│   ├── Dockerfile          # Flink image with PyFlink
│   ├── processor.py        # Stream processing logic
│   └── requirements.txt    # Python dependencies
└── generator/
    ├── Dockerfile          # Generator image
    └── generator.py        # Synthetic data generator
```

## Anomaly Detection Logic

| CO₂ Level (ppm) | Status |
|-----------------|--------|
| > 420 | High Warning |
| > 400 | Warning |
| ≤ 400 | Normal |

## Useful Commands

```bash
# Check service health
docker compose ps

# View Kafka topic data
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic sdg.public.co2_measurements --max-messages 5

# Check Flink job status
docker exec flink-jobmanager /opt/flink/bin/flink list

# Stop and clean up
docker compose down -v
```

## Requirements

- Docker & Docker Compose
- ~4GB RAM for all containers

## License

MIT
