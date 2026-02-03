# SDGStreaming

Real-time CO₂ anomaly detection pipeline using Change Data Capture (CDC).

## Architecture

```
PostgreSQL (Source) → Debezium → Kafka → Apache Flink → PostgreSQL (Sink) → Power BI
```

| Component | Purpose |
|-----------|---------|
| **PostgreSQL** | Source of Truth & Analytics Storage |
| **Debezium** | Captures database changes (`co2_measurements`) |
| **Kafka** | Event streaming platform |
| **Flink** | Real-time stream processing & Aggregation |
| **Power BI** | Real-time Dashboards & Historical Reporting |

## Quick Start

```bash
# Start all services
docker-compose up -d --build

# Wait ~15 seconds for Debezium to be ready, then register connector
bash register_connector.sh

# Submit Flink job
docker exec -it flink-jobmanager /opt/flink/bin/flink run -py /opt/flink/usrlib/processor.py
```

## Project Structure

```
SDGStreaming/
├── docker-compose.yml      # Service orchestration
├── init.sql                # Database schema (Tables + Stats Views)
├── debezium-connector.json # CDC configuration
├── register_connector.sh   # Connector setup script
├── flink_job/
│   ├── Dockerfile          # Flink image with PyFlink
│   ├── processor.py        # Stream processing logic
│   └── requirements.txt    # Python dependencies
└── generator/
    ├── Dockerfile          # Generator image
    └── generator.py        # Synthetic data generator
```

## Data Pipeline & Visualization

The system processes raw sensor data into three specific layers for visualization:

1.  **Real-Time Status** (`current_co2_status`):
    *   **Logic**: Deduplicated "Upsert" stream.
    *   **Use Case**: Live Gauge Charts (Latest value per sensor).
    
2.  **Hourly Trends** (`co2_hourly_aggregates`):
    *   **Logic**: 1-hour tumbling window aggregation.
    *   **Use Case**: Bar Charts (Hourly Comparison across locations).

3.  **Analytics Views** (`sensor_stats_24h`, `daily_patterns`):
    *   **Logic**: SQL Views calculating rolling 24h averages and spatial heatmaps.
    *   **Use Case**: Line Charts (Trend vs Baseline) & Heat Maps.

## Anomaly Detection Logic

| CO₂ Level (ppm) | Status | Alert Action |
|-----------------|--------|--------------|
| > 420 | **CRITICAL** | Log to `co2_alerts` |
| > 400 | **WARNING** | Log to `co2_alerts` |
| ≤ 400 | **NORMAL** | Ignored |

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

## Power BI Setup

This guide details how to connect Power BI to the PostgreSQL database for visualization.

## Database Connection

1.  **Get Data**: Open Power BI Desktop -> Home -> Get Data -> PostgreSQL database.
2.  **Server**: `localhost` (or your Docker host IP).
3.  **Port**: `5432` (Ensure port mapping `-p 5432:5432` is active in `docker-compose.yml`).
4.  **Database**: `sdg_streaming`.
5.  **Authentication**:
    *   **User**: `user`
    *   **Password**: `password` (Default from `docker-compose`)
6.  **Import Mode**: Select **DirectQuery** for real-time dashboards (Gauge, Alerts) or **Import** for historical analysis (Heat Map, Bar Charts). DirectQuery is recommended for live monitoring.
