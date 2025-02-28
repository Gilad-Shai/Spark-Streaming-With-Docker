# Spark-Streaming-With-Docker
# Spark Streaming With Docker

This repository showcases a comprehensive data processing pipeline built with PySpark Structured Streaming, leveraging Apache Kafka for real-time messaging and MinIO as an S3-compatible storage solution. The project simulates a car telemetry system where sensor data is generated, enriched with metadata, analyzed for anomalies, and aggregated for monitoring. Designed to run within a Dockerized environment, it demonstrates key concepts of stream processing, data enrichment, alerting, and aggregation.

The pipeline consists of seven scripts, each represented by a Python script that performs a specific function in the workflow:

- **ModelCreation**: Prepares static metadata for cars (models and colors) stored in MinIO.
- **CarsGenerator**: Generates a dataset of cars with unique identifiers and attributes, stored in MinIO.
- **DataGenerator**: Produces real-time sensor data for cars and streams it to Kafka.
- **DataEnrichment**: Enriches sensor data with car metadata and computes an expected gear, streaming to Kafka.
- **AlertDetection**: Filters enriched data for anomaly conditions, streaming alerts to Kafka.
- **AlertCounter**: Aggregates alert data over time windows, displaying statistics in the console.

Each script builds on the previous one, creating a cohesive flow from data generation to actionable insights, all within a local Docker setup.

## Prerequisites

- **Docker**: Required to run the services defined in `docker-compose.yml`.
- **Docker Compose**: Used to orchestrate multi-container setup.
- **Python 3**: For running Python scripts.
- **Kafka Python Client**: Needed for `DataGenerator` (`pip install kafka-python` inside the `python` container).

## Dependencies

These dependencies are derived from the provided `docker-compose.yml` and are required to run the exercises:

- **MinIO**: S3-compatible storage (`minio/minio:latest`)
  - Ports: `9001:9000` (API), `9002:9001` (Console)
- **ZooKeeper**: Kafka dependency (`wurstmeister/zookeeper:latest`)
  - Port: `2181:2181`
- **Kafka**: Messaging system (`wurstmeister/kafka:latest`)
  - Port: `9092:9092`
  - Depends on: ZooKeeper
- **Kafdrop**: Kafka UI (`obsidiandynamics/kafdrop:latest`)
  - Port: `9003:9000`
  - Depends on: Kafka
- **Python**: Python environment with Kafka support (`python:latest`)
  - Installs `kafka-python` for message handling.
- **Spark**: Apache Spark environment (`bitnami/spark:latest`)
  - Ports: `7077:7077` (Spark Master), `8080:8080` (Spark Master UI), `4040:4040` (Spark Application UI)

## Setup

1. **Start Docker Compose**:
   ```bash
   docker-compose up -d
   ```

2. **Access the MinIO Console**:
   - Navigate to `http://localhost:9002` to access the MinIO console.
   - Default credentials: `minioadmin/minioadmin`.

3. **Kafka UI**:
   - Navigate to `http://localhost:9003` to access the Kafka UI (Kafdrop).

4. **Running the Scripts**:
   - The scripts can be run inside the `python` or `spark` containers by mounting your project directory into `/app` and executing the Python scripts as needed.
