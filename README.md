# Real-time Weather Data Lake Pipeline

This repository contains a robust, real-time data streaming pipeline designed to ingest weather data from an external API, stream it through Apache Kafka, and store it in an Apache Iceberg table on Amazon S3, with metadata managed by AWS Glue Data Catalog. Data is partitioned by city location for optimized analytical querying.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Architecture Diagram](#architecture-diagram)
3. [Components Used](#components-used)
4. [Prerequisites](#prerequisites)
5. [Setup Instructions](#setup-instructions)
   - [Step 1: Dockerized Kafka Cluster](#step-1-dockerized-kafka-cluster)
   - [Step 2: Python Weather Data Producer](#step-2-python-weather-data-producer)
   - [Step 3: AWS S3, IAM, and Glue Setup](#step-3-aws-s3-iam-and-glue-setup)
   - [Step 4: PySpark Streaming Application](#step-4-pyspark-streaming-application)
6. [Running the Pipeline](#running-the-pipeline)
7. [Verification](#verification)
8. [Cleanup](#cleanup)
9. [Future Enhancements](#future-enhancements)
10. [Troubleshooting](#troubleshooting)

---

## Project Overview

This pipeline addresses the challenge of real-time data ingestion and storage for analytical purposes. It provides a scalable and fault-tolerant solution by leveraging industry-standard technologies:

- **Data Ingestion**: A Python script fetches current weather data from WeatherAPI.com.
- **Real-time Messaging**: Apache Kafka acts as a central hub for streaming the raw weather events.
- **Stream Processing & Storage**: Apache Spark Structured Streaming consumes data from Kafka, transforms it, and writes it to S3 in Apache Iceberg format.
- **Data Lake Format**: Apache Iceberg provides transactional capabilities, schema evolution, and time travel for the data stored in S3.
- **Metadata Management**: AWS Glue Data Catalog stores the Iceberg table metadata, making it discoverable by querying services like Amazon Athena.
- **Optimization**: The Iceberg table is partitioned by `city_name`, significantly improving query performance for location-based analysis.

## Architecture Diagram

```
+----------------+       +-------------------+       +-----------------+
|   Weather API  |<----->| Python Producer   |------>|  Apache Kafka   |
| (WeatherAPI.com)|       | (weather_producer.py) |       | (Docker)        |
+----------------+       +-------------------+       +--------^--------+
                                                              | (weather_raw_data topic)
                                                              |
+----------------+       +-------------------------+       +-----------------+
| AWS Glue Data  |<------| PySpark Streaming App   |<------|  Apache Kafka   |
| Catalog        |       | (spark_iceberg_weather_ |       | (Docker)        |
| (Metadata)     |<----->|   stream.py)            |------>|                 |
+--------^-------+       +-------------------------+       +--------^--------+
         |                                                       | (Iceberg data)
         |                                                       |
         |         +-------------------------------------+       |
         |         |  Apache Iceberg Table (S3 Storage)  |<-------+
         |         |  (Partitioned by city_name)         |
         |         +-------------------------------------+
         |
         +<----------Query Tools (e.g., Amazon Athena)
```

## Components Used

- **Apache Kafka (2.x/3.x)**: Distributed streaming platform.
- **Apache ZooKeeper**: Distributed coordination service for Kafka.
- **Docker & Docker Compose**: For containerizing Kafka and ZooKeeper.
- **Python 3.7+**: For the weather API producer.
- **confluent-kafka-python, requests, python-dotenv**: Python libraries for API calls and messaging.
- **Apache Spark (3.x) & PySpark**: Unified analytics engine for large-scale data processing.
- **Apache Iceberg**: Open table format for large analytic datasets.
- **AWS Glue Data Catalog**: Managed metadata repository.
- **Amazon S3**: Scalable object storage for the data lake.
- **Amazon Athena**: Interactive query service to analyze data in S3.
- **AWS IAM**: For managing permissions.

## Prerequisites

Ensure you have the following installed and configured:

- **Docker Desktop**
- **Java Development Kit (JDK) 8+** (`java -version`)
- **Python 3.7+** (`python3 --version`)
- **Apache Spark** (e.g., `spark-3.4.1-bin-hadoop3`)
- **AWS CLI** (`aws configure`)
- **WeatherAPI.com API Key**

---

## Setup Instructions

### Step 1: Dockerized Kafka Cluster

1. Create and navigate to your project directory:
   ```bash
   mkdir kafka-docker && cd kafka-docker
   ```
2. Create a `docker-compose.yml` with ZooKeeper and Kafka services.
3. Start the cluster:
   ```bash
   ```

docker compose up -d

````
4. Create the Kafka topic:
```bash
docker exec kafka kafka-topics --create --topic weather_raw_data --bootstrap-server kafka:29092 --replication-factor 1 --partitions 1
````

### Step 2: Python Weather Data Producer

1. Create and navigate to the producer directory:
   ```bash
   mkdir weather_producer && cd weather_producer
   ```
2. Set up a virtual environment and install dependencies:
   ```bash
   ```

python3 -m venv venv source venv/bin/activate pip install confluent-kafka requests python-dotenv

````
3. Create a `.env` file with your API key and Kafka settings.
4. Add `weather_producer.py` (fetches and publishes weather data).
5. Run the producer:
   ```bash
python weather_producer.py
````

### Step 3: AWS S3, IAM, and Glue Setup

1. **S3 Bucket**: Create a bucket (e.g., `your-pyspark-weather-iceberg-lake-123`).
2. **IAM Policy**: Create and attach permissions for S3, Glue, and DynamoDB (for Iceberg locks).
3. **Glue Database**: Create `weather_data_lake_iceberg` in AWS Glue.
4. (Optional) **Glue Crawler**: Infer schema from sample JSON in S3.

### Step 4: PySpark Streaming Application

1. Create and navigate to the Spark app directory:
   ```bash
   mkdir pyspark-iceberg-app && cd pyspark-iceberg-app
   ```
2. Download required JARs to a `spark-jars` folder.
3. Add `spark_iceberg_weather_stream.py` (reads from Kafka, writes to Iceberg).
4. Update configuration placeholders (paths, bucket, AWS region, database).

---

## Running the Pipeline

1. **Kafka**: In `kafka-docker` directory, run:
   ```bash
   ```

docker compose up -d

````
2. **Producer**: In `weather_producer`, run:
   ```bash
python weather_producer.py
````

3. **Spark Streaming**:
   ```bash
   ```

/path/to/spark/bin/spark-submit \
\--master local[\*] \
\--packages \
org.apache.spark\:spark-sql-kafka-0-10\_2.12:3.4.1,\
org.apache.iceberg\:iceberg-spark-runtime-3.4\_2.12:1.4.0,\
software.amazon.awssdk\:aws-java-sdk-bundle:2.17.290 \
\--conf spark.sql.catalog.glue\_catalog=org.apache.iceberg.spark.SparkCatalog \
... \
spark\_iceberg\_weather\_stream.py

````

## Verification

- **S3**: Check `warehouse/weather_events_by_location/` for partitioned data.
- **Glue**: Verify `weather_events_by_location` table schema and partitions.
- **Athena**: Run queries using partition pruning:
  ```sql
SELECT city_name, temperature_celsius, weather_description,
       FROM_UNIXTIME(event_timestamp_ms / 1000) AS event_time
FROM weather_data_lake_iceberg.weather_events_by_location
WHERE city_name = 'London'
ORDER BY event_timestamp_ms DESC
LIMIT 5;
````

## Cleanup

1. Stop applications (Ctrl+C).
2. In `kafka-docker`:
   ```bash
   ```

docker compose down --volumes

```
3. Delete AWS resources:
   - S3 buckets
   - Glue database & tables
   - IAM policy & user
   - DynamoDB lock table

## Future Enhancements

- **Production Deployment**: EMR, AWS Glue ETL, Databricks.
- **Schema Evolution**: Handle changes gracefully.
- **Data Quality**: Pre-write validation.
- **Monitoring & Alerting**: CloudWatch, Prometheus/Grafana.
- **CI/CD**: Automate deployment.
- **Error Handling**: Robust retry and fallback logic.
- **Cost Optimization**: Tune cluster size, storage classes, file sizes.

## Troubleshooting

- **401 Unauthorized**: Check `.env` API key.
- **Kafka Connection Issues**: Ensure containers are running and bootstrap settings match.
- **Spark ClassNotFoundException**: Verify JAR paths and versions.
- **AWS Permissions Errors**: Validate IAM policy permissions.

```
