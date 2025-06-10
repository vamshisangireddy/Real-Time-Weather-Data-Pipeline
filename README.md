**Real-Time Weather Data Pipeline Capstone Project**
**Overview**

This capstone project involves building a real-time weather data pipeline that ingests high-frequency
weather data and delivers it to a cloud data warehouse for visualization. The pipeline integrates multiple big
data engineering components: a weather API as the data source, Apache Kafka for streaming ingestion,
Apache Spark (Structured Streaming) for real-time processing, Amazon S3 with Apache Iceberg as a
data lake storage layer, AWS Glue or Iceberg’s REST Catalog for metadata management, Snowflake as the
query engine (unmanaged Iceberg table), and Power BI for real-time dashboards. Apache Airflow
orchestrates the end-to-end workflow, and the entire project is containerized with Docker and deployed on
Kubernetes for scalability and easy management. This project simulates a real-world IoT/streaming
scenario – weather data is continually fetched and processed, demonstrating key data engineering skills in
streaming ETL, data lakehouse architecture, cloud integration, and analytics visualization.
