from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, IntegerType, ArrayType, MapType, StructField

# --- Spark Session Configuration ---
# You need to specify all the JARs required by Spark for Kafka, Iceberg, and AWS.
# Make sure the versions match your Spark and Iceberg installations.
# The JARs should be in the '~/spark-jars' directory you created.
SPARK_JARS = (
    "~/spark-jars/spark-sql-kafka-0-10_2.12-3.4.1.jar,"
    "~/spark-jars/iceberg-spark-runtime-3.4_2.12-1.4.0.jar,"
    "~/spark-jars/aws-java-sdk-bundle-1.12.693.jar"
)

# Replace with your Kafka broker address and the S3 bucket/region
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "weather_raw_data"
S3_ICEBERG_WAREHOUSE = "s3://your-pyspark-iceberg-weather-data/warehouse" # Path in S3 for Iceberg data
AWS_REGION = "us-west-2" # Your AWS region

# Glue Catalog and Iceberg Table details
GLUE_CATALOG_DATABASE = "weather_data_lake_pyspark" # Your Glue database name
ICEBERG_TABLE_NAME = "weather_events_iceberg" # Name of the Iceberg table

spark = SparkSession.builder \
    .appName("WeatherIcebergStreaming") \
    .config("spark.jars", SPARK_JARS) \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", S3_ICEBERG_WAREHOUSE) \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.glue_catalog.lock-impl", "org.apache.iceberg.aws.glue.DynamoDbLockManager") \
    .config("spark.sql.catalog.glue_catalog.region", AWS_REGION) \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "glue_catalog") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Set log level to INFO for more detailed output
spark.sparkContext.setLogLevel("INFO")
print("Spark Session created successfully.")

# --- Define Schema for Incoming Kafka JSON Messages ---
# This schema must match the structure of the JSON messages produced by your Python script.
# Adjust types and nested structures as per your actual weather data.
# This schema is based on the OpenWeatherMap API example.
raw_weather_schema = StructType([
    StructField("coord", StructType([
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True)
    ]), True),
    StructField("weather", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("main", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True)
    ])), True),
    StructField("base", StringType(), True),
    StructField("main", StructType([
        StructField("temp", DoubleType(), True),
        StructField("feels_like", DoubleType(), True),
        StructField("temp_min", DoubleType(), True),
        StructField("temp_max", DoubleType(), True),
        StructField("pressure", IntegerType(), True),
        StructField("humidity", IntegerType(), True)
    ]), True),
    StructField("visibility", IntegerType(), True),
    StructField("wind", StructType([
        StructField("speed", DoubleType(), True),
        StructField("deg", IntegerType(), True)
    ]), True),
    StructField("clouds", StructType([
        StructField("all", IntegerType(), True)
    ]), True),
    StructField("dt", LongType(), True),
    StructField("sys", StructType([
        StructField("type", IntegerType(), True),
        StructField("id", LongType(), True),
        StructField("country", StringType(), True),
        StructField("sunrise", LongType(), True),
        StructField("sunset", LongType(), True)
    ]), True),
    StructField("timezone", IntegerType(), True),
    StructField("id", LongType(), True), # City ID
    StructField("name", StringType(), True), # City name
    StructField("cod", IntegerType(), True),
    StructField("ingestion_timestamp_ms", LongType(), True) # Timestamp added by your producer
])

# --- Read Stream from Kafka ---
# We read the 'value' field from Kafka, which contains our JSON weather data.
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

print(f"Reading from Kafka topic: {KAFKA_TOPIC}")

# --- Process and Transform Data ---
# Select the 'value' column (which is binary) and cast it to String.
# Then parse the JSON string using the defined schema.
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), raw_weather_schema)) \
    .select("data.*") # Flatten the 'data' struct to get all fields

# --- Data Transformations (Optional but recommended) ---
# Example: Extracting and renaming fields for the target Iceberg table.
# This makes the Iceberg table schema cleaner for analytics.
processed_df = parsed_df.select(
    col("name").alias("city_name"),
    col("main.temp").alias("temperature_celsius"),
    col("main.humidity").alias("humidity_percent"),
    col("wind.speed").alias("wind_speed_mps"),
    col("weather").getItem(0).getField("description").alias("weather_description"),
    col("ingestion_timestamp_ms").alias("event_timestamp_ms")
)

# Add a processing timestamp for auditing
processed_df = processed_df.withColumn("processing_timestamp", current_timestamp())

# --- Write Stream to Iceberg Table on S3 via Glue Catalog ---
# The 'append' mode is common for streaming ingestion.
# The `checkpointLocation` is crucial for fault tolerance in Spark Structured Streaming.
# Iceberg supports schema evolution, so you might not need to pre-define the table in Glue
# if you set spark.sql.iceberg.schema.auto.infer=true (which is default).
print(f"Writing to Iceberg table: {ICEBERG_TABLE_NAME} in Glue database: {GLUE_CATALOG_DATABASE}")
print(f"Iceberg warehouse location: {S3_ICEBERG_WAREHOUSE}")

query = processed_df \
    .writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", f"{S3_ICEBERG_WAREHOUSE}/_checkpoints/{ICEBERG_TABLE_NAME}") \
    .option("fanout-enabled", "true") # Recommended for better write performance
    .toTable(f"{GLUE_CATALOG_DATABASE}.{ICEBERG_TABLE_NAME}")

# Start the streaming query and wait for termination
print("Starting Spark Structured Streaming query...")
query.awaitTermination()

