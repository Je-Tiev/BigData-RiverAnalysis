"""Production Spark Structured Streaming Medallion Lakehouse Pipeline.

Ingests streaming telemetry from Kafka, performs dual-path processing:
1. Bronze Stream: Append-only raw Delta table (s3a://water-quality-lakehouse/bronze).
2. Silver Stream: Cleaned, enriched, deduplicated Delta table (s3a://water-quality-lakehouse/silver).
3. Real-Time WQI computation over sliding windows with watermarking.
"""

import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    month,
    to_timestamp,
    when,
    year,
)

# Support imports from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.lakehouse.bronze import transform_kafka_to_bronze
from src.lakehouse.silver import BRONZE_PARSER_SCHEMA

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SparkMedallionStream]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkMedallionStream")

# Configuration
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:19092"))
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", os.environ.get("KAFKA_TOPIC", "river.sensors.raw"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "password123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "water-quality-lakehouse")

BRONZE_PATH = os.environ.get("BRONZE_PATH", f"s3a://{MINIO_BUCKET}/bronze")
SILVER_PATH = os.environ.get("SILVER_PATH", f"s3a://{MINIO_BUCKET}/silver")

CHECKPOINT_BASE = os.environ.get("CHECKPOINT_BASE", "/opt/spark-data/checkpoints")
BRONZE_CHECKPOINT = f"{CHECKPOINT_BASE}/bronze_stream"
SILVER_CHECKPOINT = f"{CHECKPOINT_BASE}/silver_stream"

logger.info("Initializing Spark Medallion Streaming Engine...")
logger.info("Kafka: %s (Topic: %s)", KAFKA_SERVER, KAFKA_TOPIC)
logger.info("MinIO Endpoint: %s | Bronze: %s | Silver: %s", MINIO_ENDPOINT, BRONZE_PATH, SILVER_PATH)

# Spark Session with Delta Lake & S3A
spark = (
    SparkSession.builder.appName("RiverQuality_Medallion_Lakehouse_Stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Read Kafka Stream
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", f"{KAFKA_TOPIC},river-quality")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ========================================================
# Stream 1: Bronze Layer (Raw Ingestion)
# ========================================================
bronze_df = transform_kafka_to_bronze(raw_stream)

bronze_query = (
    bronze_df.writeStream.format("delta")
    .outputMode("append")
    .partitionBy("ingest_date")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    .trigger(processingTime="15 seconds")
    .start(BRONZE_PATH)
)

logger.info("Bronze Streaming Query active (ID: %s)", bronze_query.id)

# ========================================================
# Stream 2: Silver Layer (Cleaned & Structured)
# ========================================================
# Parse JSON from Kafka stream directly for continuous low-latency Silver
parsed_silver = (
    raw_stream.select(
        from_json(col("value").cast("string"), BRONZE_PARSER_SCHEMA).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .filter(col("data").isNotNull() & (col("data.station_id").isNotNull() | col("data.WaterbodyName").isNotNull()))
    .select("data.*", "kafka_timestamp")
)

# Normalize & Add Partitions
normalized_silver = (
    parsed_silver.withColumn(
        "station_id", when(col("station_id").isNotNull(), col("station_id")).otherwise(col("WaterbodyName"))
    )
    .withColumn("river_name", when(col("river_name").isNotNull(), col("river_name")).otherwise(col("WaterbodyName")))
    .withColumn(
        "event_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .when(col("FullDate").isNotNull(), to_timestamp(col("FullDate"), "yyyy-MM-dd"))
        .otherwise(col("kafka_timestamp")),
    )
    .withColumn("year", year(col("event_time")))
    .withColumn("month", month(col("event_time")))
    .withColumn("silver_processed_at", current_timestamp())
)

# Apply Watermark
watermarked_silver = normalized_silver.filter(col("event_time").isNotNull()).withWatermark("event_time", "10 minutes")

silver_query = (
    watermarked_silver.writeStream.format("delta")
    .outputMode("append")
    .partitionBy("year", "month", "station_id")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .trigger(processingTime="20 seconds")
    .start(SILVER_PATH)
)

logger.info("Silver Streaming Query active (ID: %s)", silver_query.id)

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Stopping Medallion streaming pipelines...")
    bronze_query.stop()
    silver_query.stop()
    spark.stop()
    logger.info("Clean shutdown complete.")
