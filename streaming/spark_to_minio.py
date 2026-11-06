"""Structured Streaming Ingestion to MinIO Lakehouse (Bronze Layer).

Streams validated sensor telemetry from Kafka directly to MinIO Object Storage
partitioned by ingest_date and station_id (ready for Delta Lake conversion).
"""

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_format,
    from_json,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SparkMinIOStream]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkMinIOStream")

# Configuration
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:19092"))
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC_RAW", os.environ.get("KAFKA_TOPIC", "river.sensors.raw"))

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", os.environ.get("MINIO_ROOT_USER", "admin"))
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", os.environ.get("MINIO_ROOT_PASSWORD", "password123"))
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "water-quality-lakehouse")

CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/opt/spark-data/checkpoints/minio_bronze_stream")
OUTPUT_PATH = os.environ.get("OUTPUT_PATH", f"s3a://{MINIO_BUCKET}/bronze")

# Spark Session
spark = (
    SparkSession.builder.appName("RiverQuality_to_MinIO_Bronze")
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session initialized with S3A MinIO connector (%s)", MINIO_ENDPOINT)

# Unified Schema
sensor_schema = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("station_name", StringType(), True),
        StructField("river_name", StringType(), True),
        StructField("basin_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("ph", DoubleType(), True),
        StructField("dissolved_oxygen", DoubleType(), True),
        StructField("conductivity", DoubleType(), True),
        StructField("ammonia", DoubleType(), True),
        StructField("bod5", DoubleType(), True),
        StructField("chloride", DoubleType(), True),
        StructField("total_hardness", DoubleType(), True),
        StructField("turbidity_ntu", DoubleType(), True),
        StructField("flow_rate_m3s", DoubleType(), True),
        StructField("ccme_score", DoubleType(), True),
        StructField("quality_label", StringType(), True),
        StructField("is_anomaly", BooleanType(), True),
        StructField("anomaly_reason", StringType(), True),
        # Legacy support
        StructField("FullDate", StringType(), True),
        StructField("WaterbodyName", StringType(), True),
        StructField("Conductivity @25°C", StringType(), True),
        StructField("Ammonia-Total (as N)", StringType(), True),
        StructField("BOD - 5 days (Total)", StringType(), True),
        StructField("Total Hardness (as CaCO3)", StringType(), True),
        StructField("CCME_Values", StringType(), True),
    ]
)

# Read Kafka
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", f"{KAFKA_TOPIC},river-quality")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse & Normalize
parsed_stream = raw_stream.select(
    from_json(col("value").cast("string"), sensor_schema).alias("data"),
    col("timestamp").alias("kafka_ingest_time"),
    col("offset").alias("kafka_offset"),
)

normalized_stream = (
    parsed_stream.filter(col("data").isNotNull())
    .select("data.*", "kafka_ingest_time", "kafka_offset")
    .withColumn("station_id", when(col("station_id").isNotNull(), col("station_id")).otherwise(col("WaterbodyName")))
    .withColumn("river_name", when(col("river_name").isNotNull(), col("river_name")).otherwise(col("WaterbodyName")))
    .withColumn(
        "event_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .when(col("FullDate").isNotNull(), to_timestamp(col("FullDate"), "yyyy-MM-dd"))
        .otherwise(col("kafka_ingest_time")),
    )
    .withColumn("ingest_date", date_format(col("event_time"), "yyyy-MM-dd"))
)

logger.info("Writing streaming Bronze raw data to: %s (Partitioned by ingest_date)", OUTPUT_PATH)

query = (
    normalized_stream.writeStream.outputMode("append")
    .format("parquet")
    .option("path", OUTPUT_PATH)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .partitionBy("ingest_date")
    .trigger(processingTime="30 seconds")
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Stream stopping on operator request...")
    query.stop()
    spark.stop()
    logger.info("MinIO stream stopped cleanly.")
