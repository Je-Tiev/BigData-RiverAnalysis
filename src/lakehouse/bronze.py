"""Medallion Lakehouse - Bronze Layer Ingestion Module.

Handles raw telemetry ingestion from Kafka into Delta Lake Bronze tables.
Stores 100% untouched raw JSON payloads alongside Kafka ingestion metadata
(partition, offset, timestamp) using append-only ACID commits.
Partitioned by ingest_date for efficient date-range partitioning and lifecycle retention.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_date,
    current_timestamp,
    date_format,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logger = logging.getLogger("LakehouseBronze")


# Schema definition for Bronze Raw Table
BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType(), True),
        StructField("raw_payload", StringType(), False),
        StructField("kafka_timestamp", TimestampType(), True),
        StructField("kafka_partition", IntegerType(), True),
        StructField("kafka_offset", LongType(), True),
        StructField("ingest_timestamp", TimestampType(), False),
        StructField("ingest_date", StringType(), False),
    ]
)


def transform_kafka_to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Transforms raw Kafka DataFrame into the canonical Bronze schema format."""
    return kafka_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        current_timestamp().alias("ingest_timestamp"),
        date_format(current_date(), "yyyy-MM-dd").alias("ingest_date"),
    )


def start_bronze_streaming_job(
    spark: SparkSession,
    kafka_bootstrap: str,
    kafka_topic: str,
    bronze_output_path: str,
    checkpoint_location: str,
    trigger_interval: str = "15 seconds",
):
    """Starts a continuous Spark Structured Streaming job writing to Delta Lake Bronze layer."""
    logger.info("Connecting to Kafka topic '%s' at %s...", kafka_topic, kafka_bootstrap)
    logger.info("Bronze Delta Lake Target: %s", bronze_output_path)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    bronze_stream = transform_kafka_to_bronze(raw_stream)

    query = (
        bronze_stream.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("ingest_date")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger_interval)
        .start(bronze_output_path)
    )

    logger.info("Bronze Streaming query active (ID: %s)", query.id)
    return query


def write_bronze_batch(
    spark: SparkSession,
    raw_records_df: DataFrame,
    bronze_output_path: str,
) -> int:
    """Writes a static batch of raw records to the Bronze Delta table (for backfill/testing)."""
    bronze_df = transform_kafka_to_bronze(raw_records_df)
    record_count = bronze_df.count()

    logger.info("Writing %d raw records to Bronze Delta: %s", record_count, bronze_output_path)
    bronze_df.write.format("delta").mode("append").partitionBy("ingest_date").save(bronze_output_path)

    return record_count
