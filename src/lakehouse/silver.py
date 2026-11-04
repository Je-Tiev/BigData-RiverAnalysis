"""Medallion Lakehouse - Silver Layer Processing Module.

Cleanses, normalizes, validates, enriches, and deduplicates raw Bronze records.
Enforces physical schema constraints and applies Delta Lake ACID MERGE (upsert)
to guarantee idempotency and exactly-once semantics.
Partitioned by year, month, and station_id for high-performance temporal-spatial queries.
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    date_format,
    from_json,
    month,
    row_number,
    to_timestamp,
    when,
    year,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

logger = logging.getLogger("LakehouseSilver")

# Parser schema for raw JSON inside Bronze
BRONZE_PARSER_SCHEMA = StructType([
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
    # Legacy fields
    StructField("FullDate", StringType(), True),
    StructField("WaterbodyName", StringType(), True),
    StructField("Conductivity @25°C", StringType(), True),
    StructField("Ammonia-Total (as N)", StringType(), True),
    StructField("BOD - 5 days (Total)", StringType(), True),
    StructField("Total Hardness (as CaCO3)", StringType(), True),
    StructField("CCME_Values", StringType(), True),
])


def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    """Parses raw JSON from Bronze, validates physical boundaries, enriches, and formats for Silver."""
    # 1. Parse JSON payload
    parsed_df = bronze_df.select(
        col("kafka_key"),
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("ingest_timestamp"),
        from_json(col("raw_payload"), BRONZE_PARSER_SCHEMA).alias("data"),
    )

    # 2. Filter out unparseable rows
    valid_df = parsed_df.filter(
        col("data").isNotNull() &
        (col("data.station_id").isNotNull() | col("data.WaterbodyName").isNotNull())
    ).select(
        col("kafka_timestamp"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("ingest_timestamp"),
        "data.*",
    )

    # 3. Standardize and normalize columns
    normalized_df = valid_df \
        .withColumn(
            "station_id",
            when(col("station_id").isNotNull(), col("station_id"))
            .otherwise(col("WaterbodyName"))
        ) \
        .withColumn(
            "river_name",
            when(col("river_name").isNotNull(), col("river_name"))
            .otherwise(col("WaterbodyName"))
        ) \
        .withColumn(
            "event_time",
            when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
            .when(col("FullDate").isNotNull(), to_timestamp(col("FullDate"), "yyyy-MM-dd"))
            .otherwise(col("kafka_timestamp"))
        ) \
        .withColumn(
            "conductivity",
            when(col("conductivity").isNotNull(), col("conductivity"))
            .otherwise(col("Conductivity @25°C").cast(DoubleType()))
        ) \
        .withColumn(
            "ammonia",
            when(col("ammonia").isNotNull(), col("ammonia"))
            .otherwise(col("Ammonia-Total (as N)").cast(DoubleType()))
        ) \
        .withColumn(
            "bod5",
            when(col("bod5").isNotNull(), col("bod5"))
            .otherwise(col("BOD - 5 days (Total)").cast(DoubleType()))
        ) \
        .withColumn(
            "total_hardness",
            when(col("total_hardness").isNotNull(), col("total_hardness"))
            .otherwise(col("Total Hardness (as CaCO3)").cast(DoubleType()))
        ) \
        .withColumn(
            "ccme_score",
            when(col("ccme_score").isNotNull(), col("ccme_score"))
            .otherwise(col("CCME_Values").cast(DoubleType()))
        )

    # 4. Filter physical out-of-bounds corruptions
    # e.g., pH must be within [0, 14], temperature within [-5, 55] °C, DO >= 0
    clean_bounds_df = normalized_df.filter(
        (col("ph").isNull() | ((col("ph") >= 0.0) & (col("ph") <= 14.0))) &
        (col("temperature").isNull() | ((col("temperature") >= -5.0) & (col("temperature") <= 55.0))) &
        (col("dissolved_oxygen").isNull() | ((col("dissolved_oxygen") >= 0.0) & (col("dissolved_oxygen") <= 30.0))) &
        (col("ammonia").isNull() | (col("ammonia") >= 0.0)) &
        (col("turbidity_ntu").isNull() | (col("turbidity_ntu") >= 0.0)) &
        col("event_time").isNotNull()
    )

    # 5. Add temporal partition columns and audit metadata
    enriched_df = clean_bounds_df \
        .withColumn("year", year(col("event_time"))) \
        .withColumn("month", month(col("event_time"))) \
        .withColumn("silver_processed_at", current_timestamp())

    # 6. Deduplicate by (station_id, event_time) - keeping newest ingest record
    dedup_window = Window.partitionBy("station_id", "event_time").orderBy(col("ingest_timestamp").desc())
    deduped_df = enriched_df \
        .withColumn("row_num", row_number().over(dedup_window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num", "FullDate", "WaterbodyName", "CCME_Values",
              "Conductivity @25°C", "Ammonia-Total (as N)", "BOD - 5 days (Total)",
              "Total Hardness (as CaCO3)")

    return deduped_df


def upsert_to_silver_delta(
    spark: SparkSession,
    bronze_source_path: str,
    silver_target_path: str,
) -> int:
    """Reads Bronze Delta table, applies Silver transforms, and merges into Silver Delta table."""
    logger.info("Reading Bronze data from: %s", bronze_source_path)
    bronze_df = spark.read.format("delta").load(bronze_source_path)

    silver_df = transform_bronze_to_silver(bronze_df)
    record_count = silver_df.count()

    logger.info("Writing %d cleaned records to Silver Delta: %s", record_count, silver_target_path)

    # Check if target silver delta table exists
    try:
        from delta.tables import DeltaTable
        if DeltaTable.isDeltaTable(spark, silver_target_path):
            silver_table = DeltaTable.forPath(spark, silver_target_path)
            # Perform ACID MERGE on (station_id, event_time)
            silver_table.alias("target").merge(
                silver_df.alias("source"),
                "target.station_id = source.station_id AND target.event_time = source.event_time"
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
            logger.info("Delta MERGE upsert completed successfully.")
            return record_count
    except Exception as e:
        logger.warning("Could not execute DeltaTable.merge (table may not exist yet): %s. Falling back to write.", e)

    # Initial write or fallback
    silver_df.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("year", "month", "station_id") \
        .save(silver_target_path)

    logger.info("Silver Delta table initialized/appended successfully.")
    return record_count
