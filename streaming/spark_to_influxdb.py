"""Distributed Spark Structured Streaming to InfluxDB with DLQ and Watermarking.

Production-grade stream processing engine:
1. Distributed partition-level writes (eliminating Driver OOM collect() anti-pattern).
2. Dead Letter Queue (DLQ) isolation for corrupted/unparseable messages.
3. Event-time watermarking (10m) to accurately process late-arriving telemetry.
4. Real-time CCME WQI and critical alert tagging (QCVN 08-MT:2015/BTNMT).
"""

import logging
import os
import sys
from collections.abc import Iterator

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    struct,
    to_json,
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

# ===== STRUCTURED LOGGING =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [SparkInfluxStream]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("SparkInfluxStream")

# ===== CONFIGURATION =====
KAFKA_SERVER = os.environ.get("KAFKA_BOOTSTRAP", os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:19092"))
KAFKA_TOPIC_RAW = os.environ.get("KAFKA_TOPIC_RAW", os.environ.get("KAFKA_TOPIC", "river.sensors.raw"))
KAFKA_TOPIC_DLQ = os.environ.get("KAFKA_TOPIC_DLQ", "river.sensors.dlq")
KAFKA_TOPIC_ALERTS = os.environ.get("KAFKA_TOPIC_ALERTS", "river.alerts.critical")

INFLUXDB_URL = os.environ.get("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.environ.get("INFLUXDB_TOKEN", "my-super-secret-auth-token")
INFLUXDB_ORG = os.environ.get("INFLUXDB_ORG", "water-quality")
INFLUXDB_BUCKET = os.environ.get("INFLUXDB_BUCKET", "river-data")

CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/opt/spark-data/checkpoints/influxdb_stream")
DLQ_CHECKPOINT_DIR = os.environ.get("DLQ_CHECKPOINT_DIR", "/opt/spark-data/checkpoints/dlq_stream")
WATERMARK_DURATION = os.environ.get("WATERMARK_DURATION", "10 minutes")

logger.info("Initializing Spark Streaming Pipeline...")
logger.info("Kafka Server: %s | Ingestion Topic: %s", KAFKA_SERVER, KAFKA_TOPIC_RAW)
logger.info("InfluxDB Target: %s (Org: %s, Bucket: %s)", INFLUXDB_URL, INFLUXDB_ORG, INFLUXDB_BUCKET)

# ===== SPARK SESSION =====
spark = (
    SparkSession.builder.appName("RiverQuality_to_InfluxDB_Production")
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ===== CANONICAL + COMPATIBILITY SCHEMA =====
# Accommodates both canonical sensor contract and legacy CSV replay schema
sensor_schema = StructType(
    [
        # Canonical Fields (v1.2.0)
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
        # Legacy Fields (Backward compatibility)
        StructField("FullDate", StringType(), True),
        StructField("WaterbodyName", StringType(), True),
        StructField("Conductivity @25°C", StringType(), True),
        StructField("Ammonia-Total (as N)", StringType(), True),
        StructField("BOD - 5 days (Total)", StringType(), True),
        StructField("Total Hardness (as CaCO3)", StringType(), True),
        StructField("CCME_Values", StringType(), True),
    ]
)

# ===== READ KAFKA STREAM =====
raw_stream = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", f"{KAFKA_TOPIC_RAW},river-quality")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Extract string value & Kafka ingestion metadata
parsed_json_stream = raw_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("raw_payload"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    from_json(col("value").cast("string"), sensor_schema).alias("data"),
)

# ===== DEAD LETTER QUEUE (DLQ) FILTERING =====
# Identify corrupt, unparseable, or empty payloads
corrupt_stream = parsed_json_stream.filter(
    col("data").isNull() | (col("data.station_id").isNull() & col("data.WaterbodyName").isNull())
).select(
    col("kafka_key").alias("key"),
    to_json(
        struct(
            col("kafka_key"),
            col("raw_payload"),
            col("kafka_timestamp"),
            col("kafka_partition"),
            col("kafka_offset"),
            lit("SCHEMA_PARSE_FAILURE_OR_MISSING_ID").alias("dlq_reason"),
            current_timestamp().alias("dlq_timestamp"),
        )
    ).alias("value"),
)

# Start DLQ Sink (Write bad records back to Kafka DLQ Topic)
dlq_query = (
    corrupt_stream.writeStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("topic", KAFKA_TOPIC_DLQ)
    .option("checkpointLocation", DLQ_CHECKPOINT_DIR)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .start()
)

# ===== NORMALIZATION & WATERMARKING =====
valid_stream = parsed_json_stream.filter(
    col("data").isNotNull() & (col("data.station_id").isNotNull() | col("data.WaterbodyName").isNotNull())
).select("data.*", "kafka_timestamp")

# Standardize unified columns across legacy and canonical schemas
normalized_stream = (
    valid_stream.withColumn(
        "station_id", when(col("station_id").isNotNull(), col("station_id")).otherwise(col("WaterbodyName"))
    )
    .withColumn("river_name", when(col("river_name").isNotNull(), col("river_name")).otherwise(col("WaterbodyName")))
    .withColumn(
        "event_time",
        when(col("timestamp").isNotNull(), to_timestamp(col("timestamp")))
        .when(col("FullDate").isNotNull(), to_timestamp(col("FullDate"), "yyyy-MM-dd"))
        .otherwise(col("kafka_timestamp")),
    )
    .withColumn(
        "conductivity",
        when(col("conductivity").isNotNull(), col("conductivity")).otherwise(
            col("Conductivity @25°C").cast(DoubleType())
        ),
    )
    .withColumn(
        "ammonia",
        when(col("ammonia").isNotNull(), col("ammonia")).otherwise(col("Ammonia-Total (as N)").cast(DoubleType())),
    )
    .withColumn(
        "bod5", when(col("bod5").isNotNull(), col("bod5")).otherwise(col("BOD - 5 days (Total)").cast(DoubleType()))
    )
    .withColumn(
        "total_hardness",
        when(col("total_hardness").isNotNull(), col("total_hardness")).otherwise(
            col("Total Hardness (as CaCO3)").cast(DoubleType())
        ),
    )
    .withColumn(
        "ccme_score",
        when(col("ccme_score").isNotNull(), col("ccme_score")).otherwise(col("CCME_Values").cast(DoubleType())),
    )
)

# Watermarking on event_time to handle late-arriving records
watermarked_stream = normalized_stream.filter(col("event_time").isNotNull()).withWatermark(
    "event_time", WATERMARK_DURATION
)

# Add Critical Pollution Flags based on QCVN 08-MT:2015/BTNMT
alert_tagged_stream = watermarked_stream.withColumn(
    "is_critical_pollution",
    (col("dissolved_oxygen") < 4.0) | (col("ph") < 5.5) | (col("ph") > 9.0) | (col("ammonia") > 1.5),
).withColumn(
    "quality_label",
    when(col("quality_label").isNotNull(), col("quality_label"))
    .when(col("ccme_score") >= 95.0, "Excellent")
    .when(col("ccme_score") >= 80.0, "Good")
    .when(col("ccme_score") >= 65.0, "Fair")
    .when(col("ccme_score") >= 45.0, "Marginal")
    .otherwise("Poor"),
)


# ===== DISTRIBUTED PARTITION-LEVEL INFLUXDB WRITER =====
# Eliminates Driver OOM collect() Anti-Pattern
def write_partition_to_influxdb(rows_iterator: Iterator) -> None:
    """Writes a single partition's rows to InfluxDB using worker-local batching."""
    try:
        import urllib3.exceptions
        from influxdb_client import InfluxDBClient, Point
        from influxdb_client.client.exceptions import InfluxDBError
        from influxdb_client.client.write_api import SYNCHRONOUS
    except ImportError:
        # Fallback if influxdb_client is not installed on worker
        return

    client = InfluxDBClient(
        url=INFLUXDB_URL,
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
        timeout=15_000,
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    points = []

    try:
        for row in rows_iterator:
            r = row.asDict()
            event_ts = r.get("event_time")
            if not event_ts:
                continue

            station_id = str(r.get("station_id") or "UNKNOWN")
            river_name = str(r.get("river_name") or "UNKNOWN")
            quality_label = str(r.get("quality_label") or "UNKNOWN")
            is_anomaly = bool(r.get("is_anomaly") or r.get("is_critical_pollution") or False)

            point = (
                Point("water_quality")
                .tag("station_id", station_id)
                .tag("river_name", river_name)
                .tag("quality_label", quality_label)
                .tag("is_anomaly", str(is_anomaly).lower())
                .time(event_ts)
            )

            # Map float telemetry metrics
            numeric_fields = [
                "temperature",
                "ph",
                "dissolved_oxygen",
                "conductivity",
                "ammonia",
                "bod5",
                "chloride",
                "total_hardness",
                "turbidity_ntu",
                "flow_rate_m3s",
                "ccme_score",
            ]
            for field_name in numeric_fields:
                val = r.get(field_name)
                if val is not None:
                    try:
                        point.field(field_name, float(val))
                    except (ValueError, TypeError):
                        pass

            points.append(point)

            # Batch write every 250 points per worker partition
            if len(points) >= 250:
                write_api.write(bucket=INFLUXDB_BUCKET, record=points)
                points = []

        # Flush remaining points
        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)

    except (InfluxDBError, urllib3.exceptions.HTTPError, OSError) as exc:
        sys.stderr.write(f"Error writing partition to InfluxDB: {exc}\n")
    finally:
        write_api.close()
        client.close()


def process_micro_batch(batch_df, batch_id: int) -> None:
    """Dispatches distributed batch write across executors without collecting to Driver."""
    count = batch_df.count()
    if count == 0:
        return

    logger.info("Batch #%d: Processing %d records across worker partitions...", batch_id, count)
    # Execute distributed foreachPartition on executors
    batch_df.rdd.foreachPartition(write_partition_to_influxdb)
    logger.info("Batch #%d: Distributed partition write completed.", batch_id)


# ===== START STREAMING SINK =====
logger.info("Starting InfluxDB Structured Streaming Sink...")
main_query = (
    alert_tagged_stream.writeStream.outputMode("append")
    .foreachBatch(process_micro_batch)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

logger.info("Pipeline active. Waiting for incoming records...")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    logger.info("Streaming halted by user.")
    main_query.stop()
    dlq_query.stop()
    spark.stop()
    logger.info("Spark Session closed cleanly.")
