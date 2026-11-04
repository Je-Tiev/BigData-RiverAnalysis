"""Medallion Lakehouse - Gold Layer & ML Feature Store Module.

Generates business-level aggregated datasets and feature store tables from Silver:
1. river_wqi_gold: Aggregated metrics & CCME WQI scores by station and hourly/daily windows.
2. river_ml_features_gold: Rich Feature Store with multi-step lags, rolling window statistics,
   cyclical temporal encodings, and delta derivatives ready for Machine Learning.
"""

import logging
import math

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    cos,
    count,
    current_timestamp,
    date_trunc,
    expr,
    lag,
    lead,
    max as spark_max,
    min as spark_min,
    sin,
    stddev,
    when,
    year,
)
from pyspark.sql.window import Window

logger = logging.getLogger("LakehouseGold")


def compute_gold_hourly_wqi(silver_df: DataFrame) -> DataFrame:
    """Aggregates Silver records into 1-hour windows per station with aggregated metrics and CCME WQI."""
    # Window by station_id and 1-hour tumbling window
    hourly_df = silver_df \
        .groupBy(
            date_trunc("hour", col("event_time")).alias("window_start"),
            col("station_id"),
            col("river_name"),
            col("basin_name"),
            col("latitude"),
            col("longitude"),
        ) \
        .agg(
            count("*").alias("sample_count"),
            avg("temperature").alias("avg_temperature"),
            spark_min("temperature").alias("min_temperature"),
            spark_max("temperature").alias("max_temperature"),
            avg("ph").alias("avg_ph"),
            spark_min("ph").alias("min_ph"),
            spark_max("ph").alias("max_ph"),
            avg("dissolved_oxygen").alias("avg_do"),
            spark_min("dissolved_oxygen").alias("min_do"),
            spark_max("dissolved_oxygen").alias("max_do"),
            avg("conductivity").alias("avg_conductivity"),
            spark_max("conductivity").alias("max_conductivity"),
            avg("ammonia").alias("avg_ammonia"),
            spark_max("ammonia").alias("max_ammonia"),
            avg("bod5").alias("avg_bod5"),
            spark_max("bod5").alias("max_bod5"),
            avg("turbidity_ntu").alias("avg_turbidity"),
            spark_max("turbidity_ntu").alias("max_turbidity"),
            avg("flow_rate_m3s").alias("avg_flow_rate"),
            count(when(col("is_anomaly"), 1)).alias("anomaly_count"),
        )

    # Simplified Vectorized CCME WQI calculation on aggregated averages
    # Objectives: pH in [6.5, 8.5], DO in [5.0, 15.0], Ammonia <= 0.3, BOD5 <= 4.0, Turbidity <= 50.0
    wqi_df = hourly_df \
        .withColumn("fail_ph", when((col("avg_ph") < 6.5) | (col("avg_ph") > 8.5), 1.0).otherwise(0.0)) \
        .withColumn("fail_do", when(col("avg_do") < 5.0, 1.0).otherwise(0.0)) \
        .withColumn("fail_amm", when(col("avg_ammonia") > 0.3, 1.0).otherwise(0.0)) \
        .withColumn("fail_bod", when(col("avg_bod5") > 4.0, 1.0).otherwise(0.0)) \
        .withColumn("fail_turb", when(col("avg_turbidity") > 50.0, 1.0).otherwise(0.0))

    # Total variables = 5
    total_vars = 5.0
    computed_df = wqi_df \
        .withColumn("sum_failed", col("fail_ph") + col("fail_do") + col("fail_amm") + col("fail_bod") + col("fail_turb")) \
        .withColumn("f1_scope", (col("sum_failed") / total_vars) * 100.0) \
        .withColumn("f2_freq", (col("sum_failed") / total_vars) * 100.0) \
        .withColumn(
            "wqi_score",
            100.0 - (expr("sqrt(f1_scope * f1_scope + f2_freq * f2_freq)") / 1.732)
        ) \
        .withColumn(
            "quality_label",
            when(col("wqi_score") >= 95.0, "Excellent")
            .when(col("wqi_score") >= 80.0, "Good")
            .when(col("wqi_score") >= 65.0, "Fair")
            .when(col("wqi_score") >= 45.0, "Marginal")
            .otherwise("Poor")
        ) \
        .withColumn("year", year(col("window_start"))) \
        .withColumn("gold_processed_at", current_timestamp()) \
        .drop("fail_ph", "fail_do", "fail_amm", "fail_bod", "fail_turb", "sum_failed", "f1_scope", "f2_freq")

    return computed_df


def build_ml_feature_store(silver_df: DataFrame) -> DataFrame:
    """Builds the ML Feature Store with time lags, rolling statistics, and cyclical encodings."""
    # Ensure ordered window by station and time
    station_time_window = Window.partitionBy("station_id").orderBy("event_time")

    # Rolling windows (preceding 3 and 6 observations)
    rolling_3 = Window.partitionBy("station_id").orderBy("event_time").rowsBetween(-3, 0)
    rolling_6 = Window.partitionBy("station_id").orderBy("event_time").rowsBetween(-6, 0)

    # 1. Base temporal cyclical features
    # hour sin/cos (24h period) & month sin/cos (12m period)
    pi = math.pi
    feat_df = silver_df \
        .withColumn("hour", expr("hour(event_time)")) \
        .withColumn("month_num", expr("month(event_time)")) \
        .withColumn("hour_sin", sin(col("hour") * (2.0 * pi / 24.0))) \
        .withColumn("hour_cos", cos(col("hour") * (2.0 * pi / 24.0))) \
        .withColumn("month_sin", sin(col("month_num") * (2.0 * pi / 12.0))) \
        .withColumn("month_cos", cos(col("month_num") * (2.0 * pi / 12.0)))

    # 2. Lag Features for key predictors (DO, pH, Ammonia, Temperature)
    feat_df = feat_df \
        .withColumn("do_lag_1", lag("dissolved_oxygen", 1).over(station_time_window)) \
        .withColumn("do_lag_2", lag("dissolved_oxygen", 2).over(station_time_window)) \
        .withColumn("do_lag_3", lag("dissolved_oxygen", 3).over(station_time_window)) \
        .withColumn("ph_lag_1", lag("ph", 1).over(station_time_window)) \
        .withColumn("ph_lag_2", lag("ph", 2).over(station_time_window)) \
        .withColumn("temp_lag_1", lag("temperature", 1).over(station_time_window)) \
        .withColumn("amm_lag_1", lag("ammonia", 1).over(station_time_window)) \
        .withColumn("amm_lag_2", lag("ammonia", 2).over(station_time_window))

    # 3. Rolling Window Statistics
    feat_df = feat_df \
        .withColumn("do_rolling_mean_3", avg("dissolved_oxygen").over(rolling_3)) \
        .withColumn("do_rolling_std_3", stddev("dissolved_oxygen").over(rolling_3)) \
        .withColumn("ph_rolling_mean_3", avg("ph").over(rolling_3)) \
        .withColumn("amm_rolling_mean_6", avg("ammonia").over(rolling_6))

    # 4. First-order Delta (Rate of Change)
    feat_df = feat_df \
        .withColumn("delta_do", col("dissolved_oxygen") - col("do_lag_1")) \
        .withColumn("delta_ph", col("ph") - col("ph_lag_1")) \
        .withColumn("delta_ammonia", col("ammonia") - col("amm_lag_1"))

    # 5. Future Prediction Targets for Supervised ML (1-step and 3-step ahead)
    feat_df = feat_df \
        .withColumn("target_do_next_1", lead("dissolved_oxygen", 1).over(station_time_window)) \
        .withColumn("target_ph_next_1", lead("ph", 1).over(station_time_window)) \
        .withColumn("target_wqi_next_1", lead("ccme_score", 1).over(station_time_window)) \
        .withColumn("feature_store_created_at", current_timestamp())

    return feat_df


def generate_gold_tables(
    spark: SparkSession,
    silver_source_path: str,
    gold_wqi_target_path: str,
    gold_features_target_path: str,
) -> dict:
    """Generates both Gold aggregated WQI and ML Feature Store Delta tables."""
    logger.info("Reading Silver data from: %s", silver_source_path)
    silver_df = spark.read.format("delta").load(silver_source_path)

    # 1. Hourly WQI Aggregation
    logger.info("Computing Gold Hourly WQI Aggregations...")
    gold_wqi_df = compute_gold_hourly_wqi(silver_df)
    wqi_count = gold_wqi_df.count()

    gold_wqi_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "station_id") \
        .option("overwriteSchema", "true") \
        .save(gold_wqi_target_path)
    logger.info("Saved %d records to Gold WQI Delta: %s", wqi_count, gold_wqi_target_path)

    # 2. ML Feature Store
    logger.info("Generating Gold ML Feature Store...")
    features_df = build_ml_feature_store(silver_df)
    feat_count = features_df.count()

    features_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "station_id") \
        .option("overwriteSchema", "true") \
        .save(gold_features_target_path)
    logger.info("Saved %d feature vectors to Gold Feature Store: %s", feat_count, gold_features_target_path)

    return {"wqi_records": wqi_count, "feature_records": feat_count}
