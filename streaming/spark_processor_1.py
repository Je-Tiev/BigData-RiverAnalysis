# File: streaming/spark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

# 1. Khởi tạo Spark Session
# Lưu ý: Cấu hình 'spark.jars.packages' cần thiết để Spark hiểu được Kafka
spark = SparkSession.builder \
    .appName("RiverQualityStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema dữ liệu đầu vào (Dựa trên chỉ số đo pH, DO, TDS...)
# Giả sử dữ liệu từ cảm biến gửi lên dạng JSON
# Schema: chấp nhận ts là timestamp; nếu producer gửi epoch xử lý ở bước transform
schema = StructType([
    StructField("ts", StringType(), True),         # có thể là ISO string hoặc epoch number string
    StructField("location", StringType(), True),
    StructField("ph", DoubleType(), True),
    StructField("do", DoubleType(), True),
    StructField("cond", DoubleType(), True)
])

# 3. Đọc dữ liệu từ Kafka (Read Stream)
# KAFKA_BOOTSTRAP_SERVERS nên để là biến môi trường hoặc config file sau này

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
topic = os.getenv("KAFKA_TOPIC", "river-quality")

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Chuyển đổi dữ liệu (Transform)


# Parse JSON then normalize fields
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select(
        col("data.ts").alias("raw_ts"),
        col("data.location").alias("station_id"),
        col("data.ph").alias("ph_level"),
        col("data.do").alias("dissolved_oxygen"),
        col("data.cond").alias("conductivity")
    )

# Handle different timestamp formats (epoch seconds/millis OR ISO string)
with_ts = parsed_df.withColumn(
    "timestamp",
    # nếu raw_ts looks like digits -> treat as epoch (millis or seconds)
    when(col("raw_ts").rlike("^[0-9]+$") & (col("raw_ts").rlike("^.{13}$")),  # 13 digits -> millis
         to_timestamp(from_unixtime((col("raw_ts").cast("long")/1000))))
    .when(col("raw_ts").rlike("^[0-9]+$"), to_timestamp(from_unixtime(col("raw_ts").cast("long"))))  # seconds
    .otherwise(to_timestamp(col("raw_ts")))  # try ISO string
)
# Clean: drop null critical fields, watermark for late data
clean_df = with_ts.dropna(subset=["timestamp", "station_id", "ph_level"]) \
    .withWatermark("timestamp", "5 minutes")

final_df = clean_df.withColumn(
    "status",
    when((col("ph_level") < 6.5) | (col("ph_level") > 8.5), "WARNING").otherwise("NORMAL")
)
# 5. Xuất dữ liệu (Write Stream) - Test ra Console trước
# Khi hệ thống ổn định sẽ đổi sang format "delta" để ghi vào MinIO
# Sink: Delta example with checkpoint and partitioning
checkpoint = os.getenv("DELTA_CHECKPOINT", "/tmp/delta/river_checkpoints")
out_path = os.getenv("DELTA_OUTPUT", "/tmp/delta/river_data")
query = final_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint) \
    .partitionBy("station_id") \
    .start(out_path)
# query = final_df.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", False) \
#     .start()
print(">>> Streaming job started...")
query.awaitTermination()