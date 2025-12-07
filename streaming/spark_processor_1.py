# File: streaming/spark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
import platform

# Bộ package Spark cần thiết khi chạy local trên Windows (thiếu JAR dựng sẵn)
DEFAULT_SPARK_PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
    "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.7",
    "io.delta:delta-spark_2.12:3.2.0"
])
# Danh sách JAR có sẵn trong image Docker (nếu chạy trong container)
DEFAULT_LOCAL_JARS = [
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.7.jar",
    "/opt/spark/jars/kafka-clients-3.6.1.jar",
    "/opt/spark/jars/delta-spark_2.12-3.2.0.jar",
]

# 1. Khởi tạo Spark Session
# Khởi tạo biến môi trường Hadoop cho Windows để tránh lỗi thiếu HADOOP_HOME / winutils.exe
if platform.system() == "Windows":
    # Ưu tiên dùng biến môi trường đã có; nếu chưa có thì gợi ý đường dẫn mặc định
    hadoop_home = os.environ.get("HADOOP_HOME") or r"C:\hadoop\hadoop-3.3.2"
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    bin_path = os.path.join(hadoop_home, "bin")
    if bin_path not in os.environ.get("PATH", ""):
        os.environ["PATH"] = os.environ.get("PATH", "") + ";" + bin_path

extra_packages = os.getenv("SPARK_EXTRA_PACKAGES")
if platform.system() == "Windows" and not extra_packages:
    # Khi phát triển local cần tự tải package Kafka + Delta
    extra_packages = DEFAULT_SPARK_PACKAGES

builder = SparkSession.builder \
    .appName("RiverQualityStreaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={os.environ.get('HADOOP_HOME', '')}") \
    .config("spark.executor.extraJavaOptions", f"-Dhadoop.home.dir={os.environ.get('HADOOP_HOME', '')}")

if extra_packages:
    builder = builder.config("spark.jars.packages", extra_packages)

extra_jars = os.getenv("SPARK_EXTRA_JARS")
if extra_jars:
    jar_candidates = [p.strip() for p in extra_jars.split(",") if p.strip()]
else:
    jar_candidates = DEFAULT_LOCAL_JARS
local_jars = [path for path in jar_candidates if os.path.exists(path)]
if local_jars:
    builder = builder.config("spark.jars", ",".join(local_jars))

spark = builder.getOrCreate()

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

kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka-broker:19092")
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
# Sink: Delta mặc định, có thể đổi sang console bằng biến môi trường
checkpoint = os.getenv("DELTA_CHECKPOINT", "/tmp/delta/river_checkpoints")
out_path = os.getenv("DELTA_OUTPUT", "/tmp/delta/river_data")
sink_format = os.getenv("STREAM_SINK_FORMAT", "delta").lower()

if sink_format == "console":
    query = final_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", False) \
        .start()
else:
    query = final_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint) \
        .partitionBy("station_id") \
        .start(out_path)

print(f">>> Streaming job started with sink={sink_format} ...")
query.awaitTermination()