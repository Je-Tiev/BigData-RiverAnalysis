from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# ===== CẤU HÌNH =====
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka-broker:19092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'river-quality')

# MinIO settings: prefer Docker compose credentials (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)
MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT', 'minio:9000')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ROOT_USER', os.environ.get('MINIO_ACCESS_KEY', 'admin'))
MINIO_SECRET_KEY = os.environ.get('MINIO_ROOT_PASSWORD', os.environ.get('MINIO_SECRET_KEY', 'password123'))
MINIO_BUCKET = os.environ.get('MINIO_BUCKET', 'water-quality-raw')

# Use mounted spark data dir when running in compose
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR', '/opt/spark-data/checkpoints/minio_stream')

# ===== KHỞI TẠO SPARK =====
spark = SparkSession.builder \
    .appName("WaterQuality_to_MinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Đã khởi tạo Spark Session với MinIO connector")

# ===== ĐỊNH NGHĨA SCHEMA =====
# Schema phải khớp chính xác với dữ liệu từ producer
schema = StructType([
    StructField("FullDate", StringType(), True),
    StructField("WaterbodyName", StringType(), True),
    StructField("Temperature", StringType(), True),
    StructField("pH", StringType(), True),
    StructField("Dissolved Oxygen", StringType(), True),
    StructField("Conductivity @25°C", StringType(), True),
    StructField("Ammonia-Total (as N)", StringType(), True),
    StructField("BOD - 5 days (Total)", StringType(), True),
    StructField("Chloride", StringType(), True),
    StructField("Total Hardness (as CaCO3)", StringType(), True),
    StructField("CCME_Values", StringType(), True)
])

# ===== ĐỌC TỪ KAFKA =====
print(f"📡 Đang kết nối tới Kafka topic: {KAFKA_TOPIC} (bootstrap: {KAFKA_SERVER})...")
print(f"🔍 DEBUG - KAFKA_SERVER: {KAFKA_SERVER}")
print(f"🔍 DEBUG - KAFKA_TOPIC: {KAFKA_TOPIC}")
print(f"🔍 DEBUG - MINIO_ENDPOINT: {MINIO_ENDPOINT}")
print(f"🔍 DEBUG - CHECKPOINT_DIR: {CHECKPOINT_DIR}")
kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON từ Kafka value
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# ===== LÀM SẠCH VÀ CHUYỂN ĐỔI DỮ LIỆU =====
cleaned_stream = parsed_stream \
    .withColumn("timestamp", to_timestamp(col("FullDate"), "yyyy-MM-dd")) \
    .withColumn("waterbody_name", col("WaterbodyName")) \
    .withColumn("temperature", col("Temperature").cast(DoubleType())) \
    .withColumn("ph", col("pH").cast(DoubleType())) \
    .withColumn("dissolved_oxygen", col("Dissolved Oxygen").cast(DoubleType())) \
    .withColumn("conductivity", col("Conductivity @25°C").cast(DoubleType())) \
    .withColumn("ammonia", col("Ammonia-Total (as N)").cast(DoubleType())) \
    .withColumn("bod", col("BOD - 5 days (Total)").cast(DoubleType())) \
    .withColumn("chloride", col("Chloride").cast(DoubleType())) \
    .withColumn("hardness", col("Total Hardness (as CaCO3)").cast(DoubleType())) \
    .withColumn("ccme_score", col("CCME_Values").cast(DoubleType()))

# Chuyển đổi CCME_Values (số) thành quality_label (text)
cleaned_stream = cleaned_stream.withColumn(
    "quality_label",
    when(col("ccme_score") >= 95, "Excellent")
    .when(col("ccme_score") >= 80, "Good")
    .when(col("ccme_score") >= 65, "Fair")
    .when(col("ccme_score") >= 45, "Marginal")
    .otherwise("Poor")
)

# Drop các cột cũ
cleaned_stream = cleaned_stream.drop(
    "FullDate", "WaterbodyName", "CCME_Values",
    "Temperature", "pH", "Dissolved Oxygen", 
    "Conductivity @25°C", "Ammonia-Total (as N)", 
    "BOD - 5 days (Total)", "Chloride", "Total Hardness (as CaCO3)"
)

# ===== GHI VÀO MINIO =====
output_path = f"s3a://{MINIO_BUCKET}/raw_data"

print(f"💾 Bắt đầu ghi streaming data vào MinIO: {output_path}")

query = cleaned_stream \
    .writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .partitionBy("quality_label") \
    .trigger(processingTime='30 seconds') \
    .start()

print("🚀 Stream đang chạy... Nhấn Ctrl+C để dừng")

# ===== MONITOR =====
try:
    import time
    while query.isActive:
        time.sleep(10)
        progress = query.lastProgress
        if progress:
            print(f"⏱️  Batch: {progress['batchId']} | "
                  f"Input Rows: {progress['numInputRows']} | "
                  f"Processed: {progress.get('sink', {}).get('numOutputRows', 0)}")
except KeyboardInterrupt:
    print("\n⏹️  Đang dừng stream...")
    query.stop()
    spark.stop()
    print("✅ Đã đóng kết nối!")