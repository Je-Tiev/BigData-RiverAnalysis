from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, max, min, count, 
    when, current_timestamp, expr
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os

# ===== CẤU HÌNH =====
KAFKA_SERVER = os.environ.get('KAFKA_SERVER', 'kafka-broker:19092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'river-quality')

# MongoDB: use docker-compose credentials by default
MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://root:password123@mongodb:27017/?authSource=admin')
MONGODB_DATABASE = os.environ.get('MONGODB_DATABASE', 'water_quality')
MONGODB_COLLECTION = os.environ.get('MONGODB_COLLECTION', 'river_metrics')
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR', '/opt/spark-data/checkpoints/mongodb_stream')

# ===== KHỞI TẠO SPARK =====
spark = SparkSession.builder \
    .appName("WaterQuality_to_MongoDB") \
    .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
    .config("spark.mongodb.write.database", MONGODB_DATABASE) \
    .config("spark.mongodb.write.collection", MONGODB_COLLECTION) \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Đã khởi tạo Spark Session với MongoDB connector")

# ===== SCHEMA =====
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

kafka_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

# ===== XỬ LÝ VÀ TÍNH TOÁN METRICS =====
processed_stream = parsed_stream \
    .withColumn("timestamp", to_timestamp(col("FullDate"), "yyyy-MM-dd")) \
    .withColumn("temperature", col("Temperature").cast(DoubleType())) \
    .withColumn("ph", col("pH").cast(DoubleType())) \
    .withColumn("dissolved_oxygen", col("Dissolved Oxygen").cast(DoubleType())) \
    .withColumn("conductivity", col("Conductivity @25°C").cast(DoubleType())) \
    .withColumn("ammonia", col("Ammonia-Total (as N)").cast(DoubleType())) \
    .withColumn("bod", col("BOD - 5 days (Total)").cast(DoubleType())) \
    .withColumn("chloride", col("Chloride").cast(DoubleType())) \
    .withColumn("hardness", col("Total Hardness (as CaCO3)").cast(DoubleType())) \
    .withColumn("processed_at", current_timestamp())

# Thêm cảnh báo chất lượng nước
processed_stream = processed_stream.withColumn(
    "alert_level",
    when(col("CCME_Values") == "Poor", "HIGH")
    .when(col("CCME_Values") == "Marginal", "MEDIUM")
    .when(col("CCME_Values") == "Fair", "LOW")
    .otherwise("NORMAL")
).withColumn(
    "is_critical",
    (col("dissolved_oxygen") < 5.0) |  # Oxy thấp nguy hiểm
    (col("ph") < 6.0) | (col("ph") > 9.0) |  # pH bất thường
    (col("ammonia") > 2.0)  # Ammonia cao độc hại
)

# ===== TÍNH METRICS THEO TỪNG SÔNG (AGGREGATE) =====
# Sử dụng watermark để xử lý late data (10 phút)
watermarked_stream = processed_stream \
    .withWatermark("timestamp", "10 minutes")

# Tính metrics mỗi 5 phút, cập nhật mỗi 1 phút
aggregated_metrics = watermarked_stream \
    .groupBy(
        window(col("timestamp"), "5 minutes", "1 minute"),
        col("WaterbodyName")
    ) \
    .agg(
        count("*").alias("sample_count"),
        avg("temperature").alias("avg_temperature"),
        avg("ph").alias("avg_ph"),
        avg("dissolved_oxygen").alias("avg_do"),
        avg("conductivity").alias("avg_conductivity"),
        max("ammonia").alias("max_ammonia"),
        max("bod").alias("max_bod"),
        count(when(col("is_critical") == True, 1)).alias("critical_count")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("WaterbodyName").alias("river_name"),
        "*"
    ).drop("window", "WaterbodyName")

# ===== FUNCTION GHI VÀO MONGODB =====
def write_to_mongodb(batch_df, batch_id):
    """Ghi từng batch vào MongoDB using native connector"""
    row_count = batch_df.count()
    
    if row_count > 0:
        print(f"📝 Batch {batch_id}: Đang ghi {row_count} records vào MongoDB...")
        
        try:
            # Write directly with connector - simpler and more reliable
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("connection.uri", MONGODB_URI) \
                .option("database", MONGODB_DATABASE) \
                .option("collection", MONGODB_COLLECTION) \
                .save()
            
            print(f"✅ Batch {batch_id}: Đã ghi thành công {row_count} records!")
            
        except Exception as e:
            print(f"❌ Batch {batch_id}: Lỗi khi ghi - {str(e)}")
            # Log but don't fail the stream
            
    else:
        print(f"⚠️  Batch {batch_id}: Không có dữ liệu để ghi")

# ===== GHI VÀO MONGODB =====
print(f"💾 Bắt đầu streaming metrics vào MongoDB: {MONGODB_DATABASE}.{MONGODB_COLLECTION}")

query = aggregated_metrics \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_mongodb) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime='10 seconds') \
    .start()

print("🚀 Stream đang chạy... Nhấn Ctrl+C để dừng")
print("\n📊 Đang tính toán metrics theo cửa sổ 5 phút...")

# ===== CONSOLE OUTPUT (DEBUG) =====
# Uncomment dòng dưới để xem preview data trên console
# debug_query = aggregated_metrics.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

# ===== MONITOR =====
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\n⏹️  Đang dừng stream...")
    query.stop()
    spark.stop()
    print("✅ Đã đóng kết nối!")
    
# ===== HƯỚNG DẪN TẠO INDEX TRÊN MONGODB =====
"""
Sau khi chạy script, vào MongoDB shell và tạo index:

use water_quality
db.river_metrics.createIndex({ "window_start": -1, "river_name": 1 })
db.river_metrics.createIndex({ "critical_count": -1 })
db.river_metrics.createIndex({ "river_name": 1, "window_start": -1 })

Để query dữ liệu:
db.river_metrics.find({ "critical_count": { $gt: 0 } }).sort({ "window_start": -1 })
"""