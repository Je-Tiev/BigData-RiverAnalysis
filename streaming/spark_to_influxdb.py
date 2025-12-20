from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import os
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# ===== CẤU HÌNH =====
KAFKA_SERVER = os.environ.get('KAFKA_BOOTSTRAP', 'kafka-broker:19092')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'river-quality')

# InfluxDB settings
INFLUXDB_URL = os.environ.get('INFLUXDB_URL', 'http://influxdb:8086')
INFLUXDB_TOKEN = os.environ.get('INFLUXDB_TOKEN', 'my-super-secret-auth-token')
INFLUXDB_ORG = os.environ.get('INFLUXDB_ORG', 'water-quality')
INFLUXDB_BUCKET = os.environ.get('INFLUXDB_BUCKET', 'river-data')

# Checkpoint directory
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR', '/opt/spark-data/checkpoints/influxdb_stream')

# ===== KHỞI TẠO SPARK =====
spark = SparkSession.builder \
    .appName("WaterQuality_to_InfluxDB") \
    .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Đã khởi tạo Spark Session cho InfluxDB")
print(f"🔗 InfluxDB URL: {INFLUXDB_URL}")
print(f"🏢 Organization: {INFLUXDB_ORG}")
print(f"🪣 Bucket: {INFLUXDB_BUCKET}")

# ===== ĐỊNH NGHĨA SCHEMA =====
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

# ===== HÀM GHI VÀO INFLUXDB =====
def write_to_influxdb(batch_df, batch_id):
    """
    Ghi một batch DataFrame vào InfluxDB
    """
    if batch_df.count() == 0:
        print(f"⚠️  Batch {batch_id} rỗng, bỏ qua...")
        return
    
    print(f"📝 Đang ghi batch {batch_id} với {batch_df.count()} dòng vào InfluxDB...")
    
    # Tạo InfluxDB client
    client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
    write_api = client.write_api(write_options=SYNCHRONOUS)
    
    try:
        # Collect data từ batch và convert sang dict
        rows = batch_df.collect()
        
        # Tạo danh sách points
        points = []
        success_count = 0
        error_count = 0
        
        for row in rows:
            try:
                # Convert Row to dict để dễ access
                row_dict = row.asDict()
                
                # Bỏ qua nếu thiếu thông tin quan trọng
                if not row_dict.get('timestamp') or not row_dict.get('waterbody_name'):
                    error_count += 1
                    continue
                
                # Tạo Point với measurement name là "water_quality"
                point = Point("water_quality") \
                    .tag("waterbody", str(row_dict.get('waterbody_name', 'Unknown'))) \
                    .tag("quality_label", str(row_dict.get('quality_label', 'Unknown'))) \
                    .time(row_dict['timestamp'])
                
                # Thêm các fields (numeric values) - check None trước khi cast
                if row_dict.get('temperature') is not None:
                    point.field("temperature", float(row_dict['temperature']))
                if row_dict.get('ph') is not None:
                    point.field("ph", float(row_dict['ph']))
                if row_dict.get('dissolved_oxygen') is not None:
                    point.field("dissolved_oxygen", float(row_dict['dissolved_oxygen']))
                if row_dict.get('conductivity') is not None:
                    point.field("conductivity", float(row_dict['conductivity']))
                if row_dict.get('ammonia') is not None:
                    point.field("ammonia", float(row_dict['ammonia']))
                if row_dict.get('bod') is not None:
                    point.field("bod", float(row_dict['bod']))
                if row_dict.get('chloride') is not None:
                    point.field("chloride", float(row_dict['chloride']))
                if row_dict.get('hardness') is not None:
                    point.field("hardness", float(row_dict['hardness']))
                if row_dict.get('ccme_score') is not None:
                    point.field("ccme_score", float(row_dict['ccme_score']))
                
                points.append(point)
                success_count += 1
                
            except Exception as e:
                error_count += 1
                print(f"⚠️  Lỗi khi xử lý dòng: {e}")
                continue
        
        # Ghi vào InfluxDB
        if points:
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            print(f"✅ Đã ghi {success_count}/{len(rows)} points vào InfluxDB (batch {batch_id})")
            if error_count > 0:
                print(f"⚠️  Bỏ qua {error_count} dòng lỗi")
        else:
            print(f"⚠️  Không có points hợp lệ trong batch {batch_id}")
        
    except Exception as e:
        print(f"❌ Lỗi khi ghi batch {batch_id} vào InfluxDB: {e}")
        import traceback
        traceback.print_exc()
    finally:
        write_api.close()
        client.close()

# ===== BẮT ĐẦU STREAMING =====
print(f"💾 Bắt đầu ghi streaming data vào InfluxDB")

query = cleaned_stream \
    .writeStream \
    .foreachBatch(write_to_influxdb) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
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
                  f"Processing Time: {progress.get('batchDuration', 0)}ms")
except KeyboardInterrupt:
    print("\nℹ️  Đang dừng stream...")
    query.stop()
    spark.stop()
    print("✅ Đã đóng kết nối!")