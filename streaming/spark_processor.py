# File: streaming/spark_processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# 1. Khởi tạo Spark Session
# Lưu ý: Cấu hình 'spark.jars.packages' cần thiết để Spark hiểu được Kafka
spark = SparkSession.builder \
    .appName("RiverQualityStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Định nghĩa Schema dữ liệu đầu vào (Dựa trên chỉ số đo pH, DO, TDS...)
# Giả sử dữ liệu từ cảm biến gửi lên dạng JSON
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("ph_value", FloatType(), True),
    StructField("do_value", FloatType(), True), # Dissolved Oxygen
    StructField("tds_value", FloatType(), True) # Total Dissolved Solids
])

# 3. Đọc dữ liệu từ Kafka (Read Stream)
# KAFKA_BOOTSTRAP_SERVERS nên để là biến môi trường hoặc config file sau này
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "river-quality-sensors") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Chuyển đổi dữ liệu (Transform)
# Kafka gửi dữ liệu dưới dạng binary ở cột 'value', cần cast sang String và parse JSON
processed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), sensor_schema).alias("data")) \
    .select("data.*")

# 5. Xuất dữ liệu (Write Stream) - Test ra Console trước
# Khi hệ thống ổn định sẽ đổi sang format "delta" để ghi vào MinIO
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

print(">>> Streaming job started...")
query.awaitTermination()