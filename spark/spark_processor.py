import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 1. KHỞI TẠO SPARK VỚI MONGODB CONNECTOR ---
# Lưu ý dòng spark.jars.packages: Mình đã gộp cả Kafka và MongoDB vào đây
spark = SparkSession.builder \
    .appName("RiverQualityRealtimeProcessor") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/river_monitoring.sensor_data") \
    .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/river_monitoring.sensor_data") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# --- 2. ĐỊNH NGHĨA SCHEMA ---
schema = StructType([
    StructField("FullDate", StringType(), True),
    StructField("WaterbodyName", StringType(), True),
    StructField("Temperature", FloatType(), True),
    StructField("pH", FloatType(), True),
    StructField("Dissolved Oxygen", FloatType(), True),
    StructField("Conductivity @25°C", FloatType(), True),
    StructField("Ammonia-Total (as N)", FloatType(), True),
    StructField("BOD - 5 days (Total)", FloatType(), True),
    StructField("Chloride", FloatType(), True),
    StructField("Total Hardness (as CaCO3)", FloatType(), True),
    StructField("CCME_WQI", StringType(), True),
    StructField("CCME_Values", FloatType(), True)
])

# --- 3. ĐỌC TỪ KAFKA ---
logger.info("📡 Đang kết nối tới Kafka...")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "river_sensors") \
    .option("startingOffsets", "latest") \
    .load()

# --- 4. XỬ LÝ DỮ LIỆU (TRANSFORMATION) ---
# Parse JSON & Đổi tên cột
parsed_df = raw_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.FullDate").alias("timestamp"),
        col("data.WaterbodyName").alias("location"),
        col("data.Temperature").alias("temp"),
        col("data.pH").alias("ph"),
        col("data.`Dissolved Oxygen`").alias("do_mgL"),
        col("data.`Conductivity @25°C`").alias("conductivity"),
        col("data.`Ammonia-Total (as N)`").alias("ammonia"),
        col("data.`BOD - 5 days (Total)`").alias("bod"),
        col("data.CCME_WQI").alias("wqi_category_ref"),
        col("data.CCME_Values").alias("wqi_score_ref")
    )

# Đánh giá rủi ro (Data Enrichment)
processed_df = parsed_df.withColumn(
    "my_assessment",
    when(
        (col("ph").between(6.5, 8.5)) & 
        (col("do_mgL") >= 5.0) & 
        (col("ammonia") < 0.5), 
        "SAFE"
    ).otherwise("WARNING")
)

# Tạo cảnh báo
processed_df = processed_df.withColumn(
    "alert_message",
    when(col("ph") < 4.0, "ACID_HIGH_DANGER")
    .when(col("ph") > 9.0, "ALKALI_HIGH_DANGER")
    .when(col("do_mgL") < 2.0, "FISH_KILL_RISK")
    .when(col("ammonia") > 2.0, "TOXIC_WASTE")
    .otherwise(None)
)

# --- 5. LƯU VÀO MONGODB (SINK QUAN TRỌNG) ---
# Lưu ý: Checkpoint là bắt buộc để đảm bảo không mất dữ liệu
query_mongo = processed_df.writeStream \
    .format("mongodb") \
    .option("checkpointLocation", "./checkpoints/mongo_data") \
    .option("forceDeleteTempCheckpointLocation", "true") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# --- 6. HIỂN THỊ CONSOLE (Để debug) ---
# Chỉ hiện các dòng có Cảnh báo để đỡ rác màn hình
query_console = processed_df.filter(col("alert_message").isNotNull()) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

logger.info("🚀 Hệ thống đang chạy: Kafka -> Spark -> MongoDB!")
spark.streams.awaitAnyTermination()