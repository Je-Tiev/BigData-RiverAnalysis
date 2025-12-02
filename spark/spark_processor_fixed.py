import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, split, regexp_extract, regexp_replace, trim, when, expr, lower, explode, array, lit, size, from_json, to_timestamp, avg, min, max
from pyspark.sql.types import *
import os
import sys

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 0. SETUP JAVA VÀ ENVIRONMENT ---
def find_java_home():
    """Tìm JAVA_HOME tự động nếu không được set"""
    try:
        # Cách 1: Check JAVA_HOME đã set chưa
        if "JAVA_HOME" in os.environ:
            java_home = os.environ["JAVA_HOME"]
            if os.path.exists(os.path.join(java_home, "bin", "java.exe")):
                return java_home
        
        # Cách 2: Tìm Java từ registry (Windows)
        try:
            result = subprocess.run(
                ['powershell', '-Command', 
                 '(Get-Item "HKLM:\\Software\\JavaSoft\\Java Runtime Environment").Property | '
                 'ForEach-Object { $key = Get-ItemProperty "HKLM:\\Software\\JavaSoft\\Java Runtime Environment\\$_"; '
                 'if ($key.JavaHome) { return $key.JavaHome } }'],
                capture_output=True, text=True, timeout=5
            )
            if result.stdout:
                java_home = result.stdout.strip()
                if os.path.exists(java_home):
                    return java_home
        except:
            pass
        
        # Cách 3: Tìm từ Java command
        try:
            result = subprocess.run(
                ['java', '-version'], 
                capture_output=True, text=True, timeout=5
            )
            if result.returncode == 0:
                # Tìm java.exe path
                result = subprocess.run(
                    ['where', 'java'], 
                    capture_output=True, text=True, timeout=5
                )
                java_path = result.stdout.strip().split('\n')[0]
                if java_path:
                    # Trích JAVA_HOME từ bin/java.exe
                    java_home = os.path.dirname(os.path.dirname(java_path))
                    return java_home
        except:
            pass
        
        logger.warning("⚠️  Không tìm thấy JAVA_HOME tự động")
        return None
    except Exception as e:
        logger.error(f"❌ Lỗi tìm Java: {str(e)}")
        return None

# Set JAVA_HOME
java_home = find_java_home()
if java_home:
    os.environ["JAVA_HOME"] = java_home
    logger.info(f"✅ JAVA_HOME đặt thành: {java_home}")
else:
    logger.error("❌ Không tìm thấy Java. Vui lòng cài JDK 8 hoặc cao hơn!")
    logger.error("   Tải từ: https://www.oracle.com/java/technologies/downloads/")
    sys.exit(1)

# Xóa HADOOP_HOME để tránh conflict
if "HADOOP_HOME" in os.environ:
    del os.environ["HADOOP_HOME"]

# --- 1. KHỞI TẠO SPARK VỚI MONGODB CONNECTOR VÀ ERROR HANDLING ---
try:
    spark = SparkSession.builder \
        .appName("RiverQualityRealtimeProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/river_monitoring.sensor_data") \
        .config("spark.mongodb.read.connection.uri", "mongodb://localhost:27017/river_monitoring.sensor_data") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .config("spark.local.dir", "./spark-tmp") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    logger.info("✅ Spark session khởi tạo thành công")
    
except Exception as e:
    logger.error(f"❌ Lỗi khởi tạo Spark session: {str(e)}")
    raise

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
try:
    logger.info("📡 Đang kết nối tới Kafka...")
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "river_sensors") \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("✅ Kafka connection successful")
    
except Exception as e:
    logger.error(f"❌ Kafka connection failed: {str(e)}")
    raise

# --- 4. XỬ LÝ DỮ LIỆU (TRANSFORMATION) - PARSE JSON & DATA CLEANING ---
try:
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
            col("data.`Chloride`").alias("chloride"),
            col("data.`Total Hardness (as CaCO3)`").alias("hardness"),
            col("data.CCME_WQI").alias("wqi_category_ref"),
            col("data.CCME_Values").alias("wqi_score_ref")
        )
    
    # --- 5. DATA ENRICHMENT & STANDARDIZATION (tham khảo từ data_processing_final.py) ---
    
    # Chuẩn hóa location (giống như chuẩn hóa city trong final.py)
    enriched_df = parsed_df.withColumn(
        "location",
        trim(when(col("location").isNull() | (col("location") == ""), "Unknown")
             .otherwise(col("location")))
    )
    
    # Phân loại chất lượng nước dựa trên WQI
    enriched_df = enriched_df.withColumn(
        "wqi_category",
        when(col("wqi_score_ref") >= 90, "Excellent")
        .when((col("wqi_score_ref") >= 80) & (col("wqi_score_ref") < 90), "Good")
        .when((col("wqi_score_ref") >= 60) & (col("wqi_score_ref") < 80), "Fair")
        .when((col("wqi_score_ref") >= 40) & (col("wqi_score_ref") < 60), "Poor")
        .when(col("wqi_score_ref") < 40, "Very Poor")
        .otherwise(col("wqi_category_ref"))
    )
    
    # Đánh giá rủi ro chi tiết (Data Enrichment)
    enriched_df = enriched_df.withColumn(
        "quality_assessment",
        when(
            (col("ph").between(6.5, 8.5)) & 
            (col("do_mgL") >= 5.0) & 
            (col("ammonia") < 0.5) &
            (col("bod") < 3.0), 
            "SAFE"
        ).when(
            (col("ph").between(6.0, 9.0)) & 
            (col("do_mgL") >= 3.0) & 
            (col("ammonia") < 2.0),
            "ACCEPTABLE"
        ).otherwise("WARNING")
    )
    
    # Tạo cảnh báo chi tiết (thêm các loại cảnh báo từ final.py)
    enriched_df = enriched_df.withColumn(
        "alert_type",
        when(col("ph") < 4.0, "ACID_HIGH_DANGER")
        .when(col("ph") > 9.0, "ALKALI_HIGH_DANGER")
        .when(col("do_mgL") < 2.0, "FISH_KILL_RISK")
        .when(col("do_mgL") < 5.0, "LOW_DISSOLVED_OXYGEN")
        .when(col("ammonia") > 2.0, "TOXIC_AMMONIA")
        .when(col("bod") > 5.0, "HIGH_ORGANIC_POLLUTION")
        .when(col("wqi_score_ref") < 40, "CRITICAL_WATER_QUALITY")
        .otherwise(None)
    )
    
    # Thêm mức độ cảnh báo
    enriched_df = enriched_df.withColumn(
        "alert_severity",
        when(col("alert_type").isNull(), "NONE")
        .when(col("alert_type").isin("ACID_HIGH_DANGER", "ALKALI_HIGH_DANGER", "FISH_KILL_RISK", "TOXIC_AMMONIA"), "CRITICAL")
        .when(col("alert_type").isin("LOW_DISSOLVED_OXYGEN", "HIGH_ORGANIC_POLLUTION"), "WARNING")
        .when(col("alert_type") == "CRITICAL_WATER_QUALITY", "CRITICAL")
        .otherwise("INFO")
    )
    
    logger.info("✅ Data enrichment completed")
    
except Exception as e:
    logger.error(f"❌ Data transformation failed: {str(e)}")
    raise

# --- 6. AGGREGATION & BATCH STATISTICS (tham khảo batch processing từ final.py) ---
try:
    # Tạo các dataframe chứa thống kê theo location
    location_stats = enriched_df.groupBy("location").agg(
        count("*").alias("total_readings"),
        avg("temp").alias("avg_temperature"),
        avg("ph").alias("avg_ph"),
        avg("do_mgL").alias("avg_dissolved_oxygen"),
        avg("ammonia").alias("avg_ammonia"),
        avg("bod").alias("avg_bod"),
        min("wqi_score_ref").alias("min_wqi"),
        max("wqi_score_ref").alias("max_wqi")
    )
    
    # Thống kê theo chất lượng nước
    quality_distribution = enriched_df.groupBy("wqi_category").agg(
        count("*").alias("count")
    ).orderBy(col("count").desc())
    
    # Thống kê cảnh báo
    alert_distribution = enriched_df.filter(col("alert_type").isNotNull()).groupBy("alert_type", "alert_severity").agg(
        count("*").alias("alert_count")
    ).orderBy(col("alert_count").desc())
    
    logger.info("✅ Statistical calculations completed")
    
except Exception as e:
    logger.error(f"❌ Aggregation failed: {str(e)}")
    raise

# --- 7. WRITE STREAMS TO MONGODB & CONSOLE ---
try:
    # Lưu tất cả dữ liệu đã xử lý vào MongoDB
    query_mongo_raw = enriched_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_raw_data") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for raw data started")
    
    # Lưu thống kê theo location
    query_mongo_stats = location_stats.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_location_stats") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for location statistics started")
    
    # Lưu quality distribution
    query_mongo_quality = quality_distribution.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_quality_dist") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for quality distribution started")
    
    # Lưu alert distribution
    query_mongo_alerts = alert_distribution.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_alert_dist") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for alert distribution started")
    
except Exception as e:
    logger.error(f"❌ MongoDB stream failed: {str(e)}")
    raise

# --- 8. CONSOLE OUTPUT FOR DEBUGGING ---
try:
    # Hiển thị các dòng có cảnh báo CRITICAL
    query_console_critical = enriched_df.filter(col("alert_severity") == "CRITICAL") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    logger.info("✅ Console stream for critical alerts started")
    
    # Hiển thị tất cả cảnh báo
    query_console_alerts = enriched_df.filter(col("alert_type").isNotNull()) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    logger.info("✅ Console stream for all alerts started")
    
except Exception as e:
    logger.error(f"❌ Console stream failed: {str(e)}")
    raise

# --- 9. STREAM MONITORING & GRACEFUL SHUTDOWN ---
try:
    logger.info("🚀 Hệ thống đang chạy: Kafka -> Spark Streaming -> MongoDB!")
    logger.info("📊 Dữ liệu được xử lý và phân tích theo thời gian thực...")
    
    spark.streams.awaitAnyTermination()
    
except Exception as e:
    logger.error(f"❌ Stream terminated with error: {str(e)}")
    
finally:
    logger.info("🛑 Stopping all streams...")
    spark.streams.stop()
    logger.info("✅ All streams stopped gracefully")
    spark.stop()
