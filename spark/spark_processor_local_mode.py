import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, avg, min, max, when, trim
from pyspark.sql.types import *
import os
import sys
import subprocess

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- 0. SETUP JAVA ---
def find_java_home():
    """Tìm JAVA_HOME tự động"""
    try:
        # Check JAVA_HOME environment variable
        if "JAVA_HOME" in os.environ:
            java_home = os.environ["JAVA_HOME"]
            java_exe = os.path.join(java_home, "bin", "java.exe")
            if os.path.exists(java_exe):
                return java_home
        
        # Try to find java from command line (Windows)
        try:
            result = subprocess.run(['java', '-version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                result = subprocess.run(['where', 'java'], 
                                      capture_output=True, text=True, timeout=5)
                java_path = result.stdout.strip().split('\n')[0]
                if java_path:
                    # Extract JAVA_HOME from bin/java.exe path
                    java_home = os.path.dirname(os.path.dirname(java_path))
                    return java_home
        except:
            pass
        
        # Try Linux/Mac
        try:
            result = subprocess.run(['which', 'java'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                java_path = result.stdout.strip()
                if java_path:
                    java_home = os.path.dirname(os.path.dirname(java_path))
                    return java_home
        except:
            pass
        
        logger.warning("⚠️ Không tìm thấy JAVA_HOME tự động")
        return None
        
    except Exception as e:
        logger.error(f"❌ Lỗi tìm Java: {str(e)}")
        return None

# Set JAVA_HOME
java_home = find_java_home()
if java_home:
    os.environ["JAVA_HOME"] = java_home
    logger.info(f"✅ JAVA_HOME: {java_home}")
else:
    logger.error("=" * 60)
    logger.error("❌ Không tìm thấy Java!")
    logger.error("📥 Cài đặt JDK 8 hoặc 11:")
    logger.error("   - Oracle JDK: https://www.oracle.com/java/technologies/downloads/")
    logger.error("   - OpenJDK: https://adoptium.net/")
    logger.error("=" * 60)
    sys.exit(1)

# Remove HADOOP_HOME if exists to avoid conflicts
if "HADOOP_HOME" in os.environ:
    del os.environ["HADOOP_HOME"]
    logger.info("🗑️ Removed HADOOP_HOME to avoid conflicts")

# --- 1. KHỞI TẠO SPARK LOCAL MODE ---
try:
    logger.info("=" * 60)
    logger.info("🔧 Đang khởi tạo Spark Local Mode...")
    logger.info("=" * 60)
    
    spark = SparkSession.builder \
        .appName("RiverQualityRealtimeProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", 
                "mongodb://localhost:27017/river_monitoring.sensor_data") \
        .config("spark.mongodb.read.connection.uri", 
                "mongodb://localhost:27017/river_monitoring.sensor_data") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .config("spark.local.dir", "./spark-tmp") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Verify configuration
    logger.info("=" * 60)
    logger.info("✅ SPARK LOCAL MODE ACTIVATED")
    logger.info(f"🎯 Master: {spark.sparkContext.master}")
    logger.info(f"📊 Parallelism: {spark.sparkContext.defaultParallelism}")
    logger.info(f"💾 Driver Memory: 2g")
    logger.info(f"🖥️ App Name: {spark.sparkContext.appName}")
    logger.info(f"📈 Spark UI: http://localhost:4040 (when streaming)")
    logger.info("=" * 60)
    
except Exception as e:
    logger.error(f"❌ Lỗi khởi tạo Spark: {str(e)}")
    logger.error("💡 Kiểm tra:")
    logger.error("   1. Java đã cài đặt chưa: java -version")
    logger.error("   2. PySpark đã cài chưa: pip show pyspark")
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
    logger.info("📡 Đang kết nối tới Kafka (localhost:29092)...")
    
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:29092") \
        .option("subscribe", "river_sensors") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    logger.info("✅ Kafka connection successful")
    
except Exception as e:
    logger.error(f"❌ Kafka connection failed: {str(e)}")
    logger.error("💡 Kiểm tra:")
    logger.error("   1. Kafka đang chạy: docker ps | grep kafka")
    logger.error("   2. Port đã mở: docker port kafka-broker")
    logger.error("   3. Test connection: telnet localhost 29092")
    raise

# --- 4. XỬ LÝ DỮ LIỆU ---
try:
    logger.info("🔄 Đang setup data transformation pipeline...")
    
    # Parse JSON & rename columns
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
    
    # Data Enrichment - Location standardization
    enriched_df = parsed_df.withColumn(
        "location",
        trim(when(col("location").isNull() | (col("location") == ""), "Unknown")
             .otherwise(col("location")))
    )
    
    # WQI Category classification
    enriched_df = enriched_df.withColumn(
        "wqi_category",
        when(col("wqi_score_ref") >= 90, "Excellent")
        .when((col("wqi_score_ref") >= 80) & (col("wqi_score_ref") < 90), "Good")
        .when((col("wqi_score_ref") >= 60) & (col("wqi_score_ref") < 80), "Fair")
        .when((col("wqi_score_ref") >= 40) & (col("wqi_score_ref") < 60), "Poor")
        .when(col("wqi_score_ref") < 40, "Very Poor")
        .otherwise(col("wqi_category_ref"))
    )
    
    # Quality assessment
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
    
    # Alert type classification
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
    
    # Alert severity
    enriched_df = enriched_df.withColumn(
        "alert_severity",
        when(col("alert_type").isNull(), "NONE")
        .when(col("alert_type").isin("ACID_HIGH_DANGER", "ALKALI_HIGH_DANGER", 
                                      "FISH_KILL_RISK", "TOXIC_AMMONIA"), "CRITICAL")
        .when(col("alert_type").isin("LOW_DISSOLVED_OXYGEN", "HIGH_ORGANIC_POLLUTION"), "WARNING")
        .when(col("alert_type") == "CRITICAL_WATER_QUALITY", "CRITICAL")
        .otherwise("INFO")
    )
    
    logger.info("✅ Data enrichment pipeline configured")
    
except Exception as e:
    logger.error(f"❌ Data transformation failed: {str(e)}")
    raise

# --- 5. AGGREGATION ---
try:
    logger.info("📊 Đang setup aggregation queries...")
    
    # Location statistics
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
    
    # Quality distribution
    quality_distribution = enriched_df.groupBy("wqi_category").agg(
        count("*").alias("count")
    ).orderBy(col("count").desc())
    
    # Alert distribution
    alert_distribution = enriched_df.filter(col("alert_type").isNotNull()) \
        .groupBy("alert_type", "alert_severity").agg(
            count("*").alias("alert_count")
        ).orderBy(col("alert_count").desc())
    
    logger.info("✅ Aggregation queries configured")
    
except Exception as e:
    logger.error(f"❌ Aggregation setup failed: {str(e)}")
    raise

# --- 6. WRITE STREAMS ---
try:
    logger.info("💾 Đang khởi động streaming queries...")
    
    # Write enriched data to MongoDB
    query_mongo_raw = enriched_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_raw_data") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for raw data started")
    
    # Write location statistics to MongoDB
    query_mongo_stats = location_stats.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_location_stats") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for location statistics started")
    
    # Write quality distribution to MongoDB
    query_mongo_quality = quality_distribution.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_quality_dist") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for quality distribution started")
    
    # Write alert distribution to MongoDB
    query_mongo_alerts = alert_distribution.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_alert_dist") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for alert distribution started")
    
    # Console output for critical alerts
    query_console_critical = enriched_df.filter(col("alert_severity") == "CRITICAL") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    logger.info("✅ Console stream for critical alerts started")
    
    # Console output for all alerts
    query_console_alerts = enriched_df.filter(col("alert_type").isNotNull()) \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    logger.info("✅ Console stream for all alerts started")
    
except Exception as e:
    logger.error(f"❌ Stream initialization failed: {str(e)}")
    logger.error("💡 Kiểm tra:")
    logger.error("   1. MongoDB đang chạy: docker ps | grep mongodb")
    logger.error("   2. Port MongoDB: docker port mongodb")
    logger.error("   3. Test MongoDB: mongosh --eval 'db.adminCommand(\"ping\")'")
    raise

# --- 7. MONITORING & SHUTDOWN ---
try:
    logger.info("=" * 60)
    logger.info("🚀 HỆ THỐNG ĐANG CHẠY - LOCAL MODE")
    logger.info("=" * 60)
    logger.info("📊 Spark UI: http://localhost:4040")
    logger.info("🗄️ MongoDB: localhost:27017")
    logger.info("📡 Kafka: localhost:29092")
    logger.info("🎛️ Kafka UI: http://localhost:8080")
    logger.info("=" * 60)
    logger.info("⚠️ Nhấn Ctrl+C để dừng...")
    logger.info("=" * 60)
    
    # Wait for termination
    spark.streams.awaitAnyTermination()
    
except KeyboardInterrupt:
    logger.info("\n🛑 Đã nhận tín hiệu dừng từ người dùng...")
    
except Exception as e:
    logger.error(f"❌ Stream terminated with error: {str(e)}")
    
finally:
    logger.info("=" * 60)
    logger.info("🛑 Đang dừng tất cả streams...")
    try:
        for stream in spark.streams.active:
            logger.info(f"   Stopping stream: {stream.name}")
            stream.stop()
        logger.info("✅ All streams stopped gracefully")
    except:
        pass
    
    logger.info("🛑 Đang dừng Spark session...")
    spark.stop()
    logger.info("✅ Spark session stopped")
    logger.info("=" * 60)
    logger.info("👋 Hệ thống đã tắt hoàn toàn!")
    logger.info("=" * 60)