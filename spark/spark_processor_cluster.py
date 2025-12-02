import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, from_json, avg, min, max, when, trim
from pyspark.sql.types import *
import os
import sys
import subprocess

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- 0. SETUP JAVA ---
def find_java_home():
    """Tìm JAVA_HOME tự động"""
    try:
        if "JAVA_HOME" in os.environ:
            java_home = os.environ["JAVA_HOME"]
            if os.path.exists(os.path.join(java_home, "bin", "java.exe")):
                return java_home
        
        try:
            result = subprocess.run(['java', '-version'], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                result = subprocess.run(['where', 'java'], 
                                      capture_output=True, text=True, timeout=5)
                java_path = result.stdout.strip().split('\n')[0]
                if java_path:
                    java_home = os.path.dirname(os.path.dirname(java_path))
                    return java_home
        except:
            pass
        
        logger.warning("⚠️ Không tìm thấy JAVA_HOME")
        return None
    except Exception as e:
        logger.error(f"❌ Lỗi tìm Java: {str(e)}")
        return None

java_home = find_java_home()
if java_home:
    os.environ["JAVA_HOME"] = java_home
    logger.info(f"✅ JAVA_HOME: {java_home}")
else:
    logger.error("❌ Cần cài JDK 8+: https://adoptium.net/")
    sys.exit(1)

if "HADOOP_HOME" in os.environ:
    del os.environ["HADOOP_HOME"]

# --- 1. KHỞI TẠO SPARK CLUSTER MODE ---
try:
    logger.info("🔧 Đang khởi tạo Spark Cluster Mode...")
    
    spark = SparkSession.builder \
        .appName("RiverQualityRealtimeProcessor") \
        .master("spark://spark-master:7077") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.port", "7001") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "2") \
        .config("spark.cores.max", "4") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1") \
        .config("spark.mongodb.write.connection.uri", 
                "mongodb://mongodb:27017/river_monitoring.sensor_data") \
        .config("spark.mongodb.read.connection.uri", 
                "mongodb://mongodb:27017/river_monitoring.sensor_data") \
        .config("spark.sql.streaming.checkpointLocation", "./checkpoints") \
        .config("spark.local.dir", "./spark-tmp") \
        .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "60s") \
        .config("spark.scheduler.minRegisteredResourcesRatio", "0.5") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    # Verify cluster connection
    master_url = spark.sparkContext.master
    parallelism = spark.sparkContext.defaultParallelism
    
    logger.info("=" * 60)
    logger.info("✅ SPARK CLUSTER MODE ACTIVATED")
    logger.info(f"🎯 Master URL: {master_url}")
    logger.info(f"📊 Default Parallelism: {parallelism}")
    logger.info(f"💾 Executor Memory: 1g")
    logger.info(f"🔢 Executor Cores: 2")
    logger.info("=" * 60)
    
except Exception as e:
    logger.error(f"❌ Lỗi khởi tạo Spark Cluster: {str(e)}")
    logger.error("💡 Kiểm tra:")
    logger.error("   1. docker ps | grep spark-master")
    logger.error("   2. http://localhost:8080 (Spark Master UI)")
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
        .option("kafka.bootstrap.servers", "kafka-broker:29092") \
        .option("subscribe", "river_sensors") \
        .option("startingOffsets", "latest") \
        .load()
    
    logger.info("✅ Kafka connection successful")
    
except Exception as e:
    logger.error(f"❌ Kafka connection failed: {str(e)}")
    raise

# --- 4. XỬ LÝ DỮ LIỆU ---
try:
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
    
    # Data Enrichment
    enriched_df = parsed_df.withColumn(
        "location",
        trim(when(col("location").isNull() | (col("location") == ""), "Unknown")
             .otherwise(col("location")))
    )
    
    enriched_df = enriched_df.withColumn(
        "wqi_category",
        when(col("wqi_score_ref") >= 90, "Excellent")
        .when((col("wqi_score_ref") >= 80) & (col("wqi_score_ref") < 90), "Good")
        .when((col("wqi_score_ref") >= 60) & (col("wqi_score_ref") < 80), "Fair")
        .when((col("wqi_score_ref") >= 40) & (col("wqi_score_ref") < 60), "Poor")
        .when(col("wqi_score_ref") < 40, "Very Poor")
        .otherwise(col("wqi_category_ref"))
    )
    
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
    
    enriched_df = enriched_df.withColumn(
        "alert_severity",
        when(col("alert_type").isNull(), "NONE")
        .when(col("alert_type").isin("ACID_HIGH_DANGER", "ALKALI_HIGH_DANGER", 
                                      "FISH_KILL_RISK", "TOXIC_AMMONIA"), "CRITICAL")
        .when(col("alert_type").isin("LOW_DISSOLVED_OXYGEN", "HIGH_ORGANIC_POLLUTION"), "WARNING")
        .when(col("alert_type") == "CRITICAL_WATER_QUALITY", "CRITICAL")
        .otherwise("INFO")
    )
    
    logger.info("✅ Data enrichment completed")
    
except Exception as e:
    logger.error(f"❌ Data transformation failed: {str(e)}")
    raise

# --- 5. AGGREGATION ---
try:
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
    
    quality_distribution = enriched_df.groupBy("wqi_category").agg(
        count("*").alias("count")
    ).orderBy(col("count").desc())
    
    alert_distribution = enriched_df.filter(col("alert_type").isNotNull()) \
        .groupBy("alert_type", "alert_severity").agg(
            count("*").alias("alert_count")
        ).orderBy(col("alert_count").desc())
    
    logger.info("✅ Statistical calculations completed")
    
except Exception as e:
    logger.error(f"❌ Aggregation failed: {str(e)}")
    raise

# --- 6. WRITE STREAMS ---
try:
    query_mongo_raw = enriched_df.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_raw_data") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for raw data started")
    
    query_mongo_stats = location_stats.writeStream \
        .format("mongodb") \
        .option("checkpointLocation", "./checkpoints/mongo_location_stats") \
        .option("forceDeleteTempCheckpointLocation", "true") \
        .outputMode("update") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    logger.info("✅ MongoDB stream for location statistics started")
    
    query_console_critical = enriched_df.filter(col("alert_severity") == "CRITICAL") \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 20) \
        .start()
    
    logger.info("✅ Console stream for critical alerts started")
    
except Exception as e:
    logger.error(f"❌ Stream failed: {str(e)}")
    raise

# --- 7. MONITORING ---
try:
    logger.info("=" * 60)
    logger.info("🚀 HỆ THỐNG ĐANG CHẠY - CLUSTER MODE")
    logger.info("📊 Spark Master UI: http://localhost:8080")
    logger.info("📈 Application UI: http://localhost:4040")
    logger.info("=" * 60)
    
    spark.streams.awaitAnyTermination()
    
except Exception as e:
    logger.error(f"❌ Stream terminated: {str(e)}")
    
finally:
    logger.info("🛑 Stopping all streams...")
    spark.streams.stop()
    logger.info("✅ All streams stopped gracefully")
    spark.stop()