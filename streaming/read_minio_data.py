from pyspark.sql import SparkSession

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password123"
MINIO_BUCKET = "water-quality-raw"

spark = SparkSession.builder \
    .appName("ReadMinIO") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Đọc tất cả data
df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/raw_data")

print("📊 TỔNG SỐ DÒNG:", df.count())

print("\n📋 SCHEMA:")
df.printSchema()

print("\n🔍 CÁC CỘT CÓ SẴN:")
print(df.columns)

print("\n📝 MẪU DATA (10 dòng đầu):")
df.show(10, truncate=False)

print("\n📈 PHÂN BỐ CHẤT LƯỢNG NƯỚC:")
df.groupBy("quality_label").count().orderBy("count", ascending=False).show()

print("\n🌡️ THỐNG KÊ NHIỆT ĐỘ THEO CHẤT LƯỢNG:")
df.groupBy("quality_label").agg(
    {"temperature": "avg", "ph": "avg", "dissolved_oxygen": "avg"}
).show()

print("\n🏞️ TOP 10 SÔNG CÓ NHIỀU RECORDS NHẤT:")
df.groupBy("waterbody_name").count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

print("\n📊 DỮ LIỆU CHI TIẾT (20 dòng):")
df.select(
    "timestamp", "waterbody_name", "temperature", "ph", 
    "dissolved_oxygen", "ccme_score", "quality_label"
).show(20, truncate=False)

spark.stop()