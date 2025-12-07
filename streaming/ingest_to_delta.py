from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, year, month
import os

# Configuration - adjust if needed
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
BUCKET = os.getenv('DL_BUCKET', 'datalake')

def create_spark_session():
    builder = SparkSession.builder.appName('ingest_to_delta')
    # Required packages for Delta + S3A
    builder = builder.config('spark.jars.packages', 'io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4')
    builder = builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    builder = builder.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

    # Hadoop / S3A configs to point to MinIO
    builder = builder.config('spark.hadoop.fs.s3a.endpoint', MINIO_ENDPOINT)
    builder = builder.config('spark.hadoop.fs.s3a.access.key', MINIO_ACCESS_KEY)
    builder = builder.config('spark.hadoop.fs.s3a.secret.key', MINIO_SECRET_KEY)
    builder = builder.config('spark.hadoop.fs.s3a.path.style.access', 'true')
    builder = builder.config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    return spark


def main():
    spark = create_spark_session()

    csv_path = '/data/kafka/sorted_water_quality.csv'
    if not os.path.exists(csv_path):
        raise SystemExit(f'CSV not found at {csv_path}. Ensure compose mounts project root to /data')

    df = spark.read.option('header', True).csv(csv_path)
    # parse date
    df2 = df.withColumn('timestamp', to_timestamp(df['FullDate']))
    df2 = df2.withColumn('year', year(df2['timestamp'])).withColumn('month', month(df2['timestamp']))

    target_path = f's3a://{BUCKET}/delta/river/measurements'
    (df2.write.format('delta')
        .partitionBy('year', 'month')
        .mode('overwrite')
        .save(target_path))

    print('Wrote Delta table to', target_path)


if __name__ == '__main__':
    main()
