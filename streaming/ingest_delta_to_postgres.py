from pyspark.sql import SparkSession
import os

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
BUCKET = os.getenv('DL_BUCKET', 'datalake')

POSTGRES_URL = os.getenv('POSTGRES_URL', 'jdbc:postgresql://postgres:5432/riverdb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'river')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'riverpass')


def create_spark_session():
    builder = SparkSession.builder.appName('delta_to_postgres')
    # packages: delta-core, hadoop-aws for s3a, postgres jdbc
    builder = builder.config('spark.jars.packages', 'io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0')
    builder = builder.config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    builder = builder.config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')

    # S3A / MinIO config
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

    delta_path = f's3a://{BUCKET}/delta/river/measurements'
    print('Reading Delta table from', delta_path)
    df = spark.read.format('delta').load(delta_path)

    # optional: select / cast columns here
    # write to Postgres via JDBC
    props = {
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
        'driver': 'org.postgresql.Driver'
    }

    print('Writing to Postgres at', POSTGRES_URL)
    df.write.jdbc(POSTGRES_URL, 'public.measurements', mode='overwrite', properties=props)
    print('Write complete')


if __name__ == '__main__':
    main()
