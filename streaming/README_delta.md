# Delta ingestion with Spark -> MinIO (Data Lake)

This file explains how to write the repository CSV into a Delta Lake table stored on MinIO (used as S3-compatible object store).

Prerequisites
- Docker & docker-compose
- `docker-compose up -d minio spark-master spark-worker mongodb` from the `Docker/` folder

Run ingestion (from `Docker/` folder):

1. Start required services:

```powershell
docker-compose up -d minio spark-master spark-worker mongodb
```

2. Run the Spark job using `spark-submit` inside the `spark-master` container. This example includes required packages for Delta and Hadoop AWS support.

```powershell
# Execute spark-submit with extra packages for delta-core and hadoop-aws
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --packages io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /data/streaming/ingest_to_delta.py
```

Notes
- The compose sets AWS-like credentials for MinIO; Spark job uses `s3a://datalake/...` path. The job writes Delta files into the `datalake` bucket on MinIO.
- If you prefer using the `delta-spark` Python package, install it in the Spark container or use a custom image.
- After writing, you can read the Delta table using Spark with the same configs or use tools that support Delta Lake.

Postgres sync
1. Ensure Postgres is running (`docker-compose up -d postgres pgadmin`).
2. Run the Spark job that reads Delta and writes to Postgres. Example (from `Docker/`):

```powershell
docker-compose exec spark-master \
  /opt/spark/bin/spark-submit \
  --packages io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0 \
  /data/streaming/ingest_delta_to_postgres.py
```

This will write the Delta table into Postgres table `public.measurements` (mode `overwrite` in the script).

Notes:
- If your Spark container cannot download packages, either build a custom Spark image with the JARs placed in `/opt/spark/jars/` or run spark-submit from a machine with network access to Maven central.
- You can change JDBC mode `overwrite` → `append` in `ingest_delta_to_postgres.py` for incremental loads.
