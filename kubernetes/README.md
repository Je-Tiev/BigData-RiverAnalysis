# Kubernetes (GKE) - Deploy InfluxDB + Grafana + MinIO

Thư mục này chứa các file YAML để deploy **InfluxDB**, **Grafana** và **MinIO** lên **GKE**.

## Thông tin cấu hình (khớp với `docker-compose.yml`)

- **InfluxDB**

  - Version: `influxdb:2.7`
  - Org: `water-quality`
  - Bucket: `river-data`
  - User: `admin`
  - Password: `password123`
  - Token admin: `my-super-secret-auth-token`
  - Port: **8086**

- **Grafana**

  - Admin user: `admin`
  - Admin password: `admin123`
  - Port: **3000**
  - Datasource InfluxDB (Flux) đã được provision tự động.
  - Dashboard `Water Quality Dashboard` đã được import tự động.

- **MinIO**
  - Image: `minio/minio:latest`
  - User: `admin`
  - Password: `password123`
  - S3 API Port: **9000**
  - Console Port: **9001**

## Deploy lên GKE

Chạy các lệnh sau (PowerShell / Linux đều tương tự):

```bash
kubectl apply -f kubernetes/00-namespace.yml
kubectl apply -f kubernetes/10-influxdb.yml
kubectl apply -f kubernetes/20-grafana.yml
kubectl apply -f kubernetes/30-minio.yml
kubectl apply -f kubernetes/40-kafka.yml
kubectl apply -f kubernetes/50-spark.yml
```

Đợi 1-3 phút để Service `LoadBalancer` có External IP:

```bash
kubectl get svc -n river-analysis
```

## Port/điểm truy cập để dùng

- **InfluxDB (để Spark ghi dữ liệu)**

  - **Trong cluster**: `http://influxdb.river-analysis.svc.cluster.local:8086`
  - **Từ bên ngoài**: `http://<INFLUXDB_EXTERNAL_IP>:8086`

- **Grafana (để đăng nhập và xem dashboard)**

  - **Từ bên ngoài**: `http://<GRAFANA_EXTERNAL_IP>:3000`
  - **Login**: `admin` / `admin123`

- **MinIO**
  - **S3 API**: `http://<MINIO_EXTERNAL_IP>:9000`
  - **Console**: `http://<MINIO_EXTERNAL_IP>:9001`
  - **Login**: `admin` / `password123`

## Tạo bucket MinIO

Sau khi vào Console MinIO, tạo bucket tên: `water-quality-raw`.

## Kafka (2 cổng: internal + external)

- **Trong cluster (Spark/consumer chạy trong GKE)**: `kafka.river-analysis.svc.cluster.local:19092`
- **Từ bên ngoài (producer/consumer chạy ngoài cluster)**: `<KAFKA_EXTERNAL_IP>:29092`

### Cập nhật advertised listeners cho Kafka (bắt buộc)

Kafka bắt buộc `advertised.listeners` phải là địa chỉ mà client **thực sự truy cập được**.
Sau khi Service `kafka-external` có External IP, chạy:

```bash
kubectl get svc kafka-external -n river-analysis
kubectl set env deploy/kafka -n river-analysis \
  KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka:19092,PLAINTEXT_HOST://<KAFKA_EXTERNAL_IP>:29092"
kubectl rollout restart deploy/kafka -n river-analysis
```

## Spark (Standalone) + Streaming apps (dùng file `_deploy.py`)

- **Spark Master (trong cluster)**: `spark-master.river-analysis.svc.cluster.local:7077`
- **Spark Master Web UI**: port `8080` (gợi ý xem bằng port-forward):

```bash
kubectl port-forward svc/spark-master -n river-analysis 8080:8080
```

File `kubernetes/50-spark.yml` sẽ tạo:

- `spark-master` + `spark-worker` (cluster Spark Standalone)
- `spark-stream-minio` chạy `spark_to_minio_deploy.py` (Kafka -> MinIO)
- `spark-stream-influxdb` chạy `spark_to_influxdb_deploy.py` (Kafka -> InfluxDB)

Lưu ý: để dùng image `spark-custom:3.5.7` trên GKE, bạn cần **push image lên registry** mà cluster pull được (GCR/Artifact Registry) và sửa lại trường `image:` trong `kubernetes/50-spark.yml` nếu cần.

## Gợi ý cấu hình Spark (khi chạy ngoài cluster)

- `INFLUXDB_URL`: `http://<INFLUXDB_EXTERNAL_IP>:8086`
- `INFLUXDB_TOKEN`: `my-super-secret-auth-token`
- `INFLUXDB_ORG`: `water-quality`
- `INFLUXDB_BUCKET`: `river-data`

## Deployed
grafana: http://34.87.137.129:3000
influxDB: http://34.126.110.108:8086
minio: http://34.142.247.135:9001
