# Hướng dẫn triển khai lên Google Kubernetes Engine (GKE)

Hướng dẫn này sẽ giúp bạn triển khai hệ thống River Quality Monitoring lên GKE.

## Yêu cầu

1. Google Cloud SDK (gcloud) đã được cài đặt và cấu hình
2. kubectl đã được cài đặt
3. Quyền truy cập vào một GKE cluster
4. Docker image `hiesu19/spark-custom:3.5.7` đã được push lên DockerHub (đã hoàn thành ✅)

## Bước 1: Kết nối với GKE Cluster

```bash
# Lấy credentials cho cluster
gcloud container clusters get-credentials [CLUSTER-NAME] --zone [ZONE] --project [PROJECT-ID]

# Kiểm tra kết nối
kubectl cluster-info
```

## Bước 3: Triển khai các thành phần

### 3.1. Tạo Namespace và Storage Class

```bash
kubectl apply -f namespace.yaml
kubectl apply -f storage-class.yaml
```

### 3.2. Triển khai Kafka

```bash
# Kafka Controller
kubectl apply -f kafka-controller.yaml

# Đợi Kafka Controller sẵn sàng
kubectl wait --for=condition=ready pod -l app=kafka-controller -n river-analysis --timeout=300s

# Kafka Broker
kubectl apply -f kafka-broker.yaml

# Đợi Kafka Broker sẵn sàng
kubectl wait --for=condition=ready pod -l app=kafka-broker -n river-analysis --timeout=300s
```

### 3.3. Lấy và Cập nhật External IP của Kafka LoadBalancer

Sau khi Kafka broker đã sẵn sàng, đợi LoadBalancer có external IP (có thể mất vài phút):

```bash
# Kiểm tra external IP
kubectl get svc kafka-broker -n river-analysis

# Sử dụng script helper để tự động cập nhật
chmod +x update-kafka-external-ip.sh
./update-kafka-external-ip.sh
```

Script này sẽ:
1. Đợi LoadBalancer có external IP
2. Tự động cập nhật KAFKA_ADVERTISED_LISTENERS trong StatefulSet
3. Restart pod để áp dụng thay đổi

**Lưu ý**: Nếu không dùng script, bạn có thể cập nhật thủ công:

```bash
# Lấy external IP
EXTERNAL_IP=$(kubectl get svc kafka-broker -n river-analysis -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# Cập nhật StatefulSet
kubectl set env -n river-analysis statefulset/kafka-broker \
  KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://kafka-broker:19092,PLAINTEXT_HOST://${EXTERNAL_IP}:29092"

# Restart pod
kubectl delete pod -n river-analysis kafka-broker-0
```

### 3.4. Triển khai MinIO và MongoDB

```bash
kubectl apply -f minio.yaml
kubectl apply -f mongodb.yaml

# Đợi các service sẵn sàng
kubectl wait --for=condition=ready pod -l app=minio -n river-analysis --timeout=300s
kubectl wait --for=condition=ready pod -l app=mongodb -n river-analysis --timeout=300s
```

### 3.5. Tạo ConfigMaps và Persistent Volumes

```bash
kubectl apply -f configmaps.yaml
kubectl apply -f persistent-volumes.yaml
```

### 3.6. Triển khai Spark

```bash
# Spark Master và Worker cho MinIO
kubectl apply -f spark-master-minio.yaml
kubectl apply -f spark-worker-minio.yaml

# Spark Master và Worker cho MongoDB
kubectl apply -f spark-master-mongodb.yaml
kubectl apply -f spark-worker-mongodb.yaml

# Đợi Spark Masters sẵn sàng
kubectl wait --for=condition=ready pod -l app=spark-master-minio -n river-analysis --timeout=300s
kubectl wait --for=condition=ready pod -l app=spark-master-mongodb -n river-analysis --timeout=300s
```

## Bước 4: Cấu hình Producer từ Local

### 4.1. Cập nhật producer.py

Sửa file `kafka/producer.py` để sử dụng external IP của Kafka LoadBalancer:

```python
# Thay đổi dòng này
KAFKA_SERVER = 'localhost:29092' 

# Thành IP từ LoadBalancer (ví dụ)
KAFKA_SERVER = '34.123.45.67:29092'
```

### 4.2. Tạo Kafka Topic (nếu cần)

```bash
# Port-forward Kafka broker để tạo topic từ local
kubectl port-forward -n river-analysis svc/kafka-broker 9092:19092

# Trong terminal khác, tạo topic
docker run -it --rm apache/kafka:4.0.1 kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic river-quality \
  --partitions 3 \
  --replication-factor 1
```

Hoặc sử dụng Kafka pod trực tiếp:

```bash
kubectl exec -it -n river-analysis kafka-broker-0 -- \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:19092 \
  --create \
  --topic river-quality \
  --partitions 3 \
  --replication-factor 1
```

### 4.3. Chạy Producer từ Local

```bash
cd kafka
python3 producer.py
```

## Bước 5: Chạy Spark Jobs

### 5.1. Chạy Spark Job cho MinIO

```bash
kubectl exec -it -n river-analysis deployment/spark-master-minio -- \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master-minio:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/jobs/spark_to_minio.py
```

### 5.2. Chạy Spark Job cho MongoDB

```bash
kubectl exec -it -n river-analysis deployment/spark-master-mongodb -- \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master-mongodb:7078 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.0 \
  /opt/jobs/spark_to_mongodb.py
```

Hoặc sử dụng Jobs (chạy một lần):

```bash
kubectl apply -f spark-jobs.yaml
```

## Bước 6: Kiểm tra và Monitoring

### 6.1. Kiểm tra Pods

```bash
kubectl get pods -n river-analysis
```

### 6.2. Xem Logs

```bash
# Kafka
kubectl logs -f -n river-analysis kafka-broker-0

# Spark Master
kubectl logs -f -n river-analysis deployment/spark-master-minio

# Spark Worker
kubectl logs -f -n river-analysis deployment/spark-worker-minio
```

### 6.3. Port Forward để truy cập Web UI

```bash
# Spark Master MinIO Web UI
kubectl port-forward -n river-analysis svc/spark-master-minio 8080:8080

# Spark Master MongoDB Web UI
kubectl port-forward -n river-analysis svc/spark-master-mongodb 8080:8080

# MinIO Console
kubectl port-forward -n river-analysis svc/minio 9001:9001
# Truy cập: http://localhost:9001 (admin/password123)
```

## Bước 7: Tạo MinIO Bucket

1. Port-forward MinIO console: `kubectl port-forward -n river-analysis svc/minio 9001:9001`
2. Truy cập http://localhost:9001
3. Đăng nhập: `admin` / `password123`
4. Tạo bucket: `water-quality-raw`

## Troubleshooting

### Kafka không kết nối được từ local

1. Kiểm tra firewall rules của GKE cluster cho phép traffic từ IP của bạn
2. Kiểm tra LoadBalancer service đã có external IP chưa:
   ```bash
   kubectl get svc kafka-broker -n river-analysis
   ```
3. Kiểm tra Kafka broker logs:
   ```bash
   kubectl logs -f -n river-analysis kafka-broker-0
   ```

### Spark không kết nối được Kafka

1. Kiểm tra Kafka broker đang chạy:
   ```bash
   kubectl get pods -n river-analysis | grep kafka
   ```
2. Kiểm tra network connectivity từ Spark pod:
   ```bash
   kubectl exec -it -n river-analysis deployment/spark-master-minio -- nc -zv kafka-broker 19092
   ```

### Persistent Volume không được tạo

1. Kiểm tra StorageClass:
   ```bash
   kubectl get storageclass
   ```
2. Kiểm tra PVC:
   ```bash
   kubectl get pvc -n river-analysis
   ```

## Dọn dẹp

```bash
# Xóa tất cả resources
kubectl delete namespace river-analysis

# Hoặc xóa từng file
kubectl delete -f spark-jobs.yaml
kubectl delete -f spark-worker-mongodb.yaml
kubectl delete -f spark-master-mongodb.yaml
kubectl delete -f spark-worker-minio.yaml
kubectl delete -f spark-master-minio.yaml
kubectl delete -f mongodb.yaml
kubectl delete -f minio.yaml
kubectl delete -f kafka-broker.yaml
kubectl delete -f kafka-controller.yaml
kubectl delete -f persistent-volumes.yaml
kubectl delete -f configmaps.yaml
kubectl delete -f storage-class.yaml
kubectl delete -f namespace.yaml
```

## Lưu ý

1. **Image Spark**: Image `hiesu19/spark-custom:3.5.7` đã được push lên DockerHub và sẵn sàng sử dụng
2. **Storage**: StorageClass `standard-ssd` sử dụng `pd-ssd` provisioner của GKE
3. **LoadBalancer**: Kafka broker sử dụng LoadBalancer để expose port 29092 ra ngoài
4. **Resource Limits**: Có thể điều chỉnh resource requests/limits trong các file deployment tùy theo nhu cầu
5. **Security**: Trong production, nên sử dụng secrets cho passwords và enable authentication/authorization

