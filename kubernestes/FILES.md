# Danh sách các file Kubernetes Manifests

## Cấu trúc thư mục

```
kubernestes/
├── namespace.yaml                    # Namespace cho toàn bộ hệ thống
├── storage-class.yaml                # StorageClass cho GKE (pd-ssd)
├── kafka-controller.yaml             # Kafka Controller (KRaft mode)
├── kafka-broker.yaml                 # Kafka Broker với LoadBalancer
├── minio.yaml                        # MinIO StatefulSet và Service
├── mongodb.yaml                      # MongoDB StatefulSet và Service
├── configmaps.yaml                   # ConfigMap chứa Spark jobs (Python scripts)
├── persistent-volumes.yaml           # PVCs cho Spark data
├── spark-master-minio.yaml           # Spark Master cho MinIO pipeline
├── spark-worker-minio.yaml           # Spark Workers cho MinIO pipeline
├── spark-master-mongodb.yaml         # Spark Master cho MongoDB pipeline
├── spark-worker-mongodb.yaml         # Spark Workers cho MongoDB pipeline
├── spark-jobs.yaml                   # Spark Jobs (optional, để chạy streaming jobs)
├── kustomization.yaml                # Kustomize file để triển khai tất cả
├── deploy.sh                         # Script tự động triển khai
├── update-kafka-external-ip.sh      # Script cập nhật Kafka external IP
├── README.md                         # Hướng dẫn chi tiết
└── FILES.md                          # File này
```

## Mô tả các file

### Core Infrastructure

- **namespace.yaml**: Tạo namespace `river-analysis` để chứa tất cả resources
- **storage-class.yaml**: Định nghĩa StorageClass `standard-ssd` sử dụng GKE persistent disk SSD

### Kafka

- **kafka-controller.yaml**: 
  - Kafka Controller node (KRaft mode, không cần Zookeeper)
  - StatefulSet với 1 replica
  - Persistent volume 10Gi

- **kafka-broker.yaml**:
  - Kafka Broker node
  - StatefulSet với 1 replica
  - Service LoadBalancer để expose port 29092 ra ngoài
  - Persistent volume 20Gi
  - Port 19092 cho internal communication, port 9092 cho external

### Data Storage

- **minio.yaml**:
  - MinIO StatefulSet với 1 replica
  - Service ClusterIP
  - Persistent volume 50Gi
  - Credentials: admin/password123

- **mongodb.yaml**:
  - MongoDB StatefulSet với 1 replica
  - Service ClusterIP
  - Persistent volume 20Gi
  - Credentials: root/password123

### Spark

- **spark-master-minio.yaml**:
  - Spark Master cho pipeline MinIO
  - Deployment với 1 replica
  - Port 7077 (Spark), 8080 (Web UI)
  - Mount ConfigMap chứa Spark jobs

- **spark-worker-minio.yaml**:
  - Spark Workers cho pipeline MinIO
  - Deployment với 2 replicas
  - Kết nối tới spark-master-minio

- **spark-master-mongodb.yaml**:
  - Spark Master cho pipeline MongoDB
  - Deployment với 1 replica
  - Port 7078 (Spark), 8080 (Web UI)

- **spark-worker-mongodb.yaml**:
  - Spark Workers cho pipeline MongoDB
  - Deployment với 2 replicas
  - Kết nối tới spark-master-mongodb

### Configuration

- **configmaps.yaml**:
  - ConfigMap `spark-jobs` chứa 2 Python scripts:
    - `spark_to_minio.py`: Stream từ Kafka → MinIO
    - `spark_to_mongodb.py`: Stream từ Kafka → MongoDB

- **persistent-volumes.yaml**:
  - PVC `spark-data-minio`: 10Gi cho Spark checkpoints (MinIO pipeline)
  - PVC `spark-data-mongodb`: 10Gi cho Spark checkpoints (MongoDB pipeline)

### Scripts

- **deploy.sh**: Script tự động triển khai tất cả resources theo thứ tự đúng
- **update-kafka-external-ip.sh**: Script tự động cập nhật Kafka advertised listeners với external IP

### Utilities

- **kustomization.yaml**: Kustomize file để triển khai tất cả resources cùng lúc
- **spark-jobs.yaml**: Optional - Spark Jobs để chạy streaming (có thể dùng thay vì exec vào pod)

## Thứ tự triển khai

1. Namespace và StorageClass
2. Kafka Controller
3. Kafka Broker
4. MinIO và MongoDB
5. ConfigMaps và Persistent Volumes
6. Spark Masters và Workers
7. Cập nhật Kafka External IP

## Lưu ý

1. **Image Spark**: Image `hiesu19/spark-custom:3.5.7` đã được push lên DockerHub và sẵn sàng sử dụng
2. **External IP**: Kafka LoadBalancer có thể mất vài phút để có external IP
3. **Storage**: Tất cả persistent volumes sử dụng `standard-ssd` StorageClass
4. **Resources**: Có thể điều chỉnh resource requests/limits trong các file deployment
5. **Security**: Trong production, nên sử dụng Secrets thay vì hardcode passwords

