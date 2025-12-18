#!/bin/bash
# Script tự động triển khai hệ thống lên GKE

set -e

NAMESPACE="river-analysis"

echo "🚀 Bắt đầu triển khai hệ thống River Quality Monitoring lên GKE..."
echo ""

# Kiểm tra kubectl
if ! command -v kubectl &> /dev/null; then
    echo "❌ kubectl chưa được cài đặt. Vui lòng cài đặt kubectl trước."
    exit 1
fi

# Kiểm tra kết nối cluster
if ! kubectl cluster-info &> /dev/null; then
    echo "❌ Không thể kết nối tới Kubernetes cluster. Vui lòng kiểm tra kết nối."
    exit 1
fi

echo "✅ Đã kết nối tới cluster"
echo ""

# Bước 1: Tạo namespace và storage class
echo "📦 Bước 1: Tạo namespace và storage class..."
kubectl apply -f namespace.yaml
kubectl apply -f storage-class.yaml
echo "✅ Hoàn thành"
echo ""

# Bước 2: Triển khai Kafka Controller
echo "📦 Bước 2: Triển khai Kafka Controller..."
kubectl apply -f kafka-controller.yaml
echo "⏳ Đang đợi Kafka Controller sẵn sàng..."
kubectl wait --for=condition=ready pod -l app=kafka-controller -n $NAMESPACE --timeout=300s || true
echo "✅ Hoàn thành"
echo ""

# Bước 3: Triển khai Kafka Broker
echo "📦 Bước 3: Triển khai Kafka Broker..."
kubectl apply -f kafka-broker.yaml
echo "⏳ Đang đợi Kafka Broker sẵn sàng..."
kubectl wait --for=condition=ready pod -l app=kafka-broker -n $NAMESPACE --timeout=300s || true
echo "✅ Hoàn thành"
echo ""

# Bước 4: Triển khai MinIO và MongoDB
echo "📦 Bước 4: Triển khai MinIO và MongoDB..."
kubectl apply -f minio.yaml
kubectl apply -f mongodb.yaml
echo "⏳ Đang đợi MinIO và MongoDB sẵn sàng..."
kubectl wait --for=condition=ready pod -l app=minio -n $NAMESPACE --timeout=300s || true
kubectl wait --for=condition=ready pod -l app=mongodb -n $NAMESPACE --timeout=300s || true
echo "✅ Hoàn thành"
echo ""

# Bước 5: Tạo ConfigMaps và Persistent Volumes
echo "📦 Bước 5: Tạo ConfigMaps và Persistent Volumes..."
kubectl apply -f configmaps.yaml
kubectl apply -f persistent-volumes.yaml
echo "✅ Hoàn thành"
echo ""

# Bước 6: Triển khai Spark
echo "📦 Bước 6: Triển khai Spark..."
kubectl apply -f spark-master-minio.yaml
kubectl apply -f spark-worker-minio.yaml
kubectl apply -f spark-master-mongodb.yaml
kubectl apply -f spark-worker-mongodb.yaml
echo "⏳ Đang đợi Spark Masters sẵn sàng..."
kubectl wait --for=condition=ready pod -l app=spark-master-minio -n $NAMESPACE --timeout=300s || true
kubectl wait --for=condition=ready pod -l app=spark-master-mongodb -n $NAMESPACE --timeout=300s || true
echo "✅ Hoàn thành"
echo ""

# Bước 7: Cập nhật Kafka External IP
echo "📦 Bước 7: Cập nhật Kafka External IP..."
if [ -f "update-kafka-external-ip.sh" ]; then
    chmod +x update-kafka-external-ip.sh
    ./update-kafka-external-ip.sh || echo "⚠️  Script cập nhật external IP gặp lỗi, vui lòng kiểm tra thủ công"
else
    echo "⚠️  Không tìm thấy script update-kafka-external-ip.sh"
    echo "📌 Vui lòng chạy thủ công: ./update-kafka-external-ip.sh"
fi
echo ""

# Hiển thị thông tin
echo "📊 Thông tin triển khai:"
echo ""
echo "Pods:"
kubectl get pods -n $NAMESPACE
echo ""
echo "Services:"
kubectl get svc -n $NAMESPACE
echo ""
echo "Kafka External IP:"
kubectl get svc kafka-broker -n $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "Chưa có external IP (có thể mất vài phút)"
echo ""
echo "✅ Triển khai hoàn tất!"
echo ""
echo "📝 Các bước tiếp theo:"
echo "1. Đợi LoadBalancer có external IP (nếu chưa có)"
echo "2. Cập nhật producer.py với external IP của Kafka"
echo "3. Tạo MinIO bucket 'water-quality-raw' qua web console"
echo "4. Chạy producer.py từ local"
echo "5. Chạy Spark jobs"
echo ""
echo "Xem README.md để biết thêm chi tiết."

