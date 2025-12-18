#!/bin/bash
# Script để cập nhật KAFKA_ADVERTISED_LISTENERS với external IP của LoadBalancer

NAMESPACE="river-analysis"
KAFKA_SERVICE="kafka-broker"
KAFKA_POD="kafka-broker-0"

echo "Đang lấy external IP của Kafka LoadBalancer..."

# Đợi LoadBalancer có external IP
while true; do
    EXTERNAL_IP=$(kubectl get svc -n $NAMESPACE $KAFKA_SERVICE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
    if [ -n "$EXTERNAL_IP" ]; then
        echo "✅ External IP: $EXTERNAL_IP"
        break
    fi
    echo "⏳ Đang đợi LoadBalancer có external IP..."
    sleep 5
done

# Cập nhật environment variable trong pod
echo "Đang cập nhật KAFKA_ADVERTISED_LISTENERS..."

# Lấy advertised listeners hiện tại
CURRENT_LISTENERS=$(kubectl exec -n $NAMESPACE $KAFKA_POD -- env | grep KAFKA_ADVERTISED_LISTENERS | cut -d'=' -f2)

# Tạo advertised listeners mới với external IP
NEW_LISTENERS="PLAINTEXT://kafka-broker:19092,PLAINTEXT_HOST://${EXTERNAL_IP}:29092"

# Cập nhật trong StatefulSet
kubectl set env -n $NAMESPACE statefulset/$KAFKA_SERVICE \
  KAFKA_ADVERTISED_LISTENERS="$NEW_LISTENERS"

echo "✅ Đã cập nhật KAFKA_ADVERTISED_LISTENERS"
echo "📝 Advertised Listeners: $NEW_LISTENERS"
echo ""
echo "⚠️  Lưu ý: Pod sẽ được restart để áp dụng thay đổi"
echo "📌 Sử dụng IP này trong producer.py: $EXTERNAL_IP:29092"

