### Hướng Dẫn Cài Đặt Nhanh: Kafka Producer trên Kubernetes


## Bước 1: Build "Hộp" Docker cho Ứng Dụng 📦
Cài Docker và Minikube

Khởi động Minikube: Đảm bảo cluster của bạn đang chạy và có đủ tài nguyên.

PowerShell
```
minikube start --memory 4096 --cpus 4
```
Kết nối Terminal với Docker của Minikube: Bước cực kỳ quan trọng này ra lệnh cho terminal của bạn sử dụng Docker bên trong Minikube, thay vì Docker trên máy tính.

PowerShell
```
minikube -p minikube docker-env | Invoke-Expression
```

## Bước 2: Di chuyển vào thư mục kafka: File Dockerfile của bạn nằm trong thư mục kafka, vì vậy bạn phải chạy lệnh build từ đây.

```
cd kafka
```
Build Image:

```
docker build -t kafka-producer-app .
```
## Bước 3: Triển Khai Toàn Bộ Hệ Thống 🚀
Áp dụng file cấu hình: Chạy lệnh này từ thư mục gốc của project.

PowerShell
```
kubectl apply -f kafka-full.yml
```
Xem các Pod khởi động: Hãy chờ cho đến khi tất cả các pod đều có trạng thái Running.

PowerShell
```
kubectl get pods -w
```
## Bước 4: Kiểm Tra Kết Quả ✅
Khi pod producer đã chạy, hãy kiểm tra log của nó để xem script Python của bạn hoạt động.

Lấy tên Pod của Producer: Đầu tiên, liệt kê các pod để tìm tên đầy đủ của producer.

PowerShell
```
kubectl get pods
```

Hãy tìm tên bắt đầu bằng kafka-producer-deployment-...

Xem Log: Sử dụng cờ -f để theo dõi log khi có tin nhắn mới được gửi.

PowerShell
```
kubectl logs -f <ten-pod-producer-cua-ban>
```
Bạn sẽ thấy các dòng GỬI: ... xuất hiện đều đặn!

## Các Lệnh Hữu Ích
Để cập nhật code Python:

Build lại image (làm lại Bước 1).

Ra lệnh cho Kubernetes khởi động lại producer với image mới:

PowerShell 
```
kubectl rollout restart deploy/kafka-producer-deployment
```
Để dọn dẹp và xóa mọi thứ:

PowerShell
```
kubectl delete -f kafka-full.yml
```