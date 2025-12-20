#  Big Data River Quality Monitoring System

Hệ thống giám sát chất lượng nước sông real-time sử dụng Kafka, Spark Streaming, MinIO và InfluxDB.

---
## Mục tiêu
* Giám sát, xử lý và phân tích dữ liệu cảm biến nước sông theo thời gian thực.
* Phát hiện bất thường, cảnh báo và hiển thị dashboard

## Thành viên

- Nguyễn Thái Hiếu: 20225127 [@Hiesu19](https://www.github.com/Hiesu19)
- Phí Hoàng Việt: 20225429 [@Je-Tiev](https://github.com/Je-Tiev)
- Vũ Mạnh Hưng: 20225198 [@vmh714](https://github.com/vmh714)
- Hà Huy Dương: 20225183 [@brown2004](https://github.com/brown2004)
- Nguyễn Đức Dương: 20225122 [@duongdeptrai](https://github.com/duongdeptrai)
## Mục lục

- [Tổng quan hệ thống](#tổng-quan-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Hướng dẫn chạy](#hướng-dẫn-chạy)
- [Monitoring & Debug](#monitoring--debug)
- [Kiểm tra kết quả](#kiểm-tra-kết-quả)
- [Dừng hệ thống](#dừng-hệ-thống)

---

## Tổng quan hệ thống

### Kiến trúc

```
CSV Data → Producer → Kafka → Spark Streaming → MinIO (Raw Data)
                                               → InfluxDB (Time-series Metrics)
```

### Các thành phần

| Component | Port | Mô tả |
|-----------|------|-------|
| Kafka Broker | 29092 | Message queue |
| Kafka UI | 8080 | Web UI quản lý Kafka |
| Spark Master | 9090 | Spark cluster master |
| Spark Worker | 9091 | Spark executor |
| MinIO | 30001 | Object storage (S3-compatible) |
| InfluxDB | 8086 | Time-series database |
| Grafana | 30003 | Dashboard visualization |

---

## Cài đặt

### 1. Clone project

```bash
git clone <repository-url>
cd BIGDATA-RIVERANALYSIS
```

### 2. Chuẩn bị file dữ liệu

Đảm bảo file `WQI Results on Dataset.csv` nằm trong thư mục gốc.

### 3. Build Docker images

```bash
# Linux/Mac
docker-compose build

# Windows PowerShell
docker-compose build
```

**Thời gian:** 5-10 phút (lần đầu tiên)

### 4. Khởi động các services

```bash
# Linux/Mac
docker-compose up -d

# Windows PowerShell
docker-compose up -d
```

### 5. Kiểm tra trạng thái

```bash
# Linux/Mac
docker-compose ps

# Windows PowerShell
docker-compose ps
```

**Kết quả mong đợi:** Tất cả services đều có status `Up`

```
NAME              STATUS
kafka-broker      Up
kafka-controller  Up
kafka-ui          Up
minio             Up
mongodb           Up
spark-master      Up
spark-worker      Up
grafana           Up
```

---

## Hướng dẫn chạy

### Bước 1: Xử lý dữ liệu nguồn

```bash
# Linux/Mac
python sort_the_source.py

# Windows
python sort_the_source.py
```

**Output:** File `sorted_water_quality_1.csv` được tạo

---

### Bước 2: Tạo MinIO Bucket qua Web UI

1. Mở: http://localhost:30001
2. Đăng nhập: `admin` / `password123`
3. Nhấn **"Create Bucket"**
4. Tên bucket: `water-quality-raw`
5. Nhấn **"Create"**

---

### Bước 4: Chạy Kafka Producer

**Dùng terminal/PowerShell**

```bash
# Linux/Mac
cd kafka
pip3 install kafka-python
python3 producer.py

# Windows PowerShell
cd kafka
pip install kafka-python
python producer.py
```

**Log mong đợi:**

```
Đã kết nối Kafka Producer tại localhost:29092 thành công!
Bắt đầu 'phát lại' (replay) dữ liệu từ file: sorted_water_quality_1.csv
GỬI: [Time: 2020-01-01, Sông: Thames River]
GỬI: [Time: 2020-01-01, Sông: Credit River]
GỬI: [Time: 2020-02-01, Sông: Don River]
...
```

**Giữ terminal này chạy liên tục**

**Kiểm tra trong Kafka UI:**
- http://localhost:8080 → Topics → `river_sensors` → Messages
- Phải thấy messages đang tăng dần

---

### Bước 5: Chạy Spark Job - Ghi vào MinIO

**Mở terminal/PowerShell mới**

```bash
# Linux/Mac
docker exec -it spark-master-minio /opt/spark/bin/spark-submit \
  --master spark://spark-master-minio:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/jobs/spark_to_minio.py

# Windows PowerShell
docker exec -it spark-master-minio /opt/spark/bin/spark-submit `
  --master spark://spark-master-minio:7077 `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 `
  /opt/jobs/spark_to_minio.py
```

**Log mong đợi:**

```
✅ Đã khởi tạo Spark Session với MinIO connector
📡 Đang kết nối tới Kafka topic: river_sensors...
💾 Bắt đầu ghi streaming data vào MinIO: s3a://water-quality-raw/raw_data
🚀 Stream đang chạy... Nhấn Ctrl+C để dừng
```

**Giữ terminal này chạy liên tục**

**Kiểm tra trong MinIO:**
- http://localhost:30001 → Buckets → `water-quality-raw` → `raw_data/`
- Phải thấy các thư mục: `quality_label=Good`, `quality_label=Fair`, etc.
- Mỗi thư mục chứa file `.parquet`

---

### Bước 6: Chạy Spark Job - Ghi vào InfluxDB

**Mở terminal/PowerShell mới**

```bash
# Linux/Mac
docker exec -it spark-master-influxdb /opt/spark/bin/spark-submit \
  --master spark://spark-master-influxdb:7078 \
  -- deploy-mode client \
  /opt/jobs/spark_to_influxdb.py

# Windows PowerShell
docker exec -it spark-master-influxdb /opt/spark/bin/spark-submit `
  --master spark://spark-master-influxdb:7078 `
  --deploy-mode client `
  /opt/jobs/spark_to_influxdb.py
```

**Log mong đợi:**

```
✅ Đã khởi tạo Spark Session cho InfluxDB
🔗 InfluxDB URL: http://influxdb:8086
📡 Đang kết nối tới Kafka topic: river_sensors...
💾 Bắt đầu streaming metrics vào InfluxDB: water-quality.river-data
🚀 Stream đang chạy...
📊 Đang tính toán metrics theo cửa sổ 5 phút...
🔍 Batch 0: Đang ghi 15 records vào InfluxDB...
✅ Batch 0: Đã ghi thành công!
```

**Giữ terminal này chạy liên tục**

---

## Monitoring & Debug

### Web UIs

| Service | URL | Login |
|---------|-----|-------|
| Kafka UI | http://localhost:8080 | - |
| Spark Master | http://localhost:9090 | - |
| Spark Worker | http://localhost:9091 | - |
| InfluxDB UI | http://localhost:8086 | admin / password123 |
| MinIO Console | http://localhost:30001 | admin / password123 |
| Grafana | http://localhost:30003 | admin / admin123 |

### Xem logs

```bash
# Linux/Mac
# Logs tất cả services
docker-compose logs -f

# Logs của service cụ thể
docker-compose logs -f spark-master
docker-compose logs -f influxdb

# Windows PowerShell
# Logs tất cả services
docker-compose logs -f

# Logs của service cụ thể
docker-compose logs -f spark-master
docker-compose logs -f kafka-broker
docker-compose logs -f influx-broker
```

### Kiểm tra resource usage

```bash
# Linux/Mac
docker stats

# Windows PowerShell
docker stats
```

---

## Dừng hệ thống

### Dừng tạm thời (giữ lại dữ liệu)

```bash
# Linux/Mac
# Dừng Producer: Nhấn Ctrl+C trong terminal Producer
# Dừng Spark jobs: Nhấn Ctrl+C trong terminal Spark

# Dừng tất cả containers
docker-compose stop

# Khởi động lại
docker-compose start

# Windows PowerShell
# Dừng Producer: Nhấn Ctrl+C trong PowerShell Producer
# Dừng Spark jobs: Nhấn Ctrl+C trong PowerShell Spark

# Dừng tất cả containers
docker-compose stop

# Khởi động lại
docker-compose start
```

### Dừng hoàn toàn (giữ lại dữ liệu)

```bash
# Linux/Mac và Windows PowerShell
docker-compose down
```

### Xóa toàn bộ (bao gồm cả dữ liệu)

```bash
# Linux/Mac và Windows PowerShell
docker-compose down -v

# Xóa tất cả images (tùy chọn)
docker-compose down -v --rmi all
```

---

## 📁 Cấu trúc thư mục

```
BIGDATA-RIVERANALYSIS/
├── kafka/
│   ├── Dockerfile
│   └── producer.py
├── streaming
│   ├── spark_to_minio.py
│   └── spark_to_influxdb.py
├── Dockerfile.spark              
├── docker-compose.yml
├── sort_the_source.py
├── sorted_water_quality_1.csv    # ← Được tạo bởi sort_the_source.py
├── WQI Results on Dataset.csv    # ← File dữ liệu gốc
└── README.md                     
```

---

## 📚 Tài liệu tham khảo

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [MinIO Documentation](https://min.io/docs/minio/linux/index.html)
- [MongoDB Manual](https://www.mongodb.com/docs/manual/)

---