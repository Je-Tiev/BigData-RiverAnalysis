# River Quality Monitoring System - Spark Processing Guide

## 📋 Mục lục
- [Tổng quan](#tổng-quan)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Kiến trúc hệ thống](#kiến-trúc-hệ-thống)
- [Option 1: Local Mode (Đơn giản - Development)](#option-1-local-mode-đơn-giản---development)
- [Option 2: Cluster Mode (Production - Thực tế)](#option-2-cluster-mode-production---thực-tế)
- [So sánh Local vs Cluster](#so-sánh-local-vs-cluster)
- [Xử lý lỗi thường gặp](#xử-lý-lỗi-thường-gặp)
- [Monitoring & Debugging](#monitoring--debugging)

---

## 🎯 Tổng quan

Hệ thống xử lý dữ liệu chất lượng nước sông theo thời gian thực sử dụng:
- **Apache Kafka**: Message streaming
- **Apache Spark**: Stream processing
- **MongoDB**: Data storage
- **Python**: Application logic

Hệ thống hỗ trợ **2 chế độ chạy**:
1. **Local Mode**: Đơn giản, phù hợp cho development/testing
2. **Cluster Mode**: Production-ready, có thể scale

---

## 💻 Yêu cầu hệ thống

### Phần mềm bắt buộc:
- ✅ **Docker Desktop** (≥ 20.x) & **Docker Compose** (≥ 2.x)
- ✅ **Java JDK** (8, 11, hoặc 17)
  - Download: [Adoptium OpenJDK](https://adoptium.net/)
  - Verify: `java -version`
- ✅ **Python** (≥ 3.8)
  - Download: [Python.org](https://www.python.org/downloads/)
  - Verify: `python --version`

### Python packages:
```bash
pip install pyspark==3.5.0
pip install kafka-python
pip install pymongo
```

### Hardware tối thiểu:
| Component | Local Mode | Cluster Mode |
|-----------|------------|--------------|
| RAM | 4 GB | 8 GB |
| CPU | 2 cores | 4 cores |
| Disk | 10 GB | 20 GB |

---

## 🏗️ Kiến trúc hệ thống

### Local Mode Architecture:
```
┌─────────────────────────────────────────────┐
│  Docker Containers                          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │  Kafka   │  │ MongoDB  │  │ Kafka UI │ │
│  └──────────┘  └──────────┘  └──────────┘ │
└────────┬────────────┬────────────┬─────────┘
         │            │            │
    ┌────┴────────────┴────────────┴─────┐
    │  HOST MACHINE                      │
    │  ┌──────────────────────────────┐  │
    │  │  Spark (Local Mode)          │  │
    │  │  spark_processor_local.py    │  │
    │  └──────────────────────────────┘  │
    └────────────────────────────────────┘
```

### Cluster Mode Architecture:
```
┌──────────────────────────────────────────────────────┐
│  Docker Containers                                    │
│  ┌──────────┐  ┌──────────┐  ┌─────────────────────┐│
│  │  Kafka   │  │ MongoDB  │  │  Spark Master       ││
│  └──────────┘  └──────────┘  │  (Cluster Manager)  ││
│                               └──────────┬──────────┘│
│                                          │           │
│                               ┌──────────┴──────────┐│
│                               │  Spark Worker(s)    ││
│                               │  (Task Executors)   ││
│                               └─────────────────────┘│
└───────────────────────────────────────────┬──────────┘
                                            │
                              ┌─────────────┴──────────┐
                              │  HOST MACHINE          │
                              │  spark_processor_      │
                              │  cluster.py            │
                              └────────────────────────┘
```

---

## 🟢 Option 1: Local Mode (Đơn giản - Development)

### ✅ Khi nào sử dụng Local Mode?
- 🎓 Học tập, nghiên cứu
- 🔧 Development và testing
- 💻 Chạy trên laptop cá nhân
- 📊 Xử lý dữ liệu nhỏ (< 10GB/day)

### 📁 Cấu trúc thư mục:
```
river-monitoring/
├── docker-compose-local.yml          # ← Docker config (không có Spark containers)
├── spark_processor_local_mode.py     # ← Code Python
├── data/
│   └── river_data.csv
├── checkpoints/                      # ← Spark checkpoints (auto-created)
├── spark-tmp/                        # ← Spark temp dir (auto-created)
└── README.md
```

### 🚀 Bước 1: Chuẩn bị Docker Compose

Sử dụng file `docker-compose-local.yml` (KHÔNG bao gồm spark-master/spark-worker):

```yaml
# docker-compose-local.yml
version: '3.8'

services:
  kafka-broker:
    image: apache/kafka:4.0.1
    container_name: kafka-broker
    ports:
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
      CLUSTER_ID: kafka-cluster-1
    networks:
      - bigdata-network

  kafka-controller:
    image: apache/kafka:4.0.1
    container_name: kafka-controller
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:9093,2@kafka-controller:9093
      CLUSTER_ID: kafka-cluster-1
    networks:
      - bigdata-network

  mongodb:
    image: mongo:5.0
    container_name: mongodb
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_DATABASE: river_monitoring
    volumes:
      - mongodb_data:/data/db
    networks:
      - bigdata-network

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-broker:29092
      DYNAMIC_CONFIG_ENABLED: 'true'
    depends_on:
      - kafka-broker
    networks:
      - bigdata-network

networks:
  bigdata-network:
    driver: bridge

volumes:
  mongodb_data:
```

### 🚀 Bước 2: Khởi động Docker containers

```bash
# Start containers
docker-compose -f docker-compose-local.yml up -d

# Verify containers đang chạy
docker ps

# Kết quả mong đợi:
# ✅ kafka-broker
# ✅ kafka-controller  
# ✅ mongodb
# ✅ kafka-ui
# ❌ spark-master (KHÔNG CÓ)
# ❌ spark-worker (KHÔNG CÓ)

# Check logs nếu cần
docker-compose -f docker-compose-local.yml logs -f
```

### 🚀 Bước 3: Kiểm tra services

```bash
# Test Kafka
docker exec -it kafka-broker kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Test MongoDB
docker exec -it mongodb mongosh --eval "db.adminCommand('ping')"

# Hoặc truy cập UIs:
# - Kafka UI: http://localhost:8080
# - MongoDB: mongosh hoặc MongoDB Compass (localhost:27017)
```

### 🚀 Bước 4: Chạy Spark code (trên HOST)

```bash
# CHẠY TRÊN HOST MACHINE (không trong Docker)
python spark_processor_local_mode.py

# Output mong đợi:
# ✅ JAVA_HOME: C:\Program Files\Java\jdk-11
# ✅ SPARK LOCAL MODE ACTIVATED
# 🎯 Master: local[*]
# 📊 Parallelism: 8
# 📡 Kafka connection successful
# 💾 MongoDB streams started
# 🚀 HỆ THỐNG ĐANG CHẠY - LOCAL MODE
```

### 🛑 Bước 5: Dừng hệ thống

```bash
# Dừng Spark code (Ctrl+C trong terminal đang chạy Python)

# Dừng Docker containers
docker-compose -f docker-compose-local.yml down

# Xóa volumes nếu muốn reset data
docker-compose -f docker-compose-local.yml down -v
```

---

## 🔵 Option 2: Cluster Mode (Production - Thực tế)

### ✅ Khi nào sử dụng Cluster Mode?
- 🏢 Môi trường production
- 📈 Cần scale (xử lý nhiều data)
- 🎓 Học kiến trúc distributed system
- 💼 Demo cho portfolio/interview

### 📁 Cấu trúc thư mục:
```
river-monitoring/
├── docker-compose-cluster.yml        # ← Docker config (có Spark containers)
├── spark_processor_cluster.py        # ← Code Python
├── data/
├── checkpoints/
└── README.md
```

### 🚀 Bước 1: Chuẩn bị Docker Compose

Sử dụng file `docker-compose-cluster.yml` (BAO GỒM spark-master/spark-worker):

```yaml
# docker-compose-cluster.yml
version: '3.8'

services:
  # === KAFKA SERVICES ===
  kafka-broker:
    image: apache/kafka:4.0.1
    container_name: kafka-broker
    ports:
      - "29092:29092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:29092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker:9092,PLAINTEXT_HOST://localhost:29092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 3
      CLUSTER_ID: kafka-cluster-1
    networks:
      - bigdata-network

  kafka-controller:
    image: apache/kafka:4.0.1
    container_name: kafka-controller
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: controller
      KAFKA_LISTENERS: CONTROLLER://0.0.0.0:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-broker:9093,2@kafka-controller:9093
      CLUSTER_ID: kafka-cluster-1
    networks:
      - bigdata-network

  # === MONGODB ===
  mongodb:
    image: mongo:5.0
    container_name: mongodb
    ports:
      - "27017:27017"
    environment:
      MONGO_INITDB_DATABASE: river_monitoring
    volumes:
      - mongodb_data:/data/db
    networks:
      - bigdata-network

  # === SPARK CLUSTER ===
  spark-master:
    image: bitnami/spark:3.5.7
    container_name: spark-master
    ports:
      - "7077:7077"    # Cluster port
      - "8081:8080"    # Web UI
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    networks:
      - bigdata-network

  spark-worker:
    image: bitnami/spark:3.5.7
    container_name: spark-worker
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=2G
      - SPARK_WORKER_CORES=2
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    networks:
      - bigdata-network

  # === KAFKA UI ===
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-broker:29092
      DYNAMIC_CONFIG_ENABLED: 'true'
    depends_on:
      - kafka-broker
    networks:
      - bigdata-network

networks:
  bigdata-network:
    driver: bridge

volumes:
  mongodb_data:
```

### 🚀 Bước 2: Khởi động Docker containers

```bash
# Start ALL containers (bao gồm Spark cluster)
docker-compose -f docker-compose-cluster.yml up -d

# Verify containers đang chạy
docker ps

# Kết quả mong đợi:
# ✅ kafka-broker
# ✅ kafka-controller
# ✅ mongodb
# ✅ kafka-ui
# ✅ spark-master    ← MỚI
# ✅ spark-worker    ← MỚI

# Chờ 10-15 giây để Spark cluster khởi động
```

### 🚀 Bước 3: Verify Spark cluster

```bash
# Check Spark Master Web UI
# Mở browser: http://localhost:8081

# Phải thấy:
# ✅ Status: ALIVE
# ✅ Workers: 1
# ✅ Cores: 2 Total, 2 Available
# ✅ Memory: 2.0 GB Total, 2.0 GB Available

# Check logs
docker logs spark-master
docker logs spark-worker
```

### 🚀 Bước 4: Chạy Spark code (cluster mode)

```bash
# CHẠY TRÊN HOST MACHINE
python spark_processor_cluster.py

# Output mong đợi:
# ✅ JAVA_HOME: C:\Program Files\Java\jdk-11
# 🔧 Đang khởi tạo Spark Cluster Mode...
# ✅ SPARK CLUSTER MODE ACTIVATED
# 🎯 Master URL: spark://spark-master:7077
# 📊 Default Parallelism: 2
# 💾 Executor Memory: 1g
# 🔢 Executor Cores: 2
# 📡 Kafka connection successful
# 🚀 HỆ THỐNG ĐANG CHẠY - CLUSTER MODE
```

### 🚀 Bước 5: Monitor cluster

```bash
# Spark Master UI
http://localhost:8081
# Xem: Workers, Running Applications, Completed Applications

# Spark Application UI (khi job đang chạy)
http://localhost:4040
# Xem: Jobs, Stages, Storage, Environment

# Kafka UI
http://localhost:8080
# Xem: Topics, Messages, Consumer Groups
```

### 🛑 Bước 6: Dừng hệ thống

```bash
# Dừng Spark code (Ctrl+C)

# Dừng Docker containers
docker-compose -f docker-compose-cluster.yml down

# Clean up volumes
docker-compose -f docker-compose-cluster.yml down -v
```

---

## ⚖️ So sánh Local vs Cluster

| Tiêu chí | Local Mode | Cluster Mode |
|----------|------------|--------------|
| **Độ phức tạp** | 🟢 Đơn giản | 🟡 Trung bình |
| **Setup time** | 🟢 5 phút | 🟡 10-15 phút |
| **Docker containers** | 4-5 | 7-8 |
| **RAM cần** | 4 GB | 8 GB |
| **CPU cần** | 2 cores | 4 cores |
| **Spark Master** | `.master("local[*]")` | `.master("spark://spark-master:7077")` |
| **Kafka address** | `localhost:29092` | `kafka-broker:29092` (trong Docker)<br>`localhost:29092` (từ host) |
| **MongoDB address** | `localhost:27017` | `mongodb:27017` (trong Docker)<br>`localhost:27017` (từ host) |
| **Scalability** | ❌ Không scale | ✅ Thêm workers dễ dàng |
| **Production-ready** | ❌ Không | ✅ Có |
| **Learning value** | 🟡 Cơ bản | 🟢 Cao (distributed system) |
| **Debug** | 🟢 Dễ | 🟡 Phức tạp hơn |
| **Use case** | Dev, Testing, Learning | Production, Portfolio, Scale |

### 📊 Khi nào dùng gì?

```
┌─────────────────────────────────────────────────────┐
│  Nếu bạn...                          → Chọn         │
├─────────────────────────────────────────────────────┤
│  Đang học Spark lần đầu              → Local Mode   │
│  Chỉ muốn test code nhanh            → Local Mode   │
│  Laptop yếu (4GB RAM)                → Local Mode   │
│  Muốn học kiến trúc thực tế          → Cluster Mode │
│  Cần cho portfolio/CV                → Cluster Mode │
│  Chuẩn bị deploy production          → Cluster Mode │
│  Data lớn (>10GB/day)                → Cluster Mode │
└─────────────────────────────────────────────────────┘
```

---

## ⚠️ Lưu ý quan trọng khi chạy cả 2 modes

### 🚨 **NGUYÊN TẮC: KHÔNG chạy đồng thời 2 modes**

#### ❌ **KHÔNG ĐƯỢC LÀM:**
```bash
# Terminal 1
python spark_processor_local_mode.py      # Đang chạy

# Terminal 2
python spark_processor_cluster.py         # ❌ XUNG ĐỘT!
```

#### ✅ **LÀM ĐÚNG:**
```bash
# Chạy Local Mode
python spark_processor_local_mode.py
# Ctrl+C để dừng

# SAU ĐÓ mới chạy Cluster Mode
python spark_processor_cluster.py
```

### 🔴 **Lý do KHÔNG được chạy đồng thời:**

1. **Xung đột port Spark UI**
   - Cả 2 đều dùng port `4040` cho Application UI
   - Process thứ 2 sẽ bị lỗi hoặc dùng port khác (4041, 4042...)

2. **Xung đột Kafka Consumer Group**
   - Cả 2 đều đọc từ cùng topic `river_sensors`
   - Kafka sẽ rebalance partitions → dữ liệu bị phân tán

3. **Xung đột MongoDB writes**
   - Cả 2 đều ghi vào cùng collection
   - Có thể gây duplicate data hoặc checkpoint conflicts

4. **Xung đột Checkpoint directory**
   - Cả 2 đều dùng `./checkpoints/`
   - Spark sẽ báo lỗi checkpoint corruption

### ✅ **Cách chuyển đổi giữa 2 modes:**

#### **Từ Local → Cluster:**
```bash
# 1. Dừng Local Mode (Ctrl+C)

# 2. Stop Docker local
docker-compose -f docker-compose-local.yml down

# 3. XÓA checkpoints cũ (QUAN TRỌNG!)
rm -rf ./checkpoints/*
# Windows: rmdir /s /q checkpoints

# 4. Start Cluster
docker-compose -f docker-compose-cluster.yml up -d

# 5. Chờ 10 giây để Spark cluster ready

# 6. Chạy Cluster Mode
python spark_processor_cluster.py
```

#### **Từ Cluster → Local:**
```bash
# 1. Dừng Cluster Mode (Ctrl+C)

# 2. Stop Docker cluster
docker-compose -f docker-compose-cluster.yml down

# 3. XÓA checkpoints cũ (QUAN TRỌNG!)
rm -rf ./checkpoints/*

# 4. Start Local
docker-compose -f docker-compose-local.yml up -d

# 5. Chạy Local Mode
python spark_processor_local_mode.py
```

### 🗑️ **Script tự động clean checkpoints:**

Tạo file `reset.sh` (Linux/Mac) hoặc `reset.bat` (Windows):

**Linux/Mac (reset.sh):**
```bash
#!/bin/bash
echo "🧹 Cleaning checkpoints..."
rm -rf ./checkpoints/*
rm -rf ./spark-tmp/*
echo "✅ Clean completed!"
```

**Windows (reset.bat):**
```bat
@echo off
echo 🧹 Cleaning checkpoints...
rmdir /s /q checkpoints 2>nul
rmdir /s /q spark-tmp 2>nul
mkdir checkpoints
mkdir spark-tmp
echo ✅ Clean completed!
```

**Sử dụng:**
```bash
# Linux/Mac
chmod +x reset.sh
./reset.sh

# Windows
reset.bat
```

### 📋 **Checklist trước khi chuyển mode:**

- [ ] Dừng Python process hiện tại (Ctrl+C)
- [ ] Stop Docker containers (`docker-compose down`)
- [ ] Xóa checkpoints (`rm -rf ./checkpoints/*`)
- [ ] Xóa spark-tmp (`rm -rf ./spark-tmp/*`)
- [ ] Start Docker với config mới
- [ ] Verify containers đang chạy (`docker ps`)
- [ ] Chạy Python script mới

---

## 🐛 Xử lý lỗi thường gặp

### ❌ Lỗi 1: Java not found

```bash
ERROR: JAVA_HOME is not set and no 'java' command could be found

# FIX:
# 1. Cài Java JDK 8/11/17
# Download: https://adoptium.net/

# 2. Set JAVA_HOME
# Windows:
set JAVA_HOME=C:\Program Files\Java\jdk-11
set PATH=%JAVA_HOME%\bin;%PATH%

# Linux/Mac:
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# 3. Verify
java -version
```

### ❌ Lỗi 2: Kafka connection refused

```bash
ERROR: Connection to node -1 (localhost:29092) could not be established

# FIX:
# 1. Check Kafka đang chạy
docker ps | grep kafka

# 2. Check port
docker port kafka-broker

# 3. Test connection
telnet localhost 29092
# Hoặc
nc -zv localhost 29092

# 4. Restart Kafka nếu cần
docker-compose restart kafka-broker
```

### ❌ Lỗi 3: MongoDB timeout

```bash
ERROR: com.mongodb.MongoTimeoutException: Timed out after 30000 ms

# FIX:
# 1. Check MongoDB đang chạy
docker ps | grep mongodb

# 2. Test connection
docker exec -it mongodb mongosh --eval "db.adminCommand('ping')"

# 3. Check port
docker port mongodb

# 4. Restart MongoDB
docker-compose restart mongodb
```

### ❌ Lỗi 4: Spark Master connection refused (Cluster Mode)

```bash
ERROR: Could not connect to spark-master:7077

# FIX:
# 1. Check Spark Master đang chạy
docker ps | grep spark-master

# 2. Check Spark Master UI
# Browser: http://localhost:8081

# 3. Check logs
docker logs spark-master
docker logs spark-worker

# 4. Verify worker connected
# Trong Spark Master UI phải thấy: Workers: 1

# 5. Restart Spark cluster
docker-compose restart spark-master spark-worker
```

### ❌ Lỗi 5: Port already in use

```bash
ERROR: Bind for 0.0.0.0:4040 failed: port is already allocated

# FIX:
# 1. Find process using port
# Windows:
netstat -ano | findstr :4040
taskkill /PID <process_id> /F

# Linux/Mac:
lsof -i :4040
kill -9 <process_id>

# 2. Hoặc dừng Spark process cũ
# Tìm và kill tất cả process Python đang chạy Spark
```

### ❌ Lỗi 6: Checkpoint already exists

```bash
ERROR: Checkpoint directory already exists

# FIX:
# Xóa checkpoints cũ
rm -rf ./checkpoints/*

# Windows:
rmdir /s /q checkpoints
mkdir checkpoints
```

### ❌ Lỗi 7: Insufficient memory

```bash
ERROR: Not enough memory to create Java heap

# FIX Local Mode:
# Giảm memory config trong code
.config("spark.driver.memory", "1g")  # Từ 2g → 1g
.config("spark.executor.memory", "1g")

# FIX Cluster Mode:
# Sửa docker-compose-cluster.yml
environment:
  - SPARK_WORKER_MEMORY=1G  # Từ 2G → 1G
```

---

## 📊 Monitoring & Debugging

### 🌐 **Web UIs:**

| Service | URL | Mục đích |
|---------|-----|----------|
| Kafka UI | http://localhost:8080 | Monitor Kafka topics, messages |
| Spark Master UI | http://localhost:8081 | Monitor cluster (Cluster Mode only) |
| Spark Application UI | http://localhost:4040 | Monitor running job |
| MongoDB Compass | localhost:27017 | Browse database |

### 📝 **Useful Docker commands:**

```bash
# View all containers
docker ps -a

# View logs
docker logs -f kafka-broker
docker logs -f mongodb
docker logs -f spark-master
docker logs -f spark-worker

# Enter container
docker exec -it kafka-broker bash
docker exec -it mongodb mongosh

# Check resource usage
docker stats

# Restart specific service
docker-compose restart kafka-broker

# View networks
docker network ls
docker network inspect bigdata-network
```

### 🔍 **Debugging Spark jobs:**

```bash
# Check Spark logs trong console output

# Check Spark UI (http://localhost:4040)
# - Jobs: Xem progress của từng job
# - Stages: Chi tiết từng stage
# - Storage: Memory/Disk usage
# - Environment: Config đang dùng
# - Executors: Resource usage

# Check checkpoints
ls -la ./checkpoints/

# Check Spark temp
ls -la ./spark