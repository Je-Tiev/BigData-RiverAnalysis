import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer

# --- CẤU HÌNH KAFKA ---
KAFKA_SERVER = 'localhost:29092' 
KAFKA_TOPIC = 'river-quality'
DELAY_SECONDS = 1 

# --- DANH SÁCH SÔNG GIẢ LẬP (10 SÔNG) ---
RIVER_NAMES = [
    "Sông Hồng", "Sông Đà", "Sông Lô", "Sông Thái Bình", 
    "Sông Mã", "Sông Lam", "Sông Hương", 
    "Sông Hàn", "Sông Sài Gòn", "Sông Đồng Nai"
]

# --- KẾT NỐI KAFKA ---
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f" Đã kết nối Kafka Producer tại {KAFKA_SERVER} thành công!")
except Exception as e:
    print(f" Lỗi! Không thể kết nối Kafka: {e}")
    exit()

# --- HÀM SINH DỮ LIỆU NGẪU NHIÊN ---
def generate_sensor_data():
    now = datetime.now()
    #  Giờ:Phút:Giây để dữ liệu realtime không bị trùng thời gian
    full_date = now.strftime("%Y-%m-%d %H:%M:%S")
    
    river = random.choice(RIVER_NAMES)
    
    record = {
        "FullDate": full_date,
        "WaterbodyName": river,
        
        # Các chỉ số random 
        "Temperature": round(random.uniform(10.0, 32.0), 1),
        "pH": round(random.uniform(6.5, 8.5), 2),
        "Dissolved Oxygen": round(random.uniform(4.0, 10.0), 1),
        
       
        "Conductivity @25Â°C": round(random.uniform(150, 600), 1),
        
        "Ammonia-Total (as N)": round(random.uniform(0.01, 0.5), 3),
        "BOD - 5 days (Total)": round(random.uniform(1.0, 5.0), 1),
        "Chloride": round(random.uniform(10, 50), 1),
        "Total Hardness (as CaCO3)": round(random.uniform(50, 300), 1),
        
        # CCME Values giả lập số thực dài
        "CCME_Values": str(random.uniform(60, 100)) 
    }
    return record

# --- VÒNG LẶP GỬI DỮ LIỆU ---
try:
    print(f" Bắt đầu sinh dữ liệu và bắn vào topic '{KAFKA_TOPIC}'...")
    
    while True:
        # 1. Sinh bản ghi mới
        data = generate_sensor_data()
        
        # 2. Gửi vào Kafka
        producer.send(KAFKA_TOPIC, value=data)
        
        # 3. In TOÀN BỘ dữ liệu ra màn hình để kiểm tra
        print(data)
        
        # 4. Nghỉ 0.1 giây
        time.sleep(DELAY_SECONDS)


except Exception as e:
    print(f" Đã xảy ra lỗi khi gửi: {e}")
finally:
    producer.flush() 
    producer.close()
    print("Đã đóng kết nối Kafka.")