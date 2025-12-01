import csv
import json
import time
import os 
from kafka import KafkaProducer


# cai dat kafka
KAFKA_SERVER = 'localhost:29092' 
KAFKA_TOPIC = 'river_sensors'
DELAY_SECONDS = 1 

# lay duong dan day du den file nay
script_dir = os.path.dirname(os.path.abspath(__file__))

# lay duong dan den file data
DATA_FILE = os.path.join(script_dir, "sorted_water_quality_1.csv")

# ket noi kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print(f"Đã kết nối Kafka Producer tại {KAFKA_SERVER} thành công!")
except Exception as e:
    print(f"Lỗi! Không thể kết nối Kafka: {e}")
    exit() 

# ban vao kafka
try:
    print(f"Bắt đầu 'phát lại' (replay) dữ liệu từ file: {DATA_FILE}")
    
    with open(DATA_FILE, mode='r') as file:
        # doc file csv, moi dong la 1 dict
        csv_reader = csv.DictReader(file)
        
        for row in csv_reader:
            
            print(row)

            producer.send(KAFKA_TOPIC, value=row)
            # print(f"GỬI: [Time: {row['FullDate']}, Sông: {row['WaterbodyName']}]")
            print(f"GỬI: [Time: {row['FullDate']}, Sông: {row['WaterbodyName']}]", flush=True)
            
            time.sleep(DELAY_SECONDS) 

except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file nguồn '{DATA_FILE}'.")
    print(f"Script đang tìm ở: {script_dir}")
except KeyboardInterrupt:
    print("\nĐã dừng bởi người dùng...")
except Exception as e:
    print(f"Đã xảy ra lỗi khi gửi: {e}")
finally:
    producer.flush() 
    producer.close()
    print("Đã đóng kết nối Kafka.")