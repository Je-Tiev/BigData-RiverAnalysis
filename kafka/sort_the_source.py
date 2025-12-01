import pandas as pd
import os 

script_dir = os.path.dirname(os.path.abspath(__file__))

input_file = os.path.join(script_dir, "WQI Results on Dataset.csv")
output_file = os.path.join(script_dir, "sorted_water_quality_1.csv")

print(f"Bắt đầu xử lý file: {input_file}") 

try:

    df = pd.read_csv(input_file)

    month_map = {
        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4,
        'May': 5, 'Jun': 6, 'Jul': 7, 'Aug': 8,
        'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }
    df['MonthNumber'] = df['SampleDate'].map(month_map)
    df['FullDate'] = pd.to_datetime(df[['Years', 'MonthNumber']].rename(columns={'Years': 'year', 'MonthNumber': 'month'}).assign(day=1))
    df_sorted = df.sort_values(by='FullDate')
    print("Đã sắp xếp dữ liệu theo thời gian.")

    # --- PHẦN ĐÃ SỬA: LẤY THÊM CÁC CỘT QUAN TRỌNG ---
    selected_columns = [
        'FullDate', 
        'WaterbodyName', 
        'Temperature',                # BẮT BUỘC: Nhiệt độ ảnh hưởng tới mọi phản ứng
        'pH', 
        'Dissolved Oxygen',           # Oxy hòa tan
        'Conductivity @25°C',         # Độ dẫn điện
        'Ammonia-Total (as N)',       # Phát hiện nước thải sinh hoạt
        'BOD - 5 days (Total)',       # Phát hiện ô nhiễm hữu cơ
        'Chloride',                   # Độ mặn/công nghiệp
        'Total Hardness (as CaCO3)',  # Độ cứng
        # 'CCME_WQI',                   # Kết quả tham khảo (Dùng để so sánh/train AI)
        'CCME_Values'                 # Nhãn phân loại (Good/Poor...)
    ]
    # -----------------------------------------------

    df_final = df_sorted[selected_columns].copy()
    df_final['FullDate'] = df_final['FullDate'].dt.strftime('%Y-%m-%d')
    print(f"Đã chọn lọc {len(selected_columns)} cột cốt lõi cho Big Data.")


    df_final.to_csv(output_file, index=False)
    
    print(f"\n--- HOÀN THÀNH! ---")
    print(f"Đã tạo thành công file: '{output_file}'")

except KeyError as e:
    print(f"Lỗi: Tên cột {e} không khớp với file CSV gốc. Bạn hãy kiểm tra kỹ lại tiêu đề file CSV nhé.")
except FileNotFoundError:
    print(f"Lỗi: Không tìm thấy file đầu vào '{input_file}'.")
except Exception as e:
    print(f"Đã xảy ra lỗi: {e}")