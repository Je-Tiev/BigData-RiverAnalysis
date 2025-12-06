import csv
import os
from pymongo import MongoClient
from datetime import datetime

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:password123@mongodb:27017/?authSource=admin")
client = MongoClient(MONGO_URI)
db = client['river_data']
col = db['measurements']


def ensure_indexes():
    col.create_index([("timestamp", 1)])
    col.create_index([("site", 1)])


def load_csv(path):
    inserted = 0
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ts = datetime.fromisoformat(row['FullDate'])
            except Exception:
                continue
            doc = {
                'timestamp': ts,
                'site': row.get('WaterbodyName'),
                'pH': try_float(row.get('pH')),
                'dissolved_oxygen': try_float(row.get('Dissolved Oxygen')),
                'conductivity': try_float(row.get('Conductivity @25°C')),
                'raw': row
            }
            col.insert_one(doc)
            inserted += 1
    return inserted


def try_float(v):
    try:
        return float(v) if v not in (None, '') else None
    except Exception:
        return None


if __name__ == '__main__':
    # mounted project root is at /app/data (readonly) per docker-compose
    csv_path = '/app/data/kafka/sorted_water_quality.csv'
    if not os.path.exists(csv_path):
        print('CSV file not found at', csv_path)
        print('Make sure you mounted the project root into /app/data or run loader from repo root.')
        raise SystemExit(1)
    print('Ensuring indexes...')
    ensure_indexes()
    print('Loading CSV:', csv_path)
    count = load_csv(csv_path)
    print('Inserted', count, 'documents')
