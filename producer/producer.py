# producer.py
import pandas as pd
from kafka import KafkaProducer
import json
import time
import math
# =========================
# CÀI ĐẶT PRODUCER
# =========================
KAFKA_BROKER = "kafka:9092"  # hoặc "kafka:9092" trong docker
TOPIC = "health-topic"
INPUT_FILE = "/data/health_data.csv"   # file CSV hoặc Excel
NUM_WORKERS = 10                 # số worker bạn muốn demo

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# =========================
# ĐỌC DỮ LIỆU
# =========================
def read_csv_safe(path):
    for enc in ["utf-8", "utf-8-sig", "cp1258", "latin1"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise RuntimeError("❌ Cannot decode CSV file")

df = read_csv_safe(INPUT_FILE)

print(df.columns.tolist())

# =========================
# NORMALIZE + RENAME COLUMNS
# =========================
df.columns = df.columns.str.strip().str.lower()

COLUMN_MAP = {
    "patient id": "PatientID",
    "heart rate": "HeartRate",
    "respiratory rate": "RespiratoryRate",
    "body temperature": "BodyTemperature",
    "oxygen saturation": "OxygenSaturation",
    "systolic blood pressure": "BloodPressureSys",
    "diastolic blood pressure": "BloodPressureDia",
    "age": "Age",
    "gender": "Gender"
}

df = df.rename(columns=COLUMN_MAP)
print("✅ AFTER RENAME:")
print(df.columns.tolist())

# =========================
# ADD REQUIRED STREAMING FIELDS
# =========================
df["sent_at"] = time.time()
df["is_end"] = False

# =========================
# VALIDATE REQUIRED COLUMNS
# =========================
required_cols = [
    "PatientID",
    "HeartRate",
    "RespiratoryRate",
    "BodyTemperature",
    "OxygenSaturation",
    "BloodPressureSys",
    "BloodPressureDia",
    "Age",
    "Gender",
    "sent_at",
    "is_end"
]

# =========================
# CHIA DỮ LIỆU CHO NHIỀU WORKER
# =========================
# Dùng key để Kafka phân phối các record đều theo partition
def get_partition_key(patient_id, num_workers):
    return str(int(patient_id) % num_workers)

# =========================
# GỬI DỮ LIỆU
# =========================
for index, row in df.iterrows():
    msg = {
        "PatientID": str(row["PatientID"]),
        "HeartRate": float(row["HeartRate"]),
        "RespiratoryRate": float(row["RespiratoryRate"]),
        "BodyTemperature": float(row["BodyTemperature"]),
        "OxygenSaturation": float(row["OxygenSaturation"]),
        "BloodPressureSys": float(row["BloodPressureSys"]),
        "BloodPressureDia": float(row["BloodPressureDia"]),
        "Age": int(row["Age"]),
        "Gender": str(row["Gender"]),
        "sent_at": float(row["sent_at"]),
        "is_end": False
    }
    partition_key = get_partition_key(row["PatientID"], NUM_WORKERS)
    producer.send(TOPIC, key=partition_key.encode("utf-8"), value=msg)
    print("📤 Sent:", msg)
    # Tốc độ gửi, tùy chỉnh
    time.sleep(0.001)

# Gửi record kết thúc (is_end=True)
for i in range(NUM_WORKERS):
    end_record = {
        "PatientID": f"END_{i}",
        "HeartRate": None,
        "RespiratoryRate": None,
        "BodyTemperature": None,
        "OxygenSaturation": None,
        "BloodPressureSys": None,
        "BloodPressureDia": None,
        "Age": None,
        "Gender": None,
        "sent_at": time.time(),
        "is_end": True
    }
    producer.send(TOPIC, key=str(i).encode("utf-8"), value=end_record)

producer.flush()
print(f"✅ All data sent to Kafka for {NUM_WORKERS} workers.")
