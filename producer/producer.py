# producer.py
import pandas as pd
import json
import time
from kafka import KafkaProducer

TOPIC = "health-topic"
BOOTSTRAP = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_excel("/data/health_data.xlsx")

start_time = time.time()

for _, r in df.iterrows():
    msg = {
        # ---- Identity ----
        "PatientID": str(r.get("PatientID")),
        "State": r.get("State"),
        "Sex": r.get("Sex"),
        "GeneralHealth": r.get("GeneralHealth"),
        "AgeCategory": r.get("AgeCategory"),

        # ---- Body ----
        "HeightInMeters": float(r.get("HeightInMeters")) if pd.notna(r.get("HeightInMeters")) else None,
        "WeightInKilograms": float(r.get("WeightInKilograms")) if pd.notna(r.get("WeightInKilograms")) else None,
        "BMI": float(r.get("BMI")) if pd.notna(r.get("BMI")) else None,

        # ---- Medical history ----
        "HadHeartAttack": r.get("HadHeartAttack"),
        "HadAngina": r.get("HadAngina"),
        "HadStroke": r.get("HadStroke"),
        "HadAsthma": r.get("HadAsthma"),
        "HadCOPD": r.get("HadCOPD"),
        "HadDepressiveDisorder": r.get("HadDepressiveDisorder"),
        "HadKidneyDisease": r.get("HadKidneyDisease"),
        "HadArthritis": r.get("HadArthritis"),
        "HadDiabetes": r.get("HadDiabetes"),

        # ---- Functional & behavior ----
        "DifficultyWalking": r.get("DifficultyWalking"),
        "SmokerStatus": r.get("SmokerStatus"),
        "AlcoholDrinkers": r.get("AlcoholDrinkers"),

        # ---- Streaming metadata ----
        "sent_at": time.time(),
        "is_end": False
    }
    producer.send(TOPIC, msg)
    print("📤 Sent:", msg)
    time.sleep(0.01)  # giả lập streaming

# 🔴 END SIGNAL
producer.send(TOPIC, {
    "PatientID": "END",
    "is_end": True,
    "sent_at": time.time()
})

producer.flush()

print(f"✅ Sent {len(df)} records in {time.time() - start_time:.2f} seconds")
