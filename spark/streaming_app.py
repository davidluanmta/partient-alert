# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, from_json, lit
# import os
# import time

# MODE = os.getenv("MODE", "local")

# spark = SparkSession.builder.appName(f"Health-Benchmark-{MODE}").getOrCreate()
# spark.sparkContext.setLogLevel("ERROR")

# schema = """
# PatientID STRING,
# Sex STRING,
# AgeCategory STRING,
# BMI DOUBLE,
# HadHeartAttack STRING,
# HadAngina STRING,
# HadStroke STRING,
# sent_at DOUBLE
# """

# df = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "health-topic") \
#     .load()

# json_df = df.selectExpr("CAST(value AS STRING)") \
#     .select(from_json(col("value"), schema).alias("d")) \
#     .select("d.*")

# # Tính latency
# metrics = json_df.withColumn("latency_ms", (lit(time.time()) - col("sent_at"))*1000)

# # Logic cảnh báo
# alerts = metrics.filter(
#     (col("BMI") >= 30) &
#     ((col("HadHeartAttack")=="Yes") | 
#      (col("HadAngina")=="Yes") | 
#      (col("HadStroke")=="Yes"))
# )

# def foreach_batch(batch_df, batch_id):
#     if batch_df.count() == 0:
#         return

#     start = time.time()
#     stats = batch_df.agg(
#         {"latency_ms":"avg"}).collect()[0]
#     duration = time.time() - start
#     throughput = batch_df.count() / duration if duration > 0 else 0

#     out = spark.createDataFrame(
#         [(MODE, stats["avg(latency_ms)"], throughput)],
#         ["mode","avg_latency_ms","throughput"]
#     )
#     out.coalesce(1).write.mode("append").option("header",True).csv("/opt/bitnami/spark/app/benchmark")

# # alerts.writeStream.foreachBatch(foreach_batch).start().awaitTermination()
# query_start = time.time()
# query = alerts.writeStream \
#     .foreachBatch(foreach_batch) \
#     .trigger(once=True) \
#     .option("checkpointLocation", "/tmp/checkpoints/health") \
#     .start()
# query.awaitTermination()
# query_end = time.time()
# print(f"TOTAL RUN TIME: {query_end - query_start:.2f} seconds")


# streaming_app.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, concat_ws
import time
import os

MODE = os.getenv("MODE", "local")

spark = SparkSession.builder \
    .appName(f"Health-Benchmark-{MODE}") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = """
PatientID STRING,
State STRING,
Sex STRING,
GeneralHealth STRING,
AgeCategory STRING,
HeightInMeters DOUBLE,
WeightInKilograms DOUBLE,
BMI DOUBLE,
HadHeartAttack STRING,
HadAngina STRING,
HadStroke STRING,
HadAsthma STRING,
HadCOPD STRING,
HadDepressiveDisorder STRING,
HadKidneyDisease STRING,
HadArthritis STRING,
HadDiabetes STRING,
DifficultyWalking STRING,
SmokerStatus STRING,
AlcoholDrinkers STRING,
sent_at DOUBLE,
is_end BOOLEAN
"""

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "health-topic") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("d")) \
    .select("d.*")

metrics = json_df.withColumn(
    "latency_ms",
    (lit(time.time()) - col("sent_at")) * 1000
)
# =========================
# RISK RULES
# =========================
risk_df = metrics.withColumn(
    "risk_reason",
    concat_ws(
        "; ",
        when(
            (col("BMI") >= 30) &
            ((col("HadHeartAttack") == "Yes") |
             (col("HadAngina") == "Yes") |
             (col("HadStroke") == "Yes")),
            "⚠ Obesity + Cardiovascular disease"
        ),
        when(
            (col("BMI") >= 30) & (col("HadDiabetes") == "Yes"),
            "⚠ Obesity + Diabetes"
        ),
        when(
            (col("SmokerStatus") == "Current smoker") &
            ((col("HadCOPD") == "Yes") | (col("HadAsthma") == "Yes")),
            "⚠ Smoker + Respiratory disease"
        ),
        when(
            col("DifficultyWalking") == "Yes",
            "⚠ Mobility limitation"
        ),
        when(
            col("GeneralHealth") == "Poor",
            "⚠ Poor general health"
        )
    )
).withColumn(
    "is_risk",
    col("risk_reason") != ""
)
# =========================
# STREAM CONTROL
# =========================
query_start = time.time()
should_stop = {"value": False}
def foreach_batch(batch_df, batch_id):
    print(f"🔄 Processing batch {batch_id}")
    if batch_df.count() == 0:
        return

    # 🛑 END SIGNAL
    if batch_df.filter(col("is_end") == True).count() > 0:
        print("🛑 END signal received. Stopping Spark.")
        should_stop["value"] = True
        return
    # 🚨 RISK ALERTS
    risk_rows = batch_df.filter(col("is_risk") == True)

    if risk_rows.count() > 0:
        print("🚨🚨🚨 HIGH RISK PATIENTS 🚨🚨🚨")
        risk_rows.select(
            "PatientID",
            "AgeCategory",
            "BMI",
            "SmokerStatus",
            "GeneralHealth",
            "risk_reason"
        ).show(truncate=False)
    # 📊 BENCHMARK
    start = time.time()
    latency = batch_df.agg({"latency_ms": "avg"}).collect()[0][0]
    duration = time.time() - start
    throughput = batch_df.count() / duration if duration > 0 else 0

    result = spark.createDataFrame(
        [(MODE, latency, throughput)],
        ["mode", "avg_latency_ms", "throughput"]
    )

    result.write.mode("append").option("header", True) \
        .csv("/opt/spark-apps/benchmark")

query = risk_df.writeStream \
    .foreachBatch(foreach_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/health") \
    .start()
# 👇 Driver loop kiểm soát lifecycle
while query.isActive:
    if should_stop["value"]:
        print("✅ Stopping streaming query cleanly")
        query.stop()
        break
    time.sleep(1)

query.awaitTermination()

query_end = time.time()
print(f"⏱ TOTAL RUN TIME: {query_end - query_start:.2f} seconds")
spark.stop()