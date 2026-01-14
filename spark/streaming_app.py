# streaming_app.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, when, concat_ws, avg
import time
import os
import atexit

# =========================
# SPARK SESSION
# =========================
spark = SparkSession.builder \
    .appName("Health-Streaming-Performance") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# =========================
# GLOBAL METRICS
# =========================
metrics_global = {
    "total_records": 0,
    "total_batches": 0,
    "start_time": time.time()
}

# =========================
# SHUTDOWN HOOK (FINAL SUMMARY)
# =========================
def print_final_summary():
    elapsed = time.time() - metrics_global["start_time"]
    throughput = (
        metrics_global["total_records"] / elapsed
        if elapsed > 0 else 0
    )

    print("""
====================================
FINAL STREAMING SUMMARY
====================================
TOTAL RECORDS PROCESSED : {}
TOTAL BATCHES           : {}
TOTAL TIME (s)          : {:.2f}
AVG THROUGHPUT (r/s)    : {:.2f}
====================================
""".format(
        metrics_global["total_records"],
        metrics_global["total_batches"],
        elapsed,
        throughput
    ))

atexit.register(print_final_summary)

# =========================
# SCHEMA
# =========================
schema = """
PatientID STRING,
HeartRate DOUBLE,
RespiratoryRate DOUBLE,
BodyTemperature DOUBLE,
OxygenSaturation DOUBLE,
BloodPressureSys DOUBLE,
BloodPressureDia DOUBLE,
Age INT,
Gender STRING,
sent_at DOUBLE,
is_end BOOLEAN
"""

# =========================
# KAFKA SOURCE
# =========================
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "health-topic") \
    .option("startingOffsets", "earliest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("d")) \
    .select("d.*")

# =========================
# LATENCY
# =========================
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
        when((col("HeartRate") < 40) | (col("HeartRate") > 120),
             "⚠ Abnormal heart rate"),
        when((col("BloodPressureSys") > 160) | (col("BloodPressureDia") > 100),
             "⚠ High blood pressure"),
        when(col("OxygenSaturation") < 90,
             "⚠ Low SpO₂"),
        when((col("BodyTemperature") >= 38) | (col("BodyTemperature") < 35),
             "⚠ Abnormal temperature"),
        when((col("RespiratoryRate") < 8) | (col("RespiratoryRate") > 30),
             "⚠ Abnormal respiratory rate")
    )
).withColumn(
    "is_risk",
    col("risk_reason") != ""
)

# =========================
# FOREACH BATCH
# =========================
should_stop = {"value": False}

def foreach_batch(batch_df, batch_id):
    batch_count = batch_df.count()
    if batch_count == 0:
        return

    metrics_global["total_records"] += batch_count
    metrics_global["total_batches"] += 1

    elapsed = time.time() - metrics_global["start_time"]
    throughput = metrics_global["total_records"] / elapsed

    avg_latency = batch_df.agg(avg("latency_ms")).collect()[0][0]

    # Executors / Workers
    executors = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    records_per_worker = batch_count / max(executors, 1)

    print(f"""
==============================
BATCH ID            : {batch_id}
Records in batch    : {batch_count}
Total records       : {metrics_global["total_records"]}
Elapsed time (s)    : {elapsed:.2f}

Executors (workers) : {executors}
Records / worker    : {records_per_worker:.2f}

Avg latency (ms)    : {avg_latency:.2f}
Throughput (r/s)    : {throughput:.2f}
==============================
""")

    # 🚨 ALERT
    risk_rows = batch_df.filter(col("is_risk") == True)
    if risk_rows.count() > 0:
        print("🚨 HIGH RISK PATIENTS")
        risk_rows.select(
            "PatientID",
            "HeartRate",
            "BloodPressureSys",
            "BloodPressureDia",
            "OxygenSaturation",
            "RespiratoryRate",
            "BodyTemperature",
            "risk_reason"
        ).show(truncate=False)

    # 🛑 STOP SIGNAL
    if batch_df.filter(col("is_end") == True).count() > 0:
        print("🛑 END signal received. Stopping Spark.")
        should_stop["value"] = True

# =========================
# START STREAM
# =========================
query = risk_df.writeStream \
    .foreachBatch(foreach_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/health") \
    .start()

# =========================
# DRIVER LOOP
# =========================
while query.isActive:
    if should_stop["value"]:
        print("✅ Stopping streaming query cleanly")
        query.stop()
        break
    time.sleep(1)

query.awaitTermination()
