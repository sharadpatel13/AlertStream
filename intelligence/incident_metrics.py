import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("IncidentMetrics") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Input & Output Paths
# ---------------------------
INPUT_PATH = "C:/Users/SHARAD/output/*.csv"  # Absolute path
OUTPUT_PATH = "C:/Users/SHARAD/incident-iq/storage/processed/incident_metrics"
os.makedirs(OUTPUT_PATH, exist_ok=True)

# ---------------------------
# Read Aggregated Incident Data
# ---------------------------
df = spark.read.option("header", True).csv(INPUT_PATH)

# Cast numeric columns
df = df.select(
    col("system"),
    col("high_severity_count").cast("int"),
    col("window_start"),
    col("window_end")
)

print("\n=== Raw Aggregated Data ===")
df.show(truncate=False)

# ---------------------------
# Metric 1: Total High Severity Incidents per System
# ---------------------------
total_per_system = df.groupBy("system") \
    .sum("high_severity_count") \
    .withColumnRenamed("sum(high_severity_count)", "total_high_severity")

print("\n=== Total High Severity Incidents ===")
total_per_system.show(truncate=False)

# ---------------------------
# Metric 2: Average Incidents per Window
# ---------------------------
avg_per_system = df.groupBy("system") \
    .agg(avg("high_severity_count").alias("avg_per_window"))

print("\n=== Average Incidents Per Window ===")
avg_per_system.show(truncate=False)

# ---------------------------
# Metric 3: Peak Incident Window
# ---------------------------
peak_window = df.orderBy(col("high_severity_count").desc()).limit(1)

print("\n=== Peak Incident Window ===")
peak_window.show(truncate=False)

# ---------------------------
# Simple Spike Detection Rule (count >= 3)
# ---------------------------
spikes = df.filter(col("high_severity_count") >= 3).dropDuplicates()

if spikes.count() > 0:
    print("\n=== Detected Incident Spikes (count >= 3) ===")
    spikes.show(truncate=False)
else:
    print("\nNo spikes detected.")

# ---------------------------
# Save Final Metrics
# ---------------------------
final_df = df.join(total_per_system, on="system", how="left") \
             .join(avg_per_system, on="system", how="left")

final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", True) \
    .csv(OUTPUT_PATH)

print(f"\nMetrics saved to: {OUTPUT_PATH}")

# ---------------------------
# Stop Spark
# ---------------------------
spark.stop()
