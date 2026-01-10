import os
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------
# Environment
# ---------------------------
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["SPARK_LOCAL_DIRS"] = r"C:/Users/SHARAD/spark_temp"

# ---------------------------
# Helper: Reset checkpoints
# ---------------------------
def reset_checkpoint(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)

reset_checkpoint("C:/Users/SHARAD/checkpoint_csv")
reset_checkpoint("C:/Users/SHARAD/checkpoint_console")

# ---------------------------
# Make sure output folder exists
# ---------------------------
os.makedirs("C:/Users/SHARAD/output", exist_ok=True)

# ---------------------------
# Spark Session
# ---------------------------
spark = SparkSession.builder \
    .appName("IncidentStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7") \
    .config("spark.hadoop.home.dir", r"C:\hadoop") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Schema
# ---------------------------
schema = StructType([
    StructField("incident_id", StringType()),
    StructField("system", StringType()),
    StructField("component", StringType()),
    StructField("alert_type", StringType()),
    StructField("severity", StringType()),
    StructField("message", StringType()),
    StructField("timestamp", StringType())
])

# ---------------------------
# Kafka Stream
# ---------------------------
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "incident_alerts") \
    .option("startingOffsets", "earliest") \
    .load()

# ---------------------------
# Parse JSON & cast timestamp
# ---------------------------
parsed = kafka_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
                 .select("data.*") \
                 .withColumn(
                     "timestamp",
                     to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
                 )

# ---------------------------
# Filter high severity
# ---------------------------
high_severity = parsed.filter(col("severity") == "HIGH")

# ---------------------------
# Aggregate by system (10-second windows)
# ---------------------------
aggregated = high_severity \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("system")
    ).agg(count("incident_id").alias("high_severity_count"))

# Flatten window
aggregated_flat = aggregated.select(
    col("system"),
    col("high_severity_count"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end")
)

# ---------------------------
# Write to console
# ---------------------------
console_query = aggregated_flat.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", "C:/Users/SHARAD/checkpoint_console") \
    .trigger(processingTime="10 seconds") \
    .start()

# ---------------------------
# Write to CSV (timestamped folder)
# ---------------------------
output_path = f"C:/Users/SHARAD/output/run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
os.makedirs(output_path, exist_ok=True)

csv_query = aggregated_flat.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", "C:/Users/SHARAD/checkpoint_csv") \
    .option("header", True) \
    .trigger(processingTime="10 seconds") \
    .start()

# ---------------------------
# Await termination safely
# ---------------------------
try:
    csv_query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping streaming...")
finally:
    console_query.stop()
    csv_query.stop()
    print(f"Streaming stopped. CSV output is ready at: {output_path}")
