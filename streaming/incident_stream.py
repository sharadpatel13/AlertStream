import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# ---------------------------
# Environment
# ---------------------------
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["SPARK_LOCAL_DIRS"] = r"C:/Users/SHARAD/spark_temp"

# Make sure output and checkpoint directories exist
os.makedirs("C:/Users/SHARAD/output", exist_ok=True)
os.makedirs("C:/Users/SHARAD/checkpoint_new", exist_ok=True)

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
    .trigger(processingTime="10 seconds") \
    .start()

# ---------------------------
# Write to CSV
# ---------------------------
csv_query = aggregated_flat.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "C:/Users/SHARAD/output") \
    .option("checkpointLocation", "C:/Users/SHARAD/checkpoint_new") \
    .option("header", True) \
    .trigger(processingTime="10 seconds") \
    .start()

# ---------------------------
# Await termination safely
# ---------------------------
try:
    csv_query.awaitTermination()  # runs continuously until stopped
except KeyboardInterrupt:
    print("Stopping streaming...")
finally:
    console_query.stop()
    csv_query.stop()
    print("Streaming stopped. CSV output is ready.")
