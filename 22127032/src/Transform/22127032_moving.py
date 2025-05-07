from pyspark.sql import SparkSession
from pyspark.sql.functions import (from_json, col, window, avg, stddev, struct, to_json, array, lit)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import time

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "btc-price"
OUTPUT_TOPIC = "btc-price-moving"
APP_NAME = "MovingStatistics"
LATE_DATA_TOLERANCE = "10 seconds"
CHECKPOINT_BASE_DIR = "/tmp/checkpoint_dir"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

print("Initializing SparkSession...")
spark = SparkSession.builder \
    .appName(APP_NAME) \
    .master("local[*]") \
    .config("spark.jars.packages", KAFKA_PACKAGE) \
    .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}") \
    .getOrCreate()

print(f"\n--- ACTUAL SPARK VERSION DETECTED: {spark.version} ---\n")
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])
print("Input schema defined.")

print(f"Reading from Kafka topic: {INPUT_TOPIC}...")
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", INPUT_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()
print("Kafka source stream created.")

print("Parsing JSON and casting timestamp...")
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
print("Data parsed.")

df_with_watermark = df_parsed \
    .withWatermark("event_time", LATE_DATA_TOLERANCE)

windows = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]
window_strings = ["30s", "1m", "5m", "15m", "30m", "1h"]
print(f"Defined windows: {window_strings}")

print("Calculating aggregates for each window...")
queries = []

for w_duration, w_string in zip(windows, window_strings):
    print(f"Defining aggregation for window: {w_string} ({w_duration})")
    window_df = df_with_watermark \
        .groupBy(col("symbol"), window(col("event_time"), w_duration)) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        ) \
        .select(
            col("symbol"),
            col("window.end").alias("timestamp"),
            lit(w_string).alias("window"),
            col("avg_price"),
            col("std_price")
        )
    
    output_df = window_df.select(
        to_json(struct(
            col("timestamp"),
            col("symbol"),
            array(struct(col("window"), col("avg_price"), col("std_price"))).alias("stats")
        )).alias("value")
    )
    
    query = output_df.writeStream \
        .format("kafka") \
        .outputMode("append") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("topic", OUTPUT_TOPIC) \
        .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_{w_string}") \
        .start()
    queries.append(query)
    print(f"Query started for {w_string} window -> topic: {OUTPUT_TOPIC}")

print("\nCreating debug console output for first window (30s)...")
debug_df = df_with_watermark \
    .groupBy(col("symbol"), window(col("event_time"), "30 seconds")) \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("symbol"),
        col("window.end").alias("timestamp"),
        lit("30s").alias("window"),
        col("avg_price"),
        col("std_price")
    )

console_query = debug_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()
queries.append(console_query)

try:
    time.sleep(10)
    for query in queries:
        query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping queries...")
    for query in queries:
        query.stop()
    print("Queries stopped.")

print("Script finished.")