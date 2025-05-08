from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json, col, window, avg, stddev, struct, to_json,
    lit, collect_list, first, min
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

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
    .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_main") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.streaming.statefulOperator.allowMultiple", "true") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .getOrCreate()

print(f"\n--- ACTUAL SPARK VERSION DETECTED: {spark.version} ---\n")
spark.sparkContext.setLogLevel("ERROR")

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
    .load()
print("Kafka source stream created.")

print("Parsing JSON and casting timestamp...")
df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", col("timestamp").cast(TimestampType()))
print("Data parsed and event_time column created.")

df_with_watermark = df_parsed \
    .withWatermark("event_time", LATE_DATA_TOLERANCE)
print(f"Watermark defined on 'event_time' with tolerance: {LATE_DATA_TOLERANCE}")

windows = ["30 seconds", "1 minute", "5 minutes", "15 minutes", "30 minutes", "1 hour"]
window_strings = ["30s", "1m", "5m", "15m", "30m", "1h"]
# windows = ["5 seconds", "10 seconds", "15 seconds", "20 seconds", "25 seconds", "30 seconds"]
# window_strings = ["5s", "10s", "15s", "20s", "25s", "30s"] 
print(f"Defined windows: {window_strings}")

print("Calculating aggregates for each window...")
stat_df_dict = {}
for w_duration, w_string in zip(windows, window_strings):
    print(f"Defining aggregation for window: {w_string} ({w_duration})")
    window_df = df_with_watermark \
        .groupBy(col("symbol"), window(col("event_time"), w_duration).alias("window")) \
        .agg(
            avg("price").alias("avg_price"),
            stddev("price").alias("std_price")
        ) \
        .select(
            col("window").alias("window"),
            col("window.end").alias("we"),
            col("window.start").alias("ws"),
            col("symbol"),
            struct(
                lit(w_string).alias("window"),
                col("avg_price"),
                col("std_price")
            ).alias("stats_struct"),
        )
    stat_df_dict[w_string] = window_df
print("Individual window aggregations defined and stored in a dictionary.")

if not stat_df_dict:
    print("Error: No window statistics DataFrames were generated. Exiting.")
    spark.stop()
    exit()

print("Unioning individual window statistics DataFrames from the dictionary...")
stream = stat_df_dict[window_strings[0]]
for w_string in window_strings[1:]:
    print(f"Unioning DataFrame for window: {w_string}")
    stream = stream.unionByName(stat_df_dict[w_string])
print("Union complete.")

print("Performing final aggregation and formatting for output...")
result_df = stream \
    .groupBy(col("we"), col("symbol"), window(stream.window, windows[0])) \
    .agg(collect_list("stats_struct").alias("stats")) \
    .select(
        col("we").alias("timestamp"),
        col("symbol"),
        col("stats"),
    ) \
    .select(to_json(struct("*")).alias("value")
    )
print("Final result DataFrame schema defined.")

print(f"Starting query to write aggregated output to Kafka topic: {OUTPUT_TOPIC}...")
query = result_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_aggregated_output") \
    .start()

print(f"Query '{query.name if query.name else query.id}' started. Writing to topic: {OUTPUT_TOPIC}")
print("Streaming query will run until manually stopped or an error occurs.")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("KeyboardInterrupt detected. Stopping query...")
except Exception as e:
    print(f"An error occurred during query execution: {e}")
finally:
    print("Stopping SparkSession...")
    query.stop() 
    spark.stop()
    print("SparkSession stopped. Script finished.")

