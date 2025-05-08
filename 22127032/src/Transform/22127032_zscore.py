from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

APP_NAME = "TransformZscore"
CHECKPOINT_BASE_DIR = "/tmp/spark_checkpoints_zscore"
LATE_DATA_TOLERANCE = "10 seconds"

MAVEN_REPO_URL = "https://maven-central-eu.storage-download.googleapis.com/maven2/"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC_PRICE = "btc-price"
INPUT_TOPIC_MOVING = "btc-price-moving"
OUTPUT_TOPIC = "btc-price-zscore"

# Initialize Spark Session
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.jars.repositories", MAVEN_REPO_URL)
    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
    .config("spark.cores.max", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# Define schemas for parsing Kafka messages
price_schema = T.StructType([
    T.StructField("symbol", T.StringType(), False),
    T.StructField("price", T.StringType(), False),
    T.StructField("timestamp", T.TimestampType(), False),
])

moving_schema = T.StructType([
    T.StructField("timestamp", T.TimestampType(), False),
    T.StructField("symbol", T.StringType(), False),
    T.StructField("stats", T.ArrayType(T.StructType([
        T.StructField("window", T.StringType(), False),
        T.StructField("avg_price", T.DoubleType(), False),
        T.StructField("std_price", T.DoubleType(), False),
    ]))),
])

# Read from btc-price Kafka topic
price_kafka_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC_PRICE)
    .option("startingOffsets", "latest") 
    .load()
)

# Read from btc-price-moving Kafka topic
moving_kafka_raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC_MOVING)
    .option("startingOffsets", "latest") 
    .option("failOnDataLoss", "false")
    .load()
)

# Parse and prepare the price stream
price_parsed = (
    price_kafka_raw.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), price_schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
    .withWatermark("timestamp", LATE_DATA_TOLERANCE)
    .groupBy("symbol", "timestamp")
    .agg(F.avg("price").alias("price"))
)

# Parse and prepare the moving statistics stream
moving_parsed = (
    moving_kafka_raw.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), moving_schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", F.date_trunc("second", F.col("timestamp")))
    .withWatermark("timestamp", LATE_DATA_TOLERANCE)
)

# Perform the stream-stream join
joined = price_parsed.join(
    moving_parsed,
    on=["timestamp", "symbol"],
)

# Explode the stats array and compute Z-score
exploded_stats = joined.select(
    F.col("timestamp"),
    F.col("symbol"),
    F.col("price"),
    F.explode(F.col("stats")).alias("stat")
)

z_score_calculated = exploded_stats.select(
    F.col("timestamp"),
    F.col("symbol"),
    F.col("stat.window").alias("window"),
    F.when(F.col("stat.std_price") != 0,
           (F.col("price") - F.col("stat.avg_price")) / F.col("stat.std_price"))
    .otherwise(None) 
    .alias("zscore_price")
)

# Aggregate Z-scores back into a list for each timestamp and symbol
result_aggregated = (
    z_score_calculated
    .groupBy("timestamp", "symbol")
    .agg(F.collect_list(F.struct("window", "zscore_price")).alias("z_scores"))
)

# Prepare output for Kafka (serializing to JSON)
output_to_kafka = result_aggregated.select(
    F.to_json(F.struct(F.col("timestamp"), F.col("symbol"), F.col("z_scores"))).alias("value")
)

# --- Streaming Queries ---

# Query to write results to the output Kafka topic
main_kafka_query = (
    output_to_kafka.writeStream.format("kafka")
    .outputMode("append") 
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_kafka_output_main") 
    .start()
)

# Query to print the joined stream to the console for debugging
console_query_joined = (
    joined.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False) 
    .option("numRows", 10)     
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_console_joined")
    .start()
)

print(f"Spark application '{APP_NAME}' started.")
print(f"Reading from Kafka topics: '{INPUT_TOPIC_PRICE}', '{INPUT_TOPIC_MOVING}' at {KAFKA_BROKER}")
print(f"Writing Z-scores to Kafka topic: '{OUTPUT_TOPIC}' at {KAFKA_BROKER}")
print(f"Global default checkpoint base: {CHECKPOINT_BASE_DIR}/{APP_NAME}_global_default")
print(f"Main Kafka query checkpoint: {CHECKPOINT_BASE_DIR}/{APP_NAME}_kafka_output_main")
print(f"Console query (joined) checkpoint: {CHECKPOINT_BASE_DIR}/{APP_NAME}_console_joined")
print("Console output for 'joined' stream is active for debugging.")

# Await termination of the queries
try:
    main_kafka_query.awaitTermination() 
except KeyboardInterrupt:
    print("KeyboardInterrupt received. Stopping queries...")
except Exception as e:
    print(f"An exception occurred: {e}")
finally:
    print("Stopping all active streaming queries.")
    if 'main_kafka_query' in locals() and main_kafka_query.isActive:
        try:
            main_kafka_query.stop()
            print("Main Kafka query stopped.")
        except Exception as e:
            print(f"Error stopping main_kafka_query: {e}")
            
    if 'console_query_joined' in locals() and console_query_joined.isActive:
        try:
            console_query_joined.stop()
            print("Console (joined) query stopped.")
        except Exception as e:
            print(f"Error stopping console_query_joined: {e}")
            
    # Add similar blocks for console_query_price and console_query_moving if they were active

    spark.stop()
    print("Spark session stopped.")
