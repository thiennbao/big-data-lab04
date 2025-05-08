from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


APP_NAME = "Load"
CHECKPOINT_BASE_DIR = "/tmp/spark_checkpoints/checkpoint_dir" # Changed to be inside the volume mount

MAVEN_REPO_URL = "https://maven-central-eu.storage-download.googleapis.com/maven2/"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
MONGO_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1" # Check for latest compatible version

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "btc-price-zscore"
MONGO_URI = "mongodb://mongo:27017"
MONGO_DATABASE = "btc"


spark = (
    SparkSession.builder.appName(APP_NAME)
    .master("local[*]") # This is okay for running within spark-client
    .config("spark.jars.packages", f"{KAFKA_PACKAGE},{MONGO_PACKAGE}")
    .config("spark.jars.repositories", MAVEN_REPO_URL)
    # REMOVE global checkpoint location from here
    # .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_mongo")
    .config("spark.mongodb.output.uri", MONGO_URI)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

zscore_schema = T.StructType([
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("symbol", T.StringType(), True),
    T.StructField("z_scores", T.ArrayType(T.StructType([
        T.StructField("window", T.StringType(), True),
        T.StructField("zscore_price", T.DoubleType(), True),
    ])), True),
])


zscore_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


zscore_parsed = (
    zscore_kafka
    .selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), zscore_schema).alias("data"))
    .select("data.*")
)

output = zscore_parsed.withColumn("zscore", F.explode(F.col("z_scores"))).select(
    F.col("timestamp"),
    F.col("symbol"),
    F.col("zscore.window").alias("window"),
    F.col("zscore.zscore_price").alias("zscore_price")
)


def create_mongo_query(window_value):
    # Ensure checkpoint location is unique and within the mounted volume for persistence
    checkpoint_location = f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_mongo/{window_value}"
    print(f"MongoDB query for window {window_value} using checkpoint: {checkpoint_location}")
    return (
        output.filter(F.col("window") == window_value).writeStream
        .format("mongodb")
        .option("checkpointLocation", checkpoint_location)
        .option("spark.mongodb.connection.uri", MONGO_URI) # Redundant if set in session, but harmless
        .option("spark.mongodb.database", MONGO_DATABASE)
        .option("spark.mongodb.collection", f"btc-price-zscore-{window_value}")
        .outputMode("append")
        .start()
    )

# Provide a unique checkpoint location for the console query
console_checkpoint_location = f"{CHECKPOINT_BASE_DIR}/{APP_NAME}_console"
print(f"Console query using checkpoint: {console_checkpoint_location}")
console_query = (
    output.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", console_checkpoint_location) # ADDED THIS
    .start()
)

windows = ["30s", "1m", "5m", "15m", "30m", "1h"]
queries = [create_mongo_query(w) for w in windows] + [console_query]


try:
    for query in queries:
        query.awaitTermination()
except KeyboardInterrupt:
    print("KeyboardInterrupt received. Stopping queries...")
    for query in queries:
        print(f"Stopping query: {query.id}")
        query.stop()
    spark.stop()
    print("Queries and Spark session stopped.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    print("Stopping queries due to error...")
    for query in queries:
        try:
            query.stop()
        except Exception as stop_e:
            print(f"Error stopping query {query.id if hasattr(query, 'id') else 'unknown'}: {stop_e}")
    spark.stop()
    print("Spark session stopped due to error.")