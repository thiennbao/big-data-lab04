from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


APP_NAME = "Load"
CHECKPOINT_BASE_DIR = "/tmp/checkpoint_dir"

MAVEN_REPO_URL = "https://maven-central-eu.storage-download.googleapis.com/maven2/"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
MONGO_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.1"

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "btc-price-zscore"
MONGO_URI = "mongodb://mongo:27017"
MONGO_DATABASE = "btc"


spark = (
    SparkSession.builder.appName(APP_NAME)
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", f"{KAFKA_PACKAGE},{MONGO_PACKAGE}")
    .config("spark.jars.repositories", MAVEN_REPO_URL)
    .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}")
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
    return (
        output.filter(F.col("window") == window_value).writeStream
        .format("mongodb")
        .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}/{window_value}")
        .option("spark.mongodb.connection.uri", MONGO_URI)
        .option("spark.mongodb.database", MONGO_DATABASE)
        .option("spark.mongodb.collection", f"btc-price-zscore-{window_value}")
        .outputMode("append")
        .start()
    )
console_query = (
    output.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

windows = ["30s", "1m", "5m", "15m", "30m", "1h"]
queries = [create_mongo_query(w) for w in windows] + [console_query]


try:
    for query in queries:
        query.awaitTermination()
except KeyboardInterrupt:
    for query in queries:
        query.stop()
    spark.stop()
    print("Queries stopped")

