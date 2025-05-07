from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


APP_NAME = "TransformZscore"
CHECKPOINT_BASE_DIR = "/tmp/checkpoint_dir"
LATE_DATA_TOLERANCE = "10 seconds" 

MAVEN_REPO_URL = "https://maven-central-eu.storage-download.googleapis.com/maven2/"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "btc-price", "btc-price-moving"
OUTPUT_TOPIC = "btc-price-zscore"


spark = (
    SparkSession.builder.appName(APP_NAME)
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.jars.repositories", MAVEN_REPO_URL)
    .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


price_schema = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("timestamp", T.StringType(), True),
])
moving_schema = T.StructType([
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("symbol", T.StringType(), True),
    T.StructField("stats", T.ArrayType(T.StructType([
        T.StructField("window", T.StringType(), True),
        T.StructField("avg_price", T.DoubleType(), True),
        T.StructField("std_price", T.DoubleType(), True),
    ])), True),
])


price_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC[0])
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)
moving_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC[1])
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)


price_parsed = (
    price_kafka.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), price_schema).alias("data"))
    .select("data.*")
)
moving_parsed = (
    moving_kafka.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), moving_schema).alias("data"))
    .select("data.*")
)


joined = price_parsed.join(moving_parsed, on=["timestamp", "symbol"])
exploded = joined.select("timestamp", "symbol", "price", F.explode("stats").alias("stat"))
z_score = exploded.select(
    F.col("timestamp"),
    F.col("symbol"),
    F.col("stat.window"),
    F.expr("(price - stat.avg_price) / stat.std_price").alias("zscore_price"),
)
result = (
    z_score
    .withColumn("timestamp", F.col("timestamp").cast(T.TimestampType()))
    .withWatermark("timestamp", LATE_DATA_TOLERANCE)
    .groupBy("timestamp", "symbol")
    .agg(F.collect_list(F.struct("window", "zscore_price")).alias("z_scores"))
)
output = result.select(F.to_json(F.struct("*")).alias("value"))


main_query = (
    output.writeStream
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}/zscore")
    .start()
)
console_query = (
    output.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

try:
    main_query.awaitTermination()
    console_query.awaitTermination()
except KeyboardInterrupt:
    main_query.stop()
    console_query.stop()
    spark.stop()
    print("Query stopped")


# docker exec spark-client /opt/bitnami/spark/bin/spark-submit 
# --master spark://spark-master:7077 
# --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 
# --repositories https://maven-central-eu.storage-download.googleapis.com/maven2/
# /opt/bitnami/spark/work/src/load.py