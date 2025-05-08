from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T


APP_NAME = "Bonus"
CHECKPOINT_BASE_DIR = "/tmp/checkpoint_dir"
LATE_DATA_TOLERANCE = "10 seconds" 

MAVEN_REPO_URL = "https://maven-central-eu.storage-download.googleapis.com/maven2/"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"

KAFKA_BROKER = "kafka:9093"
INPUT_TOPIC = "btc-price"
OUTPUT_TOPIC = "btc-price-higher", "btc-price-lower"


spark = (
    SparkSession.builder.appName(APP_NAME)
    .master("local[*]")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.jars.repositories", MAVEN_REPO_URL)
    .config("spark.sql.streaming.checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


price_schema = T.StructType([
    T.StructField("symbol", T.StringType(), True),
    T.StructField("price", T.DoubleType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
])


price_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)
price_parsed = (
    price_kafka.selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), price_schema).alias("data"))
    .select("data.*")
)


state = []
def process_batch(batch, query_function):
    global state
    records = batch.collect()
    records = sorted(records, key=lambda r: r['timestamp'])
    results = []
    for symbol in set(r['symbol'] for r in records):
        current = [r for r in records if r['symbol'] == symbol]
        combined = sorted(state + current, key=lambda r: r['timestamp'])
        for i, base in enumerate(combined):
            found_higher = False
            found_lower = False
            for future in combined[i+1:]:
                dt = (future.timestamp - base.timestamp).total_seconds()
                if dt > 20:
                    break
                if not found_higher and future.price > base.price:
                    results.append(Row(timestamp=base.timestamp, higher_window=round(dt, 3), lower_window=None))
                    found_higher = True
                if not found_lower and future.price < base.price:
                    results.append(Row(timestamp=base.timestamp, higher_window=None, lower_window=round(dt, 3)))
                    found_lower = True
                if found_higher and found_lower:
                    break
            if not found_higher:
                results.append(Row(timestamp=base.timestamp, higher_window=20.0, lower_window=None))
            if not found_lower:
                results.append(Row(timestamp=base.timestamp, higher_window=None, lower_window=20.0))
        if current:
            latest = max(r['timestamp'] for r in current)
            state = [r for r in combined if (latest - r.timestamp).total_seconds() <= 20] # clear passed records
    if results:
        result_df = spark.createDataFrame(results)
        higher = result_df.filter("higher_window is not null").select(
            F.to_json(F.struct("timestamp", "higher_window")).alias("value")
        )
        lower = result_df.filter("lower_window is not null").select(
            F.to_json(F.struct("timestamp", "lower_window")).alias("value")
        )
        query_function(higher, lower)


def create_main_query(higher_df, lower_df):
    for i, df in enumerate([higher_df, lower_df]):
        (
            df.write.format("kafka")
            .mode("append")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("topic", OUTPUT_TOPIC[i])
            .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{APP_NAME}/zscore")
            .save()
        )
def create_console_query(higher_df, lower_df):
    higher_df.write.format("console").mode("append").save()
    lower_df.write.format("console").mode("append").save()


main_query = (
    price_parsed.withWatermark("timestamp", "10 seconds")
    .writeStream.foreachBatch(lambda batch, _: process_batch(batch, create_main_query))
    .outputMode("append")
    .start()
)
console_query = (
    price_parsed.withWatermark("timestamp", "10 seconds")
    .writeStream.foreachBatch(lambda batch, _: process_batch(batch, create_console_query))
    .outputMode("append")
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