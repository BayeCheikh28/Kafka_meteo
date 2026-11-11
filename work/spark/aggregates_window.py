from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, avg, min, max, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
INPUT_TOPIC = "weather_transformed"

spark = SparkSession.builder.appName("weather-aggregates-window").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

message_schema = StructType([
    StructField("event_time", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("windspeed", DoubleType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("weather_code", StringType()),
    StructField("wind_alert_level", StringType()),
    StructField("heat_alert_level", StringType()),
])

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = raw_stream.select(
    from_json(col("value").cast("string"), message_schema).alias("data")
).select("data.*")

with_watermark = parsed.withWatermark("event_time", "1 minute")

aggregated = (
    with_watermark
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("country"),
        col("city"),
    )
    .agg(
        count(when(col("wind_alert_level") == "level_1", 1)).alias("wind_alert_level_1_count"),
        count(when(col("wind_alert_level") == "level_2", 1)).alias("wind_alert_level_2_count"),
        count(when(col("heat_alert_level") == "level_1", 1)).alias("heat_alert_level_1_count"),
        count(when(col("heat_alert_level") == "level_2", 1)).alias("heat_alert_level_2_count"),
        count("*").alias("total_alerts"),
        avg("temperature").alias("avg_temperature"),
        min("temperature").alias("min_temperature"),
        max("temperature").alias("max_temperature"),
    )
)

query = (
    aggregated.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
