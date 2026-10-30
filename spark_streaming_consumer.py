#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, from_unixtime, to_timestamp, window
from pyspark.sql.types import FloatType, IntegerType, LongType, StructField, StructType

TOPIC = "sensor_data"

spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType(), True),
    StructField("temperature", FloatType(), True),
    StructField("humidity", FloatType(), True),
    StructField("timestamp", LongType(), True)
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_df
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
)

windowed_stats = (
    parsed_df
    .groupBy(window(col("event_time"), "1 minute"), "sensor_id")
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    )
)

query = (
    windowed_stats.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
