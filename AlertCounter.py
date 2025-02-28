from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum, max, when
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AlertCounter") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

input_topics = "alert-data"

input_schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("event_time", StringType(), False),
    StructField("car_id", IntegerType(), False),
    StructField("driver_id", IntegerType(), False),
    StructField("speed", IntegerType(), False),
    StructField("rpm", IntegerType(), False),
    StructField("gear", IntegerType(), False),
    StructField("brand_name", StringType(), False),
    StructField("model_name", StringType(), False),
    StructField("color_name", StringType(), False),
    StructField("expected_gear", IntegerType(), False)
])

kafka_df = spark\
    .readStream\
    .format("kafka")\
    .option("startingOffsets", "earliest")\
    .option("kafka.bootstrap.servers", 'kafka:9092')\
    .option("subscribe", input_topics)\
    .option("failOnDataLoss", "false")\
    .load()

kafka_df = kafka_df \
    .select(from_json(col("value").cast("string"), input_schema).alias("data"))\
    .select("data.*") \
    .withColumn("event_time", col("event_time").cast("timestamp"))


agg_kafka_df = kafka_df\
    .withWatermark("event_time", "15 minutes")\
    .groupBy(window("event_time", "15 minutes"))\
    .agg(
        count("*").alias("num_of_rows"),
        sum(when(col("color_name") == 'Black', 1).otherwise(0)).alias("num_of_black"),
        sum(when(col("color_name") == 'White', 1).otherwise(0)).alias("num_of_white"),
        sum(when(col("color_name") == 'Silver', 1).otherwise(0)).alias("num_of_silver"),
        max("speed").alias("maximum_speed"),
        max("gear").alias("maximum_gear"),
        max("rpm").alias("maximum_rpm")
    )

console_sink = agg_kafka_df\
    .writeStream\
    .format("console")\
    .outputMode("complete")\
    .trigger(processingTime="1 minute")\
    .option("truncate", "false")\
    .start()

console_sink.awaitTermination()
# spark.stop()