from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, struct, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("AlertDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

input_topic = 'samples-enriched'
output_topic = 'alert-data'

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
    .option("startingOffsets", "latest")\
    .option("kafka.bootstrap.servers", 'kafka:9092')\
    .option("subscribe", input_topic)\
    .option("failOnDataLoss", "false")\
    .load()

kafka_df = kafka_df\
    .select(from_json(col("value").cast("string"), input_schema).alias("value")).select("value.*")

filtered_kafka_df = kafka_df.filter(
    (col("speed")>120) &
    (col("expected_gear") != col("gear")) &
    (col("rpm")>6000)
)

output_df = filtered_kafka_df\
    .select(to_json(struct("*")).alias("value"))

write_to_kafka = output_df\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:9092")\
    .option("topic", output_topic)\
    .option("checkpointLocation", "/tmp/checkpoint")\
    .trigger(processingTime="1 second")\
    .start()

write_to_kafka.awaitTermination()
#spart.stop()