from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, round, struct, to_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DataEnrichment") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

input_topic = 'sensors-sample'
output_topic = 'samples-enriched'
cars_df = spark.read.parquet("s3a://spark/data/dims/cars").select("car_id", "driver_id", "model_id", "color_id")
car_models_df = spark.read.parquet("s3a://spark/data/dims/car_models").select("model_id", "car_brand", "car_model")
car_colors_df = spark.read.parquet("s3a://spark/data/dims/car_colors").select("color_id", "color_name")

input_schema = StructType([
    StructField("event_id",IntegerType(),False),
    StructField("event_time",StringType(),False),
    StructField("car_id",IntegerType(),False),
    StructField("speed",IntegerType(),False),
    StructField("rpm",IntegerType(),False),
    StructField("gear",IntegerType(),False)
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
    .select(from_json(col("value").cast("string"),input_schema).alias("value")).select("value.*")

enriched_df = kafka_df\
    .join(cars_df,"car_id","left")\
    .join(car_models_df,"model_id","left")\
    .join(car_colors_df,"color_id","left")\
    .withColumn("expected_gear",round(col("speed")/30).cast("int"))\
    .select(
        col("event_id"),
        col("event_time"),
        col("car_id"),
        col("driver_id"),
        col("speed"),
        col("rpm"),
        col("gear"),
        col("car_brand").alias("brand_name"),
        col("car_model").alias("model_name"),
        col("color_name"),
        col("expected_gear")
    )

output_df = enriched_df\
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