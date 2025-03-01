from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local[*]").appName("CarsGenerator").getOrCreate()

car_models = spark.read.parquet("s3a://spark/data/dims/car_models")
car_colors = spark.read.parquet("s3a://spark/data/dims/car_colors")

cars_df = (
    spark.range(1,21)
    .withColumn("car_id",(F.lit(1000001) + F.col("id")).cast(IntegerType()))
    .withColumn("driver_id",F.floor(F.rand() * 900000000 + 100000000).cast(IntegerType()))
    .withColumn("model_id",F.floor(F.rand()* 7 + 1).cast(IntegerType()))
    .withColumn("color_id",F.floor(F.rand() * 7 + 1).cast(IntegerType()))
)

cars_df.write.parquet("s3a://spark/data/dims/cars", mode="overwrite")

cars_df.show()

spark.stop()


