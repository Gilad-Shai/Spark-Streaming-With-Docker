from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.master("local[*]").appName("DataGenerator").getOrCreate()

schema = StructType([
    StructField("model_id",IntegerType(),False),
    StructField("car_brand",StringType(),False),
    StructField("car_model",StringType(),False)
])

data = [
    (1,"Mazda","3"),
    (2,"Mazda","6"),
    (3,"Toyota","Corolla"),
    (4,"Hyundai","i20"),
    (5,"Kia","Sportage"),
    (6,"Kia","Rio"),
    (7,"Kia","Picanto")
]

car_model = spark.createDataFrame(data,schema)

car_model.write.parquet("s3a://spark/data/dims/car_models",mode='overwrite')


schema = StructType([
    StructField("color_id",IntegerType(),False),
    StructField("color_name",StringType(),False)
])

data = [
    (1,"Black"),
    (2,"Red"),
    (3,"Gray"),
    (4,"White"),
    (5,"Green"),
    (6,"Blue"),
    (7,"Pink")
]

car_colors = spark.createDataFrame(data,schema)

car_colors.write.parquet("s3a://spark/data/dims/car_colors",mode="overwrite")


spark.stop()