from pyspark.sql import SparkSession
import json
import time
import random
from kafka import KafkaProducer

spark = SparkSession.builder.master("local[*]").appName("DataGenerator").getOrCreate()

cars_df = spark.read.parquet("s3a://spark/data/dims/cars")
cars_list = [row["car_id"] for row in cars_df.select("car_id").collect()]

topic = 'sensors-sample'
brokers = ['kafka:9092']

producer = KafkaProducer(
    bootstrap_servers = brokers,
    value_serializer = lambda v: json.dumps(v).encode("utf8")
) 


event_counter = 0

while True:
    for car_id in cars_list:
        event_counter += 1
        event_data ={
            "event_id": int(time.time()*1000) + event_counter,
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "car_id": car_id,
            "speed": random.randint(0,200),
            "rpm": random.randint(0,8000),
            "gear": random.randint(1,7)


        }
        producer.send(topic,value=event_data)
        print(f"Sent data for car_id {car_id}: {event_data}")
    
    producer.flush()
    time.sleep(1)