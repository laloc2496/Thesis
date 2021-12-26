
from kafka.vendor import six
from pyspark.sql import SparkSession
from object import Object
from utils import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local").getOrCreate()


df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", "topic1") \
    .load()

#data = df.selectExpr("CAST(value AS STRING)")
data = df.select(from_json(col("value").cast("string"),
                 Object.prototype_structed_streaming()).alias("data"))

data = data.select("data.*")
data.writeStream.format("console")\
    .outputMode("append").start()\
    .awaitTermination()
spark.stop()
