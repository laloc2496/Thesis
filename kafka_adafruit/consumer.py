
from pyspark.sql import SparkSession
from utils import *

spark= SparkSession.builder.master(SPARK_MASTER).getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
  .option("subscribe", "topic1") \
  .load()
data=df.selectExpr("CAST(value AS STRING)")


data.writeStream.format("console").outputMode("append").start().awaitTermination()
spark.stop()