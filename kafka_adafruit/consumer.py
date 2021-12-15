
from pyspark.sql import SparkSession


spark= SparkSession.builder.master("local").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "10.28.8.136:9092") \
  .option("subscribe", "topic1") \
  .load()
df.selectExpr("CAST(value AS STRING)")