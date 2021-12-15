
from pyspark.sql import SparkSession
from utils import *

spark= SparkSession.builder.master("local").getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
  .option("subscribe", TOPIC_KAFKA) \
  .load()
df.selectExpr("CAST(value AS STRING)")