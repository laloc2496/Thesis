
import json
from kafka.vendor import six
from pyspark.sql import SparkSession
from object import Object
from utils import *
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import *

#from kafka import KafkaConsumer
#import io
#from confluent_kafka import Consumer, KafkaError
#from avro.io import DatumReader, BinaryDecoder
#import avro.schema
#import json
import requests
from pyspark.sql.column import Column, _to_java_column 

#def from_avro(col, jsonFormatSchema): 
 #   sc = SparkContext._active_spark_context 
  #  avro = sc._jvm.org.apache.spark.sql.avro
  #  f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
  #  return Column(f(_to_java_column(col), jsonFormatSchema)) 


#def to_avro(col): 
 #   sc = SparkContext._active_spark_context 
 #   avro = sc._jvm.org.apache.spark.sql.avro
 #   f = getattr(getattr(avro, "package$"), "MODULE$").to_avro
 #   return Column(f(_to_java_column(col))) 



response = requests.get("http://schema-registry:8081/subjects/spark-smart-village/versions/latest")
schema = json.loads(response.content.decode())['schema']
print(schema)

#schema_registry_url = "http://schema-registry:8081/"

spark = SparkSession.builder.master("yarn").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("subscribe", "topic1") \
    .option("mode", "PERMISSIVE") \
    .load()

data = df.select(from_avro(col("value"), schema, options={"mode":"PERMISSIVE"}).alias("data")).select("data.*")


#data.writeStream.format("console")\
#     .outputMode("append").start()\

#data.writeStream.format("csv").option("checkpointLocation", "/user/root/checkpoint/").option("path","/user/root/test/").outputMode("append").start().awaitTermina 

def write_data(df,batchID):
 # df.coalesce(1).write.mode("append").option("header","true").csv("/user/root/test/")
  # name_ts="time"
  print(df.head(1))
  if len(df.head(1))!=0:
    partition_format="HH-dd-MMMM-yyyy"
    PARTITION='partition'
    df=df.withColumn('time',to_timestamp(col('time'),'HH:mm:ss dd-MM-yyyy'))
    df=df.withColumn(PARTITION,date_format(col('time'),partition_format))
    id=df.first()[0]
    df=df.drop("id")
    url=f"/user/root/data/{id}/"
    print(url)
    df.coalesce(1).write.mode("append").partitionBy(PARTITION).option("header",'true').csv(url)
data.writeStream.foreachBatch(write_data).start().awaitTermination()

spark.stop()



 
