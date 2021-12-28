
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

data = df.select(from_json(col("value").cast("string"),
                 Object.prototype_structed_streaming()).alias("data")).select("data.*")


# data.writeStream.format("console")\
#     .outputMode("append").start()\
#     .awaitTermination()

#data.writeStream.format("csv").option("checkpointLocation", "/user/root/checkpoint/").option("path","/user/root/test/").outputMode("append").start().awaitTermination()

def write_data(df,batchID):
 # df.coalesce(1).write.mode("append").option("header","true").csv("/user/root/test/")
  # name_ts="time"
  if len(df.head(1))!=0:
    partition_format="HH-dd-MMMM-yyyy"
    PARTITION='partition'
    df=df.withColumn('time',to_timestamp(col('time'),'HH:mm:ss dd-MM-yyyy'))
    df=df.withColumn(PARTITION,date_format(col('time'),partition_format))
    id=df.first()[0]
    df=df.drop("id")
    url=f"/user/root/data/{id}/"
    df.coalesce(1).write.mode("append").partitionBy(PARTITION).option("header",'true').csv(url)
data.writeStream.foreachBatch(write_data).start().awaitTermination()

spark.stop()



 