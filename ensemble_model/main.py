
from EnsembleStacking import current_partition
from pyspark.sql import SparkSession
from utils import SPARK_MASTER
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
from mlflow import run as run_checkpoint
import time

# Get latest data by time interval to check wheather Irgriration ?

DELAY = 120
feeds = ['sensors']
THRESHOLD = 40
FEATURES = ['humidity', 'light']



# TODO
# [ ]dieu chinh linh dong FEATURES


def list2String(s):
    return " ".join(s)


if __name__ == "__main__":
    spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
    while True:
        for feed_id in feeds:
            #path = "data/sensors/partition=13-28-December-2021"

            # Unnote this row below to get real time data 
            # (MAKE SURE producer and consumer run before)
            path=f'data/{feed_id}/'+current_partition()
            df = spark.read.csv(path, header=True).orderBy(
                "time", ascending=False).limit(1)
            df = df.select(['soil'])
            soil = float(df.collect()[0]['soil'])
            if soil < THRESHOLD:
                parameters = {"path": path,
                              "feed": feed_id
                              }
                # Run code to predict time to irrigation and send time irrigation to motor.
                # Check result in: https://io.adafruit.com/quangbinh/feeds/sensors.motor

                run_checkpoint(uri=".", entry_point="stacking_prediction",
                               use_conda=False, parameters=parameters)
        print('Wait...')
        time.sleep(DELAY)
