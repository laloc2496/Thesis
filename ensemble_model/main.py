
from datetime import datetime as dt
from EnsembleStacking import current_partition
from pyspark.sql import SparkSession
from utils import SPARK_MASTER, TRACKING_URI
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, when
from mlflow import run as run_checkpoint
import time
import mlflow
# Get latest data by time interval to check wheather Irgriration ?
import subprocess
DELAY = 60*4
feeds = ['sensors']

FEATURES = ['humidity', 'light', 'temperature']
TIMELINE = [("6:00", "9:59", 35), ("10:00", "16:59", 50),
            ("17:00", "5:59", 65)]

# THRESHOLD=40


def list2String(s):
    return " ".join(s)


def get_threshhold():
    for a, b, thresh_hold in TIMELINE[:-1]:
        a = dt.strptime(a, "%H:%M")
        b = dt.strptime(b, "%H:%M")
        now = dt.now().strftime("%H:%M")
        now = dt.strptime(now, "%H:%M")
        if now > a and now < b:
            return thresh_hold
    return TIMELINE[-1][2]


def get_latest_path(path):
    try:
        cmd = f'hdfs dfs -ls -t  {path}'
        files = subprocess.getoutput(cmd).split('\n')[1:]
        for file in files:
            if file:
                path = file.split(' ')[-1]
                if '_SUCCESS' in path:
                    continue
                return path
    except:
        return None


def change_previous_prediction(spark: SparkSession):
    folder = '/user/root/data/retrain/'
    path = get_latest_path(folder)
    print(path)
    df = spark.read.csv(path, header=True)
    df.show()
    df = df.withColumn("label", col("label").cast("Integer"))
    df = df.withColumn("label", when(col("label") < 3, col('label')+1))
    df.write.mode("append").option("header", 'true').csv(folder)
    subprocess.run(f'hdfs dfs -rm {path}', shell=True)
    print('done')


FLAG_IRRIGATION = False
if __name__ == "__main__":
    mlflow.set_tracking_uri(TRACKING_URI)
    spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
    while True:
        THRESHOLD = get_threshhold()
        for feed_id in feeds:
            #path = "data/sensors/partition=13-28-December-2021"

            # Unnote this row below to get real time data
            # (MAKE SURE producer and consumer run before)
            # try:
            #     path=f'data/{feed_id}/'+current_partition()
            #     df = spark.read.csv(path, header=True).orderBy(
            #         "time", ascending=False).limit(1)
            # except:
            #     continue

            uri_folder = f'/user/root/data/{feed_id}/'+current_partition()
            #uri_folder = '/user/root/data/sensors/partition=13-28-December-2021'
            path = get_latest_path(uri_folder)
            if path:
                df = spark.read.csv(path, header=True)
            else:
                print("Can not load data")
                time.sleep(60)
                continue
            df = df.select(['soil'])
            soil = float(df.collect()[0]['soil'])
            if soil < THRESHOLD:
                print("Send request irrigation")
                parameters = {"path": path,
                              "feed": feed_id
                              }
                if FLAG_IRRIGATION == True and (THRESHOLD-soil) > 10:
                    change_previous_prediction(spark)

                # Run code to predict time to irrigation and send time irrigation to motor.
                # Check result in: https://io.adafruit.com/quangbinh/feeds/sensors.motor
                FLAG_IRRIGATION = True
                run_checkpoint(uri=".", entry_point="stacking_prediction",
                               use_conda=False, parameters=parameters)
                time.sleep(DELAY*2)
            else:
                FLAG_IRRIGATION = False
                print("No irrgation !")
                print('Wait...')
                time.sleep(DELAY)
