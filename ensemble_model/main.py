
from concurrent.futures import thread
from datetime import datetime as dt
from xmlrpc.client import Boolean

from sqlalchemy import true
from EnsembleStacking import current_partition, previous_partition
from pyspark.sql import SparkSession
from utils import SPARK_MASTER, TRACKING_URI
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col, when
from mlflow import run as run_checkpoint
import time
import mlflow
from EnsembleStacking import transform
# Get latest data by time interval to check wheather Irgriration ?
import subprocess
DELAY = 60*4*2


FEATURES = ['humidity', 'light', 'temperature', 'soil']
TIMELINE = [("6:00", "9:59", 30), ("10:00", "16:59", 50),
            ("17:00", "5:59", 60)]

# THRESHOLD=40

# li do ko hien thuc voting vi so luong model la it (3) trong khi so luong label lai nhieu
# cho nen truong hop moi model predict ra 1 class thi ko biet chon ntn. luc do se la van de

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
    except Exception as e:
        print(e)
        return None


def retrain_model(path=None):
    if not path:
        run_checkpoint(
            uri='.', entry_point='reservoir_sampling', use_conda=False)
        path = '/home/binh/Thesis/ensemble_model/data/data_retrain.csv'
    parameters = {
        "path": path
    }
    # run_checkpoint(uri='.', entry_point='stacking_train',
    #                use_conda=False, parameters=parameters)


def change_previous_prediction(spark: SparkSession,up=True):
    folder = '/user/root/data/retrain/'
    path = get_latest_path(folder)
    print(path)
    df = spark.read.csv(path, header=True)
    df.show()
    df = df.withColumn("label", col("label").cast("Integer"))
    if up:
        df = df.withColumn("label", when(col("label") < 3, col('label')+1))
    else:
        df = df.withColumn("label", when(col("label") > 0, col('label')-1))
    df.write.mode("append").option("header", 'true').csv(folder)
    subprocess.run(f'hdfs dfs -rm {path}', shell=True)
    print('done')


#feeds = [('prediction_SVM', 'svm'), ('prediction_DecisionTree', 'dt'),
#         ('prediction_Bayes', 'bayes'), ('prediction', 'sensors')]

feeds=[ ('prediction', 'sensors')]

FLAG_IRRIGATION = False
if __name__ == "__main__":
    mlflow.set_tracking_uri(TRACKING_URI)
    previous_train = None
    while True:

        # today = dt.now()
        # if today.day == 1 and previous_train != today.strftime("%Y-%m-%d"):
        #     previous_train = today.strftime("%Y-%m-%d")
        #     retrain_model()

        spark = SparkSession.builder.master("local").getOrCreate()
        THRESHOLD = get_threshhold()
        for predict_col, feed_id in feeds:
            features = ['humidity', 'light', 'temperature', 'soil']
            uri_folder = f'/user/root/data/{feed_id}/'+current_partition()
            #uri_folder = '/user/root/data/sensors/partition=13-28-December-2021'
            path = get_latest_path(uri_folder)
            if not path:
                uri_folder = f'/user/root/data/{feed_id}/' + \
                    previous_partition(1)
                path = get_latest_path(uri_folder)
            if path:
                df = spark.read.csv(path, header=True)
            else:
                print("Can not load data")
                continue
            df = df.select(['soil'])
            soil = float(df.collect()[0]['soil'])
            if FLAG_IRRIGATION and (soil -THRESHOLD)>10:
                change_previous_prediction(spark,False)
            if soil < THRESHOLD:
                print("Send request irrigation")
                parameters = {"path": path,
                              "feed": feed_id,
                              "predict_col": predict_col
                              }
                if FLAG_IRRIGATION == True and (THRESHOLD-soil) > 10:
                    change_previous_prediction(spark)

                # Run code to predict time to irrigation and send time irrigation to motor.
                # Check result in: https://io.adafruit.com/quangbinh/feeds/sensors.motor
                FLAG_IRRIGATION = True
                run_checkpoint(uri=".", entry_point="stacking_prediction",
                               use_conda=False, parameters=parameters)
                # time.sleep(60)

                print(f"Prediction for {feed_id}")
                print(f'Current soil: {soil}<{THRESHOLD}')
                #print(f'Uri data prediction: {path}')
                # df = spark.read.csv(path, header=True).orderBy(
                #     "time", ascending=False).limit(1)
                # df = df.select(features)
                # for feature in features:
                #     df = df.withColumn(feature, col(feature).cast(FloatType()))

                # result = transform(df)
                # features.append("prediction")
                # df = result.select(features)
                # result = result.collect()[0]
                # run_checkpoint(uri='.', entry_point='send_time_irrigation', use_conda=False, parameters={
                #     'feed_id': feed_id, 'value': int(result[predict_col])})
            else:
                FLAG_IRRIGATION = False
                print(f"No irrgation for {feed_id}!")
                print(f'Current soil: {soil}>={THRESHOLD}')
                print('Wait...')
        spark.stop()
        print('done')
        time.sleep(DELAY)
