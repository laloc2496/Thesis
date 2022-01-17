from ast import Try
from pyspark.sql import SparkSession
from EnsembleStacking import transform
from utils import SPARK_MASTER, TRACKING_URI
import mlflow
from load_data import get_test_data
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd

def caculate_score(name, dataset, metric, predict_col, label):
    evaluator = MulticlassClassificationEvaluator(
        labelCol=label,
        predictionCol=predict_col,
        metricName=metric)
    result = evaluator.evaluate(dataset)
    print(name)
    print(result)
    print('-'*10)
# fc2bfcac9fd645b5b6077402ee4869ad

# SVM
# 0.9999921952521973
# ----------
# Bayes
# 0.9999924519873225
# ----------
# DT
# 0.9999975866898242
# ----------
# meta
# 0.9999957381969236
# ----------

def caculate_all_model(dataset,predict_cols,label='label',metric='accuracy', create_spark=False):
    # predict_cols: (name, preidict_col)
    mlflow.set_tracking_uri(TRACKING_URI)
    if create_spark:
        spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
    dataset= transform(dataset)
    #df=dataset.select(["humidity","light","soil","temperature","prediction_SVM","prediction_Bayes","prediction_DecisionTree","prediction","label"])
    #df.write.option("header",'true').csv("/user/root/data_prediction/")
    dataset.show()
    print("Transform finish !")
    
    for name, col in predict_cols:
        caculate_score(name,dataset,metric,col,label)

spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
df = get_test_data(spark, "data/data_test.csv",test=False)


lst=[("SVM","prediction_SVM"),
("Bayes","prediction_Bayes"),
("DT","prediction_DecisionTree"),
("meta","prediction")]
caculate_all_model(df,lst , create_spark=False)
