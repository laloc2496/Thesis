import pandas as pd
import mlflow
from pyspark.sql import SparkSession
import numpy as np
from load_raw_data import *
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, LinearSVC, NaiveBayes

from pyspark.ml import Pipeline
from utils import *
import concurrent.futures


def get_model():
    models=dict()
    dt = DecisionTreeClassifier(predictionCol="prediction_DecisionTree")\
        .setFeaturesCol("features")\
        .setLabelCol("label")
    svm = LinearSVC(predictionCol="prediction_SVM")\
        .setFeaturesCol("features")\
        .setLabelCol("label")
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", predictionCol="prediction_Bayes")\
        .setFeaturesCol("features")\
        .setLabelCol('label')
        
    models['DecisionTree']=dt
    models['SVM']=svm
    models['Bayes']=nb
    return models
