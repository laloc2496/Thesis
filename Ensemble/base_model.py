import pandas as pd
import mlflow
from pyspark.sql import SparkSession
import numpy as np
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, LinearSVC, NaiveBayes, OneVsRest

from pyspark.ml import Pipeline
from utils import *
import concurrent.futures

from load_data import *


def get_model():
    models = dict()
    dt = DecisionTreeClassifier(predictionCol="prediction_DecisionTree")\
        .setFeaturesCol("features")\
        .setLabelCol("label")

    svm = LinearSVC()
    ovr = OneVsRest(classifier=svm, predictionCol="prediction_SVM")\
        .setFeaturesCol("features")\
        .setLabelCol("label")

    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", predictionCol="prediction_Bayes")\
        .setFeaturesCol("features")\
        .setLabelCol('label')

    models['DecisionTree'] = dt
    models['SVM'] = ovr
    models['Bayes'] = nb
    return models




def stacking(features, data, fold=5):
    models = get_model()
    data = vector_assembler(features, data)
    data_train = kFold(data, nFolds=fold)
    predict_cols = list()

    for name in models:
        validation_set = cross_validation(name, models[name], data_train)
        data = data.join(validation_set, "features", "left")
        predict_name = get_predict_col_name(name)
        predict_cols.append(predict_name)

    data = data.drop("features")
    features.extend(predict_cols)
    return data


spark = SparkSession.builder.master("local").getOrCreate()

uri = "/home/binh/Thesis/Ensemble/data/sample_data_test.csv"
df = get_train_data(spark, uri)
FEATURES = ['humidity', 'light']
stacking(FEATURES, df).show()
