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

    # svm = LinearSVC()
    # ovr = OneVsRest(classifier=svm, predictionCol="prediction_SVM")\
    #     .setFeaturesCol("features")\
    #     .setLabelCol("label")

    nb = NaiveBayes(smoothing=1.0, modelType="multinomial", predictionCol="prediction_Bayes")\
        .setFeaturesCol("features")\
        .setLabelCol('label')

    models['DecisionTree'] = dt
#    models['SVM'] = ovr
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
    return {"data": data, "features": features}


def train_base_model(features, data):
    models = get_model()
    result=dict()
    vector = vector_assembler(features)
    for name in models:
        print(f'Start train {name} model')
        model = models[name]
        pipeline = Pipeline().setStages([vector, model])
        model = pipeline.fit(data)
        result[name]=model
        save_model(model,name,features)
        print(f"Train {name} model finish !")
    return result


def train_meta_model(features, data):
    model = LogisticRegression
    vector = vector_assembler(features)
    print(features)
    lr = LogisticRegression().setFeaturesCol("features").setLabelCol("label")
    pipeline = Pipeline().setStages([vector, lr])
    model = pipeline.fit(data)
    print("Train finish !")
    return model


def save_model(model, model_name, features, accuracy=None):
#    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(experiment_name=model_name)
    with mlflow.start_run():
        mlflow.log_param("name", model_name)
        mlflow.log_param("features", listToString(features))
        mlflow.spark.log_model(model, "model")
        if accuracy:
            mlflow.log_metric("Accuracy", accuracy)

spark = SparkSession.builder.master("local").getOrCreate()
uri = "/home/binh/Thesis/Ensemble/data/sample_data_test.csv"
df = get_train_data(spark, uri)
FEATURES = ['humidity', 'light']


# stack = stacking(FEATURES, df)
# data = stack['data']
# data.show()
# features = stack['features']
# model=meta_model(features, data)
# save_model(model,"lr",features)

train_base_model(features=FEATURES,data=df)