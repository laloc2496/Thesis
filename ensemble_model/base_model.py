import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.classification import  DecisionTreeClassifier, LinearSVC, NaiveBayes, OneVsRest

from pyspark.ml import Pipeline
from utils import *
from load_data import *
from mlflow.tracking import MlflowClient
from mlflow.tracking.fluent import _get_experiment_id
 
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
    print('Start cross validation !')
    for name in models:
        print(f'Validation for {name} model')
        validation_set = cross_validation(name, models[name], data_train)
        data = data.join(validation_set, "features", "left")
        predict_name = get_predict_col_name(name)
        predict_cols.append(predict_name)

    data = data.drop("features")
    features.extend(predict_cols)
    print('Cross validation finish')
    return {"data": data, "features": features}


def train_base_model(features, data):
    models = get_model()
    result = dict()
    data = vector_assembler(features, data)
    print('Train base models')
    for name in models:
        print(f'Start train {name} model')
        model = models[name]
        pipeline = Pipeline().setStages([model])
        model = pipeline.fit(data)
        run_id = save_model(model, name, features)
        result[name] = {"model": model, "id": run_id}
        print(f"Train {name} model finish !")
    return result

def get_base_model(experiment_id=None):
    client = MlflowClient(tracking_uri=TRACKING_URI)
    experiment_id = experiment_id if experiment_id is not None else _get_experiment_id()
    all_run_infos = client.list_run_infos(experiment_id)
    latest_run = all_run_infos[0]
    bases_model = dict()
    for run_info in all_run_infos:
        full_run = client.get_run(run_info.run_id)
        params = full_run.data.params
        if 'name' in params:
            if params['name'] == 'meta_model':
                nums_model = int(params['NumModel'])

                features = params['features'].split(' ')
                features = features[:-nums_model]
                for key in params:
                    if key.find('uri_') != -1:
                        bases_model[key[4:]] = parse_uri(params[key], 'spark')
                break
    return bases_model, features

# def load_base_model():
#     models = dict()
#     models['DecisionTree'] = parse_uri(
#         'cee4828b091149dead6e6866ac144ca3', 'spark')
#     models['SVM'] = parse_uri('1fc478ce683245218c2c6fd944d68b41', 'spark')
#     models['Bayes'] = parse_uri('bc09954b9ee74505a05b8e92baed7549', 'spark')
#     return models


def predict_base_model(data):
    models,features = get_base_model()
    predict_cols = list()
    data = vector_assembler(features, data)
    for name in models:
        predicted = models[name].transform(data)
        prediction_name = get_predict_col_name(name)
        predicted = predicted.select(['features', prediction_name])
        data = data.join(predicted, "features", "left")
        predict_cols.append(prediction_name)
    data = data.drop("features")
    return data


# spark = SparkSession.builder.master("local").getOrCreate()
# uri = "/home/binh/Thesis/Ensemble/data/sample_data_test.csv"
# df = get_train_data(spark, uri)
# FEATURES = ['humidity', 'light']

 