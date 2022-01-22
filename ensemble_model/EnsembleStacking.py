
from datetime import datetime as dt
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, Transformer
from load_data import get_train_data
from utils import *
from base_model import stacking, predict_base_model, train_base_model
from pyspark.sql import SparkSession
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.tracking.fluent import _get_experiment_id
from mlflow import run as run_checkpoint
import argparse
from pyspark.sql.types import FloatType
from pyspark.sql.functions import col
import time

class EnsembleStacking():
    def __init__(self,fold=40) -> None:
        self.features_lv1 = None
        self.features_lv2 = None
        self.base_models = dict()
        self.meta_model = None
        self.model_name = 'meta_model'
        self.fold= fold

    def fit(self, dataset, features):
        self.features_lv1 = features
        self.base_models = train_base_model(features, dataset)
        stack = stacking(features, dataset, fold=self.fold)
        data_lv2 = stack['data']
        self.features_lv2 = stack['features']
        model = LogisticRegression()
        vector = vector_assembler(self.features_lv2)
        lr = LogisticRegression().setFeaturesCol("features").setLabelCol("label")
        pipeline = Pipeline().setStages([vector, lr])
        print('Train meta model')
        model = pipeline.fit(data_lv2)
        self.meta_model = model
        print('Train meta model finish')
        return model

    def save(self):
        mlflow.set_tracking_uri(TRACKING_URI)
        run_id = None
        with mlflow.start_run(run_name=self.model_name) as run:
            run_id = run.info.run_id
            mlflow.log_param("name", self.model_name)
            mlflow.log_param('NumModel', len(self.base_models))
            mlflow.log_param("features", listToString(self.features_lv2))
            for key in self.base_models:
                mlflow.log_param('uri_'+key, self.base_models[key]['id'])
            mlflow.spark.log_model(self.meta_model, "model")
            mlflow.log_param("fold",self.fold)
        return run_id

# class EnsembleStacking(Transformer):
#     def __init__(self, features) -> None:
#         super().__init__()
#         self,features=features
#     def transform(self, dataset, params=None):
#         dataset=predict_base_model(dataset,self.features)
#         return self._transform(dataset)


def get_meta_model(experiment_id=None):
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
                print(f"Run_id: {full_run.info.run_id}")
                return parse_uri(full_run.info.run_id, 'spark')


def transform(dataset):
    data = predict_base_model(dataset)
    meta_model = get_meta_model()
    result = meta_model.transform(data)
    return result


def current_partition(date=None):
    if date:
        return 'partition='+date
    current_date = dt.now().strftime("%H-%d-%B-%Y")
    return 'partition='+current_date


if __name__ == "__main__":
    mlflow.set_tracking_uri(TRACKING_URI)
    parser = argparse.ArgumentParser(
        description='This script used to train and predict model for irrigation')
    parser.add_argument("--train", "-t", type=str)
    parser.add_argument("--path", "-p", type=str)
    parser.add_argument('--features', '-f', nargs="+")
    parser.add_argument('--id', type=str)
    args = parser.parse_args()
    uri_data_train = args.train
    uri_data_predict = args.path
    features = args.features
    
    if uri_data_train:
        # train
        start = time.time()
        FOLD=30
        spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
        #uri_data_train = "/home/binh/Thesis/ensemble_model/data/sample_data_test.csv"
        try:
            df = get_train_data(spark, uri_data_train)
        except:
            print("Can not load data !")
        ensemble_stacking = EnsembleStacking(fold=FOLD)
        ensemble_stacking.fit(df, features)
        ensemble_stacking.save()

        end = time.time()
        print("Finish")
        print(f"Runtime of the program with {ensemble_stacking.fold} FOLD is {end - start}")
        
        
    elif uri_data_predict:
        # predict
        spark = SparkSession.builder.master("local").getOrCreate()
        feed_id = args.id
        #path = f'data/{feed_id}/'+current_partition()
        path = uri_data_predict
        print(f'Uri data prediction: {path}')
        df = spark.read.csv(path, header=True).orderBy(
            "time", ascending=False).limit(1)
        df = df.select(features)
        for feature in features:
            df = df.withColumn(feature, col(feature).cast(FloatType()))
        
        result = transform(df)
        url= '/user/root/data/retrain/'
        features.append("prediction")
        df=result.select(features)
        result = result.collect()[0]

        run_checkpoint(uri='.', entry_point='send_time_irrigation', use_conda=False, parameters={
                       'feed_id': feed_id, 'value': int(result['prediction'])})

        df=df.withColumnRenamed("prediction","label")
        df.write.mode("append").option("header",'true').csv(url)

#FEATURES = ['humidity', 'light']

# python3 EnsembleStacking.py -p data/sensors/partition=13-28-December-2021 --id sensors -f humidity light  >>log.txt
# spark-submit EnsembleStacking.py -t /home/binh/Thesis/ensemble_model/data/sample_data_test.csv -f humidity light >> log.txt
