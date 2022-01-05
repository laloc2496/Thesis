
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, Transformer
from load_data import get_train_data
from utils import *
from meta_model import train_meta_model
from base_model import stacking, predict_base_model, train_base_model
from pyspark.sql import SparkSession


# class PipelineWithBaseModel(Pipeline):
#     def __init__(self, features,fold=5, *, stages=...) -> None:
#         self.features = features
#         self.fold=fold
#         super().__init__(stages=stages)

#     def fit(self, dataset, params=None):
#         stack= stacking(self.features,dataset,fold=self.fold)
#         data_lv2=stack['data']
#         new_features= stack['features']
#         model= train_meta_model(new_features,data_lv2)
#         return model


class EnsembleStacking():
    def __init__(self) -> None:
        self.features_lv1 = None
        self.features_lv2 = None
        self.base_models = dict()
        self.meta_model = None
        self.model_name = 'meta_model'

    def fit(self, dataset, features):
        self.features_lv1 = features
        self.base_models = train_base_model(features, dataset)
        stack = stacking(features, df, fold=5)
        data_lv2 = stack['data']
        self.features_lv2 = stack['features']
        model = LogisticRegression()
        vector = vector_assembler(self.features_lv2)
        lr = LogisticRegression().setFeaturesCol("features").setLabelCol("label")
        pipeline = Pipeline().setStages([vector, lr])
        print('Train meta model')
        model = pipeline.fit(data_lv2)
        self.meta_model = model
        return model

    def transform(self, dataset, features, params=None):
        pass

    def save(self):
        #    mlflow.set_tracking_uri(TRACKING_URI)
        mlflow.set_experiment(experiment_name=self.model_name)
        run_id = None
        with mlflow.start_run() as run:
            run_id = run.info.run_id
            mlflow.log_param("name", self.model_name)
            mlflow.log_param("features", listToString(self.features_lv2))
            for key in self.base_models:
                mlflow.log_param(key, self.base_models[key]['id'])
            mlflow.spark.log_model(self.meta_model, "model")
        return run_id

# class EnsembleStacking(Transformer):
#     def __init__(self, features) -> None:
#         super().__init__()
#         self,features=features
#     def transform(self, dataset, params=None):
#         dataset=predict_base_model(dataset,self.features)
#         return self._transform(dataset)


spark = SparkSession.builder.master("local").getOrCreate()
uri = "/home/binh/Thesis/Ensemble/data/sample_data_test.csv"
df = get_train_data(spark, uri)
FEATURES = ['humidity', 'light']
# model= EnsembleStacking(FEATURES)
# model=model.fit(df)
# save_model(model,'lr',FEATURES)


# stack = stacking(FEATURES, df, fold=5)
# data_lv2 = stack['data']
# new_features = stack['features']


meta_model= EnsembleStacking()

meta_model.fit(df,FEATURES)
meta_model.save()