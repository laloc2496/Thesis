
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, Transformer, feature
from load_data import get_train_data
from utils import *
from meta_model import train_meta_model
from base_model import stacking, predict_base_model, train_base_model
from pyspark.sql import SparkSession
from mlflow.tracking import MlflowClient
from mlflow.tracking.fluent import _get_experiment_id


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
        print('Train meta model finish')
        return model

    def save(self):
        #    mlflow.set_tracking_uri(TRACKING_URI)
        run_id = None
        with mlflow.start_run(run_name=self.model_name) as run:
            run_id = run.info.run_id
            mlflow.log_param("name", self.model_name)
            mlflow.log_param('NumModel', len(self.base_models))
            mlflow.log_param("features", listToString(self.features_lv2))
            for key in self.base_models:
                mlflow.log_param('uri_'+key, self.base_models[key]['id'])
            mlflow.spark.log_model(self.meta_model, "model")
        return run_id

# class EnsembleStacking(Transformer):
#     def __init__(self, features) -> None:
#         super().__init__()
#         self,features=features
#     def transform(self, dataset, params=None):
#         dataset=predict_base_model(dataset,self.features)
#         return self._transform(dataset)


def get_meta_model(experiment_id=None):
    client = MlflowClient()
    experiment_id = experiment_id if experiment_id is not None else _get_experiment_id()
    all_run_infos = client.list_run_infos(experiment_id)
    latest_run = all_run_infos[0]
    bases_model = dict()
    for run_info in all_run_infos:
        full_run = client.get_run(run_info.run_id)
        params = full_run.data.params
        if params['name'] == 'meta_model':
            return parse_uri(full_run.info.run_id, 'spark')





def transform(dataset):
    data = predict_base_model(dataset)
    meta_model=get_meta_model()
    result=meta_model.transform(data).show()
    return result


spark = SparkSession.builder.master("local").getOrCreate()
uri = "/home/binh/Thesis/Ensemble/data/sample_data_test.csv"
df = get_train_data(spark, uri)

transform(df)
# FEATURES = ['humidity', 'light']
# # model= EnsembleStacking(FEATURES)
# # model=model.fit(df)
# # save_model(model,'lr',FEATURES)


# # stack = stacking(FEATURES, df, fold=5)
# # data_lv2 = stack['data']
# # new_features = stack['features']


# meta_model= EnsembleStacking()

# meta_model.fit(df,FEATURES)
# meta_model.save()


#model= parse_uri('08c8fcec2b0c47a39ae44a3592ff6522','spark')
