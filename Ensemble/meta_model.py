
# from posixpath import basename
# from mlflow.tracking import MlflowClient
# from pyspark.ml.classification import LogisticRegression

# from pyspark.ml import Pipeline
 
# from utils import *
# from load_data import *


# def train_meta_model(features, data):
#     model = LogisticRegression()
#     vector = vector_assembler(features)
#     print(features)
#     lr = LogisticRegression().setFeaturesCol("features").setLabelCol("label")
#     pipeline = Pipeline().setStages([vector, lr])
#     model = pipeline.fit(data)
#     print("Train finish !")
#     return model



 
# client = MlflowClient()
# all_run_infos = client.list_run_infos('0')
# latest_run = all_run_infos[0]
# bases_model = dict()
# for run_info in all_run_infos:
#     full_run = client.get_run(run_info.run_id)
#     params = full_run.data.params
#     if params['name'] == 'meta_model':
#         nums_model = int(params['NumModel'])

#         features = params['features'].split(' ')
#         features= features[:-nums_model]
#         for key in params:
#             if key.find('uri_') != -1:
#                bases_model[key[4:]] = parse_uri(params[key],'spark')
#         break
# print(bases_model)

