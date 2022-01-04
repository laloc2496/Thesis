from pyspark.sql.column import Column
from pyspark import SparkContext
import pandas as pd
import mlflow
from mlflow.tracking.fluent import _get_experiment_id
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
SPARK_MASTER = "local[*]"
#TRACKING_URI = "fold40"


# df = pd.read_csv("/home/binh/data/data_sonar_test.csv")

# FEATURES = list(df.columns[1:-2])

def listToString(lst):
    return " ".join(str(elem) for elem in lst)

def cross_validation(name, model, data):
    predict = None
    for fold in data:
        temp_model = model.fit(fold[0])
        validation = temp_model.transform(fold[1]).select(
            ["features", get_predict_col_name(name)])
        predict = predict.union(validation) if predict else validation
    return predict

def parse_uri(uri, flavor):

    uri = f'runs:/{uri}/model'
    if flavor == "sklearn":
        return mlflow.sklearn.load_model(uri)
    elif flavor == "spark":
        return mlflow.spark.load_model(uri)

def _already_ran(entry_point_name, parameters, experiment_id=None):
    # experiment_id = experiment_id if experiment_id is not None else _get_experiment_id()
    # client = MlflowClient(tracking_uri=TRACKING_URI)
    # all_run_infos = client.list_run_infos(experiment_id)
    # for run_info in all_run_infos:
    #     full_run = client.get_run(run_info.run_id)
    #     tags = full_run.data.tags
    #     if tags.get(mlflow_tags.MLFLOW_PROJECT_ENTRY_POINT, None) != entry_point_name:
    #         continue
    return None
    model_names = ["DecisionTree", "SVM", "Bayes"]
    if entry_point_name == "DecisionTree":
        return "d9e0a5a223cd438383bac5cdc6932d12"
    elif entry_point_name == "SVM":
        return "7c7c86aea7e04a84b1e85c6927822f85"
    elif entry_point_name == "Bayes":
        return "f9cc6d6b71ed480683ac37d50db226af"
    # return None


def get_or_run(entrypoint, parameters=None):
    experiment_name = entrypoint
    existing_run = _already_ran(entrypoint, parameters)
    if existing_run:
        print("Found existing run")
        return existing_run
    print("Launch new run")
    # mlflow.set_tracking_uri(TRACKING_URI)
    submitted_run = mlflow.run(".", entrypoint, parameters=parameters,
                               use_conda=False, experiment_name=experiment_name)
    return submitted_run.run_id


def caculate_score(data, predict_col, metricName="accuracy", labelCol="label"):
    evaluator = MulticlassClassificationEvaluator(labelCol=labelCol, predictionCol=predict_col,
                                                  metricName=metricName)
    accuracy = evaluator.evaluate(data)
    return accuracy


def vector_assembler(input_col, data=None, output_col="features"):
    vectorAssembler = VectorAssembler()\
        .setInputCols(input_col)\
        .setOutputCol(output_col)
    if data:
        return vectorAssembler.transform(data)
    else:
        return vectorAssembler


def get_predict_col_name(model_name):
    return "prediction_"+model_name


def save_model(model, model_name, features, accuracy=None):
    #    mlflow.set_tracking_uri(TRACKING_URI)
    mlflow.set_experiment(experiment_name=model_name)
    run_id=None
    with mlflow.start_run() as run:
        run_id = run.info.run_id
        mlflow.log_param("name", model_name)
        mlflow.log_param("features", listToString(features))
        mlflow.spark.log_model(model, "model")
        if accuracy:
            mlflow.log_metric("Accuracy", accuracy)
    return run_id

def rand(seed=None):

    sc = SparkContext._active_spark_context
    if seed is not None:
        jc = sc._jvm.functions.rand(seed)
    else:
        jc = sc._jvm.functions.rand()
    return Column(jc)


def kFold(dataset, nFolds=10):
    datasets = []
    # Do random k-fold split.
    seed = 1234
    h = 1.0 / nFolds
    randCol = "_rand"
    df = dataset.select("*", rand(seed).alias(randCol))
    for i in range(nFolds):
        validateLB = i * h
        validateUB = (i + 1) * h
        condition = (df[randCol] >= validateLB) & (df[randCol] < validateUB)
        validation = df.filter(condition)
        train = df.filter(~condition)
        datasets.append((train, validation))
    return datasets
