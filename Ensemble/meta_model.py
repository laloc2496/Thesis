
from pyspark.ml.classification import LogisticRegression

from pyspark.ml import Pipeline
from utils import *
from load_data import *


def train_meta_model(features, data):
    model = LogisticRegression
    vector = vector_assembler(features)
    print(features)
    lr = LogisticRegression().setFeaturesCol("features").setLabelCol("label")
    pipeline = Pipeline().setStages([vector, lr])
    model = pipeline.fit(data)
    print("Train finish !")
    return model
