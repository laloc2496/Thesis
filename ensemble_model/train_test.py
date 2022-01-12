
import mlflow


import mlflow
from pyspark.sql import SparkSession
from utils import SPARK_MASTER, parse_uri, TRACKING_URI,vector_assembler
from load_data import get_train_data
from EnsembleStacking import EnsembleStacking
FEATURES = ['humidity', 'light']
spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
mlflow.set_tracking_uri(TRACKING_URI)
uri_data_train = "/home/binh/Thesis/ensemble_model/data/sample_data_test.csv"
df = get_train_data(spark, uri_data_train)
# ensemble_stacking = EnsembleStacking()
# ensemble_stacking.fit(df, FEATURES)
# ensemble_stacking.save()


uri='7349d9ba37934a4ba9204b8eec236e05'
model= parse_uri(uri,'spark')
df= vector_assembler(FEATURES,df)
model.transform(df).show()