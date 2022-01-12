

from pyspark.sql import SparkSession
from utils import SPARK_MASTER
from load_data import get_train_data
from EnsembleStacking import EnsembleStacking
FEATURES = ['humidity', 'light']
spark = SparkSession.builder.master(SPARK_MASTER).getOrCreate()
uri_data_train = "/home/binh/Thesis/ensemble_model/data/sample_data_test.csv"
df = get_train_data(spark, uri_data_train)
ensemble_stacking = EnsembleStacking()
ensemble_stacking.fit(df, FEATURES)
ensemble_stacking.save()