from pyspark.sql.types import *
from datetime import datetime as dt
from utils import DATETIME_FORMAT
class Object:
    def __init__(self,id,humidity=None,light=None,soil=None,temperature=None):
        self.id=id
        self.humidity=humidity
        self.light=light 
        self.soil=soil
        self.temperature=temperature
        self.time=str(dt.now().strftime(DATETIME_FORMAT))

    def check(self):
        return self.humidity and self.light and self.soil and self.temperature

    def reset(self):
        self.humidity=None
        self.light=None 
        self.soil=None
        self.temperature=None
        self.time=str(dt.now().strftime(DATETIME_FORMAT))

    @staticmethod
    def prototype_structed_streaming():
        jsonschema = StructType().add("id", StringType()) \
                        .add("humidity", FloatType()) \
                        .add("light", FloatType()) \
                        .add("soil", FloatType()) \
                        .add("temperature", FloatType())\
                        .add("time",StringType())
        return jsonschema