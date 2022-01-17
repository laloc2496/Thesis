import pandas as pd
from sklearn.model_selection import train_test_split
# from sklearn import preprocessing
# le = preprocessing.LabelEncoder()
#df=pd.read_csv("/home/binh/data/data_sonar_train.csv")
# le.fit(df.Class)
# df['label'] = le.transform(df.Class)

df =pd.read_csv("data/data_new.csv")
train, test=train_test_split(df,test_size=0.3)

train.to_csv("data/data_train.csv")
test.to_csv("data/data_test.csv")

def get_train_data(spark,uri):
    df=pd.read_csv(uri)
    df= spark.createDataFrame(df)
    return df

def get_test_data(spark,uri):
    df=pd.read_csv(uri)
    df= spark.createDataFrame(df)
    return df