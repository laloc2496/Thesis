
import pandas  as pd


df= pd.read_csv("data_bien.csv")

soil=[]
humidity=[]
import random
for _,row in df.iterrows():
    if row['humidity'] < 50:
        humidity.append(random.randint(50,70))
    elif row['humidity'] >= 85:
        humidity.append(random.randint(60,80))
    else:
        humidity.append(row['humidity'])

    if row['soil'] < 30 or row['soil']>80:
        soil.append(random.randint(30,60))
    else:
        soil.append(row['soil'])

df['soil']=soil
df['humidity']=humidity
df.to_csv('data_bien1.csv')