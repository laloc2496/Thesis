
from cProfile import label
from webbrowser import get
import pandas as pd 


df= pd.read_csv("data.csv")

TEMPERATURE='temperature'
LIGHT='light'
HUMIDITY='humidity'
label=[0,1,2,3]
def gen_label(row):
    if row[TEMPERATURE]< 22:
        return 0
    else:
        if row[LIGHT] > 1000 and (row[HUMIDITY] < 65 or row[TEMPERATURE]> 29):
            return 3

        if (row[LIGHT] > 300 or row[LIGHT]> 60) and  row[HUMIDITY] < 68 and row[TEMPERATURE]> 28:
            return 2
        
        if row[LIGHT] > 100 and row[HUMIDITY] > 68 and row[HUMIDITY] < 75 and row[TEMPERATURE]> 26:
            return 1
        if row[LIGHT] < 60:
            if row[HUMIDITY]< 50:
                return 2
            if row[HUMIDITY]< 70 and row[TEMPERATURE]> 28:
                return 1
        return 0

labels=[]
for index,row in df.iterrows():
    label= gen_label(row)
    labels.append(label)

df['label']=labels

df.to_csv("data1.csv")
print("done")