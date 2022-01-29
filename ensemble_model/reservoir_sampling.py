

from operator import index
import pandas as pd
from sqlalchemy import column
from snakebite.client import Client
path_retrain = '/user/root/data/retrain'
path_goal = '/user/root/data/train'

N = 1000

client = Client('10.1.8.7', 9000)

df = []
columns = None
for l in client.text(['/user/root/data/retrain/*']):
    if not columns:
        columns = l.decode("utf-8").split('\n')[0].split(',')
    row = l.decode("utf-8").split('\n')[1].split(',')
    row = list(map(float, row))

    if len(row) == 4:
        df.append(row)

df = pd.DataFrame(df, columns=columns)
df= df.drop_duplicates(subset=columns[:-1],keep='last')
#df.to_csv('/home/binh/Thesis/ensemble_model/ahihi_drop.csv')