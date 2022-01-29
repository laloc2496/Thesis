

import random
from operator import index
import pandas as pd
from sqlalchemy import column
from snakebite.client import Client
path_retrain = '/user/root/data/retrain'
path_goal = '/home/binh/Thesis/ensemble_model/data'
N = 10
RETRAIN_PATH = '/home/binh/Thesis/ensemble_model/data/data_retrain.csv'
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
df_new_data = df.drop_duplicates(subset=columns[:-1], keep='last')
# df.to_csv('/home/binh/Thesis/ensemble_model/data/data_retrain.csv')

df_retrain: pd.DataFrame = pd.read_csv(RETRAIN_PATH)
for index, row in df_new_data.iterrows():
    if (df_retrain.shape[0] < N):
        # print('Append')
        df_retrain = df_retrain.append(row, ignore_index=True)
    else:
        # print('Replace')
        location = random.randint(0, index)
        if location < N:
            # df_retrain.iloc[location]=row
            df_retrain.loc[location, :] = row[:]
df_retrain.reset_index(drop=True,inplace=True)
df_retrain = df_retrain.loc[:, ~df_retrain.columns.str.contains('^Unnamed')]
df_retrain.to_csv(RETRAIN_PATH)