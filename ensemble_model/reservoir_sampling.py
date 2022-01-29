

import random
from operator import index
from xmlrpc.client import Boolean
import pandas as pd
import subprocess
from snakebite.client import Client
N = 10
RETRAIN_PATH = '/user/root/data/retrain/'
TRAIN_PATH = '/home/binh/Thesis/ensemble_model/data/data_retrain.csv'
HDFS_SERVER = '10.1.8.7'
client = Client(HDFS_SERVER, 9000)


def get_retrain_data() -> pd.DataFrame:
    df = list()
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
    return df_new_data


def reservoir_sampling(df_new_data: pd.DataFrame) -> Boolean:
    df_retrain: pd.DataFrame = pd.read_csv(TRAIN_PATH)
    for index, row in df_new_data.iterrows():
        if (df_retrain.shape[0] < N):
            df_retrain = df_retrain.append(row, ignore_index=True)
        else:
            location = random.randint(0, index)
            if location < N:
                df_retrain.loc[location, :] = row[:]
    # remove all unuse data
    #subprocess.run(f'hdfs dfs -rm {RETRAIN_PATH}', shell=True)
    # ------------
    df_retrain.reset_index(drop=True, inplace=True)
    df_retrain = df_retrain.loc[:, ~df_retrain.columns.str.contains('^Unnamed')]
    df_retrain = df_retrain.drop_duplicates(subset=df_retrain.columns[:-1], keep='last')
    df_retrain.to_csv(TRAIN_PATH)
    return True


if __name__ == "__main__":
    retrain_data = get_retrain_data()
    reservoir_sampling(retrain_data)
    print("Done")
