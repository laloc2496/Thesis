import pandas as pd


# dates = ["2022-01-24", "2022-01-25", "2022-01-26",
#          "2022-01-27", "2022-01-28", "2022-01-29"]
# models = ["bayes", "dt", "sensors", "svm"]


dates = ["2022-01-31", "2022-02-01", "2022-02-02", "2022-02-03", "2022-02-04"]
models = ["sensors", "voting"]
# option 0 [done] tuoi nghiem ngat nhat.
# Penalty:
# ko lệch: 0
# lệch <5: -1
# Lệch 5-10: -2
# Lệch >10: -3


# option 1 [done]
# Penalty:
# ko lệch: 0
# lệch <10: -1
# Lệch 10-15: -2
# Lệch >15: -3


# option 2 [done]
# Penalty:
# ko lệch: 0
# lệch <15: -1
# Lệch 15-20: -2
# Lệch >20: -3

def get_name(date, model):
    return f"evaluate_model/penalty/{date}_{model}.csv"


def get_penalty(soil, threshhold):
    if soil < threshhold:
        value = threshhold-soil
        if value < 15:
            return -1
        if value >= 15 and value < 20:
            return -2
        if value >= 20:
            return -3
    if soil > threshhold:
        value = soil-threshhold
        if value >= 20 and value < 25:
            return -1
        if value >= 25:
            return -2
    return 0


for date in dates:
    maxrow = None
    print('-'*10)
    print(date)
    for model in models:
        sum = 0
        count = 0
        df = pd.read_csv(get_name(date, model))
        if not maxrow:
            maxrow = df.shape[0]
        for _, row in df.iterrows():
            if count > maxrow:
                continue
            count += 1
            sum += get_penalty(row[2], row[3])

        print(f"Model {model}: {sum*3}")
