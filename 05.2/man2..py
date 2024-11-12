import pandas as pd
from sklearn.metrics import f1_score, recall_score, precision_score


def get_metrics(df2):
    f1 = f1_score(df2["target"], df2["prediction"])
    rec = recall_score(df2["target"], df2["prediction"])
    prec = precision_score(df2["target"], df2["prediction"])
    return prec, rec, f1


df = pd.read_csv("classifier_prediction_1.csv")

for i in range(1, 10):
    sl = df["prediction"].apply(lambda x: 0.0 if x < 0.1 * i else 1.0)
    sl["target"] = df["target"]
    print(sl)
    break
    print(get_metrics(sl))
