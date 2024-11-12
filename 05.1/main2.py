from sklearn.neighbors import KNeighborsRegressor as KNN
from sklearn.metrics import mean_absolute_error
import pandas as pd


def process(k):
    model = KNN(n_neighbors=k)
    model.fit(tr_X, tr_Y)
    pred = model.predict(test_X)
    mae = mean_absolute_error(test_Y, pred)
    return mae


df = pd.read_csv(input())
tr_X = df[df["is_test"] == 0].drop(columns=["ID", "is_test"])
tr_Y = tr_X.pop("Value")
test_X = df[df["is_test"] == 1].drop(columns=["ID", "is_test"])
test_Y = test_X.pop("Value")

min_mae = 10 ** 12
ans_k = None
for k in range(1, 16):
    mae = process(k)
    if mae < min_mae:
        min_mae = mae
        ans_k = k

print(ans_k)
print(round(min_mae))
