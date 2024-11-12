from sklearn.neighbors import KNeighborsRegressor as KNN
from sklearn.metrics import mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import pandas as pd

df = pd.read_csv("football_players.csv")
tr_X = df[df["is_test"] == 0].drop(columns=["ID", "is_test"])
tr_Y = tr_X.pop("Value")
test_X = df[df["is_test"] == 1].drop(columns=["ID", "is_test"])
test_Y = test_X.pop("Value")

model = KNN(n_neighbors=3)
model.fit(tr_X, tr_Y)
pred = model.predict(test_X)

mae = mean_absolute_error(test_Y, pred)
rmse = mean_squared_error(test_Y, pred, squared=False)
mape = mean_absolute_percentage_error(test_Y, pred)

print(round(mae))
print(round(rmse))
print(round(mape * 100))