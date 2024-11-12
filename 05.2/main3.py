import pandas as pd


def process(df):
    df2 = df.copy()
    df2["prediction"] = 1 - df["prediction"]
    return df2