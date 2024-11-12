import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


def process(df):
    clusters = KMeans(n_clusters=10, random_state=123)
    clusters.fit(df.drop(columns=["Best Position", "ID"]))
    counts = dict(zip(*np.unique(clusters.labels_, return_counts=True)))
    max_cluster = max(counts.items(), key=lambda x: x[1])
    players = df[clusters.labels_ == max_cluster[0]]
    counts2 = dict(zip(*np.unique(players["Best Position"], return_counts=True)))
    pop_pos = max(counts2.items(), key=lambda x: x[1])
    return pop_pos[0]