import numpy as np


def process(n):
    data = []
    for _ in range(1000):
        sample = np.random.choice([0, 1], n, p=[0.4, 0.6])
        data.append(np.mean(sample))
    data.sort()
    v1, v2 = np.percentile(data, [2.5, 97.5])
    return v2 - v1