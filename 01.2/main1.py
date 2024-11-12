from dataclasses import dataclass
from itertools import groupby
from datetime import datetime


@dataclass
class LOGdata:
    uid: str
    date: str
    value: int


@dataclass
class DAUmetric:
    date: str
    value: int



def flatten(x):
    for it in x:
        for i in it:
            yield i


def get_uid_date(s):
    uid, timestamp, *_ = s.split("\t")
    dt = timestamp[:10]
    if timestamp != "timestamp":
        return [LOGdata(uid, datetime.strptime(dt, "%Y-%m-%d"), 1)]
    return []


def reduce(gr):
    it = next(gr)
    uid, date, value = it.uid, it.date, it.value


def get_dau(gr):
    pass


with open("log.tsv") as stream:
    data = stream.readlines()
    # map
    map1 = flatten(map(get_uid_date, data))
    map1 = sorted(map1, key=lambda x: x.date)
    gr = groupby(map1, key=lambda x: x.date)
    # reduce
    red1 = map(x, )
