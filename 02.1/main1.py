from simplemr import SimpleMapReduce
from datetime import datetime
from dataclasses import dataclass


@dataclass
class LOGdata:
    uid: str
    timestamp: str
    action: str


@dataclass
class DAUmetric:
    date: str
    dau: int



def get_uid_day(s: str):
    uid, stamp, action, *_ = s.split("\t")
    dt = stamp[:10]
    if stamp != "timestamp":
        return [LOGdata(uid, dt, action)]
    return []


def get_dau(group):
    list_group = list(group)
    return [DAUmetric(list_group[0].timestamp, len({i.uid for i in list_group if i.action == "search"}))]


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
     return mrjob.map(get_uid_day). \
        reduce(get_dau, ["timestamp"]). \
        map(lambda x: [x] if x.dau != 0 else [])