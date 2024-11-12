from simplemr import SimpleMapReduce
from dataclasses import dataclass


@dataclass
class LOGdata:
    userid: str
    frequency: int


def get_userid_day(s: str):
    userid, stamp, action, *_ = s.split("\t")
    if stamp != "timestamp" and action == "checkout":
        return [LOGdata(userid, 1)]
    return []


def get_sum(gr):
    it = next(gr)
    userid, frequency = it.userid, it.frequency
    for i in gr:
        frequency += i.frequency
    return [LOGdata(userid, frequency)]


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
     return mrjob.map(get_userid_day). \
        reduce(get_sum, ["userid"])