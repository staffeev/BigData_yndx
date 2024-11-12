from simplemr import SimpleMapReduce
from dataclasses import dataclass


@dataclass
class LOGdata:
    month: str
    value: int


def get_userid_day(s: str):
    _, stamp, action, value, *_ = s.split("\t")
    dt = stamp[:7]
    if stamp != "timestamp" and action == "checkout":
        return [LOGdata(dt, float(value))]
    return []


def get_sum(gr):
    it = next(gr)
    month, value = it.month, it.value
    for i in gr:
        value += i.value
    return [LOGdata(month, value)]


def process(mrjob: SimpleMapReduce) -> SimpleMapReduce:
     return mrjob.map(get_userid_day). \
        reduce(get_sum, ["month"])


with open("log.tsv", "r") as input_stream:
    mrjob = process(SimpleMapReduce(input_stream))
    for item in mrjob.output():
        print(item.month, item.value)