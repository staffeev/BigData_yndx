from dataclasses import dataclass
from collections import OrderedDict


@dataclass(unsafe_hash=True)
class LOG:
    userid : str = ""
    timestamp : str = ""
    action : str = ""
    value : str = ""
    testids : str = ""


def create_log(s):
    log = LOG()
    for i in s.strip().split("\t"):
        key, value = i.split("=")
        setattr(log, key, value)
    return log


with open("log.dsv", encoding="utf8") as file:
    data = file.readlines()

logs = OrderedDict()
for i in data:
    log = create_log(i)
    logs[log] = logs.get(log, 0) + 1

with open("output.tsv", "w", newline="", encoding="utf8") as file:
    file.write("userid	timestamp	action	value	testids\n")
    for i in logs:
        print(i)
        file.write("\t".join((i.userid, i.timestamp, i.action, i.value, i.testids)) + "\n")