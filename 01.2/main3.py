from dataclasses import dataclass
import re


@dataclass(unsafe_hash=True)
class LOG:
    userid : int
    timestamp : int
    action : str


def conv_to_log(s):
    if s == "\n":
        return []
    args = s.strip().replace("checkᝃut", "checkout").split("\t")
    if len(args) == 3:
        uid, stamp, act = args
        uid = re.findall("\d+", uid)[0]
        return [LOG(int(uid), int(stamp), act)]
    logs = []
    for i in range(2, len(args), 2):
        uid, stamp, act = args[i - 2], args[i - 1], args[i]
        uid = re.findall("\d+", uid)[0]
        stamp = re.findall("\d+", stamp)[0]
        act = re.findall("\D+", act)[0]
        logs.append(LOG(int(uid), int(stamp), act))
    return logs


with open("user_activity_bad_log.tsv", encoding="utf8") as file:
    data = [j for i in map(conv_to_log, file) for j in i]
    data = set(data)
    stamps = {i.userid for i in data}
    all_checkouts = [i for i in data if i.action == "checkout"]
    unique_users = {i.userid for i in data}
    print(len(all_checkouts))
    print(len(unique_users))
    print(len(all_checkouts) / len(unique_users))