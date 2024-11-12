import os
from yt.wrapper import YtClient, yt_dataclass
import yt
import datetime
from itertools import groupby


@yt_dataclass
class LOG:
    userid: str
    timestamp: datetime.datetime
    action: str
    value: float
    testids: str


@yt_dataclass
class DAU:
    date: datetime.date
    users: int


def extract_day(s: LOG):
    return LOG(s.userid, s.timestamp.date(), s.action, s.value, s.testids)


def get_dau(group):
    date, users = group
    users_unique = {i.userid for i in users}
    return DAU(date, len(users_unique))


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    table = client.read_table_structured(input_name, LOG)
    map1 = map(extract_day, table)
    groups = groupby(map1, key=lambda x: x.timestamp)
    red1 = map(get_dau, groups)
    client.write_table_structured("//home/test/local_dau", DAU, list(red1))


if __name__ == "__main__":
    main()