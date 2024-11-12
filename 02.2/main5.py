import os
from yt.wrapper import YtClient, yt_dataclass, TypedJob
import yt
import datetime
from itertools import groupby
from typing import Iterable 


@yt_dataclass
class LOG:
    userid: str
    timestamp: datetime.datetime
    action: str
    value: float
    testids: str


@yt_dataclass
class UserValue:
    userid: str
    value: int = 1


class GetUserMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserValue]:
        if row.action == "checkout":
            yield UserValue(row.userid)


class GetRevenueReducer(TypedJob):
    def __call__(self, rows: Iterable[UserValue]) -> Iterable[UserValue]:
        userid = None
        count = 0
        for i in rows:
            userid = i.userid
            count += i.value
        yield UserValue(userid, count)


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_name = "//tmp/test_test_2"
    
    client.run_map_reduce(
        GetUserMapper(),
        GetRevenueReducer(),
        source_table=input_name,
        destination_table=output_name,
        reduce_by=["userid"],
    )
    table = list(client.read_table_structured(output_name, UserValue))
    table.sort(key=lambda x: (-x.value, x.userid))
    for i in table[:10]:
        print(i.userid, i.value)


if __name__ == "__main__":
    main()