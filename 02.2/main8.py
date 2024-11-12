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
class UserDate:
    userid: str
    date: datetime.date


class GetUserMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserDate]:
        yield UserDate(row.userid, row.timestamp.date())


class GetLastAction(TypedJob):
    def __call__(self, rows: Iterable[UserDate]) -> Iterable[UserDate]:
        row = next(rows)
        userid = row.userid
        last_act = row.date
        for i in rows:
            if i.date > last_act:
                last_act = i.date
        d1 = datetime.date(2022, 5, 1)
        d2 = datetime.date(2022, 5, 31)
        if d1 <= last_act <= d2:
            yield UserDate(userid, last_act)


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_name = "//home/test/churned_in_may"
    
    client.run_map_reduce(
        GetUserMapper(),
        GetLastAction(),
        source_table=input_name,
        destination_table=output_name,
        reduce_by=["userid"]
    )


if __name__ == "__main__":
    main()