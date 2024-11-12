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


@yt_dataclass
class DAU:
    date: datetime.date
    users: int


class GetUidDateMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserDate]:
        if row.action == "search":
            yield UserDate(row.userid, row.timestamp.date())


class GetDauReducer(TypedJob):
    def __call__(self, rows: Iterable[UserDate]) -> Iterable[DAU]:
        unique_users = set()
        date = None
        for i in rows:
            date = i.date
            unique_users.add(i.userid)
        yield DAU(date, len(unique_users))


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_nanme = "//home/test/search_dau"
    
    client.run_map_reduce(
        GetUidDateMapper(),
        GetDauReducer(),
        source_table=input_name,
        destination_table=output_nanme,
        reduce_by=["date"],
    )


if __name__ == "__main__":
    main()