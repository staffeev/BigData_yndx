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
    action: str
    date: datetime.date


@yt_dataclass
class UserDateFlag:
    userid: str
    date: datetime.date
    flag: int


@yt_dataclass
class DateState:
    date: datetime.date
    users_share: float


class GetUserMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserDate]:
        yield UserDate(row.userid, row.action, row.timestamp.date())


class GetFirstAction(TypedJob):
    def __call__(self, rows: Iterable[UserDate]) -> Iterable[UserDateFlag]:
        row = next(rows)
        userid = row.userid
        date = row.date
        product = row.action == "product"
        checkout = row.action == "checkout"
        for i in rows:
            product = product or (i.action == "product")
            checkout = checkout or (i.action == "checkout")
        flag = int(product and not checkout)
        yield UserDateFlag(row.userid, row.date, flag)
    

class GetDateStats(TypedJob):
    def __call__(self, rows: Iterable[UserDateFlag]) -> Iterable[DateState]:
        row = next(rows)
        date = row.date
        value = row.flag
        cnt = 1
        for row in rows:
            cnt += 1
            value += row.flag
        yield DateState(date, float(value) / float(cnt))


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_name = "//home/test/no_checkouts_ratio"
    tmp_name1 = "//tmp/ratio_tmp1"
    tmp_name2 = "//tmp/ratio_tmp2"
    
    client.run_map_reduce(
        GetUserMapper(),
        GetFirstAction(),
        source_table=input_name,
        destination_table=tmp_name1,
        reduce_by=["userid", "date"]
    )
    client.run_sort(
        source_table=tmp_name1,
        destination_table=tmp_name2,
        sort_by=["date"]
    )
    client.run_reduce(
        GetDateStats(),
        source_table=tmp_name2,
        destination_table=output_name,
        reduce_by=["date"]
    )


if __name__ == "__main__":
    main()