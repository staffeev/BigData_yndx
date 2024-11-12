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
class UserMonth:
    userid: str
    month: str


@yt_dataclass
class Dummy:
    dummy: int
    year: int
    month: int
    count: int


@yt_dataclass
class MonthStats:
    month: str
    count: int


class GetUserMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserDate]:
        yield UserDate(row.userid, row.timestamp.date())


class GetFirstAction(TypedJob):
    def __call__(self, rows: Iterable[UserDate]) -> Iterable[UserMonth]:
        row = next(rows)
        userid = row.userid
        first_act = row.date
        for i in rows:
            if i.date < first_act:
                first_act = i.date
        yield UserMonth(userid, first_act.strftime("%Y-%m"))
    

class GetMonthStats(TypedJob):
    def __call__(self, rows: Iterable[UserMonth]) -> Iterable[Dummy]:
        row = next(rows)
        month = row.month
        year, month = map(int, month.split("-"))
        count = 1
        for i in rows:
            count += 1
        yield Dummy(1, year, month, count)


class GetMaxMonth(TypedJob):
    def __call__(self, rows: Iterable[Dummy]) -> Iterable[MonthStats]:
        max_cnt = -1
        max_month = (2000, 1)
        for i in rows:
            if i.count >= max_cnt:
                if i.year >= max_month[0] and i.month >= max_month[1]:
                    max_cnt = i.count
                    max_month = (i.year, i.month)
        month = datetime.date(max_month[0], max_month[1], 1).strftime("%Y-%m")
        yield MonthStats(month, max_cnt)


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_name = "//home/test/max_acquisition_month"
    tmp_name1 = "//tmp/max_month_tmp1"
    tmp_name2 = "//tmp/max_month_tmp2"
    tmp_name3 = "//tmp/max_month_tmp3"
    tmp_name4 = "//tmp/max_month_tmp4"
    
    client.run_map_reduce(
        GetUserMapper(),
        GetFirstAction(),
        source_table=input_name,
        destination_table=tmp_name1,
        reduce_by=["userid"]
    )
    client.run_sort(
        source_table=tmp_name1,
        destination_table=tmp_name2,
        sort_by=["month"]
    )
    client.run_reduce(
        GetMonthStats(),
        source_table=tmp_name2,
        destination_table=tmp_name3,
        reduce_by=["month"]
    )
    client.run_sort(
        source_table=tmp_name3,
        destination_table=tmp_name4,
        sort_by=["dummy"]
    )
    client.run_reduce(
        GetMaxMonth(),
        source_table=tmp_name4,
        destination_table=output_name,
        reduce_by=["dummy"]
    )


if __name__ == "__main__":
    main()