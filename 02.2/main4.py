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
class MonthValue:
    month: str
    value: float


class GetMonthMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[MonthValue]:
        if row.action == "confirmation":
            yield MonthValue(row.timestamp.strftime("%Y-%m"), row.value)


class GetRevenueReducer(TypedJob):
    def __call__(self, rows: Iterable[MonthValue]) -> Iterable[MonthValue]:
        month = None
        value = 0
        for i in rows:
            month = i.month
            value += i.value
        yield MonthValue(month, value)


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_nanme = "//home/test/revenue_by_month"
    
    client.run_map_reduce(
        GetMonthMapper(),
        GetRevenueReducer(),
        source_table=input_name,
        destination_table=output_nanme,
        reduce_by=["month"],
    )


if __name__ == "__main__":
    main()