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
    action: str
    avg_per_user: float = 1


@yt_dataclass
class ActionStat:
    action: str
    avg_per_user: float


class GetUserMapper(TypedJob):
    def __call__(self, row: LOG) -> Iterable[UserValue]:
        yield UserValue(row.userid, row.action)
        yield UserValue(row.userid, "total")


class GetUserActionCount(TypedJob):
    def __call__(self, rows: Iterable[UserValue]) -> Iterable[UserValue]:
        row = next(rows)
        userid, action, value = row.userid, row.action, row.avg_per_user
        for i in rows:
            value += i.avg_per_user
        yield UserValue(userid, action, value)


class GetActionStats(TypedJob):
    def __call__(self, rows: Iterable[UserValue]) -> Iterable[ActionStat]:
        row = next(rows)
        action = row.action
        value = row.avg_per_user
        cnt = 1
        for i in rows:
            value += i.avg_per_user
            cnt += 1
        yield ActionStat(action, value / cnt)


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    tmp_name1 = "//tmp/action_stats_tmp1"
    tmp_name2 = "//tmp/action_stats_tmp2"
    output_name = "//home/test/action_stats"
    
    client.run_map_reduce(
        GetUserMapper(),
        GetUserActionCount(),
        source_table=input_name,
        destination_table=tmp_name1,
        reduce_by=["userid", "action"],
    )
    client.run_sort(
        source_table=tmp_name1,
        destination_table=tmp_name2,
        sort_by=["action"]
    )
    client.run_reduce(
        GetActionStats(),
        source_table=tmp_name2,
        destination_table=output_name,
        reduce_by=["action"]
    )


if __name__ == "__main__":
    main()