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
class LOG2:
    userid: str
    timestamp: datetime.datetime
    action: str
    value: float
    testids: str
    sessionid: str


class FindSessions(TypedJob):
    def __call__(self, rows: Iterable[LOG]) -> Iterable[LOG2]:
        row = next(rows)
        userid = row.userid
        prev_timestamp = row.timestamp
        action = row.action
        value = row.value
        testids = row.testids
        c = 0
        yield LOG2(userid, prev_timestamp, action, value, testids, f"{userid}_{c}")
        for row in rows:
            timestamp = row.timestamp
            action = row.action
            value = row.value
            testids = row.testids
            if (timestamp - prev_timestamp).total_seconds() > 1800:
                c += 1
            prev_timestamp = timestamp
            yield LOG2(userid, timestamp, action, value, testids, f"{userid}_{c}")


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    input_name = "//home/data/tutorial/ytsaurus_intro/user_activity_log"
    output_name = "//home/test/user_sessions"
    tmp_name1 = "//tmp/ses_tmp1"

    client.run_sort(
        source_table=input_name,
        destination_table=tmp_name1,
        sort_by=["userid", "timestamp"]
    )
    client.run_reduce(
        FindSessions(),
        source_table=tmp_name1,
        destination_table=output_name,
        reduce_by=["userid"]
    )


if __name__ == "__main__":
    main()