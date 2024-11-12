import os
from yt.wrapper import YtClient


def main():
    YT_PROXY = os.getenv("YT_PROXY")
    client = YtClient(proxy=YT_PROXY, config={"proxy": {"enable_proxy_discovery": False}})
    data = []
    for table in client.search(
        "//home/data/tutorial/ytsaurus_intro/examine", node_type=["table"]):
        chunks = client.get(table + "/@chunk_count")
        size = client.get(table + "/@compressed_data_size")
        data.append((table.split("/")[-1], chunks, size))
    data.sort(key=lambda x: (x[1], x[2]), reverse=True)
    for i in data:
        print(*i)


if __name__ == "__main__":
    main()