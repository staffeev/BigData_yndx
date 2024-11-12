import csv
from collections import defaultdict, deque


def bfs(node, g, visited):
    visited[node] = True
    deq = deque([node])
    count = 0
    path = [node]
    while deq:
        cur = deq.popleft()
        for i, c in g.get(cur, {}).items():
            if not visited[i]:
                visited[i] = True
                deq.append(i)
                path.append(i)
                count += c
    return count, path


data = list(csv.reader(open("01.1/transaction_logs.csv", encoding="utf8")))

edges = defaultdict(lambda: defaultdict(int))
emails, numbers = set(), set() 
indexes = {}
em_ix, num_ix = -1, -1
em_dix, num_dix = {}, {}
indexes = []
for (em, num) in data:
    if em not in em_dix:
        em_ix += 1
        em_dix[em] = em_ix
    if num not in num_dix:
        num_ix += 1
        num_dix[num] = num_ix
    indexes.append((em_dix[em], num_dix[num]))

for (em, num) in indexes:
    edges[em][num + len(em_dix)] += 1
    edges[num + len(em_dix)][em] += 1

n = len(em_dix) + len(num_dix)
visited = [False] * n
res = defaultdict(int)

for i in range(n):
    if not visited[i]:
        cur_res, path = bfs(i, edges, visited)
        res[i] += cur_res

print(max(res.values()))



    

