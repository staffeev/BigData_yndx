import re
import csv


def find_number(s):
    return "".join(re.findall("\d", s))


with open("ticket_logs.csv", encoding="utf8") as file:
    data = csv.reader(file)
    d = {}
    for i in data:
        number = find_number(i[1])
        if len(number) == 11:
            d[i[0]] = d.get(i[0], []) + [number]


key = max(d.items(), key=lambda x: len(x[1]))
print(len(set(d[key[0]])))