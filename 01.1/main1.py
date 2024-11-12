import json
import csv

with open("01.1/cases.json", encoding="utf8") as file:
    data = json.load(file)


for i in data:
    i.pop("description")
data.sort(key=lambda x: x["date"], reverse=True)

with open("01.1/incidents.csv", "w", encoding="utf8", newline="") as file:
    table = csv.DictWriter(file, fieldnames=["creature", "place", "danger", "date"])
    table.writeheader()
    table.writerows(data)