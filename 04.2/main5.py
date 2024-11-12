import csv


min_support = float(input())

with open("store_data.csv", encoding="utf8") as file:
    data = list(map(str.strip, file.readlines()))

counts = {}
for i in data:
    products = i.split(",")
    for j in range(len(products)):
        for z in range(j + 1, len(products)):
            a, b = products[j], products[z]
            counts[(a, b)] = counts.get((a, b), 0) + 1
            counts[(b, a)] = counts.get((b, a), 0) + 1

with open("output.csv", "w", encoding="utf8", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["first_item", "second_item", "support"])
    for i in counts:
        counts[i] /= len(data)
        if counts[i] >= min_support:
            writer.writerow([i[0], i[1], str(counts[i])])
