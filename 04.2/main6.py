import csv

min_support = float(input())
min_confidence = float(input())

with open("store_data.csv", encoding="utf8") as file:
    data = list(map(str.strip, file.readlines()))

counts_pairs = {}
counts_ind = {}
for i in data:
    products = list(set(i.split(",")))
    for j in range(len(products)):
        a = products[j]
        counts_ind[a] = counts_ind.get(a, 0) + 1
        for z in range(j + 1, len(products)):
            b = products[z]
            counts_pairs[(a, b)] = counts_pairs.get((a, b), 0) + 1
            counts_pairs[(b, a)] = counts_pairs.get((b, a), 0) + 1

with open("output.csv", "w", encoding="utf8", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["first_item", "second_item", "confidence"])
    for i in counts_pairs:
        c1, c2 = counts_ind[i[0]], counts_ind[i[1]]
        conf = counts_pairs[i] / c1
        supp = counts_pairs[i] / len(data)
        if supp >= min_support and conf >= min_confidence:
            writer.writerow([i[0], i[1], str(conf)])
