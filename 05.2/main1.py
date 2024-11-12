TP = int(input())
FP = int(input())
TN = int(input())
FN = int(input())

precision = TP / (TP + FP)
recall = TP / (TP + FN)
accuracy = (TP + TN) / (TP + TN + FP + FN)

print(round(precision, 4), round(recall, 4), round(accuracy, 4), sep="\n")