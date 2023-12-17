a = "the sky is dark the moon is bright"
b = "the moon in the sky is bright"

aList = []
bList = []

for i in range(len(a) - 1):
    aList.append(a[i] + a[i+1])
for i in range(len(b) - 1):
    bList.append(b[i] + b[i+1])

same = 0
nosame = 0
for i in aList:
    for j in bList:
        if i == j:
            same += 1
        else:
            nosame += 1