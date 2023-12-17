import math
import sys
import functools
from pyspark import SparkContext, SparkConf

def removeStopWord(s):
    duplicate = {}
    for item in s:
        if item in stopword.value:
            s.remove(item)
        elif item in duplicate:
            s.remove(item)
        else:
            duplicate[item] = 1
    return s

def numHeadFun(s):
    s1 = {}
    for list in s:
        for item in list:
            if item in s1:
                s1[item] += 1
            else:
                s1[item] = 1
    s2 = []
    for k in s1:
        s2.append((k, s1[k]))
    return s2


def func(s):
    s1 = []
    for item in s:
        s1.append((item[0], round(item[1] * math.log10(countYear / countWordPerYear[item[0]]), 6)))
    return s1

# compare for sort
def cmp(a, b):
    return (a > b) - (a < b) 

# compare for sort
def strcmp(str1,str2):
    i = 0
    while i<len(str1) and i<len(str2):
            outcome = cmp(str1[i],str2[i])
            if outcome:
                    return outcome
            i +=1
    return cmp(len(str1),len(str2))

# compare for sort
def compare(x, y):
    if (y[1] - x[1]) == 0:
        return strcmp(x[0],y[0])
    else:
        return y[1] - x[1]

def sort(s):
    s.sort(key = functools.cmp_to_key(compare))
    return s

def take(s):
    if takeTop >= len(s):
        return s
    s1 = []
    for i in range(takeTop):
        s1.append(str(s[i][0] + "," + str(s[i][1])))
    
    return ";".join(s1)
        
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    sc = SparkContext('local','detectpopular')
    text = sc.textFile(sys.argv[1])
    # broadcast stopward
    stopword = sc.broadcast(sys.argv[3])
    # take top n word
    takeTop = int(sys.argv[4])

    # map -> (year, [word1, word2, word3, ...])
    # mapValue(removeStopWord) -> remove stopword and duplicated word
    # groupByKey() -> (year, [[word1, word2, ...], [word1, word2, ...]])
    # mapValues(numHeadFun) -> (year, [(word1, 3), (word2, 2), ...])
    tmpPair = text.map(lambda x: (x[0:4], x.split(",")[1].split(" "))).mapValues(removeStopWord).groupByKey().mapValues(numHeadFun)
    # count the number of years in dataset
    tmpPair.persist()
    countYear = tmpPair.count()
    
    # count the number of years having t
    tmpList = tmpPair.map(lambda x: x[1]).collect()
    countWordPerYear = {}
    for item in tmpList:
        for k in item:
            if k[0] in countWordPerYear:
                countWordPerYear[k[0]] += 1
            else:
                countWordPerYear[k[0]] = 1
    # caculate the result -> sort -> take top n
    res = tmpPair.mapValues(func).mapValues(sort).mapValues(take).sortByKey()
    res.map(lambda x : "%s\t%s" %(x[0], x[1])).saveAsTextFile(sys.argv[2])
    sc.stop()