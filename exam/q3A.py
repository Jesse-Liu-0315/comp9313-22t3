import math
import sys
import functools
from pyspark import SparkContext, SparkConf

def func(s):
    input = s.split(' ')
    for item in rankList:
        if input[1] == item[0]:
            return (input[0] + ": " + input[1] + "," + str(item[1].index((input[0], input[2])) + 1))



if __name__ == "__main__":
    sc = SparkContext('local','rank')
    text = sc.textFile("sample.txt")
    text.persist()
    rank = text.map(lambda x: (x.split(" ")[1], (x.split(" ")[0], x.split(" ")[2])))
    rank = rank.groupByKey().mapValues(list).mapValues(lambda x: sorted(x, key = lambda x: x[1], reverse = True))
    rankList = rank.collect()

    res = text.map(func)
    res.saveAsTextFile("output")