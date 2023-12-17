import math
import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    def removeStopWord(s):
        for item in s:
            if item in stopword.value:
                s.remove(item)
        return s
    
    def toPair(s):
        s1 = []
        for item in s:
            s1.append((item,1))
        return s1

    def numHeadFun(s):
        s1 = {}
        for list in s:
            s2 = {}
            for item in list:
                if item[0] not in s2:
                    s2[item[0]] = 1
            for k in s2:
                if k in s1:
                    s1[k] = s1[k] + 1
                else:
                    s1[k] = 1
        return s1
    

    sc = SparkContext('local','detectpopular')
    text = sc.textFile(sys.argv[1])
    stopword = sc.broadcast(sys.argv[3])
    tmpPair = text.map(lambda x: (x[0:4], x.split(",")[1].split(" "))).mapValues(removeStopWord).mapValues(toPair)
    reduceByYear = tmpPair.groupByKey().mapValues(numHeadFun)
    reduceByYear.persist()
    print(reduceByYear.count)
    #print("++++++++++++++++++++++++++++++++++++")
    reduceByYear.saveAsTextFile(sys.argv[2])
    sc.stop()