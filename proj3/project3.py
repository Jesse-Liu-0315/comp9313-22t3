"using pyspark DataFrame API to do some analysis on the data"
# support content is not integer
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys
import itertools as it

# sort the context of each id by the frequency of the word
def sortFunc(s):
    index_dict = dict(zip(tokenPop, it.count()))
    return s[0], sorted(s[1], key=lambda x: index_dict[x]), s[2]

# get the prefix length of all context
def partitionFunc(s):
    numItem = len(s[1])
    l = numItem * simRate
    p = numItem - l + 1
    count = 0
    for item in s[1]:
        if count >= p or count >= numItem:
            return
        yield Row(item, s[2] + '-' + s[0]+','+' '.join(s[1]))
        count += 1



# count the similarity rate
def simFunc(s):
    #lambda x: (x['RID1'], x['RID2'],(len(set(x['file1'].split(" ")) & set(x['file2'].split(" "))) / (len(x['file1'].split(" ")) + len(x['file2'].split(" ")) - len(set(x['file1'].split(" ")) & set(x['file2'].split(" "))))))
    countDup = len(set(s[0].split(" ")) & set(s[1].split(" ")))
    return s[2], s[3], countDup / (len(s[0].split(" ")) + len(s[1].split(" ")) - countDup)

# list all the rid pairs not in the same group
def permutationsFunc(s):
    #lambda x: list(it.permutations(x, 2))
    tmp = []
    for item1 in s:
        for item2 in s:
            if item1 != item2:
                list1 = item1.split('-')
                list2 = item2.split('-')
                # [file1 content, file2 content]
                if list1[0] == '1' and list2[0] == '2':
                    tmp.append([list1[1], list2[1]])
    #print(tmp)
    return tmp


if __name__ == "__main__":
    
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    # enlarge thememory
    memory = '16g'
    pyspark_submit_args = ' --driver-memory ' + memory + ' pyspark-shell'
    os.environ["PYSPARK_SUBMIT_ARGS"] = pyspark_submit_args

    spark = SparkSession.builder.master("local").appName("similarity").getOrCreate()
    file1RDD = spark.sparkContext.textFile(sys.argv[1])
    file2RDD = spark.sparkContext.textFile(sys.argv[2])
    simRate = float(sys.argv[3])
    # create union dataframe
    file1 = file1RDD.map(lambda x : x.strip().split(" ")).map(lambda x: Row(RID = x[0] , content = x[1:], file = str(1)))
    file2 = file2RDD.map(lambda x : x.strip().split(" ")).map(lambda x: Row(RID = x[0] , content = x[1:], file = str(2)))
    file1DF = spark.createDataFrame(file1)
    file2DF = spark.createDataFrame(file2)
    fileDF = file1DF.union(file2DF)
    

    # sort token by frequency(map each word to a number)
    wordCount = fileDF.select(explode(fileDF.content).alias("word")).groupBy("word").agg(count("*").alias("count"))
    # sort the word by the number of times it appears
    wordCount = wordCount.orderBy(col("count").asc())
    # generate the list of word, from unpopluar to popular
    tokenPop = wordCount.select("word").collect()
    #['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
    tokenPop = [x[0] for x in tokenPop]
    


    # find similar id pairs
    # partition using prefixes

    # sort the context of each id & generate key-value pairs
    # ['RID': '1', content: 'a b c d e f g h i j', file: '1'] -> ['key': 'a', 'value': '1-1,a b c d e f g h i j']
    filePartition = fileDF.rdd.map(sortFunc).flatMap(partitionFunc)

    # verify similarity
    # reduce by key to get the similar id list
    # ['key': 'a', 'value': '1-1,a b c d e f g h i j'] -> ['key': 'a', 'value': '1-1,a b c d e f g h i j;1-2,a b c d e f g h i j']
    fileVerify = filePartition.reduceByKey(lambda x,y: x+';'+y)
    # list all the rid pairs not in the same group
    # ['key': 'a', 'value': '1-1,a b c d e f g h i j;1-2,a b c d e f g h i j'] -> ['key': 'a', 'value': ['1-1,a b c d e f g h i j','1-2,a b c d e f g h i j']] -> ['key': 'a', 'value': [['1,a b c d e f g h i j','21,a b c d e f g h i j'], ...]]
    fileExtract = fileVerify.map(lambda x: (x[0], x[1].split(';')))\
        .filter(lambda x: len(x[1]) > 1).mapValues(permutationsFunc).toDF()
    # explode all the rid pairs with content
    # ['key': 'a', 'value': [['1,a b c d e f g h i j','21,a b c d e f g h i j'], ...]] -> ['RID1': '1', 'RID2': '21', 'file1': 'a b c d e f g h i j', 'file2': 'a b c d e f g h i j']
    fileCombineKey = fileExtract.select(explode(fileExtract._2).alias("word"))\
                                .withColumn("file1", col("word")[0])\
                                .withColumn("file2", col("word")[1])\
                                .drop("word").distinct()
    fileCombineKey = fileCombineKey.withColumn("RID1", split(fileCombineKey['file1'], ",")[0].cast('integer'))\
                                    .withColumn("RID2", split(fileCombineKey['file2'], ",")[0].cast('integer'))\
                                    .withColumn("file1", split(fileCombineKey['file1'], ",")[1])\
                                    .withColumn("file2", split(fileCombineKey['file2'], ",")[1])
    # count the similarity rate
    res = fileCombineKey.rdd.map(simFunc).toDF(['RID1', 'RID2', 'sim1'])\
                                        .withColumn("sim1", round(col('sim1'), 6))\
                                        .filter(col('sim1') >= simRate)
    # save
    res.sort("RID1", "RID2").rdd.map(lambda x: "(" + str(x['RID1']) + "," + str(x['RID2']) + ")" + "\t" + str(x['sim1'])).saveAsTextFile(sys.argv[4])
    
    
     