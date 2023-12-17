from turtle import shape
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

import sys

# remove the stop word and duplicated word in each headline
def removeStopWord(s):
    s1 = {}
    for item in s:
        if item in stopword:
            s.remove(item)
        else:
            s1[item] = 1
    return s1


if __name__ == "__main__":
    
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    spark = SparkSession.builder.master("local").appName("detectpopular").getOrCreate()
    fileRDD = spark.sparkContext.textFile(sys.argv[1])
    stopword = spark.sparkContext.textFile(sys.argv[3]).flatMap(lambda line: line.split()).collect() #collect the stopword in a list
    
    # create dataframe
    headline = fileRDD.map(lambda x : x.split(","))\
            .map(lambda x: Row(year = x[0][0:4], headline = removeStopWord(x[1].split(" "))))
    headlineDF = spark.createDataFrame(headline)
    # split a headline
    splitHeadDF = headlineDF.select(headlineDF.year, explode(headlineDF.headline).alias("word", "value"))

    # groupHead.value = dataframe of the number of headlines containing t in y
    groupHead = splitHeadDF.groupBy("year|4   |50", "word").agg(sum("value").alias("value"))
    # numYear = the number of headlines containing t in y
    numYearDF = splitHeadDF.groupBy("year").agg(sum("value"))
    numYear = numYearDF.count()
    # numYearWordDF.value2 = dataframe of the number of years having t
    numYearWordDF = groupHead.select("year","word").groupBy("word").agg(count("*").alias("value2"))
    


    # join groupHead in numYearWordDF as a, b where a.word = b.word, add number of years in dataset in each row 
    res = groupHead.join(numYearWordDF, "word").withColumn("numYear", lit(numYear))
    # count the exact result value for each word in each year
    res = res.withColumn("res", round(res.value * log10(res.numYear/res.value2), 6))
    res = res.select(res.year, res.word, res.res)
    # select the top n words in each year
    windowDept = Window.partitionBy("Year").orderBy(col("year"), col("res").desc(), col("word"))
    resTop = res.withColumn("row", row_number().over(windowDept)).filter(col("row") <= sys.argv[4])
    # combine the word in each year
    resFinal = resTop.select("year", concat_ws(",", resTop.word, resTop.res).alias("pairRes")).groupBy("year")\
            .agg(collect_list("pairRes").alias("listRes"))\
            .withColumn("fullRes", concat_ws(";", "listRes"))\
            .select(concat_ws("\t", "year", "fullRes"))
    #resMerge = resTop.select("year", concat_ws(",", resTop.word, resTop.res).alias("pairRes"))
    #resStr = resMerge.groupBy("year").agg(collect_list("pairRes").alias("listRes"))
    #resCom = resStr.withColumn("fullRes", concat_ws(";", "listRes")).select("year", "fullRes")
    #resFin = resCom.select(concat_ws("\t", resCom.year, resCom.fullRes))
    resFinal.write.text(sys.argv[2])
    #resFinal.show(50)