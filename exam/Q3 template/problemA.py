from pyspark import SparkContext, SparkConf
import sys

class Problem:  
    

    def run(self, inputPath, outputPath):
        def func(s):
            input = s.split(' ')
            for item in rankList:
                if input[1] == item[0]:
                    return (input[0] + ": " + input[1] + "," + str(item[1].index((input[0], input[2])) + 1))

        
        conf = SparkConf().setAppName("project2_rdd")
        sc = SparkContext(conf=conf)
        fileRDD = sc.textFile(inputPath)

        fileRDD.persist()
        rank = fileRDD.map(lambda x: (x.split(" ")[1], (x.split(" ")[0], x.split(" ")[2])))
        rank = rank.groupByKey().mapValues(list).mapValues(lambda x: sorted(x, key = lambda x: x[1], reverse = True))
        rankList = rank.collect()

        rankRDD = fileRDD.map(func)

        rankRDD.saveAsTextFile(outputPath)
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong arguments")
        sys.exit(-1)
    Problem().run(sys.argv[1], sys.argv[2])