from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys

class Problem:  

    def run(self, inputPath, outputPath):

        spark = SparkSession.builder.master("local").appName("Problem").getOrCreate()
        fileDF = spark.read.text(inputPath)

        grades = fileDF.withColumn("grades", split(col("value"), ":")[1])\
                    .withColumn("grades", split(col("grades"), ";"))\
                    .select(explode(col("grades")).alias("grades"))
        gradesPair = grades.withColumn("grades", split(col("grades"), ","))\
                    .withColumn("course", col("grades")[0])\
                    .withColumn("grade", col("grades")[1])\
                    .select("course", "grade")
        avgDF = gradesPair.groupBy("course").agg(avg("grade").alias("avgGrade")).orderBy(col("course").asc())
    

        avgDF.write.format('csv').save(outputPath)
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong arguments")
        sys.exit(-1)
    Problem().run(sys.argv[1], sys.argv[2])