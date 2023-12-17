from turtle import shape
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("Problem").getOrCreate()
    fileDF = spark.read.text("sample1.txt")
    grades = fileDF.withColumn("grades", split(col("value"), ":")[1])\
                    .withColumn("grades", split(col("grades"), ";"))\
                    .select(explode(col("grades")).alias("grades"))
    gradesPair = grades.withColumn("grades", split(col("grades"), ","))\
                    .withColumn("course", col("grades")[0])\
                    .withColumn("grade", col("grades")[1])\
                    .select("course", "grade")
    res = gradesPair.groupBy("course").agg(avg("grade").alias("avgGrade")).orderBy(col("course").asc())
    res.write.format('csv').save("output1")