import os

#os.environ["SPARK_HOME"] = "C:\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("sample.txt", 1)

    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add)

    counts.saveAsTextFile("output2")
    sc.stop()

