import sys
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.3.1-bin-hadoop2.7\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 10)
lines = ssc.socketTextStream("localhost", 1234)
# Split each line into words
words = lines.flatMap(lambda line: line.split(" "))
words = words.filter(lambda x: (x not in x))
# Count each word in each batch
pairs = words.map(lambda word : (len(word), word))
pair1 = pairs.mapValues(lambda value: [value])
#wordCounts = pairs.reduceByKey(lambda x, y: x + " " + y)
wordCounts = pair1.reduceByKey(lambda x, y: x + y)
#char_counts = pairs.flatMap(lambda each: each[0]).map(lambda char: char).map(lambda c: (c, 1)).reduceByKey(lambda v1, v2: v1 + v2)
#char_counts = wordCounts.flatMap(lambda x : [(w.lower(), 1) for w in x.split(" ")]).reduceByKey(lambda x, y: x + y)


# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
#char_counts.pprint()
ssc.start()  # Start the computation
ssc.awaitTermination()
