import os
#os.environ["SPARK_HOME"] = "D:\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="C:\winutils"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    #Basic Transformation

    nums = sc.parallelize([1, 2, 3])
    # Pass each element through a function
    squares = nums.map(lambda x: x*x)

    # => {1, 4, 9}# Keep elements passing a predicate
    even = squares.filter(lambda x: x % 2 == 0)

    # => {4}# Map each element to zero or more others
    nums.flatMap(lambda x: range(0, x))
    # => {0, 0, 1, 0, 1, 2}


    #Basic Actions

    # Retrieve RDD contents as a local collection
    #Collect throws the Py4JavaError, to use take or saveAsTextFile in it's place
    #numsCollect = nums.collect() # => [1, 2, 3]

    #  Return first K elements
    numsTake=nums.take(2)   # => [1, 2]

    for n in numsTake:
        print(n)

    #  Count number of elements
    # Count throws the Py4JavaError, to use take or saveAsTextFile in it's place
    #print(nums.count())   # => 3

    #  Merge elements with an associative function
    # numsReduced=nums.reduce(lambda x, y: x + y)  # => 6

    #  Write elements to a text file
    nums.saveAsTextFile("numsSaved")

    #Key-Value Operations

    pets = sc.parallelize([('cat', 1), ('dog', 1), ('cat', 2)])
    petsReduced=pets.reduceByKey(lambda x,y: x + y)  # => {(cat, 3), (dog, 1)}
    petsReduced.saveAsTextFile("petsReduced")

    petsGrouped = pets.groupByKey()# => {(cat, Seq(1, 2)), (dog, Seq(1)}

    petsGrouped.saveAsTextFile("petsGrouped")

    #petsSorted = pets.sortByKey()# => {(cat, 1), (cat, 2), (dog, 1)}

    #petsSorted.saveAsTextFile("petsSorted")