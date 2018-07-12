from pyspark.sql import SparkSession
from pyspark.sql import *


## 1. Import the dataset and create data framesdirectly on import.
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("C:\\Users\\ruthv\\PycharmProjects\\CS5590_BigDataProgramming\\Spark Python Code\\ICP3\\ConsumerComplaints.csv",header=True);
#df.show()
#print(df.collect())

## 2. Save to file - in the end

## 3. Write aparseLinemethod to split the comma-delimited row and create a Data frame
df.createOrReplaceTempView("consumer")

#print(df.columns)

#df.groupBy(df.columns).count().show()

df1 = df.limit(5)
df2 = df.limit(10)
unionDf = df1.unionAll(df2)
unionDf.show()

unionDf.orderBy('Company').show()

## 6. Zip codes
df.orderBy('Zip Code').show()

##  --------------------- PART 2 --------------------- ##

## 7. Aggregate functions

sqlDF = spark.sql("SELECT max(`Zip Code`) FROM consumer")
sqlDF.show()

sqlDF = spark.sql("SELECT avg(`Zip Code`) FROM consumer")
sqlDF.show()

## 8. duplicates

df.dropDuplicates()
df.show()

## 9. 13th row
df13 = df.take(13)
df13[-1]

## 10. Join operation
joined_df = df1.join(df2, df1.Company == df2.Company)


# bonus
spark.sql("create table consumer_hive as select * from consumer");
sqlContext = HiveContext(sc)


sqlContext.sql("use default")
sqlContext.sql("show tables").show()
