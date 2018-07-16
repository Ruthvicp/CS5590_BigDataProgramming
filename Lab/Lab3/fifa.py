from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.types import DoubleType

## a. Import the dataset and create df and print Schema
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("WorldCupMatches.csv",header=True);
df.show()
print(df.schema)

## b. Perform  10  intuitive  questions  in  Dataset  (e.g.:  pattern 
##    recognition,  topic discussion, most  important  terms,  etc.).
##   Use  your  innovation  to  think  out  of box.

# 1 Renaming columns
df=df.withColumnRenamed('Home Team Name', 'Home_Team')
df=df.withColumnRenamed('Away Team Name', 'Away_Team')
df=df.withColumnRenamed('Away Team Goals', 'Away_Team_Goals')
df=df.withColumnRenamed('Home Team Goals', 'Home_Team_Goals')
# 2 Changing the structType of columns from String to Double
df=df.withColumn('Away_Team_Goals',df['Away_Team_Goals'].cast(DoubleType()))
df=df.withColumn('Home_Team_Goals',df['Home_Team_Goals'].cast(DoubleType()))
print(df.schema)
# 3 Filter football matches from year 2014
df.filter(df.Year.like("2014")).show()   ## year 2014
# 4 Count the total no. of matches played in 2014
print("Matches in 2014 : " +str(df.filter(df.Year.like("2014")).count()))
# 5 Count the no.of times Argetina has appeared in semi-finals
df.filter(df.Stage.like("Semi-finals") & (df.Home_Team == 'Argentina')).count()
# 6 Display all the finale matches
df.filter(df.Stage=='Final').show()
# 7 pairing stage wise matches with home team
df.crosstab('Stage','Home_Team').show()
# 8 finding mean, max, min etc of the attendance over the entire matches
df.describe(['Attendance']).show()
# 9 Display the max away goals scored year wise
df.groupBy('Year').max('Away_Team_Goals').dropna().show()
# 10 Display the sum of home goals scored, grouped by each of the each team country
df.groupBy('Home_Team').sum('Home_Team_Goals').orderBy('Home_Team').dropna().show(15,truncate=True)

print("--------------------------------")

# Perform any 5 queries in Spark RDD’s and Spark Data Frames. 
# Compare the results
import os

os.environ["SPARK_HOME"] = "D:\\spark-2.3.1-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="C:\\Program Files\\Hadoop\\"
from operator import add

from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    lines = sc.textFile("WorldCups.csv", 1)
    header = lines.first()
    content = lines.filter(lambda line: line != header)
    # creating an RDD
    rdd = content.map(lambda line: (line.split(","))).collect()
    #print(rdd)
    # no. of Columns
    rdd_len = content.map(lambda line: len(line.split(","))).distinct().collect()
    print(rdd_len)
    print("---------- 1. Venues & goals scored ------------")
    # venue - hosted country with highest goals (From RDD)
    rdd1 = (content.filter(lambda line: line.split(",")[6] != "NULL")
    .map(lambda line: (line.split(",")[1], int(line.split(",")[6])))
    .takeOrdered(10, lambda x : -x[1]))
    print(rdd1)
    
    from pyspark.sql.types import StructType
    from pyspark.sql.types import StructField
    from pyspark.sql.types import StringType,IntegerType
    schema = StructType([StructField('Year', StringType(), True),
                         StructField('Country', StringType(),True),
                         StructField('Winner', StringType(), True),
                         StructField('Runners-Up', StringType(),True),
                         StructField('Third', StringType(),True),
                         StructField('Fourth', StringType(),True),
                         StructField('GoalsScored', StringType(),True),
                         StructField('QualifiedTeams', StringType(),True),
                         StructField('MatchesPlayed', StringType(),True),
                         StructField('Attendance', StringType(),True)])
    # Create data frame from the RDD
    df = spark.createDataFrame(rdd,schema)
    df.show()
    df=df.withColumn('GoalsScored',df['GoalsScored'].cast(IntegerType()))
    df=df.withColumnRenamed('Runners-Up', 'Runnersup')
    # venue - hosted country with highest goals (From DF)
    df.select("Country","GoalsScored").orderBy("GoalsScored", ascending = False).show(20, truncate = False)
    # venue - hosted country with highest goals (From DF - SQL)
    df.createOrReplaceTempView("df_table")
    spark.sql(" SELECT Country,GoalsScored FROM df_table order by "+ 
              "GoalsScored Desc Limit 10").show()
    
    print("---------- 2. Year, venue country = winning country ------------")
    # using RDD
    (content.filter(lambda line:line.split(",")[1]==line.split(",")[2])
    .map(lambda line: (line.split(",")[0],line.split(",")[1], line.split(",")[2]))
    .collect())
    # using DF
    df.select("Year","Country","Winner").filter(df["Country"]==df["Winner"]).show()
    # using DF - SQL
    spark.sql(" SELECT Year,Country,Winner FROM df_table where Country == Winner order by Year").show()
    
    print("-------------- 3. Details of years ending in ZERO ---------------")
    # using RDD
    years = ["1930","1950","1970","1990","2010"]
    (content.filter(lambda line: line.split(",")[0] in years)
    .map(lambda line: (line.split(",")[0],line.split(",")[2],line.split(",")[3])).collect())
    # using DF
    df.select("Year","Winner","Runnersup").filter(df.Year.isin(years)).show()
    # using DF - SQL
    
    spark.sql(" SELECT Year,Winner,Runnersup FROM df_table  WHERE " +
              " Year IN ('1930','1950','1970','1990','2010') ").show()
    
    print("-------------- 4. 2014 world cup stats --------------")
    # using RDD
    (content.filter(lambda line:line.split(",")[0]=="2014")
    .map(lambda line: (line.split(","))).collect())
    # using DF
    df.filter(df.Year=="2014").show()
    # using DF - Sql
    spark.sql(" Select * from df_table where Year == 2014 ").show()
    
    print("------------- 5. Max matches played -----------------")
    # Using RDD
    (content.filter(lambda line:line.split(",")[8] == "64")
    .map(lambda line: (line.split(","))).collect())
    # using DF
    df=df.withColumn('MatchesPlayed',df['MatchesPlayed'].cast(IntegerType()))
    df.filter(df.MatchesPlayed == 64).show()
    # using DF - SQL
    spark.sql(" Select * from df_table where MatchesPlayed in " +
              "(Select Max(MatchesPlayed) from df_table )" ).show()
    
    
    # https://datascienceplus.com/dataframes-vs-rdds-in-spark-part-1/

    
                    