import os
from pyspark.ml.feature import VectorAssembler
os.environ["SPARK_HOME"] = "C:\\spark-2.3.1-bin-hadoop2.7\\spark-2.3.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
#import numpy
# Load training data
from pyspark.ml.linalg import SparseVector
# from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

data = spark.read.load("C:/Users/ruthv/.spyder-py3/PySpark_MLib/classification/Absenteeism_at_work.csv", format="csv", header=True, delimiter=",")
data = data.withColumn("df", data["Disciplinary_failure"] - 0).withColumn("label", data['Reason for absence'] - 0)
data.show()
assem = VectorAssembler(inputCols=["df"], outputCol='features')
data = assem.transform(data)
# Split the data into train and test
train,test = data.randomSplit([0.6, 0.4], 1234)
# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
# train the model
model = nb.fit(train)
# select example rows to display.
predictions = model.transform(test)
predictions.show()
# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
