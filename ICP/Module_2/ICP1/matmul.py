from pyspark.python.pyspark.shell import spark
from pyspark.sql import Row
from pyspark.serializers import PickleSerializer, AutoBatchedSerializer
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, LongType, DoubleType

def multiply_matrices(spark, A, B):
    # https://notes.mindprince.in/2013/06/07/sparse-matrix-multiplication-using-sql.html
    A.registerTempTable("A")

    #if enable_broadcast:
     #   B = broadcast(B)

    B.registerTempTable("B")
    return spark.sql("""
       SELECT
           A.row row,
           B.col col,
           SUM(A.val * B.val) val
       FROM
           A
       JOIN B ON A.col = B.row
       GROUP BY A.row, B.col
       """)


COO_MATRIX_SCHEMA = StructType([
    StructField('row', LongType()),
    StructField('col', LongType()),
    StructField('val', DoubleType())
])

mat_a = spark.sparkContext.parallelize([
    (0, 1, 1.0),
    (0, 4, 9.0),
    (1, 2, 3.0),
    (2, 3, 2.0)
])
mat_b = spark.sparkContext.parallelize([
    (0, 0, 1.0),
    (2, 1, 7.0),
    (3, 2, 2.0)
])

A = spark.createDataFrame(mat_a, COO_MATRIX_SCHEMA)
B = spark.createDataFrame(mat_b, COO_MATRIX_SCHEMA)

result = multiply_matrices(spark, A, B ).collect()
print(result)





