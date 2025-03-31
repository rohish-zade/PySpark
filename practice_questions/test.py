# Task: Perform a simple arithmetic operation on DataFrame columns in PySpark.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("ArithmeticOperation").gerOrCreate()

""" Question: Given a PySpark DataFrame with two columns col1 and col2, perform the following arithmetic operations:
- Add the values of col1 and col2
- Subtract the values of col1 from col2
- Multiply the values of col1 and col2
- Divide the values of col1 by col2 (assuming no division by zero) """

# Sample Data:
data = [
    (10, 5), 
    (20, 4),
    (30, 3),
    (40, 2),
    (50, 1)
] 

# Create DataFrame 
df = spark.createDataFrame(data, ["col1", "col2"])
df.show()

# perform the arithmetic operations
df1 = df.withColumn("addition", col("col1") + col("col2")) \
        .withColumn("Subtraction", col("col1") - col("col2")) \
        .withColumn("product", col("col1") * col("col2")) \
        .withColumn("division", col("col1") / df.when(col("col2")==0, None).otherwise(col("col2")))
df1.show()
