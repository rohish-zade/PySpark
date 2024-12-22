# https://leetcode.com/problems/recyclable-and-low-fat-products/description/?envType=study-plan-v2&envId=top-sql-50

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# sample data
data = [
    (0, 'Y', 'N'),
    (1, 'Y', 'Y'),
    (2, 'N', 'Y'),
    (3, 'Y', 'Y'),
    (4, 'N', 'N')
]

# schema
columns = ["product_id", "low_fats", "recyclable"]

# Create the DataFrame
df = spark.createDataFrame(data, schema=columns)

# Write a solution to find the ids of products that are both low fat and recyclable.
result_df = df.filter((col("low_fats")=='Y') & (col("recyclable")=='Y'))
result_df.select(col("product_id")).show()