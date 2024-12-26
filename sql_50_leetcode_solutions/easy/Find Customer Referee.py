# https://leetcode.com/problems/find-customer-referee/?envType=study-plan-v2&envId=top-sql-50

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# sample data
data = [
    [1, 'Will', None], 
    [2, 'Jane', None], 
    [3, 'Alex', 2],
    [4, 'Bill', None], 
    [5, 'Zack', 1], 
    [6, 'Mark', 2]
]

# schema
columns=['id', 'name', 'referee_id']

df = spark.createdataFrame(data, columns)

# solution
result_df = df.filter((col("referee_id").isNull()) | (col("referee_id") != 2))
result_df.select("name").show

# alternate solution: using coalesce

from pyspark.sql.functions import coalesce, lit
df.filter(coalesce(col("referee_id"), lit(-1)) != 2).select(col("name")).show()

"""
☑️ If referee_id is null, coalesce(col("referee_id"), lit(-1)) will return -1.
  - The condition (-1 != 2) will be True, so this row will be included in the result.
  
☑️ If referee_id is a value that is not 2, say 3, 
then coalesce(col("referee_id"), lit(-1)) will return 3.
  - The condition (3 != 2) will be True, so this row will also be included.
  
☑️ If referee_id is 2, coalesce(col("referee_id"), lit(-1)) will return 2.
  - The condition (2 != 2) will be False, so this row will be excluded.
"""