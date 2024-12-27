https://leetcode.com/problems/managers-with-at-least-5-direct-reports/description/?envType=study-plan-v2&envId=top-sql-50

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("CreateDataFrame").getOrCreate()

# data
data = [[101, 'John', 'A', None], [102, 'Dan', 'A', 101], [103, 'James', 'A', 101], [104, 'Amy', 'A', 101], [105, 'Anne', 'A', 101], [106, 'Ron', 'B', 101]]

# schema
schema=['id', 'name', 'department', 'managerId']

# dataframe
df = spark.createDataFrame(data, schema)

# solution:

# step 1: found out the manager_id having at least 5 reportees
df1 = df.groupBy("manager_id").count().filter(col("count") >= 5)

# step 2: join the df and df1 to get find managers with at least 5 reportees
result_df = df.join(df1, df.id==df1.manager_id, how='inner')

# step 3: select name field and show
result_df.select("name").show()