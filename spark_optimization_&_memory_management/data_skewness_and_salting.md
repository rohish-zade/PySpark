# Data skewness in Spark
Data skewness in Apache Spark refers to an uneven distribution of data across partitions. This imbalance causes certain partitions to contain significantly more data than others, leading to performance issues due to excessive computation on a few worker nodes while others remain underutilized.

Data skewness in Spark happens when a small percentage of partitions get most of the data being processed.

### Causes
- This can be caused by `transformations` that change data partitioning like `join`, `groupBy`, and `orderBy`. For example, joining on a key that is not evenly distributed across the cluster, causing some partitions to be very large and not allowing Spark to process data in parallel.
- Imbalanced Data Distribution – Some partitions may have a lot more data than others due to how the data is structured or written.
- Uneven File Sizes in HDFS/S3 – Some files may be significantly larger, leading to an imbalance in task execution.

### OUTCOME
- **Slow-Running Stages/Tasks:** certain operations will take very long because a given worker is working with too much data.
- **Spilling Data to Disk:** if data does not fit in memory on a worker, it will be written to disk which takes much longer.
- **Out of Memory errors:** if worker runs out of disk space, an error is thrown.

### How to Detect Data Skewness?
- **Check Job Execution in UI** – The Spark UI will show certain tasks taking significantly longer to complete.
- **Check Partition Sizes** – Use df.rdd.glom().map(len).collect() to inspect partition sizes.
-` Check Skewed Keys in Joins` – Run df.groupBy("key").count().orderBy(desc("count")).show(10) to find keys with excessive records.

------

# Solutions for Data Skewness

## 1. Increase the Number of Partitions
- Repartition large datasets using repartition(n) or df.repartition("key").

## 2. Use `broadcast join` for Small Datasets
- If one dataset is small, broadcast it to avoid shuffle:

## 3. Adaptive Query Execution (AQE)
- Spark 3.x has built-in skew join optimization (`spark.sql.adaptive.skewJoin.enabled = true`).

## 4. Handling Data Skewness in Spark with Salting

We face data skewness in Spark where one or a few partitions accumulate all the data, leading to inefficient processing and slower job performance. The solution is **Salting**.

---

### What is Salting?

Salting is a technique to evenly distribute data across partitions by "salting" keys with random values to break up hotspots that cause data skew. It balances workload, improves parallelism, and enhances performance in Spark jobs.

---

### How It Works

Imagine we are joining two tables on `customer_id`, but 90% of the data has the same `customer_id`. Without salting, this causes Spark to dump a huge portion of the data into one partition, leading to long processing times.

By **salting**, we add a random suffix to `customer_id` (e.g., `customer_id_1`, `customer_id_2`) and do the same for both tables involved in the join. This "breaks up" the skewed data into multiple and evenly distributed keys.

---

### Steps to Apply Salting

1. **📍 Add Salt to Keys**  
   Generate a random salt value and append it to the join key. For example, if we have `customer_id`, we create keys like `customer_id_1`, `customer_id_2`, etc.

2. **📍 Replicate Data for the Join**  
   Ensure both tables involved in the join have matching salted keys. 

3. **📍 Perform the Join**  
   Spark distributes the join more evenly across partitions using salted keys.

---

### Code Example

```python
from pyspark.sql import functions as F

# 1️⃣ Add salt to the join keys
salted_df1 = df1.withColumn("salt", F.expr("floor(rand() * 10)"))
salted_df2 = df2.withColumn("salt", F.expr("floor(rand() * 10)"))

# 2️⃣ Create salted join keys
salted_df1 = salted_df1.withColumn("join_key", F.concat(F.col("customer_id"), F.col("salt")))
salted_df2 = salted_df2.withColumn("join_key", F.concat(F.col("customer_id"), F.col("salt")))

# 3️⃣ Join on salted keys
result = salted_df1.join(salted_df2, "join_key")
```

### Key Takeaways

- 🔸 Salting is highly effective for joins and aggregations where data skew is an issue.
- 🔸 It improves performance and resource utilization by spreading data across partitions.