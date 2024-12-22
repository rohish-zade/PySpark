# Handling Data Skewness in Spark with Salting

We face data skewness in Spark where one or a few partitions accumulate all the data, leading to inefficient processing and slower job performance. The solution is **Salting**.

---

## ✅ What is Salting?

Salting is a technique to evenly distribute data across partitions by "salting" keys with random values to break up hotspots that cause data skew. It balances workload, improves parallelism, and enhances performance in Spark jobs.

---

## ✅ How It Works

Imagine we are joining two tables on `customer_id`, but 90% of the data has the same `customer_id`. Without salting, this causes Spark to dump a huge portion of the data into one partition, leading to long processing times.

By **salting**, we add a random suffix to `customer_id` (e.g., `customer_id_1`, `customer_id_2`) and do the same for both tables involved in the join. This "breaks up" the skewed data into multiple and evenly distributed keys.

---

## ✅ Steps to Apply Salting

1. **📍 Add Salt to Keys**  
   Generate a random salt value and append it to the join key. For example, if we have `customer_id`, we create keys like `customer_id_1`, `customer_id_2`, etc.

2. **📍 Replicate Data for the Join**  
   Ensure both tables involved in the join have matching salted keys. 

3. **📍 Perform the Join**  
   Spark distributes the join more evenly across partitions using salted keys.

---

## ✅ Code Example

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

## ✅ Key Takeaways

- 🔸 Salting is highly effective for joins and aggregations where data skew is an issue.
- 🔸 It improves performance and resource utilization by spreading data across partitions.