## Adaptive Query Execution (AQE)

### What is AQE?
- Adaptive Query Execution is a feature introduced in Apache Spark 3.0 that dynamically optimizes query execution plans at runtime based on the actual data being processed.
- Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan. 
- Unlike static optimization, AQE adjusts to real-time statistics, making it especially effective for large-scale data with varying characteristics.

### Enabling AQE in Spark
-  It is enabled by default since Apache Spark 3.2.0.
- To enable AQE, set the following configuration in your Spark session:
  - `spark.conf.set("spark.sql.adaptive.enabled", "true")`
- Additional configurations include:
  - `spark.sql.adaptive.coalescePartitions.enabled:` Enables coalescing of shuffle partitions.
  - `spark.sql.adaptive.skewJoin.enabled:` Enables skew join optimization.
  - `spark.sql.adaptive.broadcastJoinThreshold:` Threshold for dynamically choosing broadcast joins.


### Features of AQE
below are the 3 major feature of AQE:
1. Dynamically coalescing shuffle partitions
2. Dynamically switching join strategies
3. Dynamically optimizing skew joins

#### Dynamically coalescing shuffle partitions
- AQE can dynamically reduce the number of shuffle partitions to avoid generating too many small partitions. 
- It checks the data size after shuffling and merges small partitions together, leading to fewer but larger partitions and improved parallelism.
- AQE chooses the best number of partitions automatically.

**Without AQE:** 
- The default number of shuffle partitions (e.g., 200 by default) might result in many small partitions, wasting resources and increasing scheduling overhead.

**With AQE:** Spark adjusts the number of partitions at runtime to better match the data size, ensuring efficient use of resources.

  ![dynamically coalescing shuffle partitions](https://github.com/rohish-zade/PySpark/blob/main/materials/coalescing-shuffle-partitions.jpg)

**Example:**

Imagine a shuffle stage where the actual data size is only 50MB, but Spark’s default configuration creates 200 partitions(for any joins and aggregations).
- `Without AQE:` Spark generates 200 tiny partitions, each processing only ~0.25MB. This increases scheduling overhead and executor idle time.

- `With AQE:` Spark coalesces the partitions dynamically to, say, 10 partitions, each processing ~5MB, optimizing execution time.
- `spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")`


#### Dynamically switching join strategies
- Spark can switch between different join strategies (e.g., shuffle join, broadcast join) at runtime based on the size of the data.
- If the data is small enough, it may use a broadcast join (more efficient) instead of a shuffle join (which is more costly)

**Example:**

Joining a large sales table with a small promotions table:

- `Without AQE:` If the promotions table size is underestimated, Spark might use a shuffle join, resulting in unnecessary shuffling.
- `With AQE:` During runtime, Spark detects that the promotions table is small enough (e.g., <10MB) and switches to a broadcast join, reducing shuffle overhead.
- `spark.conf.set("spark.sql.adaptive.broadcastJoinThreshold", "10485760")`  # 10MB


#### Dynamically optimizing skew joins(uneven partitions)
- In cases where data is skewed (some partitions are much larger than others), AQE can detect this and optimize the join by splitting the skewed partitions into smaller ones, ensuring more balanced and efficient processing.
- Spark detects data skew (i.e., when one or more partitions have significantly more data than others) at runtime and splits the skewed partitions into smaller chunks.

**Without AQE:**
- A skewed partition may cause one executor to handle most of the work, while others remain underutilized, leading to slow execution or even failures.

**With AQE:** 
- Skewed partitions are split dynamically, ensuring balanced resource utilization and faster query execution.

**Example:**

Joining a large sales table with a products table where most rows in sales have the same product_id (e.g., product_id = 1).

- `Without AQE:` The partition containing product_id = 1 is overloaded, causing a bottleneck.
- `With AQE:` Spark identifies this skew during runtime and splits the overloaded partition into smaller sub-partitions, distributing the workload across executors.
- `spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")`


### Benefits of AQE
- Improved query execution time through dynamic optimizations.
- Better resource utilization by handling skewed data and reducing unnecessary overhead.
- Simplified tuning of Spark jobs as AQE minimizes manual optimizations.

### Limitations
- Overhead of Runtime Statistics: Gathering and using statistics at runtime adds slight overhead.
- Complexity in Debugging: Dynamic changes in query plans can make debugging more complex.
- Dependent on Accurate Statistics: Effectiveness relies on the accuracy of runtime statistics.