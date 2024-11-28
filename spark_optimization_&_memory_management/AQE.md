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



### Benefits of AQE
- Improved query execution time through dynamic optimizations.
- Better resource utilization by handling skewed data and reducing unnecessary overhead.
- Simplified tuning of Spark jobs as AQE minimizes manual optimizations.

### Limitations
- Overhead of Runtime Statistics: Gathering and using statistics at runtime adds slight overhead.
- Complexity in Debugging: Dynamic changes in query plans can make debugging more complex.
- Dependent on Accurate Statistics: Effectiveness relies on the accuracy of runtime statistics.