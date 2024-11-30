# cache() and persist() in Apache Spark

In Apache Spark, cache() and persist() are used to store intermediate results of RDDs, DataFrames, or Datasets in memory or on disk to optimize performance.

Spark Cache and persist are optimization techniques for iterative and interactive Spark applications to improve the performance of the jobs or applications.

## cache()
- Cache is used to store the data in memory for future use.
- It is a shorthand for persist() with the default storage level of MEMORY_AND_DISK.
- This means the data is first stored in memory, and if there is insufficient memory, it spills to disk.
- It's suitable for simple use cases where the default storage level suffices.
- Example:
  ```python
  rdd.cache()
  df.cache()
  ```

## persist()
- persist provides flexibility to choose different storage levels based on the use case.
- Storage levels include `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, MEMORY_ONLY_SER, and OFF_HEAP.
- It's ideal for advanced scenarios where you need specific control over how the data is stored.
- Example:
  ```python
  from pyspark import StorageLevel

  rdd.persist(StorageLevel.MEMORY_ONLY)
  df.persist(StorageLevel.DISK_ONLY)
```
