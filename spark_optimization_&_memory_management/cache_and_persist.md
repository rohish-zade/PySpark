# cache() and persist() in Apache Spark

In Apache Spark, cache() and persist() are used to store intermediate results of RDDs, DataFrames, or Datasets in memory or on disk to optimize performance.

Spark Cache and persist are optimization techniques for iterative and interactive Spark applications to improve the performance of the jobs or applications.

## cache()
- Cache is used to store the data in memory for future use.
- It is a shorthand for `persist()` with the default storage level of `MEMORY_AND_DISK`.
- This means the data is first stored in memory, and if there is insufficient memory, it spills to disk.
- It's suitable for simple use cases where the default storage level suffices.
- Example:
  ```python
  rdd.cache()
  df.cache()
  ```

## persist()
- persist provides flexibility to choose different storage levels based on the use case.
- Storage levels include `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `MEMORY_ONLY_SER`, and `OFF_HEAP`.
- It's ideal for advanced scenarios where you need specific control over how the data is stored.
- Example:
  ```python
  from pyspark import StorageLevel

  rdd.persist(StorageLevel.MEMORY_ONLY)
  df.persist(StorageLevel.DISK_ONLY)
  ```

### Storage Levels in Persist()
In Spark, storage levels determine how RDDs, DataFrames, or Datasets are stored in memory and/or disk during computation. Each storage level has its trade-offs based on memory and performance requirements.

##### MEMORY_ONLY
- Data is stored only in memory
- If the data cannot fit into memory, the partitions that don't fit are recomputed when needed.
- Use this when the data fits comfortably in memory, and recomputing is inexpensive.
- `Performance`:
  - Fastest option (no disk I/O).
  - Not fault-tolerant if data is lost due to memory constraints.
- `rdd.persist(StorageLevel.MEMORY_ONLY)`

##### MEMORY_AND_DISK
- Data is stored in memory if possible.
- If it doesn’t fit, it spills the remaining data to disk.
- Ideal for large datasets that cannot entirely fit in memory.
- Slightly slower than MEMORY_ONLY due to potential disk I/O, but more fault-tolerant.
- `rdd.persist(StorageLevel.MEMORY_AND_DISK)`

##### DISK_ONLY
- Data is stored exclusively on disk.
- No memory usage for caching.
- Use when memory is very limited or for large datasets where recomputation is costly.
- Slowest option due to frequent disk I/O.
- `rdd.persist(StorageLevel.DISK_ONLY)`

##### MEMORY_ONLY_SER (Serialized Memory Only)
- Data is stored in memory in a serialized format, reducing memory usage.
- Partitions that don’t fit in memory are recomputed.
- Use when you want to store more data in memory at the cost of serialization/deserialization overhead.
- Slower than MEMORY_ONLY because of serialization costs but more memory-efficient.
- `rdd.persist(StorageLevel.MEMORY_ONLY_SER)`


##### OFF_HEAP (Off-Heap Storage)
- Data is stored in off-heap memory (outside the JVM heap).
- Requires enabling Spark's off-heap memory settings.
- Useful for applications that want to bypass JVM garbage collection issues.
- Requires specific configuration `(spark.memory.offHeap.enabled=true)`.
- Can be faster for specific use cases but requires additional setup.
- `rdd.persist(StorageLevel.OFF_HEAP)`


## Difference between cache() and persist() in Spark:
`CACHE` and `PERSIST` do the same job to help in retrieving intermediate data used for computation quickly by storing it in memory, while by caching we can store intermediate data used for calculation only in memory , persist additionally offers caching with more options/flexibility.

  | **Aspect**               | **`cache()`**                          | **`persist()`**                          |
  |--------------------------|-----------------------------------------|  ------------------------------------------|
  | **Definition**            | A shorthand for `persist()` with the default storage   level. | Allows specifying a custom storage level for caching data. |
  | **Default Storage Level** | `MEMORY_AND_DISK`.                    | User-defined (e.g.,   `MEMORY_ONLY`, `DISK_ONLY`, etc.).      |
  | **Flexibility**           | No customization of storage levels.   | Fully customizable   for various storage levels.             |
  | **Ease of Use**           | Simple and quick to use.              | Requires specifying   the desired storage level.             |
  | **Use Case**              | When default storage level suffices.  | When specific   control over storage level is needed.        |

  
**NOTE:**
- Both cache() and persist() store data in memory or disk, so to release resources, we can use `unpersist()`.

**When to Use:**
- Use `cache()` for simplicity when the default behavior is sufficient.
- Use `persist()` when you need more control over storage levels for performance tuning.
