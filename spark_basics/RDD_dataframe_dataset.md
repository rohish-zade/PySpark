## Resilient Distributed Dataset (RDD)
- RDDs are the building blocks of any Spark application. 
- RDDs Stands for:
  - `Resilient`: Fault tolerant and is capable of rebuilding data on failure
  - `Distributed`: Distributed data among the multiple nodes in a cluster
  - `Dataset`: Collection of partitioned data with values

- When you load the data into a Spark application, it creates an RDD which stores the loaded data.
- RDD is immutable, meaning that it cannot be modified once created, but it can be transformed at any time.
- Each dataset in RDD is divided into logical partitions. These partitions are stored and processed on various machines of a cluster.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/rdd-in-spark.webp)

- With RDDs, you can perform two types of operations:
  1. **Transformations**: Transformations are operations that create a new RDD from an existing one without immediately executing the computation, using lazy evaluation (e.g., `map`, `filter`).
 
  2. **Actions**: Actions are operations that trigger the execution of transformations and return a result to the driver or write output to storage (e.g., `collect`, `count`).