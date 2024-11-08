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

### Features of an RDD:

- `Immutability`: Once an RDD is created, it cannot be changed. Any transformation on RDD results in the creation of a new RDD.

- `Fault Tolerance`: RDDs are inherently fault-tolerant. They store the lineage of operations (i.e., a sequence of transformations used to create the dataset) and can recompute lost data in case of failure.

- `Lazy Evaluation`: Transformations on RDDs are lazy, meaning Spark doesn’t execute them immediately. Instead, it builds a directed acyclic graph (DAG) of transformations and applies them only when an action (like collect() or count()) is triggered.

- `Partitioning`: RDDs are divided into partitions, which are distributed across nodes in a cluster. This ensures parallel processing of data.

- `In-Memory Processing`: RDDs can be cached in memory to speed up the process of repeated computations on the same dataset. This avoids reading data from disks multiple times.

### Disadvantages of RDD
- `No Optimization by Spark`: RDDs do not benefit from built-in optimizations like DataFrames and Datasets, which use Spark's Catalyst Optimizer for query execution.
- `Low-Level API`: RDDs offer a low-level API, requiring more effort and code to perform simple operations compared to higher-level abstractions like DataFrames.
- `Memory Consumption`: RDDs store data as Java objects, which can consume more memory due to the overhead of object creation, whereas DataFrames store data in an optimized, memory-efficient format (off-heap storage).
- `Limited Support for Advanced Analytics`: RDDs lack advanced functions like optimized SQL queries, machine learning, and graph processing APIs that are natively supported in higher-level APIs like DataFrames and Datasets.
- `Schema Awareness`: RDDs are not schema-aware, meaning they lack the automatic type checking and column pruning capabilities present in DataFrames and Datasets.


### When do we need an RDD?
You should consider using RDDs in Apache Spark under the following circumstances or use cases:

- **Low-Level Control**: If you need fine-grained control over your data processing, such as specifying exactly how data should be partitioned and processed at the lowest level (e.g., custom partitioning or control over data placement), RDDs are a better fit.

- **Unstructured Data:** RDDs are useful when working with unstru`ctured or `semi-structured` data that doesn’t fit into Spark’s higher-level APIs like DataFrames or Datasets, which are primarily designed for structured data.

- **Complex Processing:** When the computations are complex, involving custom transformations that aren’t easily expressed using the high-level DataFrame or Dataset API (e.g., using complex `map`, `filter`, or `reduce` operations).

- **Type Safety:** When working with strongly-typed data, you might prefer RDDs because they maintain type safety, especially when using languages like Scala, giving you compile-time safety checks.

- **When Datasets/DataFrames Are Not Suitable:** If the performance benefits of DataFrames or Datasets (like Catalyst optimizer, Tungsten execution engine) are not necessary for your application, or when schema enforcement is not needed, you may opt to use RDDs.


### Why we should not use an RDD?
There are several reasons why RDDs may not be the best choice in many situations, especially when compared to higher-level APIs like `DataFrames` and `Datasets` in Apache Spark:

**Lack of Optimization:**
- `RDDs` do not benefit from the Catalyst Optimizer or Tungsten execution engine, which are used in DataFrames and Datasets to optimize queries and execution plans.
- DataFrames and Datasets can execute faster by leveraging these optimizations, especially for SQL-like queries and structured data.

**Manual Optimization Required:**
- With RDDs, you must handle performance optimizations manually, such as managing memory, data partitioning, and optimizing joins.

**No Schema Enforcement:**
- RDDs do not enforce a schema, which means there’s no inherent structure to the data. This can lead to errors that are only caught at runtime.

**Poor Performance for Structured Data:**
- RDDs are less efficient for working with structured data. Since RDDs don’t take advantage of schema information, operations like joins, aggregations, and filtering are slower than with DataFrames or Datasets, which use optimized query planning.

**More Memory and Network Overhead:**
- RDDs can have higher memory consumption and network overhead because they rely on Java serialization, which can be more costly than the off-heap memory management used by DataFrames and Datasets.

**Limited APIs:**
- RDDs provide limited built-in operations for complex data processing. In contrast, DataFrames and Datasets offer more higher-level operations (such as SQL-like operations, aggregation functions, and grouping), making them more versatile for many use cases.

**Debugging Complexity:**
- RDD transformations are lazy, meaning they do not immediately execute and can lead to harder-to-debug issues, especially when complex transformations are applied.


## DataFrame in Apache Spark:
In Spark SQL, a `DataFrame` is a distributed collection of data organized into named columns, similar to a table in a relational database. 

A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.

It is one of the most commonly used data structures in Spark because it offers both `ease of use` and `performance optimizations` over working with RDDs.

### Key Features of DataFrames:

- `Schema-Aware:` DataFrames have a defined schema with named columns, making them suitable for structured and semi-structured data.
- `Optimized Execution`: They benefit from Spark’s Catalyst Optimizer and Tungsten engine, offering automatic query and execution optimizations.
- `Lazy Evaluation:` Transformations on DataFrames are lazy, meaning they are computed only when an action like` show()` or `count()` is triggered.
- `High-Level API:` Provides a high-level, SQL-like API for easy data manipulation through operations like `filtering`, `selecting`, and `aggregating`.
- `SQL Integration:` DataFrames can be queried using SQL syntax and can be registered as temporary views for seamless SQL querying.
-` Distributed Processing:` DataFrames are distributed across the cluster, enabling parallel data processing and scalability.
- `Fault Tolerance:` DataFrames automatically recover from failures using lineage information, ensuring reliability.
- `In-Memory Processing:` Data can be cached in memory for faster access during iterative operations, improving performance.
- `Supports Multiple Data Sources:` DataFrames can read and write data from various formats like CSV, JSON, Parquet, JDBC, etc.
- `Support for UDFs:` Custom transformations can be applied using user-defined functions (UDFs) on DataFrame columns.
- `Adaptive Query Execution:` DataFrames take advantage of Adaptive Query Execution (AQE) to optimize queries at runtime.
- `Batch and Streaming Support:` DataFrames can handle both batch and streaming data seamlessly using the same API.


## Dataset in Apache Spark:
A Dataset is a distributed collection of data. It provides a strong typing and a more functional programming interface.

It combines the best features of both RDDs (Resilient Distributed Datasets) and DataFrames, allowing users to work with both structured and unstructured data in a type-safe manner.

### Features of Datasets in Apache Spark:
- `Strongly Typed:` Datasets are type-safe, meaning they enforce compile-time type checking. This is particularly beneficial in languages like Scala and Java, where type mismatches can be caught during compilation.

- `High-Level Abstraction:` Datasets provide a high-level API for working with structured data while still allowing for functional programming constructs, such as map(), flatMap(), filter(), and groupBy().

- `Optimized Execution:` Like DataFrames, Datasets benefit from the Catalyst Optimizer and Tungsten execution engine, allowing for automatic optimization of query plans and efficient execution.

- `Interoperability with DataFrames`: Datasets can be easily converted to and from DataFrames. This means you can leverage the advantages of both APIs depending on your needs.

- `Support for Encoders:` Datasets use encoders to convert data between JVM objects and Spark’s internal representation. This provides efficient serialization and deserialization, improving performance.

- `Seamless Integration with SQL`: Datasets can be queried using SQL, allowing users to combine declarative SQL syntax with functional programming operations.

- `Distributed Processing:` Like RDDs and DataFrames, Datasets are distributed across a Spark cluster, enabling parallel processing of large datasets.

- `Fault Tolerance:` Datasets are inherently fault-tolerant, leveraging Spark's lineage information to recover lost partitions in case of failure.

- `Native Support for Complex Data Types:` Datasets can handle complex data types such as `arrays`, `maps`, and `structs`, making them versatile for a wide range of data processing tasks.

- `Integration with Machine Learning:` Datasets can be used directly in Spark's MLlib for machine learning tasks, allowing for efficient data handling in modeling and prediction.


## Difference Betweeen RDDs, DataFrames, and Datasets in Apache Spark:

  | Feature               | RDD (Resilient Distributed   Dataset)                             | DataFrame                                       |   Dataset                                          |
  |-----------------------|---------------------------------------------------------------|  ------------------------------------------------|  -------------------------------------------------|
  | **Definition**        | A distributed collection of objects, not necessarily   structured. | A distributed collection of data organized into named columns (like a   table). | A distributed collection of data that provides strong typing and a functional   programming interface. |
  | **Schema**            | No schema; can hold any type of data.                         |   Schema-aware with defined column names and types. | Strongly typed; enforces compile-time   type checking (in Scala and Java). |
  | **Optimization**      | No optimization; manual optimization required.                |   Optimized using Catalyst and Tungsten.         | Also optimized by Catalyst and Tungsten,   providing the same benefits as DataFrames. |
  | **Data Manipulation** | Operations are performed using transformations (map, filter,   etc.). | Uses a high-level API with SQL-like operations (select, filter, groupBy). |   Combines functional programming constructs with high-level operations, allowing both SQL   and functional manipulations. |
  | **Performance**       | Generally slower for structured data due to lack of   optimizations. | Generally faster due to optimizations and efficient memory usage. | Fast   performance due to optimizations, similar to DataFrames. |
  | **Interoperability**  | No direct interoperability with DataFrames; must convert.    |   Can be converted to and from Datasets easily. | Can be converted to and from DataFrames   easily. |
  | **Type Safety**       | No type safety; types are checked at runtime.                |   No compile-time type safety; dynamic typing.   | Strongly typed; provides compile-time   type safety in Scala and Java. |
  | **Use Cases**         | Ideal for unstructured data, low-level transformations, or when   fine-grained control is needed. | Best for structured data processing, analytics, and SQL   queries. | Best for working with structured data while requiring type safety and   functional programming capabilities. |
  | **UDF Support**       | Supports user-defined functions (UDFs) but requires more   boilerplate. | Supports UDFs easily with high-level API.     | Supports UDFs seamlessly   with strong typing. |
  | **Data Sources**      | Can work with any type of data source.                       |   Reads from structured data sources like CSV, JSON, Parquet, etc. | Also reads from   structured data sources, benefiting from optimizations. |


### When to use what??
- ***RDD:*** Best for low-level operations and unstructured data; no optimizations or schema.

- ***DataFrame:*** Best for structured data with optimizations and SQL-like operations; schema-aware but dynamically typed.

- ***Dataset:*** Combines the advantages of RDDs and DataFrames, offering strong typing and functional programming while benefiting from optimizations; best for structured data where type safety is important.