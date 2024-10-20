## SparkSession vs SparkContext

SparkSession and SparkContext are both entry points to Spark functionality, but they have some differences.

### SparkContext
- SparkContext was the main entry point for Spark programming with RDDs and connecting to the Spark cluster in earlier versions of Spark or PySpark.
- SparkContext allows you to create RDDs, accumulators, and broadcast variables, as well as access Spark services and perform jobs. 
- SparkContext is designed for low-level programming and fine-grained control over Spark operations.


### SparkSession
- SparkSession was introduced in Spark 2.0 and became the preferred/unified entry point for programming with DataFrames and Datasets, which are higher-level abstractions than RDDs.
- It encapsulates all the functionalities of SparkContext, SQLContext, and HiveContext, providing a more user-friendly interface.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/sparkSession.webp)

## When to use what??
In modern Spark applications, it's recommended to use `SparkSession` for its simplicity and comprehensive capabilities, as it incorporates the functionalities of SparkContext.

- Use `SparkContext` if you are primarily working with RDDs and need low-level control over distributed data processing.

- Use `SparkSession` for most new applications, as it provides a more user-friendly interface, supports structured data processing with DataFrames and Datasets, and integrates SQL functionalities.
