## DAG and Lazy Evaluation in Apache Spark

### Directed Acyclic Graph (DAG)
- In Spark, a Directed Acyclic Graph (DAG) represents the sequence of computations performed on data. 
- When transformations are applied to an RDD, Spark builds a DAG of stages (which consist of a series of tasks). 
- DAG helps Spark to optimize executions, achieve parallelism, and provide fault tolerance.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/DAG-in-spark.webp)

- The DAG can be broken down as follows:
  - `Directed`: It means a direct connection from one node to another. 
  - `Acyclic`: It means that there are no cycles or loops present. Whenever a transformation occurs, it cannot return to its earlier state.
  - `Graph`: A graph is a combination of vertices and edges. Here vertices represent the RDDs, and the edges, represent the operation to be applied to the RDDs.


## Lazy Evaluation in Apache Spark
- Lazy Evaluation in Sparks means Spark will not start the execution of the process until an Action is called.
- This means that when you apply a transformation (like `map()`, `filter()`, etc.), Spark does not execute it immediately. Instead, it builds a `DAG` of transformations that describe the computation. 
- The actual execution only happens when an action (like `collect()`, `count()`, `save()`, etc.) is called.

**Advantages:**
- `Optimization`: 
  - Spark can optimize the chain of transformations before executing them, improving performance (e.g., by pipelining operations or minimizing data shuffling).
- `Fault Tolerance:` 
  - Since Spark knows the complete lineage of transformations, it can recompute any lost partitions if a failure occurs during execution.

### Let's understand how Lazy Evaluation works and how DAG will be created by taking an example of the below code:

  ```Python
  # Reading the CSV File: this section has two actions `read` and `inferSchema` so spark will create 2 jobs and for each job DAG will be created (triggers execution)
  flight_data = spark.read.format("csv") \
      .option("delimiter", "\t") \
      .option("header", "true") \
      .option("inferSchema", "true") \
      .load("dbfs:/databricks-datasets/flights/airport-codes-na.txt")
  
  # Repartitioning the Data:  # repartition is wide transformation so no job will be created   but the DAG wil be created, this is called azy Evaluation
  flight_data_repartition = flight_data.repartition(3) 
  
  # Filtering Data for the United States:
  us_flight_data = flight_data.filter(col("Country") == "USA") # narrow   transformation
  
  # Filtering for Texas : narrow transformation 
  us_texas_data = us_flight_data.filter(
      (col("City") == "India")
  )
  # Showing the Results: show is an action (triggers execution)
  us_texas_data.show()
  ```

 - When we run the above code in Databrics, We see 3 job is created by spark for each actions i.e `read`, `inferSchema`, and `show`
   ![](https://github.com/rohish-zade/PySpark/blob/main/materials/dag_and_lazy_evaulation_1.png)
- we can see the DAG created by each job
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/dag_for_job.png)
- **`For each action one job will be created and for each job one DAG will be created.`**
- When we execute only the first part of code: we can see only 2 jobs created
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/read_csv_dag.png)
- when we execute only transformations: spark did not start the job execution so we dont see any jobs, this is lazy evaluated.
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/no_job_for_transformation.png)


### Note: read() is action or not?? will it trigger job??
In Spark, reading data from a source using the `DataFrameReader` (like spark.read.format("csv").load()) does not trigger a job immediately. Instead, it only creates a logical plan but In `Databricks and many other environments`, reading from a file (like CSV) using .read() can trigger a job, depending on the underlying implementation of the data source API. Here's why:

- `Catalyst Optimizer and Lazy Evaluation:`
  - Spark normally follows a lazy evaluation model, where transformations are only planned but not executed until an action is called.
  - However, file-reading operations are a bit different, especially when using formats like `CSV`, `Parquet`, or `ORC`.
  - When you call read(), Spark needs to interact with the underlying storage system (like `HDFS`, `S3`, or `local filesystem`) to infer the schema or read the metadata of the file.
  - This metadata-reading process requires some computation, which results in Spark triggering a job to gather the file details. The job does not read the entire file's contents at this point but does perform some operations.

- `Job Execution for .read() in CSV:`
  - If your file is in CSV format, Spark might trigger a job to:
    - **Infer schema** from the first few lines of the file.
    - **Read file partitions** to figure out how the data should be split across the cluster.

**Yes, a job can be triggered by .read() in Databricks (and some other environments) because of schema inference or metadata reading.**

**This behavior may vary slightly depending on the file format, data source, and Spark environment you're using.**