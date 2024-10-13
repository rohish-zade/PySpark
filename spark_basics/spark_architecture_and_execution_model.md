## Apache Spark Abstraction Layers
Apache Spark Architecture is based on two main abstractions:
- Resilient Distributed Dataset (RDD)
- Directed Acyclic Graph (DAG)

#### Resilient Distributed Dataset (RDD)
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

#### Directed Acyclic Graph (DAG)
- In Spark, a Directed Acyclic Graph (DAG) represents the sequence of computations performed on data. 
- When transformations are applied to an RDD, Spark builds a DAG of stages (which consist of a series of tasks). 
- DAG helps Spark to optimize executions, achieve parallelism, and provide fault tolerance.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/DAG-in-spark.webp)

- The DAG can be broken down as follows:
  - `Directed`: It means a direct connection from one node to another. 
  - `Acyclic`: It means that there are no cycles or loops present. Whenever a transformation occurs, it cannot return to its earlier state.
  - `Graph`: A graph is a combination of vertices and edges. Here vertices represent the RDDs, and the edges, represent the operation to be applied to the RDDs.


## Apache Spark Architecture

- Apache Spark works in a `master-slave` architecture where the master is called `Driver` and slaves are called `Workers` where all the components and layers of Spark are loosely coupled.
- Master manages, maintains, and monitors the slaves while slaves are the actual workers who perform the processing tasks.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/spark-architecture.png)




## How to execute Spark Programs?
1. Interactive Clients: `spark-shell`, `Notebook`
2. Submit Job: `spark-submit`, `Databricks Notebook`, `Rest API`

