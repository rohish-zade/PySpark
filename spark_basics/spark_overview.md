## What is Apache Spark?
Apache spark is a unified computing engine and set of libraries for parallel data processing on computer cluster.

Lets break down the definition: 

#### Unified Engine:
- Spark is design to support wide range of task over the same computing engine.
- for example: Data Scientist, Data Analyst and Data Engineers all can use the same platform for their analysis, transformation or modeling.
- Spark provides a single, unified engine for handling different types of big data processing workloads, including:
  - `Batch Processing:` Through the Spark Core API.
  - `Stream Processing:` Using Spark Streaming for real-time data.
  - `Machine Learning:` Through the MLlib library for scalable machine learning.
  - `Graph Processing:` Via the GraphX library for graph data processing.

#### Computing Engine:
- Spark is limited to computing engine. It doesn't store the data.
- Spark acts as the core framework that runs data operations on different types of data, whether structured or unstructured.
- Spark can connect with different data sources like `S3`, `ADLS`, `HDFS`, `JDBC/ODBC` etc.
- Spark works with almost all data storage.

#### Libraries:
Spark comes with built-in libraries for different tasks:
- `Spark SQL` for handling structured data.
- `MLlib` for machine learning.
- `GraphX` for graph processing.
- `Spark` Streaming for real-time stream processing.
 
Libraries are pre-written code collections that provide useful functions and modules, allowing developers to quickly implement common functionalities without building everything from scratch.

#### Parallel data processing:
- Spark can distribute the workload across multiple machines (nodes in a cluster) to process large amounts of data simultaneously, in parallel.
- Each node processes a portion of the data simultaneously to achieve faster and more efficient computation.

#### Computer Clusters:
- Spark works on clusters, which are groups of computers (nodes) working together together as a single system to perform tasks, typically by distributing workloads across multiple machines to increase performance, scalability, and fault tolerance..
- Each computer processes a portion of the data, enabling Spark to handle large datasets efficiently by distributing the load across the cluster.
- In a cluster we have master node and slave nodes.


## Why Apache Spark?

#### Big data problems with Hadoop
Despite its advantages, Hadoop faced several limitations:

**Performance Issues:** 
- `Disk I/O:` Hadoop’s reliance on disk-based storage and processing introduced significant latency, especially for iterative algorithms used in data analysis and machine learning.
- `Batch Processing:` Hadoop is primarily designed for batch processing, making it less effective for real-time data processing.

**Complexity**:
- Writing MapReduce jobs requires a steep learning curve and extensive boilerplate code, complicating development for many users.

**Limited Processing Capabilities:**
- `Single Paradigm:` Hadoop’s MapReduce model is not well-suited for all types of data processing tasks, particularly those requiring real-time analytics.

**High Latency:**
- `Slow Job Execution:` The intermediate data writing to disk during MapReduce jobs leads to high latency, making it unsuitable for low-latency applications.


#### How Spark Overcomes Hadoop Problems

- `Performance Improvements:` By leveraging in-memory computing, Spark significantly reduces processing times, especially for iterative tasks that are common in machine learning and data analysis.
- `Rich Ecosystem:` Spark provides a comprehensive set of libraries for various data processing tasks, making it a versatile platform for big data solutions.
- `Support for Real-Time Processing`: With Spark Streaming, organizations can process real-time data streams, enabling immediate insights and analytics.
- ` User-Friendly:` The high-level APIs and interactive programming capabilities of Spark make it easier for developers and data scientists to work with data.


## Difference Between Hadoop and Spark

| Feature                  | Hadoop                                                                 | Spark                                                                                     |
|--------------------------|------------------------------------------------------------------------|------------------------------------------------------------------------------------------|
| Processing Model         | Primarily batch processing using MapReduce                             | Supports batch, streaming, and interactive processing with in-memory computing           |
| Performance              | Disk-based processing, leading to higher latency                       | In-memory processing, significantly faster (up to 100x) for certain workloads           |
| Ease of Use              | Complex API with extensive boilerplate code                            | High-level APIs in multiple languages (Python, Scala, Java, R) simplify coding          |
| Data Processing          | Suitable for batch jobs; less efficient for iterative algorithms       | Excellent for iterative algorithms and real-time data processing                        |
| Data Storage             | Uses HDFS for distributed storage                                      | Can use HDFS, but also works with various storage systems (S3, Cassandra, HBase)      |
| Fault Tolerance          | Data replication across nodes in HDFS; fault-tolerant through MapReduce| Fault-tolerant through RDDs (Resilient Distributed Datasets) with lineage information and DAG  |
| Library Ecosystem        | Limited to batch processing (MapReduce)                               | Rich ecosystem including MLlib (machine learning), GraphX (graph processing), and Spark SQL |
| Resource Management       | Managed by YARN (Yet Another Resource Negotiator)                    | Can run on YARN, Mesos, Kubernetes, or its own standalone cluster manager                |
| Real-Time Processing     | Limited support; primarily designed for batch processing               | Supports real-time stream processing through Spark Streaming                            |
| Community and Support    | Established community, widely used in enterprises                     | Rapidly growing community, increasingly adopted for modern data solutions               |


## Features of Apache Spark

- **In-Memory Computation:** Spark processes data in memory for faster computations, significantly speeding up data analysis tasks.
- **Distributed Processing using Parallelize:** The parallelize function allows users to distribute data across multiple nodes for parallel processing, enhancing performance.
- **Support for Multiple Cluster Managers:** Spark can run on various cluster managers like Standalone, YARN, Mesos, and Kubernetes, providing flexibility in deployment.
- **Fault-Tolerant:** Spark ensures fault tolerance through RDD lineage, enabling automatic recovery of lost data without restarting jobs.
- **Immutable:** RDDs and DataFrames are immutable, meaning they cannot be modified after creation, simplifying data management.
- **Lazy Evaluation:** Spark uses lazy evaluation, delaying computation until an action is called, which allows for optimization of the execution plan.
- **Cache & Persistence**: Spark supports caching and persistence of datasets, enabling faster access to frequently used data by storing it in memory or on disk.
- **Inbuilt Optimization when using DataFrames:** DataFrames benefit from Catalyst, Spark's query optimizer, which enhances performance through various optimization techniques.
- **Supports ANSI SQL:** Spark SQL supports ANSI SQL syntax, allowing users to interact with structured data using familiar SQL queries.


## Advantages of Apache Spark

- **Speed**: Spark's in-memory processing enables lightning-fast data computations, significantly outperforming traditional disk-based systems.
- **Ease of Use**: With user-friendly APIs in multiple languages, Spark simplifies the development of big data applications for both novice and experienced programmers.
- **Advanced Analytics:** Spark includes built-in libraries for machine learning and graph processing, facilitating sophisticated analytics and data insights.
- **Multilingual:** Spark supports various programming languages, including `Scala`, `Python`, `Java`, and `R`, allowing developers to use their preferred languages.
- **Versatile Data Sources:** Spark can process data from various sources, including Hadoop HDFS, AWS S3, Databricks DBFS, Azure Blob Storage, and multiple file systems.
- **Built-in Libraries:** Spark includes native libraries for machine learning and graph processing, enabling advanced analytics within the same framework.
- **Dynamic in Nature:** Spark can dynamically adjust resource allocation and processing tasks based on the workload, optimizing performance and efficiency.
**Powerful Engine:** Spark's unified engine can handle diverse workloads, including batch processing, streaming, and advanced analytics, making it a versatile big data tool.


## Disadvantages of Apache Spark:

- **File Management System:** It does not have a file management system of its own. It depends on other cloud-based systems or Hadoop for file management.
- **Memory Consumption:** Spark's in-memory processing can lead to high memory usage, requiring substantial resources and careful cluster management.
- **Cost:** Running Spark at scale can become expensive, particularly when leveraging cloud resources due to high compute and memory requirements.
-- **Inefficiency with Small Data:** Spark's overhead can make it inefficient for processing small datasets compared to traditional tools.
- **Dependency on Java Virtual Machine (JVM):** Running on the JVM can introduce performance overhead and complicate garbage collection issues.
- **Lack of Built-in Visualization:** Spark does not provide built-in visualization tools, requiring external applications for data analysis and visualization.


## Apache Spark EcoSystem
The Apache Spark ecosystem consists of various components that extend Spark’s capabilities for different data processing tasks.

![](https://github.com/rohish-zade/PySpark/blob/main/materials/spark_ecosystem.png)

### Spark Core
- The Spark Core is the heart of Spark and performs the core functionality.
- The foundational engine of the Spark ecosystem responsible for basic functions like `task scheduling`, `memory management`, `fault recovery`, `interacting with storage systems` and `distributed execution`.
- Provides APIs for RDD (Resilient Distributed Dataset) creation, transformations, and actions.

### Spark SQL
- The Spark SQL is built on the top of Spark Core.
- A module for structured data processing, allowing users to query data using SQL as well as DataFrame and Dataset APIs.
- Provides a unified interface for working with structured data across different formats, and supports ANSI SQL, enabling integration with BI tools.

### Spark Streaming
- Used for real-time data processing, Spark Streaming allows for processing of live data streams, making it suitable for applications like log processing and fraud detection.
- Processes data in micro-batches and integrates with data sources like Kafka, Flume, and HDFS.

### MLlib (Machine Learning Library)
- Spark’s machine learning library that includes algorithms and utilities for tasks such as classification, regression, clustering, collaborative filtering, and dimensionality reduction.
- Supports distributed machine learning, simplifying the creation of scalable machine learning models.

### GraphX
- A library for graph processing and graph-parallel computation, allowing users to create, transform, and query graphs at scale.
- Enables distributed computation on graph data, such as social network analysis or recommendation systems.

