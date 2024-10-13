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

**Driver Program:**
- The Driver is the main program that runs on the master node and is responsible for coordinating the entire Spark application.
- It creates `Spark contexts` which is the main entry point for a Spark application.
- The Driver is responsible for several tasks, including :
  - Breaking down the application into tasks
  - Scheduling tasks
  - Monitoring tasks
  - Gathering results

**SparkContext:**
- SparkContext is the main entry point for Spark functionality.
- It will connect our Spark application withh execution environment or cluster.
- Tells Spark how & where to access a cluster.
- It creates execution graph (DAG), RDD's, manages paritions, sheduling etc.
- Before Spark 2.0, `SparkContext` was used separately. after Spark 2.0+ SparkSession does all the things.

**Cluster Manager:**
- The Cluster Manager is responsible for allocating resources (CPU, memory) to the Spark jobs.
- The cluster manager works with Spark Context to manage `job execution`, `allocate resources`, `manage Nodes` and `schedule tasks` across worker nodes, using either Spark’s standalone manager or third-party managers like Apache Mesos or Hadoop YARN.
- The Spark supports below cluster managers:
  - `Standalone:` It is a simple, resilient cluster manager that can handle work failures. It makes cluster setup in Spark easy, and it can be easily run on Windows, Linux, or Mac
  - `Apache Mesos:` It is the first open-source cluster manager that is able to handle the workload in a distributed environment. It provides efficient dynamic resource sharing and isolation.
  - `Hadoop YARN (Yet Another Resource Negotiator):` A resource management layer for Hadoop that allows Spark to run on existing Hadoop clusters, managing resources and scheduling tasks.
  - `Kubernetes:` Kubernetes is an open-source framework that can deploy, scale, and manage containerized apps.
- We have two deploy modes when the Cluster Manager is YARN
  - `Cluster` – Framework launches the driver inside of the cluster
  - `Client` – Driver runs in the client

**Worker Nodes:**
- Worker Nodes (also called slave nodes) are the machines that execute the tasks assigned by the master node.
- Each worker node has executors running on it, which are responsible for executing tasks and storing data partitions in memory or disk.
- The worker nodes communicate their progress back to the master node and may run multiple tasks in parallel depending on the available resources.
- They handle actual data processing by executing the tasks (i.e., running transformations and actions on RDDs or DataFrames).
- Key Terms:
  - **Executor**: 
    - A virtual entity which will have CPU, Memory and Network Bandwidth.
    - The Executor is a program that runs on the worker nodes and is responsible for executing the tasks assigned by the Driver.
    - The Executor is responsible for several tasks, including:
      - `Running tasks`, `Reporting status`, `Storing intermediate data`, `Releasing resources`
  - **Task**: Unit of work that will be sent to one executor.
  - **Job**: A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. save, collect)


**Spark with YARN as Resource Manager:**


## Spark Execution Workflow
- When a job is submitted, driver implicitly converts user code that contains transformations and actions into a logically `Directed Acyclic Graph` called DAG. At this stage, it also performs optimizations such as pipelining transformations.
- DAG Scheduler converts the graph into stages. A new stage is created based on the shuffling boundaries.
- Now the driver talks to the cluster manager and negotiates the resources. Cluster manager launches executors in worker nodes on behalf of the driver. At this point, the driver will send the tasks to the executors based on data placement. When executors start, they register themselves with drivers. So, the driver will have a complete view of executors that are executing the task.
- While the job is running, driver program will monitor and coordinate the running tasks. Driver node also schedules future tasks based on data placement.


## Lifecycle of Spark Application
- **Users Submits Application:** 
  - The user submits the Spark application, this submission triggers the job execution process on a cluster.
- **Driver Initiates Spark Session:** 
  - The driver is responsible for the application's lifecycle. 
  - It creates a Spark session (via SparkContext or SparkSession), which is the entry point for all Spark operations.
- **DAG Creates Logical Plan:** 
  - Spark converts the user-defined transformations into a DAG (Directed Acyclic Graph).  - This DAG represents a sequence of computations to be performed on the data. 
  - It is a logical plan that defines how Spark will execute the job.
- **Cluster Manager Allocates Resources to Execute Task:**
  - The Cluster Manager (YARN, Mesos, Kubernetes, or Spark's standalone manager) is responsible for resource allocation.
  - It assigns resources such as CPU and memory to the executors for task execution across worker nodes.
- **Task Executor Requests Resources from Cluster Manager:**
  - Each executor running on a worker node requests the required resources from the cluster manager to run its assigned tasks.
  - The executor runs computations on the data and manages intermediate results.
- **Driver Establishes Connection with Worker and Assigns Task:**
  - The driver communicates with the worker nodes and distributes tasks (small units of execution) to them.
  - These tasks are the operations that will be performed on the data partitions.
- **Worker Executes the Task and Returns Results to Driver:**
  - Each worker node (via the executor) processes the data as per the assigned tasks and returns the results to the driver after completing the computation.
- **Driver Returns the Result to User:**
  - After the driver collects the results from all worker nodes, it sends the final result back to the user or stores it in a specified location.
- **Application Comes to End:**
  - Once the job completes successfully, the application finishes.
  - All resources allocated for the job are released, and the driver exits.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/Spark-RDD-execution-workflow.png)

## How to execute Spark Programs?
1. Interactive Clients: `spark-shell`, `Notebook`
2. Submit Job: `spark-submit`, `Databricks Notebook`, `Rest API`


## Spark Execution Modes:
There are 3 types of execution modes.
### 1. Local Mode
- Spark runs in a single JVM on the local machine, simulating a cluster. 
- This is used for development, testing, and small-scale data processing.
- Example: `spark-submit --master local`

### 2. Client Mode:
- The driver runs on the client machine, while the executors run on the cluster. 
- The client must stay connected for the job to complete.
- Example: `spark-submit --master yarn --deploy-mode client`
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/client-mode-execution.png)


### Cluster Mode:
- In this mode, Spark runs on a distributed cluster. The driver and the executors run across different nodes. 
- This is ideal for production and large-scale data processing.
- Example: `spark-submit --master yarn --deploy-mode cluster`
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/cluster-mode-execution.png)


## Spark Deploy Modes:
In Apache Spark, there are two main deploy modes:
### 1. Client Mode:
- In client mode, the Spark driver runs on the machine that submits the application (the client machine). The driver connects to the cluster to perform the job, but the client machine must remain active and connected to the cluster for the job to complete.
- This mode is usually used in `development` or `interactive` data analysis sessions where the client needs to control the job execution.
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/client-mode.png)

### 2. Cluster Mode:
- In this mode, the Spark driver runs on one of the worker nodes in the cluster. 
- The client that submits the application is disconnected after submission, and the entire processing is handled by the cluster.
- This mode is typically used in `production environments` where the client does not need to stay connected to the driver.
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/cluster-mode.png)

## Difference Between Spark Deploy Mode and Execution Modes
- `Spark Execution Modes:` Spark's execution modes refer to how Spark jobs are executed within a cluster, based on the cluster manager used. Refers to where and how the entire application runs
-  `Spark Deploy Modes:` Spark's deploy mode defines where the Spark driver runs in relation to the cluster.