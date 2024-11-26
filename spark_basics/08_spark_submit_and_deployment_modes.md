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
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/client-deploy-mode.png)

### 2. Cluster Mode:
- In this mode, the Spark driver runs on one of the worker nodes in the cluster. 
- The client that submits the application is disconnected after submission, and the entire processing is handled by the cluster.
- This mode is typically used in `production environments` where the client does not need to stay connected to the driver.
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/cluster-mode.png)

## Difference Between Spark Deploy Mode and Execution Modes
- `Spark Execution Modes:` Spark's execution modes refer to how Spark jobs are executed within a cluster, based on the cluster manager used. Refers to where and how the entire application runs
-  `Spark Deploy Modes:` Spark's deploy mode defines where the Spark driver runs in relation to the cluster.


## spark-sumbit
- The `spark-submit` command line tool is used to submit Spark applications to a cluster or execute them locally. 
- It’s the primary tool for launching applications on Spark, supporting various configurations for cluster mode, application dependencies, resource allocation, and more.
- It supports various languages (Scala, Java, Python, R) and is highly configurable, allowing control over execution parameters such as cluster manager, memory, cores, deployment mode, and more.

### General Syntax
  ```bash
  spark-submit [options] <app> [app-arguments]
  ```
- `options:` Configurations for Spark, cluster, and resource allocation.
- `<app>:` Path to the application file (.jar, .py, .R).
- `[app-arguments]:` Arguments passed to the application.

### Key Options
Below are commonly used options categorized by functionality:

#### 1. Cluster and Deployment Configuration
- `--master:` Specify the cluster manager (local, yarn, mesos, k8s, spark://<host>:<port>).
- `--deploy-mode:` Deployment mode: client (driver runs locally) or cluster (driver runs on the cluster).

#### 2. Resource Allocation
- `--num-executors:` Number of executors (for cluster modes like YARN).
- `--executor-cores:` CPU cores per executor.
- `--executor-memory:` Memory per executor (e.g., 4g, 512m).
- `--driver-memory:` Memory allocated to the driver (e.g., 2g).

#### 3.Application and Dependency Management
- `--class:` Main class to execute (required for Scala/Java applications) it is optional and not required for python spark jobs.
- `--jars:` Comma-separated list of additional JARs for executors.
- `--py-files:`	Comma-separated list of Python files to distribute to the cluster.
- `--files:` Files (e.g., config files) to distribute to all nodes.

#### 4.Additional Configurations
- `--conf:` Arbitrary Spark configuration (e.g., spark.executor.memoryOverhead).
- `--properties-file:` Path to a properties file for configuration.
- `--verbose:` Print detailed debug information.

### Cluster Managers
`--master` determines the cluster manager. Common options:

**Local:** 
- Run Spark locally with threads.
- `--master local[4]`

**Standalone:** 
- Use Spark’s built-in cluster manager.
- `--master spark://<host>:7077`

**YARN:** 
- Hadoop's cluster manager.
- `--master yarn`

**Kubernetes:**
- For containerized environments.
- `--master k8s://<apiserver-host>:<port>`


### Examples

#### Python Script - Local Mode
- Run my_script.py locally with 2 cores: 
  - `spark-submit --master local[2] my_script.py`

#### Cluster Mode - YARN
- Run a Python application on YARN with 4 executors:
  ```bash
  spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-memory 2g \
  --executor-cores 2 \
  my_script.py
  ```

#### Java/Scala Application
- Run a Java/Scala application using a JAR file
  ```bash
  spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --class com.example.MainClass \
  my_app.jar arg1 arg2
  ```

### spark-submit example like producton

  ```bash
  /bin/spark-submit \
  --master local[s] \
  --deploy-mode cluster \
  --class main-class.scala \
  --jars C:\my-sql-jar\my-sql-connector.jar \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  --conf spark.sql.broadcastTimeout=3600 \
  --conf spark.sql.autoBroadcastJoinThreshold=100000 \
  --driver-memory 1G \
  --executor-memory 2G
  ```