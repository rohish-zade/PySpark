## Job, Stage, and Task in Apache Spark

In Apache Spark, the concepts of Job, Stage, and Task are fundamental to understanding how Spark processes data in a distributed environment. 

These terms relate to how Spark breaks down a complex computation into smaller, manageable units that can be executed efficiently across a cluster.

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/Job%2C_Stage_Task.png)

### Potential interview question:

1️⃣ What is a job, stage and task in spark?

2️⃣ How many jobs will be created in the given question?

3️⃣ How many stages will be created?

4️⃣ How many tasks will be created?

### Job
- A job is the highest level of execution in Spark. 
- A job is triggered when an action (e.g., `count()`, `collect()`, `saveAsTextFile()`) is called on a Spark RDD, DataFrame, or Dataset.
- In some cases, actions are triggered internally even if the user does not explicitly request them. (e.g.,`inferSchema=True`)
- A job can involve many transformations (e.g., `map()`, `filter()`, `reduceByKey()`) that need to be executed before the action can be completed.
- Internally, a job is divided into multiple stages for execution.
- **Example**: When you run `df.count()`, Spark creates a job to compute the count of the DataFrame.


### Stage
- A Stage is a smaller unit of execution within a job. 
- Spark breaks a job into stages based on the wide transformations (i.e., operations like `groupBy`, `reduceByKey`, `join`, or `sort`) that require shuffling data across the cluster.
- Each stage consists of a series of narrow transformations (like `map()`, `filter()`, `flatMap()`), which do not require data shuffling.
- Stages are executed sequentially, and one stage must be completed before the next stage can begin if there is a dependency (wide transformations introduce these dependencies).
- The number of stages depends on the number of wide transformations in a job.
- **Example**: If you have a series of `map` operations followed by a `reduceByKey`, Spark will split the computation into two stages: one for the `map` operations and another for the `reduceByKey` since it involves a shuffle.


### Task
- Tasks are the smallest units of work in Spark and are executed by the Spark executors.
- Each stage is further divided into multiple tasks, each task corresponding to a partition of the data.
- Tasks are executed in parallel on different nodes in the cluster.
- ***The number of tasks in a stage is determined by the number of partitions in the RDD or DataFrame being processed.***


## Example Scenario to answer following question:
2️⃣ How many jobs will be created in the given question?

3️⃣ How many stages will be created?

4️⃣ How many tasks will be created?

  ```python
  # Reading the CSV File: this section has 1 action `read` so spark will create 1 job and 1 job will have minimum 1 stage and 1 stage will have minimum 1 task by default
  # So in job 1, we have 1 stage, 1 task and we have only 1 partition
  employee_df = spark.read.format("csv") \ 
      .option("header", "true") \
      .load("C:\\Users\\rohish\\Documents\\data_engineering\\spark_data\\employee_file.csv")
  
  # job 2 will be from repartion() to collect() method at the last line, a job will take/considered all the tranformation untill it finds an action, in this case its from repartion() to collect()

  # Repartitioning the data: whenever there is a wide dependency tranformation one stage will be created.
  # Here one stage will be created and will be having 2 partitions
  employee_df = employee_df.repartition(2)
  
  # filter and select will be in 1 stage as they are narrow dependency tranformations and we have 2 data partitions so will be having 2 tasks each for 1 partition
  # group by again is a wide dependency tranformation so one stage will be created.
  # By default for joins, groupBy etc. 200 partitions will be created even if the data is less. so will be having 200 tasks.
  employee_df = employee_df.filter(col("salary") > 90000) \
      .select("id", "name", "age", "salary") \
      .groupBy("age").count()
  
  employee_df.collect()

  # So here in job 2, we have 3 stages(1 for partitions, 1 for select and filter, 1 for groupBy) and 203 task(1 for partition stage, 2 for select and filter, 200 for groupBy)
  ```

**Spark UI for above code execution**:
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/Sparkui_Job%2C_Stage_Task.png)

**DAG for Job 2**:
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/sparkui_for_job2.png)

**Answers as per for above code:**

2️⃣ How many jobs will be created in the given question?
  - 2 Jobs

3️⃣ How many stages will be created?
- 4 Stages

4️⃣ How many tasks will be created?
- 204 Tasks

