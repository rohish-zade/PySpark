## Spark SQL Engine
- The Spark SQL engine is the core component of Apache Spark responsible for processing `structured` and `semi-structured data`.
- It powers Spark's ability to handle SQL queries, `DataFrames`, and `Datasets`. 
- It integrates with the `core Spark engine` to optimize and run SQL queries efficiently.
- Spark SQL engine is the underlying engine that enables Spark to handle structured data efficiently, optimizing both SQL queries and DataFrame-based operations with advanced query planning, optimization, and execution capabilities.
  
  
  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/spark-sql-engine-1.png)


## Catalyst Optimizer
The Catalyst Optimizer in Spark SQL is the core component responsible for `optimizing SQL queries` or `DataFrame` operations.

It converts a `logical plan` into an `optimized execution plan` to make query execution more efficient. 

The optimization process is divided into `four` phases:
  - `Analysis`, `Logical Planning or Logical Optimization`,` Physical Planning`, `Code Generation`

  ![](https://github.com/rohish-zade/PySpark/blob/main/materials/Catalyst-Optimizer.png)

### 1. Analysis
- **What happens?:**
  - In this phase, the logical plan is analyzed to resolve references and validate the query.
- **How?:** 
  - The optimizer ensures that all references (e.g., `table names`, `column names`) are valid and checks for the existence of the required fields.
- **Tasks:**
  - `Resolving references:` Ensures that the columns and tables in the query exist.
  - `Type-checking:` Ensures that data types are compatible (e.g., comparing an integer with a string will throw an error).
- **Example:** 
  - If the query refers to a column `age`, it checks that this column actually exists in the table being queried if does not exist it will throw an *`AnalysisExeption`*.
### 2. Logical Planning or Logical Optimization


### 3. Physical Planning


### 4. Code Generation
