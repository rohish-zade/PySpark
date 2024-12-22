### Projection Pushdown
- Projection pushdown refers to selecting only the required columns as early as possible in the query execution. 
- This avoids reading unnecessary columns, thus reducing I/O, memory usage, and computational overhead.

### Predicate Pruning
- Predicate pruning refers to filtering rows as early as possible by pushing down filter conditions to the data source.
- This avoids reading rows that do not meet the conditions, saving time and resources.

## optimization techniques 
Optimizing Spark jobs is essential for reducing runtime and ensuring efficient resource utilization.

- Partitioning and Parallelism:
- Broadcast Joins:
- Caching and Persistence: 
  - For intermediate datasets that are reused multiple times, I use cache() or persist(). This avoids recomputation and speeds up subsequent operations.
- Predicate Pushdown and Column Pruning:
- Adaptive Query Execution (AQE):
  - I enable Adaptive Query Execution (AQE) to dynamically optimize query plans at runtime
- Efficient File Formats and Compression:
- Handling Data Skew:
