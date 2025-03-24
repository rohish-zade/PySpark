## Accumulator in Apache Spark
- In Apache Spark, an accumulator is a shared variable that can be updated by tasks running on different nodes in a cluster, but its value can only be accessed by the driver program.
- They are primarily used for aggregating information across tasks, such as counting events or summing values.

### Key Points about Accumulators:
- `Shared Variable:` Accumulators are shared variables, meaning they can be accessed by multiple tasks running on different executors (worker nodes) in the Spark cluster.
- `Write-Only for Workers:` Tasks can only add to an accumulator, not read its value. 
- `Driver Access:` Only the driver program can read the final value of an accumulator.
- `Lazy Evaluation:` Accumulators are only updated when an action is triggered (e.g., .count(), .collect()). 

### Use Cases:
- `Counters:` Counting the number of occurrences of specific events or errors. 
- `Sums:` Calculating the sum of values across multiple tasks. 
- `Debugging:` Tracking information for debugging purposes.
- Counting errors, warnings, or log events in a distributed system.
- Monitoring job progress without affecting transformations

### How it works:
- The driver program creates an accumulator and initializes it with a starting value. 
- Tasks running on executors can increment or update the accumulator's value using the += operator. 
- After all tasks are completed, the driver program can access the final value of the accumulator.

### Example: 
Create an accumulator to count invalid email addresses

  ```python
  # Create an accumulator to count invalid email addresses
  invalid_emails = sc.accumulator(0)  
  # Iterate through a list of email addresses
  for email in ["john.doe@example.com", "invalid_email", "jane.doe@example.com"]:
      # Check if the email is valid
      if not isValidEmail(email):
          # Increment the accumulator if the email is invalid
          invalid_emails.add(1)  
  # Print the total number of invalid emails
  print("Total invalid emails:", invalid_emails.value)
  ```