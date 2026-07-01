# Exercise: RDD Basics

## Overview
Practice creating RDDs from different sources and applying basic transformations like map, filter, and flatMap.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Prerequisites
- Completed Monday exercises
- Understanding of RDD concepts from written content

---

## Core Tasks

### Task 1: Create RDDs from Different Sources

Create a file `rdd_basics.py` and implement the following:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "RDDBasics")

# 1. Create RDD from a Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
print(f"Numbers: {numbers.collect()}")
print(f"Partitions: {numbers.getNumPartitions()}")

# 2. Create RDD with explicit partitions
# YOUR CODE: Create the same list with exactly 4 partitions

# 3. Create RDD from a range
# YOUR CODE: Create RDD from range(1, 101)

sc.stop()
```

### Task 2: Apply map() Transformation

Add these tasks to your script:

```python
# Given: numbers RDD [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Task A: Square each number
# Expected: [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
squared = # YOUR CODE

# Task B: Convert to strings with prefix
# Expected: ["num_1", "num_2", "num_3", ...]
prefixed = # YOUR CODE
```

### Task 3: Apply filter() Transformation

```python
# Task A: Keep only even numbers
# Expected: [2, 4, 6, 8, 10]
evens = # YOUR CODE

# Task B: Keep numbers greater than 5
# Expected: [6, 7, 8, 9, 10]
greater_than_5 = # YOUR CODE

# Task C: Combine - even AND greater than 5
# Expected: [6, 8, 10]
combined = # YOUR CODE
```

### Task 4: Apply flatMap() Transformation

```python
# Given sentences
sentences = sc.parallelize([
    "Hello World",
    "Apache Spark is Fast",
    "PySpark is Python plus Spark"
])

# Task A: Split into words (use flatMap)
# Expected: ["Hello", "World", "Apache", "Spark", ...]
words = # YOUR CODE

# Task B: Create pairs of (word, length)
# Expected: [("Hello", 5), ("World", 5), ...]
word_lengths = # YOUR CODE
```

### Task 5: Chain Transformations

Create a processing pipeline:

```python
# Given: log entries
logs = sc.parallelize([
    "INFO: User logged in",
    "ERROR: Connection failed",
    "INFO: Data processed",
    "ERROR: Timeout occurred",
    "DEBUG: Cache hit"
])

# Pipeline: Extract only ERROR messages, convert to uppercase words
# 1. Filter to keep only ERROR lines
# 2. Split each line into words
# 3. Convert each word to uppercase
# Expected: ["ERROR:", "CONNECTION", "FAILED", "ERROR:", "TIMEOUT", "OCCURRED"]
error_words = # YOUR PIPELINE
```

---

## Expected Output

Your script should print results for each task showing the transformations work correctly.

---

## Deliverables

1. `rdd_basics.py` - Complete script with all tasks implemented
2. Run output showing correct results

---

## Definition of Done

- [ ] RDDs created from list, with partitions, and from range
- [ ] map() transformations produce correct output
- [ ] filter() transformations produce correct output
- [ ] flatMap() correctly splits sentences into words
- [ ] Chained transformation pipeline works correctly
- [ ] Script runs without errors

---

## Additional Resources
- Written Content: `introduction-to-rdd.md`
- Written Content: `basic-rdd-operations.md`
