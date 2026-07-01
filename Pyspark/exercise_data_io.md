# Exercise: Data I/O with RDDs

## Overview
Practice loading data from files, processing it, and saving results in different formats.

**Duration:** 45-60 minutes  
**Mode:** Individual

---

## Setup

Create a sample data file `sales_data.csv`:
```csv
product_id,name,category,price,quantity
P001,Laptop,Electronics,999.99,5
P002,Mouse,Electronics,29.99,50
P003,Desk,Furniture,199.99,10
P004,Chair,Furniture,149.99,20
P005,Monitor,Electronics,299.99,15
P006,Keyboard,Electronics,79.99,30
P007,Lamp,Furniture,49.99,25
```

---

## Core Tasks

### Task 1: Load Data with textFile

Create `data_io.py`:

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "DataIO")

# Load the CSV file
lines = sc.textFile("sales_data.csv")

# Skip header line
header = lines.first()
data = lines.filter(lambda line: line != header)

print(f"Header: {header}")
print(f"Data records: {data.count()}")
print(f"First record: {data.first()}")
```

### Task 2: Parse CSV Records

```python
def parse_record(line):
    """Parse CSV line into structured data."""
    parts = line.split(",")
    return {
        "product_id": parts[0],
        "name": parts[1],
        "category": parts[2],
        "price": float(parts[3]),
        "quantity": int(parts[4])
    }

# Parse all records
parsed = data.map(parse_record)

# Show parsed data
for record in parsed.take(3):
    print(record)
```

### Task 3: Process and Save Results

```python
# Calculate revenue for each product
revenue = parsed.map(lambda r: f"{r['product_id']},{r['name']},{r['price'] * r['quantity']:.2f}")

# Save to output directory
# YOUR CODE: Use saveAsTextFile to save revenue data
```

### Task 4: Load Multiple Files

Create additional test files and load with wildcards:

```python
# YOUR CODE: Create sales_data_2.csv with more records
# YOUR CODE: Load all CSV files using wildcard pattern
# all_data = sc.textFile("sales_data*.csv")
```

### Task 5: Coalesce Output

Save to a single output file:

```python
# YOUR CODE: Use coalesce(1) before saveAsTextFile
# This creates a single output file instead of multiple parts
```

---

## Deliverables

1. `data_io.py` - Complete script
2. `sales_data.csv` - Input file
3. Output directory with processed results

---

## Definition of Done

- [ ] Data loaded successfully from CSV
- [ ] Header line filtered out
- [ ] Records parsed into structured format
- [ ] Revenue calculated and saved
- [ ] Coalesce produces single output file
- [ ] Script runs without errors

---

## Additional Resources
- Written Content: `data-loading-and-saving-through-rdds.md`
