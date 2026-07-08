# GOAL: use files: customers.csv, orders.csv. Use them to print out the dataframe of customers with no orders
# | customer_id | first_name | last_name |
# | ----------- | ---------- | --------- |
# | 1002        | Bob        | Smith     |
# | 1004        | David      | Wilson    |
# | 1007        | Grace      | Moore     |
# | 1009        | Ivy        | Anderson  |
# | 1013        | Mia        | Martin    |
# | 1015        | Olivia     | Garcia    |

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("findCustomersNoOrders").master("local[*]").getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# read in csv files, and use the header for column names of the DataFrame
customers = spark.read.csv("customers.csv", header=True)
orders = spark.read.csv("orders.csv", header=True)

# Anti left join to return rows in left DataFrame with no matching rows in right
no_orders = customers.join(
    orders, 
    on=customers["customer_id"] == orders["customer_id"],
    how="left_anti"
)

no_orders.show()


# (Note) SQL equivalent of anti left join:
"""
SELECT *
FROM customers c
LEFT JOIN orders o
    ON c.customer_id = o.customer_id
WHERE o.customer_id IS NULL;
"""