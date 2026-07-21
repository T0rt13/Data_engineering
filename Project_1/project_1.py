from pyspark.sql import SparkSession, functions as F 
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, FloatType, DateType

spark = (
    SparkSession.builder

    # Set a name for the Spark application (shows up in Spark UI/logs).
    .appName("DataIngestion")

    # Enable Apache Iceberg SQL extensions so Spark understands
    # Iceberg-specific SQL commands and table operations.
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )

    # Register a catalog named "glue_catalog".
    # Spark will use this catalog whenever tables are referenced with
    # the prefix "glue_catalog".
    .config(
        "spark.sql.catalog.glue_catalog",
        "org.apache.iceberg.spark.SparkCatalog"
    )

    # Tell Spark that this catalog should use AWS Glue
    # as the metadata store for Iceberg tables.
    .config(
        "spark.sql.catalog.glue_catalog.catalog-impl",
        "org.apache.iceberg.aws.glue.GlueCatalog"
    )

    # Specify the S3 warehouse location where Iceberg table data
    # and metadata files will be stored.
    .config(
        "spark.sql.catalog.glue_catalog.warehouse",
        "s3://<bucket>/iceberg/"
    )

    # Configure Iceberg to use the S3FileIO implementation
    # for reading and writing data in Amazon S3.
    .config(
        "spark.sql.catalog.glue_catalog.io-impl",
        "org.apache.iceberg.aws.s3.S3FileIO"
    )

    # Create the Spark session with all of the above settings.
    .getOrCreate()
)



orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("ship_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", StringType(), True),
    StructField("discount_pct", FloatType(), True),
    StructField("total_amount", FloatType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", StringType(), True),
    StructField("cost", StringType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("weight_kg", FloatType(), True),
    StructField("created_date", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("country", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("is_active", BooleanType(), True),
    StructField("loyalty_points", IntegerType(), True)
])


orders_df = spark.read.options(
    header=True,
).schema(orders_schema).csv(
    "s3://<bucket>/orders.csv"
)

products_df = spark.read.options(
    header=True,
).schema(products_schema).csv(
    "s3://<bucket>/products.csv"
)

customers_df = spark.read.options(
    header=True,
).schema(customers_schema).csv(
    "s3://<bucket>/customers.csv"
)

# print schemas
orders_df.printSchema()
products_df.printSchema()
customers_df.printSchema()

# Create an Iceberg database (namespace) in AWS Glue if it
# doesn't already exist.
spark.sql("""
CREATE DATABASE IF NOT EXISTS glue_catalog.iceberg_catalog_db
""")

# =============================================================
# Customers Data Cleaning
# =============================================================
customers_df_clean = customers_df.withColumn(
    "first_name", F.trim(F.col("first_name"))
)
customers_df_clean = customers_df_clean.withColumn(
    "last_name", F.trim(F.col("last_name"))
)
customers_df_clean = customers_df_clean.withColumn(
    "email", F.trim(F.col("email"))
)
customers_df_clean = customers_df_clean.withColumn(
    "phone", F.trim(F.col("phone"))
)
customers_df_clean = customers_df_clean.withColumn(
    "country", F.trim(F.col("country"))
)
customers_df_clean = customers_df_clean.withColumn(
    "state", F.trim(F.col("state"))
)
customers_df_clean = customers_df_clean.withColumn(
    "postal_code", F.trim(F.col("postal_code"))
)


#  Regex pattern for a standard email
email_pattern = r"^([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)$"
# Keep only strings that match the email pattern, otherwise return null
customers_df_clean = customers_df_clean.withColumn(
    "email",
    F.when(
        F.col("email").rlike(email_pattern),
        F.col("email")
    )
)

phone_pattern = r"^(\+?\d{1,3}[\s.-]?)?(\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}$"

customers_df_clean = customers_df_clean.withColumn(
    "phone",
    F.when(
        F.col("phone").rlike(phone_pattern),
        F.col("phone")
    )
)

# make signup_date look like 2023-07-03
date_with_dashes = F.regexp_replace("signup_date", "/", "-")
customers_df_clean = customers_df_clean.withColumn(
    "signup_date",
    F.to_date(
        F.try_to_timestamp(date_with_dashes, F.lit("yyyy-MM-dd"))
    )
)


customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    F.expr("try_cast(loyalty_points as int)")
)
customers_df_clean = customers_df_clean.withColumn(
    "loyalty_points",
    F.when(F.col("loyalty_points") < 0, 0).otherwise(F.col("loyalty_points"))
)

customers_df_clean = customers_df_clean.dropDuplicates()
customers_df_clean = customers_df_clean.dropna()

# =============================================================
# Orders Data Cleaning
# =============================================================
# ensure order_id is a positive integer
orders_df_clean = orders_df.withColumn(
    "order_id",
    F.when(
        F.col("order_id").rlike(r"^\d+$"),
        F.col("order_id")
    ).otherwise(None)
)

# ensure customer_id is a positive integer
orders_df_clean = orders_df_clean.withColumn(
    "customer_id",
    F.when(
        F.col("customer_id").rlike(r"^\d+$"),
        F.col("customer_id")
    ).otherwise(None)
)

# ensure product_id looks like: P1002
orders_df_clean = orders_df_clean.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(r"^P\d+$"),
        F.col("product_id")
    ).otherwise(None)
)

# make order_date look like 2023-07-03
date_with_dashes = F.regexp_replace("order_date", "/", "-")
orders_df_clean = orders_df_clean.withColumn(
    "order_date",
    F.to_date(
        F.try_to_timestamp(date_with_dashes, F.lit("yyyy-MM-dd"))
    )
)

# make ship_date look like 2023-07-03
date_with_dashes = F.regexp_replace("ship_date", "/", "-")
orders_df_clean = orders_df_clean.withColumn(
    "ship_date",
    F.to_date(
        F.try_to_timestamp(date_with_dashes, F.lit("yyyy-MM-dd"))
    )
)

# set quantities less than 0 to 0
orders_df_clean = orders_df_clean.withColumn(
    "quantity",
    F.when(F.col("quantity") < 0, 0).otherwise(F.col("quantity"))
)

# cast unit_price and total_amount to a float, remove $ symbol, ensure values are positive
price = F.regexp_replace(F.col("unit_price"), r"\$", "").try_cast("float")
orders_df_clean = orders_df_clean.withColumn(
    "unit_price",
    F.when(price >= 0, price)
)
total_amount = F.regexp_replace(F.col("total_amount"), r"\$", "").try_cast("float")
orders_df_clean = orders_df_clean.withColumn(
    "total_amount",
    F.when(total_amount >= 0, total_amount)
)


orders_df_clean = orders_df_clean.dropDuplicates()
orders_df_clean = orders_df_clean.dropna()

# =============================================================
# Products Data Cleaning
# =============================================================
# ensure product_id looks like: P1002
products_df_clean = products_df.withColumn(
    "product_id",
    F.when(
        F.col("product_id").rlike(r"^P\d+$"),
        F.col("product_id")
    ).otherwise(None)
)

# get rid of leading/trailing spaces
products_df_clean = products_df_clean.withColumn(
    "product_name", F.trim(F.col("product_name"))
)
products_df_clean = products_df_clean.withColumn(
    "category", F.trim(F.col("category"))
)
products_df_clean = products_df_clean.withColumn(
    "brand", F.trim(F.col("brand"))
)

# cast price and cost to a float, remove $ symbol, ensure values are positive
price = F.regexp_replace(F.col("price"), r"\$", "").try_cast("float")
products_df_clean = products_df_clean.withColumn(
    "price",
    F.when(price >= 0, price)
)
cost = F.regexp_replace(F.col("cost"), r"\$", "").try_cast("float")
products_df_clean = products_df_clean.withColumn(
    "cost",
    F.when(cost >= 0, cost)
)

# set stock_quantity and weight that are less than 0 to 0
products_df_clean = products_df_clean.withColumn(
    "stock_quantity",
    F.when(F.col("stock_quantity") < 0, 0).otherwise(F.col("stock_quantity"))
)
products_df_clean = products_df_clean.withColumn(
    "weight_kg",
    F.when(F.col("weight_kg") < 0, 0).otherwise(F.col("weight_kg"))
)

# make created_date look like 2023-07-03
date_with_dashes = F.regexp_replace("created_date", "/", "-")
products_df_clean = products_df_clean.withColumn(
    "created_date",
    F.to_date(
        F.try_to_timestamp(date_with_dashes, F.lit("yyyy-MM-dd"))
    )
)


products_df_clean = products_df_clean.dropDuplicates()
products_df_clean = products_df_clean.dropna()

# =============================================================

# Write the cleaned customers DataFrame as an Iceberg table.
(
customers_df_clean.writeTo(
        # Fully qualified table name: catalog.database.table
        "glue_catalog.iceberg_catalog_db.customers"
    )
    # Specify that the table format should be Apache Iceberg.
    .using("iceberg").createOrReplace()
)

# Query Cleaned Customers DataFrame to verify it is correct.
spark.sql("""
SELECT *
FROM glue_catalog.iceberg_catalog_db.customers
""").show()


# Write the cleaned orders DataFrame as an Iceberg table.
orders_df_clean.writeTo("glue_catalog.iceberg_catalog_db.orders") \
    .using("iceberg").createOrReplace()

spark.sql("SELECT * FROM glue_catalog.iceberg_catalog_db.orders").show()


# Write the cleaned products DataFrame as an Iceberg table.
products_df_clean.writeTo("glue_catalog.iceberg_catalog_db.products") \
    .using("iceberg").createOrReplace()

spark.sql("SELECT * FROM glue_catalog.iceberg_catalog_db.products").show()

# Stop the Spark session and release cluster resources.
spark.stop()