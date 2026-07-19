-- ================================================================================================================================
-- Note: Before the COPY INTO command, upload the CSV file to the demo_stage in snowflake:
-- PUT file://C:/Users/<username>/Documents/Data_engineering/snowflake/udf_sample_data.csv @demo_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- ================================================================================================================================

-- Create a database and schema
CREATE OR REPLACE DATABASE UDF_DEMO;

USE DATABASE UDF_DEMO;

CREATE OR REPLACE SCHEMA UDFS;

USE SCHEMA UDFS;

-- File format for CSV
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = CSV
SKIP_HEADER = 1;  

-- Internal stage
CREATE OR REPLACE STAGE demo_stage
FILE_FORMAT = csv_format;

-- Table
CREATE OR REPLACE TABLE products (
    id INTEGER,
    name STRING,
    price NUMBER(10,2)
);

-- ================================================================================================================================
-- Run this part in the terminal to upload the csv to demo_stage:
-- USE DATABASE UDF_DEMO;
-- USE SCHEMA UDFS;
-- PUT file://C:/Users/<username>/Documents/Data_engineering/snowflake/udf_sample_data.csv @demo_stage AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- ================================================================================================================================

-- Copy from demo_stage into the products table.
COPY INTO products
FROM @demo_stage/udf_sample_data.csv;

-- Confirm the data got loaded correctly
SELECT * FROM products;

-- Create a simple UDF
CREATE OR REPLACE FUNCTION apply_discount(price NUMBER(10,2))
RETURNS NUMBER(10,2)
AS
$$
    price * 0.90
$$;

-- Test it
SELECT
    id,
    name,
    price,
    apply_discount(price) AS discounted_price
FROM products;

-- ================================================================================================================================
-- Cleanup
-- ================================================================================================================================

-- Drop the UDF
DROP FUNCTION IF EXISTS apply_discount(NUMBER(10,2));

DROP TABLE IF EXISTS products;

-- Drop the demo_stage
-- Also deletes the uploaded file with the stage
DROP STAGE IF EXISTS demo_stage;

DROP FILE FORMAT IF EXISTS csv_format;

DROP SCHEMA IF EXISTS PUBLIC;

-- Return to a different database before dropping UDF_DEMO
USE DATABASE SNOWFLAKE;

-- Drop the demo database (this also removes the schema)
DROP DATABASE IF EXISTS UDF_DEMO;