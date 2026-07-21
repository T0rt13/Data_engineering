-- ============================================================
-- Snowflake -> Amazon S3 using Apache Iceberg
-- External Catalog: AWS Glue
-- ============================================================

---------------------------------------------------------------
-- 1. Create Warehouse
---------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE project1_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE project1_wh;

---------------------------------------------------------------
-- 2. Create Database
---------------------------------------------------------------

CREATE OR REPLACE DATABASE project1_db;

USE DATABASE project1_db;

---------------------------------------------------------------
-- 3. Create Schema
---------------------------------------------------------------

CREATE OR REPLACE SCHEMA project1_schema;

USE SCHEMA project1_schema;

SELECT CURRENT_VERSION();
SELECT CURRENT_REGION();
---------------------------------------------------------------
-- 4. Create External Volume + AWS Role: SnowflakeIceBerg
---------------------------------------------------------------
-- Create a new AWS role in IAM called 'SnowflakeIceBerg'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::<id>:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for the permissions, create one inline and point to your own bucket:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetBucketLocation",
-- 				"s3:ListBucket"
-- 			],
-- 			"Resource": "arn:aws:s3:::<bucket>"
-- 		},
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"s3:GetObject",
-- 				"s3:PutObject",
-- 				"s3:DeleteObject"
-- 			],
-- 			"Resource": "arn:aws:s3:::<bucket>/*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE EXTERNAL VOLUME my_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='<bucket>'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::<id>:role/SnowflakeIceBerg'
    )
)
ALLOW_WRITES = TRUE;

-- get the STORAGE_AWS_IAM_USER_ARN & STORAGE_AWS_EXTERNAL_ID
-- we will use these to update our trust policy in the role
-- this would be a best practice in prod
-- we will have to create a role with a generic trust policy first, 
-- then we can update with the information from this describe call 
DESC EXTERNAL VOLUME my_external_volume;

-- trusted entities for the SnowflakeIceberg role is updated with the new information for best security:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::<role>"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "<ExternalId>"
--                 }
--             }
--         }
--     ]
-- }


---------------------------------------------------------------
-- 5. Create Glue Catalog Integration + AWS Role: SnowflakeGlueRole
---------------------------------------------------------------

-- Create a new AWS role in IAM called 'SnowflakeGlueRole'
-- under trusted entities use (we will change later, use your own root account ID):
-- {
--   "Version": "2012-10-17",
--   "Statement": [
--     {
--       "Effect": "Allow",
--       "Principal": {
--         "AWS": "arn:aws:iam::<id>:root"
--       },
--       "Action": "sts:AssumeRole"
--     }
--   ]
-- }
-- for permissions, create an inline policy:
-- {
-- 	"Version": "2012-10-17",
-- 	"Statement": [
-- 		{
-- 			"Effect": "Allow",
-- 			"Action": [
-- 				"glue:GetDatabase",
-- 				"glue:GetDatabases",
-- 				"glue:CreateDatabase",
-- 				"glue:GetTable",
-- 				"glue:GetTables",
-- 				"glue:CreateTable",
-- 				"glue:UpdateTable",
-- 				"glue:DeleteTable"
-- 			],
-- 			"Resource": "*"
-- 		}
-- 	]
-- }

CREATE OR REPLACE CATALOG INTEGRATION my_catalog
CATALOG_SOURCE = GLUE
TABLE_FORMAT = ICEBERG
CATALOG_NAMESPACE = 'iceberg_catalog_db'
GLUE_AWS_ROLE_ARN = 'arn:aws:iam::<id>:role/SnowflakeGlueRole'
GLUE_CATALOG_ID = '<GLUE_CATALOG_ID>'
GLUE_REGION = 'us-east-2'
ENABLED = TRUE;

-- much like with our external volume, we will get values 
-- from this so that we can update our trust policy in our relevant AWS role 
DESC CATALOG INTEGRATION my_catalog;
-- update the trust policy on the SnowflakeGlueRole with the values from our describe call:
-- {
--     "Version": "2012-10-17",
--     "Statement": [
--         {
--             "Effect": "Allow",
--             "Principal": {
--                 "AWS": "arn:aws:iam::<role>"
--             },
--             "Action": "sts:AssumeRole",
--             "Condition": {
--                 "StringEquals": {
--                     "sts:ExternalId": "<ExternalId>"
--                 }
--             }
--         }
--     ]
-- }

---------------------------------------------------------------
-- 6. Create Iceberg Tables
---------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE customers
  EXTERNAL_VOLUME = 'my_external_volume'
  CATALOG = 'my_catalog'
  CATALOG_TABLE_NAME = 'customers';

CREATE OR REPLACE ICEBERG TABLE orders
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'orders';

CREATE OR REPLACE ICEBERG TABLE products
    EXTERNAL_VOLUME = 'my_external_volume'
    CATALOG = 'my_catalog'
    CATALOG_TABLE_NAME = 'products';

---------------------------------------------------------------
-- 7. Query Data
---------------------------------------------------------------

SELECT * FROM customers;

SELECT * FROM orders;

SELECT * FROM products;

---------------------------------------------------------------
-- 8. Verify Metadata
---------------------------------------------------------------

SHOW ICEBERG TABLES;

DESCRIBE ICEBERG TABLE customers;
DESCRIBE ICEBERG TABLE orders;
DESCRIBE ICEBERG TABLE products;

SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
SELECT COUNT(*) FROM products;

---------------------------------------------------------------
-- 9. Analytics: 5 analytical SQL queries
---------------------------------------------------------------
-- #1: Count the number of orders each customer has
CREATE OR REPLACE VIEW customer_summary AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS total_orders
FROM
    customers c
LEFT JOIN
    orders o
ON
    c.customer_id = o.customer_id
GROUP BY
    c.customer_id,
    c.first_name,
    c.last_name;
SELECT * FROM customer_summary;

-- #2: Count number of times each product was ordered
CREATE OR REPLACE VIEW num_product_orders AS
SELECT
    product_id,
    COUNT(product_id) AS order_count
FROM
    orders
GROUP BY 
    product_id;
SELECT * FROM num_product_orders;

-- #3: Find products that need to be restocked
CREATE OR REPLACE VIEW need_restock AS
SELECT * FROM products WHERE stock_quantity < 1;
SELECT * FROM need_restock;

-- 4: Find most profitable product
CREATE OR REPLACE VIEW most_profitable AS
SELECT 
    product_id,
    SUM(total_amount) AS total 
FROM
    orders
GROUP BY
    product_id
ORDER BY total DESC;
SELECT * FROM most_profitable LIMIT 4;

-- 5: Identify high, medium, and low value customers
CREATE OR REPLACE VIEW customer_impact AS
SELECT 
    customer_id,
    CASE 
        WHEN sum(total_amount) > 900 THEN 'High'
        WHEN sum(total_amount) < 900 AND sum(total_amount) > 200 THEN 'Medium'
        ELSE 'Low'
    END AS category
FROM
    orders
GROUP BY
    customer_id
ORDER BY category;
SELECT * FROM customer_impact;


---------------------------------------------------------------
-- 10. Cleanup
---------------------------------------------------------------

DROP TABLE customers;
DROP TABLE orders;
DROP TABLE products;

DROP SCHEMA project1_schema;
DROP DATABASE project1_db;

DROP EXTERNAL VOLUME my_external_volume;
DROP CATALOG INTEGRATION my_catalog;