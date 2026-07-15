-- ============================================================
-- Snowflake Managed Iceberg Catalog
-- ============================================================

---------------------------------------------------------------
-- 1. Create Warehouse
---------------------------------------------------------------

CREATE OR REPLACE WAREHOUSE demo_wh
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE demo_wh;

---------------------------------------------------------------
-- 2. Create Database
---------------------------------------------------------------

CREATE OR REPLACE DATABASE iceberg_demo;

USE DATABASE iceberg_demo;

---------------------------------------------------------------
-- 3. Create Schema
---------------------------------------------------------------

CREATE OR REPLACE SCHEMA demo;

USE SCHEMA demo;

SELECT CURRENT_VERSION();
SELECT CURRENT_REGION();

---------------------------------------------------------------
-- 4. Create External Volume
--    Update trust relationships for SnowflakeIceBerg 
---------------------------------------------------------------

CREATE OR REPLACE EXTERNAL VOLUME my_external_volume
STORAGE_LOCATIONS =
(
    (
        NAME='s3_location'
        STORAGE_PROVIDER='S3'
        STORAGE_BASE_URL='s3://<bucket_name>/iceberg/'
        STORAGE_AWS_ROLE_ARN='arn:aws:iam::<AWS_ID>:role/SnowflakeIceBerg'
    )
)
ALLOW_WRITES = TRUE;

DESC EXTERNAL VOLUME my_external_volume;

---------------------------------------------------------------
-- 5. Storage Integration
--    Update trust relationships for SnowflakeIceBerg 
---------------------------------------------------------------

CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN =
'arn:aws:iam::<AWS_ID>:role/SnowflakeIceBerg'
STORAGE_ALLOWED_LOCATIONS =
(
    's3://<bucket_name>/iceberg/'
);

DESC STORAGE INTEGRATION my_s3_integration;


---------------------------------------------------------------
-- 6. Create Stage For Source Parquet File
---------------------------------------------------------------

CREATE OR REPLACE STAGE titanic_stage
URL = 's3://<bucket_name>/iceberg/source/'
STORAGE_INTEGRATION = my_s3_integration;

LIST @titanic_stage;

---------------------------------------------------------------
-- 7. Create empty internal catalog Iceberg table
--    Took schema from 00000-<uuid>.metadata.json on S3
---------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE titanic
(
    PassengerId INTEGER,
    Survived INTEGER,
    Pclass INTEGER,
    Name STRING,
    Sex STRING,
    Age FLOAT,
    SibSp INTEGER,
    Parch INTEGER,
    Ticket STRING,
    Fare FLOAT,
    Cabin STRING,
    Embarked STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'my_external_volume'
BASE_LOCATION = 'internal_catalog/iceberg_demo/titanic/';

--------------------------------------------------------------
-- 8. Copy values into the table
---------------------------------------------------------------
COPY INTO titanic
FROM @titanic_stage/titanic.parquet
FILE_FORMAT = (
    TYPE = PARQUET
)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

---------------------------------------------------------------
-- 9. Verify
---------------------------------------------------------------

SHOW ICEBERG TABLES;

DESCRIBE ICEBERG TABLE titanic;

SELECT COUNT(*)
FROM titanic;

SELECT *
FROM titanic
LIMIT 10;

---------------------------------------------------------------
-- 10. Cleanup 
---------------------------------------------------------------

DROP TABLE titanic;
DROP SCHEMA demo;
DROP DATABASE iceberg_demo;
DROP EXTERNAL VOLUME my_external_volume;