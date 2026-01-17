-- Setup script for UAT test data
-- Run this SQL in your Databricks SQL Editor to create test tables and data
-- Uses CREATE TABLE AS SELECT (CTAS) which is more reliable

-- Drop existing tables
DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`;
DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_dim`;

-- Create source table WITH DATA using CTAS (single statement, very reliable!)
CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`
USING DELTA
AS
SELECT 1 as customer_id, 'Alice Smith' as name, 'alice@example.com' as email, 
       'San Francisco' as city, current_timestamp() as updated_at
UNION ALL
SELECT 2, 'Bob Jones', 'bob@example.com', 'New York', current_timestamp()
UNION ALL
SELECT 3, 'Carol White', 'carol@example.com', 'Chicago', current_timestamp();

-- Create empty dimension table (standard DDL)
CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_dim` (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
) USING DELTA;

-- Verify data
SELECT COUNT(*) as row_count FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`;
-- Should return: 3

SELECT * FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source` ORDER BY customer_id;
-- Should show Alice, Bob, and Carol

