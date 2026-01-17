-- Setup script for UAT test data
-- Run this SQL in your Databricks SQL Editor to create test tables and data
-- This avoids the Spark codegen bug when using the Python connector

-- Drop existing tables
DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`;
DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_dim`;

-- Create source table
CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source` (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP
) USING DELTA;

-- Create dimension table
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

-- Insert test data (using SELECT to avoid Spark codegen bug)
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`
SELECT 1 as customer_id, 'Alice Smith' as name, 'alice@example.com' as email, 
       'San Francisco' as city, current_timestamp() as updated_at;

INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`
SELECT 2 as customer_id, 'Bob Jones' as name, 'bob@example.com' as email, 
       'New York' as city, current_timestamp() as updated_at;

INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`
SELECT 3 as customer_id, 'Carol White' as name, 'carol@example.com' as email, 
       'Chicago' as city, current_timestamp() as updated_at;

-- Verify
SELECT COUNT(*) as row_count FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`;
SELECT * FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers_source`;

