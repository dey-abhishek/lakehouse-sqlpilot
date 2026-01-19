-- Lakehouse SQLPilot - SCD2 Integration Test Setup
-- Creates tables and sample data for SCD2 pattern testing

-- Use the test catalog and schema
USE CATALOG `lakehouse-sqlpilot`;
USE SCHEMA `lakehouse-sqlpilot-schema`;

-- ============================================================================
-- 1. SOURCE TABLE: customer_dim_source
-- ============================================================================
-- This table simulates incoming customer data with updates

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source` (
  customer_id STRING NOT NULL COMMENT 'Business key - unique customer identifier',
  customer_name STRING NOT NULL COMMENT 'Customer name',
  email STRING NOT NULL COMMENT 'Customer email',
  phone STRING COMMENT 'Customer phone number',
  address STRING COMMENT 'Customer address',
  city STRING COMMENT 'City',
  state STRING COMMENT 'State',
  country STRING COMMENT 'Country',
  customer_segment STRING COMMENT 'Customer segment (Premium, Standard, Basic)',
  account_status STRING COMMENT 'Account status (Active, Inactive, Suspended)',
  last_updated_at TIMESTAMP NOT NULL COMMENT 'Last update timestamp'
) 
USING DELTA
COMMENT 'Source table for customer dimension - simulates incoming data';

-- ============================================================================
-- 2. TARGET TABLE: customer_dim_scd2
-- ============================================================================
-- This table maintains historical records using SCD Type 2

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_scd2`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_scd2` (
  -- Business columns
  customer_id STRING NOT NULL COMMENT 'Business key - unique customer identifier',
  customer_name STRING NOT NULL COMMENT 'Customer name',
  email STRING NOT NULL COMMENT 'Customer email',
  phone STRING COMMENT 'Customer phone number',
  address STRING COMMENT 'Customer address',
  city STRING COMMENT 'City',
  state STRING COMMENT 'State',
  country STRING COMMENT 'Country',
  customer_segment STRING COMMENT 'Customer segment (Premium, Standard, Basic)',
  account_status STRING COMMENT 'Account status (Active, Inactive, Suspended)',
  
  -- SCD Type 2 metadata columns
  valid_from TIMESTAMP NOT NULL COMMENT 'Start date/time of record validity',
  valid_to TIMESTAMP NOT NULL COMMENT 'End date/time of record validity',
  is_current BOOLEAN NOT NULL COMMENT 'Flag indicating current record'
) 
USING DELTA
PARTITIONED BY (valid_from)
COMMENT 'SCD Type 2 dimension table for customer data with full history';

-- Create index on business key for faster lookups
CREATE INDEX IF NOT EXISTS idx_customer_id 
ON `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_scd2` (customer_id);

-- ============================================================================
-- 3. INSERT INITIAL SOURCE DATA (Day 1)
-- ============================================================================

INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`
VALUES
  ('C001', 'John Smith', 'john.smith@email.com', '555-0101', '123 Main St', 'San Francisco', 'CA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-01 00:00:00'),
  ('C002', 'Jane Doe', 'jane.doe@email.com', '555-0102', '456 Oak Ave', 'New York', 'NY', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-01 00:00:00'),
  ('C003', 'Bob Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', 'Basic', 'Active', TIMESTAMP '2026-01-01 00:00:00'),
  ('C004', 'Alice Williams', 'alice.w@email.com', '555-0104', '321 Elm St', 'Seattle', 'WA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-01 00:00:00'),
  ('C005', 'Charlie Brown', 'charlie.b@email.com', '555-0105', '654 Maple Dr', 'Austin', 'TX', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-01 00:00:00');

SELECT 'Initial source data loaded: ' || COUNT(*) || ' records' as status
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

-- ============================================================================
-- 4. VERIFICATION TABLES
-- ============================================================================

-- Table to store test run results
DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`scd2_test_results`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`scd2_test_results` (
  test_run_id STRING NOT NULL,
  test_name STRING NOT NULL,
  test_phase STRING NOT NULL COMMENT 'initial_load, update_records, new_records, etc.',
  records_before INT,
  records_after INT,
  current_records INT,
  historical_records INT,
  test_timestamp TIMESTAMP NOT NULL,
  status STRING NOT NULL COMMENT 'PASS or FAIL',
  notes STRING
)
USING DELTA
COMMENT 'Test results for SCD2 integration testing';

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- View current state
SELECT 'Source table ready' as message, COUNT(*) as record_count
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

SELECT 'Target table ready' as message, COUNT(*) as record_count
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_scd2`;

-- Show table schemas
DESCRIBE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;
DESCRIBE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_scd2`;


