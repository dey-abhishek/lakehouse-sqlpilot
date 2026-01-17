#!/usr/bin/env python3
"""
Manual UAT Test Data Setup Script

This script creates test tables and inserts data directly via Databricks SQL Editor API,
bypassing the Spark codegen bug that affects the Python SQL connector.

Usage:
    python setup_uat_test_data.py
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Check for required environment variables
required_vars = ['DATABRICKS_SERVER_HOSTNAME', 'DATABRICKS_TOKEN', 'DATABRICKS_WAREHOUSE_ID', 
                 'DATABRICKS_CATALOG', 'DATABRICKS_SCHEMA']
missing = [var for var in required_vars if not os.getenv(var)]
if missing:
    print(f"❌ Missing required environment variables: {', '.join(missing)}")
    print("\nPlease set these in your .env file or environment.")
    sys.exit(1)

catalog = os.getenv('DATABRICKS_CATALOG')
schema = os.getenv('DATABRICKS_SCHEMA')

print(f"""
================================================================================
UAT Test Data Setup
================================================================================

This script will create test tables in:
  Catalog: {catalog}
  Schema: {schema}

Due to a Databricks Spark codegen bug, we need to create test data manually.

INSTRUCTIONS:
============

1. Open Databricks SQL Editor in your browser
2. Connect to your SQL Warehouse
3. Run the SQL commands from: setup_uat_test_data.sql

Or copy and paste these commands:

================================================================================
""")

sql = f"""
-- Drop existing tables
DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`customers_source`;
DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`customers_dim`;

-- Create source table
CREATE TABLE `{catalog}`.`{schema}`.`customers_source` (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP
) USING DELTA;

-- Create dimension table
CREATE TABLE `{catalog}`.`{schema}`.`customers_dim` (
    customer_id INT,
    name STRING,
    email STRING,
    city STRING,
    updated_at TIMESTAMP,
    valid_from TIMESTAMP,
    valid_to TIMESTAMP,
    is_current BOOLEAN
) USING DELTA;

-- Insert test data (row by row to avoid Spark bug)
INSERT INTO `{catalog}`.`{schema}`.`customers_source`
SELECT 1 as customer_id, 'Alice Smith' as name, 'alice@example.com' as email, 
       'San Francisco' as city, current_timestamp() as updated_at;

INSERT INTO `{catalog}`.`{schema}`.`customers_source`
SELECT 2 as customer_id, 'Bob Jones' as name, 'bob@example.com' as email, 
       'New York' as city, current_timestamp() as updated_at;

INSERT INTO `{catalog}`.`{schema}`.`customers_source`
SELECT 3 as customer_id, 'Carol White' as name, 'carol@example.com' as email, 
       'Chicago' as city, current_timestamp() as updated_at;

-- Verify
SELECT COUNT(*) as row_count FROM `{catalog}`.`{schema}`.`customers_source`;
SELECT * FROM `{catalog}`.`{schema}`.`customers_source` ORDER BY customer_id;
"""

print(sql)

print("""
================================================================================

After running the SQL above, run the UAT tests:

    pytest tests/test_uat_end_to_end.py -v

================================================================================
""")

