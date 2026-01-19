-- Lakehouse SQLPilot - SCD2 Integration Test Data Updates
-- Simulates data changes over time for testing SCD2 functionality

USE CATALOG `lakehouse-sqlpilot`;
USE SCHEMA `lakehouse-sqlpilot-schema`;

-- ============================================================================
-- DAY 2 UPDATES: Some customer attributes change
-- ============================================================================

-- Update source table to simulate Day 2 changes
TRUNCATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`
VALUES
  -- C001: Email and segment changed
  ('C001', 'John Smith', 'john.smith.new@email.com', '555-0101', '123 Main St', 'San Francisco', 'CA', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-02 10:30:00'),
  
  -- C002: Address changed
  ('C002', 'Jane Doe', 'jane.doe@email.com', '555-0102', '999 Broadway St', 'New York', 'NY', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-02 11:15:00'),
  
  -- C003: No changes
  ('C003', 'Bob Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', 'Basic', 'Active', TIMESTAMP '2026-01-02 00:00:00'),
  
  -- C004: Status changed to Inactive
  ('C004', 'Alice Williams', 'alice.w@email.com', '555-0104', '321 Elm St', 'Seattle', 'WA', 'USA', 'Premium', 'Inactive', TIMESTAMP '2026-01-02 14:00:00'),
  
  -- C005: Phone and city changed
  ('C005', 'Charlie Brown', 'charlie.b@email.com', '555-9999', '654 Maple Dr', 'Dallas', 'TX', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-02 16:45:00'),
  
  -- C006: New customer
  ('C006', 'Diana Prince', 'diana.p@email.com', '555-0106', '777 Hero Ln', 'Los Angeles', 'CA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-02 17:00:00');

SELECT 'Day 2 data loaded: ' || COUNT(*) || ' records' as status
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

-- ============================================================================
-- DAY 3 UPDATES: More changes
-- ============================================================================

-- This will be used for a third test run
-- Uncomment when ready for Day 3 testing

/*
TRUNCATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`;

INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim_source`
VALUES
  -- C001: Back to Premium
  ('C001', 'John Smith', 'john.smith.new@email.com', '555-0101', '123 Main St', 'San Francisco', 'CA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-03 09:00:00'),
  
  -- C002: Suspended
  ('C002', 'Jane Doe', 'jane.doe@email.com', '555-0102', '999 Broadway St', 'New York', 'NY', 'USA', 'Standard', 'Suspended', TIMESTAMP '2026-01-03 10:00:00'),
  
  -- C003: Upgraded to Premium
  ('C003', 'Bob Johnson', 'bob.j@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-03 11:00:00'),
  
  -- C004: Reactivated
  ('C004', 'Alice Williams', 'alice.w@email.com', '555-0104', '321 Elm St', 'Seattle', 'WA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-03 12:00:00'),
  
  -- C005: No changes
  ('C005', 'Charlie Brown', 'charlie.b@email.com', '555-9999', '654 Maple Dr', 'Dallas', 'TX', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-03 00:00:00'),
  
  -- C006: Moved
  ('C006', 'Diana Prince', 'diana.p@email.com', '555-0106', '888 Justice Way', 'Los Angeles', 'CA', 'USA', 'Premium', 'Active', TIMESTAMP '2026-01-03 13:00:00'),
  
  -- C007: New customer
  ('C007', 'Eve Martinez', 'eve.m@email.com', '555-0107', '111 Tech Blvd', 'San Jose', 'CA', 'USA', 'Standard', 'Active', TIMESTAMP '2026-01-03 14:00:00');
*/


