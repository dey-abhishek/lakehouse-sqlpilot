-- ============================================================================
-- Lakehouse SQLPilot - Sample Tables for Genie Space
-- ============================================================================
-- 
-- These tables provide realistic business data for:
-- 1. Genie Space exploration (understanding data)
-- 2. SQLPilot execution (production SQL patterns)
--
-- Use Case: E-commerce Analytics
-- ============================================================================

USE CATALOG `lakehouse-sqlpilot`;
USE SCHEMA `lakehouse-sqlpilot-schema`;

-- ============================================================================
-- 1. CUSTOMERS TABLE (Dimension)
-- ============================================================================
-- Master customer data for SCD2 pattern

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers` (
  customer_id STRING NOT NULL COMMENT 'Unique customer identifier',
  customer_name STRING NOT NULL COMMENT 'Customer full name',
  email STRING NOT NULL COMMENT 'Customer email address',
  phone STRING COMMENT 'Phone number',
  address STRING COMMENT 'Street address',
  city STRING COMMENT 'City',
  state STRING COMMENT 'State/Province',
  country STRING NOT NULL COMMENT 'Country',
  postal_code STRING COMMENT 'Postal/ZIP code',
  customer_segment STRING COMMENT 'Customer segment: Premium, Standard, Basic',
  account_status STRING NOT NULL COMMENT 'Status: Active, Inactive, Suspended',
  registration_date DATE NOT NULL COMMENT 'Date customer registered',
  lifetime_value DECIMAL(18,2) COMMENT 'Total lifetime value in USD',
  last_order_date DATE COMMENT 'Date of last order',
  created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
  updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Customer master data - use for SCD2 pattern to track changes over time';

-- Insert sample customers
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers` VALUES
  ('CUST001', 'Alice Johnson', 'alice.j@email.com', '555-0101', '123 Main St', 'San Francisco', 'CA', 'USA', '94102', 'Premium', 'Active', DATE '2024-01-15', 15250.00, DATE '2026-01-10', TIMESTAMP '2024-01-15 10:00:00', TIMESTAMP '2026-01-10 15:30:00'),
  ('CUST002', 'Bob Smith', 'bob.smith@email.com', '555-0102', '456 Oak Ave', 'New York', 'NY', 'USA', '10001', 'Standard', 'Active', DATE '2024-03-20', 8940.00, DATE '2026-01-08', TIMESTAMP '2024-03-20 11:30:00', TIMESTAMP '2026-01-08 09:15:00'),
  ('CUST003', 'Carol Martinez', 'carol.m@email.com', '555-0103', '789 Pine Rd', 'Chicago', 'IL', 'USA', '60601', 'Premium', 'Active', DATE '2024-02-10', 22100.00, DATE '2026-01-12', TIMESTAMP '2024-02-10 14:20:00', TIMESTAMP '2026-01-12 16:45:00'),
  ('CUST004', 'David Lee', 'david.lee@email.com', '555-0104', '321 Elm St', 'Seattle', 'WA', 'USA', '98101', 'Basic', 'Active', DATE '2024-05-05', 3200.00, DATE '2025-12-20', TIMESTAMP '2024-05-05 09:00:00', TIMESTAMP '2025-12-20 11:00:00'),
  ('CUST005', 'Emma Wilson', 'emma.w@email.com', '555-0105', '654 Maple Dr', 'Austin', 'TX', 'USA', '78701', 'Standard', 'Active', DATE '2024-06-18', 6750.00, DATE '2026-01-05', TIMESTAMP '2024-06-18 16:30:00', TIMESTAMP '2026-01-05 14:20:00'),
  ('CUST006', 'Frank Chen', 'frank.c@email.com', '555-0106', '987 Cedar Ln', 'Los Angeles', 'CA', 'USA', '90001', 'Premium', 'Active', DATE '2024-04-12', 18900.00, DATE '2026-01-11', TIMESTAMP '2024-04-12 10:45:00', TIMESTAMP '2026-01-11 13:30:00'),
  ('CUST007', 'Grace Kim', 'grace.k@email.com', '555-0107', '147 Birch Way', 'Boston', 'MA', 'USA', '02101', 'Standard', 'Active', DATE '2024-07-22', 5600.00, DATE '2026-01-09', TIMESTAMP '2024-07-22 13:15:00', TIMESTAMP '2026-01-09 10:50:00'),
  ('CUST008', 'Henry Brown', 'henry.b@email.com', '555-0108', '258 Spruce St', 'Denver', 'CO', 'USA', '80201', 'Basic', 'Inactive', DATE '2024-08-30', 1200.00, DATE '2025-11-15', TIMESTAMP '2024-08-30 15:00:00', TIMESTAMP '2025-11-15 09:30:00'),
  ('CUST009', 'Iris Taylor', 'iris.t@email.com', '555-0109', '369 Willow Ave', 'Miami', 'FL', 'USA', '33101', 'Premium', 'Active', DATE '2024-09-14', 24500.00, DATE '2026-01-13', TIMESTAMP '2024-09-14 11:20:00', TIMESTAMP '2026-01-13 17:00:00'),
  ('CUST010', 'Jack Davis', 'jack.d@email.com', '555-0110', '741 Aspen Rd', 'Portland', 'OR', 'USA', '97201', 'Standard', 'Active', DATE '2024-10-08', 7300.00, DATE '2026-01-07', TIMESTAMP '2024-10-08 14:40:00', TIMESTAMP '2026-01-07 12:15:00');

-- ============================================================================
-- 2. PRODUCTS TABLE (Dimension)
-- ============================================================================
-- Product catalog data

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`products`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`products` (
  product_id STRING NOT NULL COMMENT 'Unique product identifier',
  product_name STRING NOT NULL COMMENT 'Product name',
  category STRING NOT NULL COMMENT 'Product category',
  subcategory STRING COMMENT 'Product subcategory',
  brand STRING NOT NULL COMMENT 'Brand name',
  unit_price DECIMAL(18,2) NOT NULL COMMENT 'Unit price in USD',
  cost DECIMAL(18,2) NOT NULL COMMENT 'Cost in USD',
  stock_quantity INT NOT NULL COMMENT 'Current stock quantity',
  is_active BOOLEAN NOT NULL COMMENT 'Is product currently active',
  created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
  updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp'
)
USING DELTA
COMMENT 'Product catalog - master product data';

-- Insert sample products
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`products` VALUES
  ('PROD001', 'Laptop Pro 15"', 'Electronics', 'Computers', 'TechBrand', 1299.99, 850.00, 45, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD002', 'Wireless Mouse', 'Electronics', 'Accessories', 'TechBrand', 29.99, 12.00, 250, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD003', 'USB-C Hub', 'Electronics', 'Accessories', 'TechBrand', 49.99, 20.00, 180, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD004', 'Ergonomic Chair', 'Furniture', 'Office', 'ComfortCo', 399.99, 200.00, 30, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD005', 'Standing Desk', 'Furniture', 'Office', 'ComfortCo', 599.99, 300.00, 25, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD006', 'LED Monitor 27"', 'Electronics', 'Displays', 'ViewTech', 349.99, 180.00, 60, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD007', 'Mechanical Keyboard', 'Electronics', 'Accessories', 'TechBrand', 129.99, 60.00, 120, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD008', 'Webcam HD', 'Electronics', 'Accessories', 'ViewTech', 79.99, 35.00, 90, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD009', 'Desk Lamp', 'Furniture', 'Lighting', 'BrightLight', 39.99, 15.00, 200, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00'),
  ('PROD010', 'Headphones Wireless', 'Electronics', 'Audio', 'SoundPro', 199.99, 90.00, 75, TRUE, TIMESTAMP '2024-01-10 08:00:00', TIMESTAMP '2026-01-10 08:00:00');

-- ============================================================================
-- 3. ORDERS TABLE (Fact Table)
-- ============================================================================
-- Order transactions for incremental append pattern

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`orders`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`orders` (
  order_id STRING NOT NULL COMMENT 'Unique order identifier',
  customer_id STRING NOT NULL COMMENT 'Customer who placed the order',
  order_date DATE NOT NULL COMMENT 'Date order was placed',
  order_timestamp TIMESTAMP NOT NULL COMMENT 'Timestamp order was placed',
  order_status STRING NOT NULL COMMENT 'Status: Pending, Shipped, Delivered, Cancelled',
  total_amount DECIMAL(18,2) NOT NULL COMMENT 'Total order amount in USD',
  discount_amount DECIMAL(18,2) COMMENT 'Discount applied in USD',
  tax_amount DECIMAL(18,2) NOT NULL COMMENT 'Tax amount in USD',
  shipping_cost DECIMAL(18,2) NOT NULL COMMENT 'Shipping cost in USD',
  payment_method STRING NOT NULL COMMENT 'Payment method: Credit Card, PayPal, etc.',
  shipping_address STRING COMMENT 'Shipping address',
  created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
)
USING DELTA
PARTITIONED BY (order_date)
COMMENT 'Order transactions - use for incremental append pattern';

-- Insert sample orders
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`orders` VALUES
  ('ORD001', 'CUST001', DATE '2026-01-10', TIMESTAMP '2026-01-10 10:30:00', 'Delivered', 1429.98, 50.00, 114.40, 15.00, 'Credit Card', '123 Main St, San Francisco, CA 94102', TIMESTAMP '2026-01-10 10:30:00'),
  ('ORD002', 'CUST002', DATE '2026-01-08', TIMESTAMP '2026-01-08 14:20:00', 'Delivered', 399.99, 0.00, 32.00, 20.00, 'PayPal', '456 Oak Ave, New York, NY 10001', TIMESTAMP '2026-01-08 14:20:00'),
  ('ORD003', 'CUST003', DATE '2026-01-12', TIMESTAMP '2026-01-12 09:15:00', 'Shipped', 949.97, 100.00, 68.00, 25.00, 'Credit Card', '789 Pine Rd, Chicago, IL 60601', TIMESTAMP '2026-01-12 09:15:00'),
  ('ORD004', 'CUST001', DATE '2026-01-13', TIMESTAMP '2026-01-13 16:45:00', 'Pending', 229.98, 0.00, 18.40, 10.00, 'Credit Card', '123 Main St, San Francisco, CA 94102', TIMESTAMP '2026-01-13 16:45:00'),
  ('ORD005', 'CUST004', DATE '2025-12-20', TIMESTAMP '2025-12-20 11:00:00', 'Delivered', 129.99, 0.00, 10.40, 8.00, 'Debit Card', '321 Elm St, Seattle, WA 98101', TIMESTAMP '2025-12-20 11:00:00'),
  ('ORD006', 'CUST005', DATE '2026-01-05', TIMESTAMP '2026-01-05 13:30:00', 'Delivered', 579.97, 20.00, 44.80, 15.00, 'PayPal', '654 Maple Dr, Austin, TX 78701', TIMESTAMP '2026-01-05 13:30:00'),
  ('ORD007', 'CUST006', DATE '2026-01-11', TIMESTAMP '2026-01-11 10:50:00', 'Shipped', 1729.96, 150.00, 126.40, 30.00, 'Credit Card', '987 Cedar Ln, Los Angeles, CA 90001', TIMESTAMP '2026-01-11 10:50:00'),
  ('ORD008', 'CUST007', DATE '2026-01-09', TIMESTAMP '2026-01-09 15:20:00', 'Delivered', 279.98, 0.00, 22.40, 12.00, 'Credit Card', '147 Birch Way, Boston, MA 02101', TIMESTAMP '2026-01-09 15:20:00'),
  ('ORD009', 'CUST009', DATE '2026-01-13', TIMESTAMP '2026-01-13 12:00:00', 'Pending', 999.98, 100.00, 72.00, 20.00, 'Credit Card', '369 Willow Ave, Miami, FL 33101', TIMESTAMP '2026-01-13 12:00:00'),
  ('ORD010', 'CUST010', DATE '2026-01-07', TIMESTAMP '2026-01-07 09:45:00', 'Delivered', 429.98, 0.00, 34.40, 15.00, 'PayPal', '741 Aspen Rd, Portland, OR 97201', TIMESTAMP '2026-01-07 09:45:00');

-- ============================================================================
-- 4. ORDER_ITEMS TABLE (Fact Detail)
-- ============================================================================
-- Individual line items for each order

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`order_items`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`order_items` (
  order_item_id STRING NOT NULL COMMENT 'Unique order item identifier',
  order_id STRING NOT NULL COMMENT 'Order this item belongs to',
  product_id STRING NOT NULL COMMENT 'Product ordered',
  quantity INT NOT NULL COMMENT 'Quantity ordered',
  unit_price DECIMAL(18,2) NOT NULL COMMENT 'Unit price at time of order',
  discount_percent DECIMAL(5,2) COMMENT 'Discount percentage applied',
  line_total DECIMAL(18,2) NOT NULL COMMENT 'Line total (quantity * unit_price * (1-discount))',
  created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp'
)
USING DELTA
COMMENT 'Order line items - detail records for each order';

-- Insert sample order items
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`order_items` VALUES
  ('ITEM001', 'ORD001', 'PROD001', 1, 1299.99, 0.00, 1299.99, TIMESTAMP '2026-01-10 10:30:00'),
  ('ITEM002', 'ORD001', 'PROD002', 2, 29.99, 0.00, 59.98, TIMESTAMP '2026-01-10 10:30:00'),
  ('ITEM003', 'ORD001', 'PROD003', 1, 49.99, 0.00, 49.99, TIMESTAMP '2026-01-10 10:30:00'),
  ('ITEM004', 'ORD002', 'PROD004', 1, 399.99, 0.00, 399.99, TIMESTAMP '2026-01-08 14:20:00'),
  ('ITEM005', 'ORD003', 'PROD006', 2, 349.99, 0.00, 699.98, TIMESTAMP '2026-01-12 09:15:00'),
  ('ITEM006', 'ORD003', 'PROD007', 1, 129.99, 0.00, 129.99, TIMESTAMP '2026-01-12 09:15:00'),
  ('ITEM007', 'ORD003', 'PROD002', 2, 29.99, 50.00, 29.99, TIMESTAMP '2026-01-12 09:15:00'),
  ('ITEM008', 'ORD004', 'PROD010', 1, 199.99, 0.00, 199.99, TIMESTAMP '2026-01-13 16:45:00'),
  ('ITEM009', 'ORD004', 'PROD002', 1, 29.99, 0.00, 29.99, TIMESTAMP '2026-01-13 16:45:00'),
  ('ITEM010', 'ORD005', 'PROD007', 1, 129.99, 0.00, 129.99, TIMESTAMP '2025-12-20 11:00:00'),
  ('ITEM011', 'ORD006', 'PROD005', 1, 599.99, 0.00, 599.99, TIMESTAMP '2026-01-05 13:30:00'),
  ('ITEM012', 'ORD007', 'PROD001', 1, 1299.99, 0.00, 1299.99, TIMESTAMP '2026-01-11 10:50:00'),
  ('ITEM013', 'ORD007', 'PROD004', 1, 399.99, 0.00, 399.99, TIMESTAMP '2026-01-11 10:50:00'),
  ('ITEM014', 'ORD008', 'PROD008', 2, 79.99, 0.00, 159.98, TIMESTAMP '2026-01-09 15:20:00'),
  ('ITEM015', 'ORD008', 'PROD009', 3, 39.99, 0.00, 119.97, TIMESTAMP '2026-01-09 15:20:00'),
  ('ITEM016', 'ORD009', 'PROD005', 1, 599.99, 0.00, 599.99, TIMESTAMP '2026-01-13 12:00:00'),
  ('ITEM017', 'ORD009', 'PROD004', 1, 399.99, 0.00, 399.99, TIMESTAMP '2026-01-13 12:00:00'),
  ('ITEM018', 'ORD010', 'PROD006', 1, 349.99, 0.00, 349.99, TIMESTAMP '2026-01-07 09:45:00'),
  ('ITEM019', 'ORD010', 'PROD008', 1, 79.99, 0.00, 79.99, TIMESTAMP '2026-01-07 09:45:00');

-- ============================================================================
-- 5. SALES_SUMMARY TABLE (Aggregated Metrics)
-- ============================================================================
-- Daily sales summary for full replace pattern

DROP TABLE IF EXISTS `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`sales_summary`;

CREATE TABLE `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`sales_summary` (
  summary_date DATE NOT NULL COMMENT 'Date of summary',
  total_orders INT NOT NULL COMMENT 'Total number of orders',
  total_revenue DECIMAL(18,2) NOT NULL COMMENT 'Total revenue in USD',
  total_discount DECIMAL(18,2) NOT NULL COMMENT 'Total discounts given',
  total_tax DECIMAL(18,2) NOT NULL COMMENT 'Total tax collected',
  avg_order_value DECIMAL(18,2) NOT NULL COMMENT 'Average order value',
  unique_customers INT NOT NULL COMMENT 'Number of unique customers',
  created_at TIMESTAMP NOT NULL COMMENT 'Record creation timestamp',
  updated_at TIMESTAMP NOT NULL COMMENT 'Record last update timestamp'
)
USING DELTA
PARTITIONED BY (summary_date)
COMMENT 'Daily sales summary - use for full replace pattern to refresh daily';

-- Insert sample summaries
INSERT INTO `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`sales_summary` VALUES
  (DATE '2026-01-05', 1, 579.97, 20.00, 44.80, 579.97, 1, TIMESTAMP '2026-01-05 23:59:00', TIMESTAMP '2026-01-05 23:59:00'),
  (DATE '2026-01-07', 1, 429.98, 0.00, 34.40, 429.98, 1, TIMESTAMP '2026-01-07 23:59:00', TIMESTAMP '2026-01-07 23:59:00'),
  (DATE '2026-01-08', 1, 399.99, 0.00, 32.00, 399.99, 1, TIMESTAMP '2026-01-08 23:59:00', TIMESTAMP '2026-01-08 23:59:00'),
  (DATE '2026-01-09', 1, 279.98, 0.00, 22.40, 279.98, 1, TIMESTAMP '2026-01-09 23:59:00', TIMESTAMP '2026-01-09 23:59:00'),
  (DATE '2026-01-10', 1, 1429.98, 50.00, 114.40, 1429.98, 1, TIMESTAMP '2026-01-10 23:59:00', TIMESTAMP '2026-01-10 23:59:00'),
  (DATE '2026-01-11', 1, 1729.96, 150.00, 126.40, 1729.96, 1, TIMESTAMP '2026-01-11 23:59:00', TIMESTAMP '2026-01-11 23:59:00'),
  (DATE '2026-01-12', 1, 949.97, 100.00, 68.00, 949.97, 1, TIMESTAMP '2026-01-12 23:59:00', TIMESTAMP '2026-01-12 23:59:00'),
  (DATE '2026-01-13', 2, 1229.96, 100.00, 90.80, 614.98, 2, TIMESTAMP '2026-01-13 23:59:00', TIMESTAMP '2026-01-13 23:59:00'),
  (DATE '2025-12-20', 1, 129.99, 0.00, 10.40, 129.99, 1, TIMESTAMP '2025-12-20 23:59:00', TIMESTAMP '2025-12-20 23:59:00');

-- ============================================================================
-- VERIFICATION & DATA QUALITY CHECKS
-- ============================================================================

-- Table counts
SELECT 'customers' as table_name, COUNT(*) as row_count 
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customers`
UNION ALL
SELECT 'products', COUNT(*) 
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`products`
UNION ALL
SELECT 'orders', COUNT(*) 
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`orders`
UNION ALL
SELECT 'order_items', COUNT(*) 
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`order_items`
UNION ALL
SELECT 'sales_summary', COUNT(*) 
FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`sales_summary`;

-- Sample queries for Genie exploration
SELECT '✅ Sample tables created successfully!' as status;


