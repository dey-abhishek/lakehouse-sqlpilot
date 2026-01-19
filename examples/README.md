# Example Plans

This directory contains example SQLPilot plans demonstrating different patterns.

## Incremental Append Example

```yaml
# plans/customer_daily_incremental.yaml
schema_version: "1.0"
plan_metadata:
  plan_id: "550e8400-e29b-41d4-a716-446655440000"
  plan_name: "customer_daily_incremental"
  description: "Daily incremental load of customer events"
  owner: "data-team@company.com"
  created_at: "2026-01-16T00:00:00Z"
  version: "1.0.0"

pattern:
  type: "INCREMENTAL_APPEND"

source:
  catalog: "prod_catalog"
  schema: "raw"
  table: "customer_events"

target:
  catalog: "prod_catalog"
  schema: "curated"
  table: "customer_daily"
  write_mode: "append"
  partition_by: ["event_date"]

pattern_config:
  watermark_column: "event_timestamp"
  watermark_type: "timestamp"

execution_config:
  warehouse_id: "abc123warehouse"
  timeout_seconds: 3600
  max_retries: 3

schedule:
  type: "cron"
  cron_expression: "0 2 * * *"
  timezone: "UTC"
```

## Full Replace Example

```yaml
# plans/product_catalog_refresh.yaml
schema_version: "1.0"
plan_metadata:
  plan_id: "660e8400-e29b-41d4-a716-446655440001"
  plan_name: "product_catalog_refresh"
  description: "Full refresh of product catalog"
  owner: "data-team@company.com"
  created_at: "2026-01-16T00:00:00Z"
  version: "1.0.0"

pattern:
  type: "FULL_REPLACE"

source:
  catalog: "prod_catalog"
  schema: "external"
  table: "product_source"

target:
  catalog: "prod_catalog"
  schema: "curated"
  table: "product_catalog"
  write_mode: "overwrite"

pattern_config: {}

execution_config:
  warehouse_id: "abc123warehouse"
  timeout_seconds: 1800
  max_retries: 3

schedule:
  type: "cron"
  cron_expression: "0 0 * * *"
  timezone: "UTC"
```

## Merge/Upsert Example

```yaml
# plans/customer_profile_sync.yaml
schema_version: "1.0"
plan_metadata:
  plan_id: "770e8400-e29b-41d4-a716-446655440002"
  plan_name: "customer_profile_sync"
  description: "Sync customer profile updates"
  owner: "data-team@company.com"
  created_at: "2026-01-16T00:00:00Z"
  version: "1.0.0"

pattern:
  type: "MERGE_UPSERT"

source:
  catalog: "prod_catalog"
  schema: "staging"
  table: "customer_updates"

target:
  catalog: "prod_catalog"
  schema: "curated"
  table: "customer_profiles"
  write_mode: "merge"

pattern_config:
  merge_keys: ["customer_id"]
  update_columns: ["name", "email", "phone", "updated_at"]

execution_config:
  warehouse_id: "abc123warehouse"
  timeout_seconds: 3600
  max_retries: 3

schedule:
  type: "manual"
```

## SCD Type 2 Example

```yaml
# plans/customer_scd2.yaml
schema_version: "1.0"
plan_metadata:
  plan_id: "880e8400-e29b-41d4-a716-446655440003"
  plan_name: "customer_scd2"
  description: "Customer dimension with history tracking"
  owner: "data-team@company.com"
  created_at: "2026-01-16T00:00:00Z"
  version: "1.0.0"

pattern:
  type: "SCD2"

source:
  catalog: "prod_catalog"
  schema: "raw"
  table: "customer_current"
  columns: ["customer_id", "name", "email", "status"]

target:
  catalog: "prod_catalog"
  schema: "curated"
  table: "customer_dim"
  write_mode: "merge"

pattern_config:
  business_keys: ["customer_id"]
  effective_date_column: "valid_from"
  end_date_column: "valid_to"
  current_flag_column: "is_current"
  end_date_default: "9999-12-31"

execution_config:
  warehouse_id: "abc123warehouse"
  timeout_seconds: 3600
  max_retries: 3

schedule:
  type: "cron"
  cron_expression: "0 3 * * *"
  timezone: "UTC"
```


