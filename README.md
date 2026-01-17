# Lakehouse SQLPilot

**A governed control plane for production SQL in the Databricks Lakehouse**

## Overview

Lakehouse SQLPilot bridges the gap between exploratory data understanding (Genie) and production SQL execution (DBSQL) with enterprise governance. It transforms business intent into auditable, versioned, deterministic SQL execution.

### What SQLPilot IS

- A governed control plane for production SQL
- A system that turns business intent into durable, auditable SQL execution
- A bridge between exploration (Genie) and production execution (DBSQL)

### What SQLPilot IS NOT

- NOT a SQL generator
- NOT a chatbot
- NOT a BI tool
- NOT a replacement for Databricks Genie or AI Assistant

## Core Principles

1. **Plan-First**: All execution starts with a typed, versioned plan
2. **Pattern-Based**: SQL generated from validated patterns, not free-form text
3. **Governed**: Unity Catalog enforcement and lineage tracking at all times
4. **Deterministic**: Same plan + same data = same output
5. **Auditable**: Full execution history and lineage
6. **Agent-Assisted**: Agents help create plans, NEVER execute SQL

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     User Layer                          │
│  Business Analysts │ Data Engineers                    │
└─────────────────┬───────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│              SQLPilot Control Plane                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │ Plan UI  │  │ Compiler │  │ Agents   │             │
│  └──────────┘  └──────────┘  └──────────┘             │
└─────────────────┬───────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│              Execution Plane                            │
│  DBSQL Warehouse │ Execution Log                       │
└─────────────────┬───────────────────────────────────────┘
                  │
┌─────────────────▼───────────────────────────────────────┐
│              Governance Layer                           │
│  Unity Catalog │ Lineage │ Audit                       │
└─────────────────────────────────────────────────────────┘
```

## Quick Start

### Installation

```bash
# Clone repository
git clone https://github.com/databricks/lakehouse-sqlpilot.git
cd lakehouse-sqlpilot

# Create virtual environment
python -m venv sqlpilot
source sqlpilot/bin/activate  # On Windows: sqlpilot\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp env.example .env
# Edit .env with your Databricks credentials
# ⚠️ IMPORTANT: NEVER commit .env file (it's gitignored)
```

> **Security Note**: Never commit credentials to git. The `.env` file is gitignored. See [SECURITY_REPORT.md](SECURITY_REPORT.md) for credential management best practices.

### Configuration

Edit the `.env` file with your Databricks credentials:

```bash
# Required: Databricks Workspace
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
DATABRICKS_CATALOG=lakehouse-sqlpilot
DATABRICKS_SCHEMA=lakehouse-sqlpilot-schema

# Authentication: Choose OAuth (recommended) or Personal Access Token
# Option 1: OAuth (Recommended for Production)
SQLPILOT_OAUTH_ENABLED=true
SQLPILOT_OAUTH_CLIENT_ID=your-oauth-client-id
SQLPILOT_OAUTH_CLIENT_SECRET=your-oauth-client-secret

# Option 2: Personal Access Token (for development/testing)
DATABRICKS_TOKEN=your-personal-access-token
```

**For production deployments**, use a secrets manager instead of `.env`:
- **Databricks Secrets** (recommended for Databricks Apps)
- **AWS Secrets Manager** (for AWS deployments)
- **Azure Key Vault** (for Azure deployments)

See [SECRETS_MANAGEMENT.md](SECRETS_MANAGEMENT.md) for secure credential storage.

All tests and scripts automatically load configuration from `.env` file or secrets manager.

**Verify your configuration**:
```bash
python check_env.py
```

See [ENV_CONFIGURATION.md](ENV_CONFIGURATION.md) for detailed configuration guide.

### Authentication

SQLPilot supports **OAuth 2.0 authentication** (recommended) for secure, enterprise-grade access:

```bash
# Enable OAuth (recommended)
export SQLPILOT_OAUTH_ENABLED=true
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export SQLPILOT_OAUTH_CLIENT_ID=your-oauth-client-id
export SQLPILOT_OAUTH_CLIENT_SECRET=your-oauth-client-secret
```

See [OAUTH_AUTHENTICATION.md](OAUTH_AUTHENTICATION.md) for detailed OAuth setup and configuration.

### Create Your First Plan

1. **Explore data with Genie** (optional but recommended):
   ```
   Ask Genie: "Show me the schema of lakehouse-sqlpilot.lakehouse-sqlpilot-schema.customer_events_raw"
   Ask Genie: "What are the latest 10 rows in customer_events_raw?"
   ```

2. **Create a plan** (YAML format):

```yaml
# Example plan using TEST catalog and schema
# Replace with your own production catalog/schema for real use
schema_version: "1.0"
plan_metadata:
  plan_id: "550e8400-e29b-41d4-a716-446655440000"
  plan_name: "customer_events_incremental"
  description: "Incremental load of customer events"
  owner: "test@databricks.com"  # Replace with your email
  created_at: "2026-01-16T00:00:00Z"
  version: "1.0.0"

pattern:
  type: "INCREMENTAL_APPEND"

source:
  catalog: "lakehouse-sqlpilot"  # TEST catalog - replace with yours
  schema: "lakehouse-sqlpilot-schema"  # TEST schema - replace with yours
  table: "customer_events_raw"

target:
  catalog: "lakehouse-sqlpilot"  # TEST catalog - replace with yours
  schema: "lakehouse-sqlpilot-schema"  # TEST schema - replace with yours
  table: "customer_events_processed"
  write_mode: "append"
  partition_by: ["event_timestamp"]

pattern_config:
  watermark_column: "event_timestamp"
  watermark_type: "timestamp"

execution_config:
  warehouse_id: "your_warehouse_id"
  timeout_seconds: 3600
  max_retries: 3

schedule:
  type: "manual"
```

3. **Validate and preview**:

```bash
# Validate plan
python -m sqlpilot.cli validate examples/test_incremental_append.yaml

# Preview generated SQL
python -m sqlpilot.cli preview examples/test_incremental_append.yaml
```

4. **Deploy and execute**:

```bash
# Deploy plan
python -m sqlpilot.cli deploy examples/test_incremental_append.yaml

# Execute manually
python -m sqlpilot.cli execute test_customer_events_incremental
```

## Test Environment

> ⚠️ **FOR TESTING ONLY** - These are staging/test environment identifiers. Do NOT use these IDs for production workloads.

For local development and testing, the project is configured with a test staging workspace:

```
TEST ENVIRONMENT (STAGING ONLY)
├─ Workspace: e2-dogfood.staging.cloud.databricks.com (STAGING)
├─ Workspace ID: 6051921418418893 (TEST ONLY)
├─ SQL Warehouse ID: 592f1f39793f7795 (TEST ONLY)
├─ Catalog: lakehouse-sqlpilot (TEST)
└─ Schema: lakehouse-sqlpilot.lakehouse-sqlpilot-schema (TEST)
```

**For production use:**
- Replace these test IDs with your own production workspace and warehouse IDs
- Configure your production catalog and schema
- See [SECURITY.md](SECURITY.md) for credential management
- See [TEST_CONFIGURATION.md](TEST_CONFIGURATION.md) for detailed setup instructions

## Supported Patterns (v1)

### 1. Incremental Append
Append new rows based on watermark column.

```yaml
pattern:
  type: "INCREMENTAL_APPEND"
pattern_config:
  watermark_column: "event_timestamp"
  watermark_type: "timestamp"
```

### 2. Full Replace
Replace entire table atomically.

```yaml
pattern:
  type: "FULL_REPLACE"
target:
  write_mode: "overwrite"
```

### 3. Merge/Upsert
Update existing rows and insert new ones.

```yaml
pattern:
  type: "MERGE_UPSERT"
pattern_config:
  merge_keys: ["customer_id"]
  update_columns: ["name", "email", "updated_at"]
```

### 4. SCD Type 2
Track history with effective dates.

```yaml
pattern:
  type: "SCD2"
pattern_config:
  business_keys: ["customer_id"]
  effective_date_column: "valid_from"
  end_date_column: "valid_to"
  current_flag_column: "is_current"
```

### 5. Snapshot
Point-in-time snapshots with partitioning.

```yaml
pattern:
  type: "SNAPSHOT"
pattern_config:
  snapshot_column: "snapshot_date"
  snapshot_value: "${execution_date}"
```

### 6-8. Additional Patterns
- Aggregate Refresh
- Surrogate Key Generation
- Deduplication

See [Pattern Documentation](docs/patterns.md) for details.

## Project Structure

```
lakehouse-sqlpilot/
├── plan-schema/           # JSON Schema and validation
│   └── v1/
│       ├── plan.schema.json
│       └── validator.py
├── compiler/              # SQL compilation engine
│   ├── patterns/          # Pattern implementations
│   ├── sql_generator.py
│   ├── template_engine.py
│   └── guardrails.py
├── execution/             # Execution engine
│   ├── executor.py
│   ├── tracker.py
│   └── retry_handler.py
├── agents/                # AI agents (suggestion, validation, etc.)
│   ├── plan_suggestion_agent.py
│   ├── explanation_agent.py
│   ├── validation_agent.py
│   └── optimization_agent.py
├── unity_catalog/         # Unity Catalog integration
│   ├── permissions.py
│   └── lineage.py
├── api/                   # REST API
│   ├── plans.py
│   ├── executions.py
│   └── preview.py
├── ui/                    # Web UI
│   └── plan-editor/       # React-based plan editor
├── tests/                 # Test suite
└── docs/                  # Documentation
```

## Key Features

### ✅ v1 Features (Current)
- Plan creation and validation
- 8 common SQL patterns
- Deterministic SQL compilation
- Preview mode (safe validation)
- Production execution with DBSQL
- Execution tracking and audit logs
- Unity Catalog integration
- Retry and idempotency
- Schedule management
- Form-based Plan UI

### 🔮 v2 Features (Future)
- Multi-statement stored procedures
- Conditional execution (IF/THEN)
- Workflow orchestration
- Cross-plan dependencies
- Advanced error handling

## Governance & Safety

### Guardrails
SQLPilot **BLOCKS**:
- DROP TABLE/DATABASE
- TRUNCATE operations
- DELETE without WHERE clause
- ALTER with breaking changes
- Cross-catalog writes (if not allowed)

SQLPilot **ALLOWS**:
- SELECT, INSERT, MERGE, CREATE OR REPLACE TABLE
- Window functions and aggregations
- Common table expressions (CTEs)

### Unity Catalog Integration
- Permission validation before compilation
- Runtime permission enforcement
- Automatic lineage tracking
- Audit log integration

## Agent System

SQLPilot includes 4 strictly bounded agents:

1. **Plan Suggestion Agent**: Helps create plans from intent
2. **Explanation Agent**: Explains what a plan will do
3. **Validation Agent**: Validates plan correctness
4. **Optimization Agent**: Suggests performance improvements

**ALL agents are FORBIDDEN from**:
- Executing SQL
- Modifying tables
- Bypassing validation
- Auto-deploying to production

## Genie ↔ SQLPilot Handoff

**Genie's Role** (Exploration):
- Understanding data structure
- Exploratory queries
- Sample data validation

**SQLPilot's Role** (Production):
- Accept validated table references
- Create versioned plans
- Generate deterministic SQL
- Execute with governance

**Boundary**: Genie CANNOT create plans. SQLPilot CANNOT answer exploratory questions.

## CLI Reference

```bash
# Validate plan
sqlpilot validate <plan_file>

# Preview SQL and sample output
sqlpilot preview <plan_file>

# Deploy plan
sqlpilot deploy <plan_file>

# Execute plan
sqlpilot execute <plan_name>

# List all plans
sqlpilot list

# Show execution history
sqlpilot history <plan_name>

# Show plan details
sqlpilot show <plan_name>
```

## API Reference

### Plans API
```
POST   /api/v1/plans              # Create plan
GET    /api/v1/plans              # List plans
GET    /api/v1/plans/{id}         # Get plan details
PUT    /api/v1/plans/{id}         # Update plan
DELETE /api/v1/plans/{id}         # Delete plan
POST   /api/v1/plans/{id}/preview # Preview plan
POST   /api/v1/plans/{id}/deploy  # Deploy plan
```

### Executions API
```
POST   /api/v1/executions         # Execute plan
GET    /api/v1/executions         # List executions
GET    /api/v1/executions/{id}    # Get execution details
POST   /api/v1/executions/{id}/cancel  # Cancel execution
```

## Testing

Comprehensive test suite for backend and frontend:

```bash
# Backend tests
source sqlpilot/bin/activate
pytest -v

# Frontend tests
cd ui/plan-editor
npm test
npx playwright test
```

**Quick Start**: See [TESTING_QUICKSTART.md](TESTING_QUICKSTART.md)  
**Full Documentation**: See [tests/README.md](tests/README.md)

### Test Coverage

- ✅ **Backend**: Plan validation, pattern generation, compiler, guardrails, E2E
- ✅ **Frontend**: Component tests, E2E workflows, accessibility
- ✅ **Security**: SQL injection prevention, permission checks, no free-form SQL
- ✅ **SCD2**: Full coverage of flagship pattern

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Databricks License. See the [LICENSE](LICENSE) file for details.

Copyright 2026 Databricks, Inc.

## Support

- Documentation: [docs/](docs/)
- Issues: [GitHub Issues](https://github.com/databricks/lakehouse-sqlpilot/issues)
- Slack: #sqlpilot-support

