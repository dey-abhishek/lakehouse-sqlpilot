#!/bin/bash
# Quick Start Script for Lakehouse SQLPilot Testing
# Sets up environment and runs basic tests

set -e  # Exit on error

echo "🚀 Lakehouse SQLPilot - Quick Start"
echo "===================================="
echo ""

# Configuration
WORKSPACE_URL="https://e2-dogfood.staging.cloud.databricks.com"
WORKSPACE_ID="6051921418418893"
WAREHOUSE_ID="592f1f39793f7795"
CATALOG="lakehouse-sqlpilot"
SCHEMA="lakehouse-sqlpilot-schema"

echo "📋 Configuration:"
echo "  Workspace: ${WORKSPACE_URL}/?o=${WORKSPACE_ID}"
echo "  Warehouse: ${WAREHOUSE_ID}"
echo "  Catalog: ${CATALOG}"
echo "  Schema: ${CATALOG}.${SCHEMA}"
echo ""

# Check if virtual environment exists
if [ ! -d "sqlpilot" ]; then
    echo "❌ Virtual environment 'sqlpilot' not found"
    echo "   Run: python3 -m venv sqlpilot"
    exit 1
fi

# Activate virtual environment
echo "🔧 Activating virtual environment..."
source sqlpilot/bin/activate

# Install dependencies
echo "📦 Installing dependencies..."
pip install -q -r requirements.txt

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  No .env file found"
    echo "   Copy env.template to .env and add your DATABRICKS_TOKEN"
    echo ""
    echo "   cp env.template .env"
    echo "   # Edit .env and add your token"
    echo ""
    
    # Create .env from template if it doesn't exist
    if [ -f "env.template" ]; then
        cp env.template .env
        echo "✅ Created .env from template"
        echo "   Please edit .env and add your DATABRICKS_TOKEN"
    fi
    exit 1
fi

# Load environment variables
echo "🔑 Loading environment variables..."
export $(cat .env | grep -v '^#' | xargs)

# Check required variables
if [ -z "$DATABRICKS_TOKEN" ] || [ "$DATABRICKS_TOKEN" = "your_token_here" ]; then
    echo "❌ DATABRICKS_TOKEN not set in .env"
    echo "   Please edit .env and add your personal access token"
    exit 1
fi

echo "✅ Environment configured"
echo ""

# Test 1: Validate example plan
echo "🧪 Test 1: Validating example plans..."
python3 << 'EOF'
import yaml
from plan_schema.v1.validator import PlanValidator

validator = PlanValidator('plan-schema/v1/plan.schema.json')

# Test incremental append plan
with open('examples/test_incremental_append.yaml') as f:
    plan = yaml.safe_load(f)
    is_valid, errors = validator.validate_plan(plan)
    if is_valid:
        print("  ✅ Incremental Append plan is valid")
    else:
        print(f"  ❌ Incremental Append plan validation failed: {errors}")
        exit(1)

# Test SCD2 plan
with open('examples/test_scd2.yaml') as f:
    plan = yaml.safe_load(f)
    is_valid, errors = validator.validate_plan(plan)
    if is_valid:
        print("  ✅ SCD2 plan is valid")
    else:
        print(f"  ❌ SCD2 plan validation failed: {errors}")
        exit(1)
EOF

echo ""

# Test 2: Compile plans to SQL
echo "🧪 Test 2: Compiling plans to SQL..."
python3 << 'EOF'
import yaml
from compiler import SQLCompiler

compiler = SQLCompiler('plan-schema/v1/plan.schema.json')

# Compile incremental append
with open('examples/test_incremental_append.yaml') as f:
    plan = yaml.safe_load(f)
    sql = compiler.compile(plan)
    if 'INSERT INTO' in sql and 'LAKEHOUSE SQLPILOT' in sql:
        print("  ✅ Incremental Append SQL generated")
    else:
        print("  ❌ Incremental Append SQL generation failed")
        exit(1)

# Compile SCD2
with open('examples/test_scd2.yaml') as f:
    plan = yaml.safe_load(f)
    sql = compiler.compile(plan)
    if 'MERGE INTO' in sql and 'INSERT INTO' in sql:
        print("  ✅ SCD2 SQL generated (2 steps)")
    else:
        print("  ❌ SCD2 SQL generation failed")
        exit(1)
EOF

echo ""

# Test 3: Run unit tests
echo "🧪 Test 3: Running unit tests..."
pytest tests/test_plan_validation.py -q
echo "  ✅ Plan validation tests passed"
echo ""

# Test 4: Show generated SQL
echo "📄 Sample Generated SQL:"
echo "========================"
python3 << 'EOF'
import yaml
from compiler import SQLCompiler

compiler = SQLCompiler('plan-schema/v1/plan.schema.json')

with open('examples/test_incremental_append.yaml') as f:
    plan = yaml.safe_load(f)
    sql = compiler.compile(plan)
    print(sql)
EOF

echo ""
echo "✅ All tests passed!"
echo ""
echo "📚 Next Steps:"
echo "  1. Review generated SQL above"
echo "  2. Run full test suite: pytest tests/ -v"
echo "  3. Set up test tables in Databricks (see TEST_CONFIGURATION.md)"
echo "  4. Test execution against Databricks warehouse"
echo ""
echo "📖 Documentation:"
echo "  - TEST_CONFIGURATION.md - Full setup guide"
echo "  - TESTING_QUICKSTART.md - Testing guide"
echo "  - README.md - Project overview"
echo ""

