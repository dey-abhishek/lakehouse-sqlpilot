#!/bin/bash
# Lakehouse SQLPilot - SCD2 Integration Test Runner
# Quick start script to run integration tests

set -e

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║   LAKEHOUSE SQLPILOT - SCD2 INTEGRATION TEST SUITE              ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Check if we're in the project root
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: Please run this script from the project root"
    exit 1
fi

# Check if virtual environment is activated
if [ -z "$VIRTUAL_ENV" ]; then
    echo "🔧 Activating virtual environment..."
    if [ -d "sqlpilot" ]; then
        source sqlpilot/bin/activate
    else
        echo "❌ Error: Virtual environment 'sqlpilot' not found"
        echo "   Please run: python3 -m venv sqlpilot && source sqlpilot/bin/activate && pip install -r requirements.txt"
        exit 1
    fi
fi

# Check if environment variables are set
if [ -z "$DATABRICKS_HOST" ] || [ -z "$DATABRICKS_TOKEN" ]; then
    echo "⚙️  Setting up environment variables..."
    
    # Check if .env file exists
    if [ ! -f ".env" ]; then
        echo ""
        echo "❌ Error: Environment variables not set and .env file not found"
        echo ""
        echo "Please create a .env file with:"
        echo "  DATABRICKS_HOST=https://e2-dogfood.staging.cloud.databricks.com/?o=6051921418418893"
        echo "  DATABRICKS_TOKEN=your-pat-token-here"
        echo "  DATABRICKS_WAREHOUSE_ID=592f1f39793f7795"
        echo "  DATABRICKS_CATALOG=lakehouse-sqlpilot"
        echo "  DATABRICKS_SCHEMA=lakehouse-sqlpilot-schema"
        echo ""
        echo "Or set them directly:"
        echo "  export DATABRICKS_HOST='https://e2-dogfood.staging.cloud.databricks.com/?o=6051921418418893'"
        echo "  export DATABRICKS_TOKEN='your-pat-token-here'"
        exit 1
    fi
    
    # Load .env file
    export $(grep -v '^#' .env | xargs)
    echo "✅ Environment variables loaded from .env"
fi

echo ""
echo "📋 Test Configuration:"
echo "  Workspace: ${DATABRICKS_HOST}"
echo "  Catalog: ${DATABRICKS_CATALOG:-lakehouse-sqlpilot}"
echo "  Schema: ${DATABRICKS_SCHEMA:-lakehouse-sqlpilot-schema}"
echo "  Warehouse: ${DATABRICKS_WAREHOUSE_ID:-592f1f39793f7795}"
echo ""

# Create integration test directory if it doesn't exist
mkdir -p tests/integration

# Make the test script executable
chmod +x tests/integration/test_scd2_integration.py

# Run the integration tests
echo "🚀 Starting integration tests..."
echo ""

python3 tests/integration/test_scd2_integration.py

TEST_EXIT_CODE=$?

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo "✅ INTEGRATION TESTS PASSED"
    echo ""
    echo "Next steps:"
    echo "  1. Review test results in tests/integration/test_results_*.json"
    echo "  2. Check execution logs in Databricks SQL Warehouse"
    echo "  3. Verify Unity Catalog lineage"
    echo "  4. Run: databricks apps deploy (for Databricks Apps deployment)"
else
    echo "❌ INTEGRATION TESTS FAILED"
    echo ""
    echo "Troubleshooting:"
    echo "  1. Check test results in tests/integration/test_results_*.json"
    echo "  2. Verify warehouse is running"
    echo "  3. Check catalog/schema permissions"
    echo "  4. Review execution logs"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

exit $TEST_EXIT_CODE

