#!/bin/bash

# Setup script for Lakehouse SQLPilot test environment
# Workspace: e2-dogfood.staging.cloud.databricks.com (ID: 6051921418418893)

set -e

echo "=========================================="
echo "Lakehouse SQLPilot - Test Setup"
echo "=========================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Creating .env file from template..."
    cp .env.example .env
    echo "✓ Created .env file"
    echo ""
    echo "⚠️  IMPORTANT: Edit .env and set your DATABRICKS_TOKEN and DEFAULT_WAREHOUSE_ID"
    echo ""
else
    echo "✓ .env file already exists"
fi

# Check if virtual environment exists
if [ ! -d "sqlpilot" ]; then
    echo "Creating virtual environment..."
    python -m venv sqlpilot
    echo "✓ Created virtual environment: sqlpilot"
else
    echo "✓ Virtual environment already exists"
fi

# Activate virtual environment
echo ""
echo "Activating virtual environment..."
source sqlpilot/bin/activate

# Install dependencies
echo ""
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✓ Dependencies installed"

echo ""
echo "=========================================="
echo "Setup Complete!"
echo "=========================================="
echo ""
echo "⚠️  SECURITY REMINDER: NEVER commit credentials to git!"
echo "   - .env file is gitignored"
echo "   - Read SECURITY.md for credential best practices"
echo ""
echo "Next steps:"
echo ""
echo "1. Edit .env and set (KEEP THIS FILE LOCAL):"
echo "   - DATABRICKS_TOKEN (get from workspace)"
echo "   - DEFAULT_WAREHOUSE_ID (get from SQL Warehouses)"
echo ""
echo "2. Verify .env is not tracked by git:"
echo "   git status | grep .env"
echo "   (should show nothing)"
echo ""
echo "3. Access test workspace:"
echo "   https://e2-dogfood.staging.cloud.databricks.com/?o=6051921418418893"
echo ""
echo "4. Create required tables using SQL from:"
echo "   TEST_CONFIGURATION.md"
echo ""
echo "5. Run test plan:"
echo "   python -m sqlpilot.cli validate examples/test_incremental_append.yaml"
echo ""
echo "=========================================="

