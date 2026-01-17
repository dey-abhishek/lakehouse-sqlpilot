#!/bin/bash
#
# Lakebase Connection Verification Script
# Verifies connection to Databricks Lakebase PostgreSQL
#

set -e

echo "=================================="
echo "Lakebase Connection Verification"
echo "=================================="
echo ""

# Check if .env file exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found"
    echo "   Copy .env.lakebase.template to .env and configure credentials"
    exit 1
fi

# Load environment variables
source .env

# Check required variables
REQUIRED_VARS=(
    "LAKEBASE_HOST"
    "LAKEBASE_USER"
    "LAKEBASE_DATABASE"
    "LAKEBASE_PASSWORD"
)

echo "📋 Checking environment variables..."
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Missing: $var"
        exit 1
    else
        echo "✓ $var is set"
    fi
done
echo ""

# Test connection using psql
echo "🔌 Testing PostgreSQL connection..."
echo "   Host: $LAKEBASE_HOST"
echo "   User: $LAKEBASE_USER"
echo "   Database: $LAKEBASE_DATABASE"
echo ""

# Build connection string
PGPASSWORD="$LAKEBASE_PASSWORD" psql \
    "host=$LAKEBASE_HOST \
     port=${LAKEBASE_PORT:-5432} \
     dbname=$LAKEBASE_DATABASE \
     user=$LAKEBASE_USER \
     sslmode=${LAKEBASE_SSLMODE:-require}" \
    -c "SELECT version();" \
    -c "SELECT current_database(), current_user;" \
    2>&1

if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Lakebase connection successful!"
    echo ""
    
    # Test creating a test table
    echo "🧪 Testing table operations..."
    PGPASSWORD="$LAKEBASE_PASSWORD" psql \
        "host=$LAKEBASE_HOST \
         port=${LAKEBASE_PORT:-5432} \
         dbname=$LAKEBASE_DATABASE \
         user=$LAKEBASE_USER \
         sslmode=${LAKEBASE_SSLMODE:-require}" \
        -c "CREATE TABLE IF NOT EXISTS sqlpilot_health_check (id SERIAL PRIMARY KEY, checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);" \
        -c "INSERT INTO sqlpilot_health_check DEFAULT VALUES RETURNING *;" \
        -c "DROP TABLE sqlpilot_health_check;" \
        2>&1
    
    if [ $? -eq 0 ]; then
        echo "✅ Table operations successful!"
    else
        echo "⚠️  Table operations failed (might be permissions)"
    fi
else
    echo ""
    echo "❌ Lakebase connection failed"
    echo "   Check your credentials in .env"
    exit 1
fi

echo ""
echo "=================================="
echo "✅ Verification Complete!"
echo "=================================="

