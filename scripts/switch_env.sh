#!/bin/bash

# switch_env.sh
# Helper script to switch between development environments

ENV=${1:-dev}

if [ "$ENV" != "dev" ] && [ "$ENV" != "test" ] && [ "$ENV" != "prod" ]; then
    echo "Usage: ./scripts/switch_env.sh [dev|test|prod]"
    echo ""
    echo "Examples:"
    echo "  ./scripts/switch_env.sh dev   # Switch to development environment"
    echo "  ./scripts/switch_env.sh test  # Switch to test environment"
    echo "  ./scripts/switch_env.sh prod  # Switch to production (reference only)"
    exit 1
fi

ENV_FILE=".env.${ENV}"

if [ ! -f "$ENV_FILE" ]; then
    echo "❌ Error: $ENV_FILE not found!"
    exit 1
fi

# Create/update the .env symlink
ln -sf "$ENV_FILE" .env

echo "✅ Switched to $ENV environment"
echo ""
echo "Active environment file: .env -> $ENV_FILE"
echo ""

# Show environment details
echo "Configuration:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep "^APP_ENV=" ".env" || echo "APP_ENV not set"
grep "^SQLPILOT_REQUIRE_AUTH=" ".env" || echo "SQLPILOT_REQUIRE_AUTH not set"
grep "^LOG_LEVEL=" ".env" || echo "LOG_LEVEL not set"
grep "^DATABRICKS_SERVER_HOSTNAME=" ".env" || echo "DATABRICKS_SERVER_HOSTNAME not set"
echo ""

if [ "$ENV" = "dev" ]; then
    echo "💡 Development Mode:"
    echo "   - Run locally: uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload"
    echo "   - URL: http://localhost:8000"
    echo "   - Docs: http://localhost:8000/docs"
elif [ "$ENV" = "test" ]; then
    echo "💡 Test Mode:"
    echo "   - For test app deployment or CI/CD"
    echo "   - Run tests: pytest -v"
elif [ "$ENV" = "prod" ]; then
    echo "⚠️  Production Mode (Reference Only):"
    echo "   - This file is for reference"
    echo "   - Actual secrets come from Databricks Secret Scope"
    echo "   - Deploy with: ./scripts/deployment/deploy_to_databricks.sh prod"
fi

echo ""
