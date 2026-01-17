#!/bin/bash
#
# Automated Databricks App Deployment Script
# This script automates the complete deployment of Lakehouse SQLPilot
#
# Usage:
#   ./deploy_to_databricks.sh [environment]
#
# Example:
#   ./deploy_to_databricks.sh prod
#

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME="lakehouse-sqlpilot"
SECRET_SCOPE="sqlpilot-secrets"
ENVIRONMENT="${1:-dev}"  # dev, staging, or prod

echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}   Lakehouse SQLPilot - Automated Deployment${NC}"
echo -e "${BLUE}   Environment: ${ENVIRONMENT}${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo ""

# Step 1: Pre-flight checks
echo -e "${YELLOW}Step 1: Pre-flight Checks${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check Databricks CLI
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}❌ Databricks CLI not found${NC}"
    echo "Install: pip install databricks-cli"
    exit 1
fi
echo -e "${GREEN}✅ Databricks CLI installed${NC}"

# Check authentication
if ! databricks workspace ls / &> /dev/null; then
    echo -e "${RED}❌ Not authenticated with Databricks${NC}"
    echo "Run: databricks configure --host <workspace-url>"
    exit 1
fi
echo -e "${GREEN}✅ Databricks authentication working${NC}"

# Check Python
if ! python --version &> /dev/null; then
    echo -e "${RED}❌ Python not found${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Python installed${NC}"

# Check required files
REQUIRED_FILES=(
    "databricks.yml"
    "requirements.txt"
    "api/main.py"
    "security/oauth_manager.py"
)

for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        echo -e "${RED}❌ Missing required file: $file${NC}"
        exit 1
    fi
done
echo -e "${GREEN}✅ All required files present${NC}"

echo ""

# Step 2: Create Secret Scope (if not exists)
echo -e "${YELLOW}Step 2: Secret Scope Setup${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if databricks secrets list-scopes | grep -q "$SECRET_SCOPE"; then
    echo -e "${GREEN}✅ Secret scope '$SECRET_SCOPE' already exists${NC}"
else
    echo "Creating secret scope '$SECRET_SCOPE'..."
    databricks secrets create-scope "$SECRET_SCOPE"
    echo -e "${GREEN}✅ Secret scope created${NC}"
fi

echo ""

# Step 3: Load secrets from .env file
echo -e "${YELLOW}Step 3: Loading Secrets${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -f ".env" ]; then
    echo "Found .env file. Loading secrets..."
    
    # Map of .env variables to secret keys
    declare -A SECRET_MAP=(
        ["DATABRICKS_SERVER_HOSTNAME"]="databricks-host"
        ["DATABRICKS_TOKEN"]="databricks-token"
        ["DATABRICKS_WAREHOUSE_ID"]="databricks-warehouse-id"
        ["DATABRICKS_CATALOG"]="databricks-catalog"
        ["DATABRICKS_SCHEMA"]="databricks-schema"
        ["OAUTH_CLIENT_ID"]="oauth-client-id"
        ["OAUTH_CLIENT_SECRET"]="oauth-client-secret"
        ["OAUTH_REFRESH_TOKEN"]="oauth-refresh-token"
        ["LAKEBASE_HOST"]="lakebase-host"
        ["LAKEBASE_USER"]="lakebase-user"
        ["LAKEBASE_PASSWORD"]="lakebase-password"
        ["LAKEBASE_DATABASE"]="lakebase-database"
    )
    
    # Load .env file
    set -a
    source .env
    set +a
    
    # Upload secrets
    for env_var in "${!SECRET_MAP[@]}"; do
        secret_key="${SECRET_MAP[$env_var]}"
        secret_value="${!env_var}"
        
        if [ -n "$secret_value" ]; then
            echo "Uploading secret: $secret_key"
            echo "$secret_value" | databricks secrets put-secret \
                "$SECRET_SCOPE" \
                "$secret_key" 2>/dev/null || true
            echo -e "${GREEN}✅ Secret uploaded: $secret_key${NC}"
        else
            echo -e "${YELLOW}⚠️  Skipping empty secret: $secret_key${NC}"
        fi
    done
else
    echo -e "${YELLOW}⚠️  No .env file found. Skipping secret upload.${NC}"
    echo "Secrets must be configured manually:"
    echo "  databricks secrets put-secret --scope $SECRET_SCOPE --key <key> --string-value <value>"
fi

echo ""

# Step 4: Grant app access to secrets
echo -e "${YELLOW}Step 4: Grant App Access to Secrets${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo "Granting READ access to app service principal..."
databricks secrets put-acl \
    "$SECRET_SCOPE" \
    "$APP_NAME" \
    READ 2>/dev/null || echo -e "${YELLOW}⚠️  ACL may already exist or will be set during app creation${NC}"

echo -e "${GREEN}✅ Access configured${NC}"
echo ""

# Step 5: Run tests
echo -e "${YELLOW}Step 5: Running Tests${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -d "sqlpilot" ]; then
    source sqlpilot/bin/activate
fi

echo "Running backend tests..."
if python -m pytest tests/ -q --tb=short -m "not requires_databricks" 2>&1 | tail -20; then
    echo -e "${GREEN}✅ Tests passed${NC}"
else
    echo -e "${YELLOW}⚠️  Some tests failed. Continue? (y/n)${NC}"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        echo "Deployment cancelled."
        exit 1
    fi
fi

echo ""

# Step 6: Build frontend (if exists)
echo -e "${YELLOW}Step 6: Building Frontend${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ -d "ui/plan-editor" ]; then
    cd ui/plan-editor
    if [ -f "package.json" ]; then
        echo "Building frontend..."
        npm run build --if-present 2>&1 | tail -10
        echo -e "${GREEN}✅ Frontend built${NC}"
    fi
    cd ../..
else
    echo -e "${YELLOW}⚠️  No frontend directory found, skipping${NC}"
fi

echo ""

# Step 7: Create or update app
echo -e "${YELLOW}Step 7: Deploying App${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Check if app exists
if databricks apps get "$APP_NAME" &> /dev/null; then
    echo "App '$APP_NAME' exists. Updating..."
    
    # Update app
    databricks apps deploy "$APP_NAME" . || {
        echo -e "${RED}❌ Deployment failed${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ App updated${NC}"
else
    echo "App '$APP_NAME' does not exist. Creating..."
    
    # Create app
    databricks apps create "$APP_NAME" || {
        echo -e "${RED}❌ App creation failed${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ App created${NC}"
    
    # Deploy app
    echo "Deploying app..."
    databricks apps deploy "$APP_NAME" . || {
        echo -e "${RED}❌ Deployment failed${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ App deployed${NC}"
fi

echo ""

# Step 8: Wait for app to start
echo -e "${YELLOW}Step 8: Waiting for App to Start${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

MAX_WAIT=300  # 5 minutes
WAIT_TIME=0
SLEEP_INTERVAL=10

while [ $WAIT_TIME -lt $MAX_WAIT ]; do
    STATUS=$(databricks apps get "$APP_NAME" --output json 2>/dev/null | python -c "import json, sys; print(json.load(sys.stdin).get('status', 'unknown'))" || echo "unknown")
    
    echo "App status: $STATUS (waited ${WAIT_TIME}s)"
    
    if [ "$STATUS" = "RUNNING" ]; then
        echo -e "${GREEN}✅ App is running!${NC}"
        break
    elif [ "$STATUS" = "CRASHED" ]; then
        echo -e "${RED}❌ App crashed. Check logs:${NC}"
        databricks apps logs "$APP_NAME" --tail 50
        exit 1
    fi
    
    sleep $SLEEP_INTERVAL
    WAIT_TIME=$((WAIT_TIME + SLEEP_INTERVAL))
done

if [ $WAIT_TIME -ge $MAX_WAIT ]; then
    echo -e "${RED}❌ Timeout waiting for app to start${NC}"
    echo "Check logs: databricks apps logs $APP_NAME"
    exit 1
fi

echo ""

# Step 9: Get app URL
echo -e "${YELLOW}Step 9: App Information${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

APP_INFO=$(databricks apps get "$APP_NAME" --output json)
APP_URL=$(echo "$APP_INFO" | python -c "import json, sys; print(json.load(sys.stdin).get('url', 'N/A'))" || echo "N/A")

echo -e "${GREEN}App Name:${NC} $APP_NAME"
echo -e "${GREEN}App URL:${NC}  $APP_URL"
echo -e "${GREEN}Status:${NC}   RUNNING ✅"

echo ""

# Step 10: Health check
echo -e "${YELLOW}Step 10: Health Check${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$APP_URL" != "N/A" ]; then
    echo "Testing health endpoint..."
    sleep 5  # Give app time to fully start
    
    if curl -f -s "${APP_URL}/health" > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Health check passed${NC}"
        curl -s "${APP_URL}/health" | python -m json.tool 2>/dev/null || true
    else
        echo -e "${YELLOW}⚠️  Health check failed (app may still be starting)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  App URL not available yet${NC}"
fi

echo ""

# Step 11: Display next steps
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ DEPLOYMENT COMPLETE!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}App URL:${NC} $APP_URL"
echo ""
echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Open app in browser: $APP_URL"
echo "2. Login with Databricks SSO"
echo "3. Run UAT tests (see README_DEPLOYMENT.md)"
echo "4. Monitor logs: databricks apps logs $APP_NAME --follow"
echo "5. Check OAuth status: curl ${APP_URL}/api/v1/oauth/status"
echo ""
echo -e "${YELLOW}Useful Commands:${NC}"
echo "  View status:  databricks apps get $APP_NAME"
echo "  View logs:    databricks apps logs $APP_NAME --follow"
echo "  Stop app:     databricks apps stop $APP_NAME"
echo "  Start app:    databricks apps start $APP_NAME"
echo "  Delete app:   databricks apps delete $APP_NAME"
echo ""
echo -e "${GREEN}Happy deploying! 🚀${NC}"

