#!/bin/bash

# setup_auth_automated.sh
# Automated authentication setup for production and test environments
# Handles service principal secret generation and Lakebase credentials

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
APP_NAME=${1:-lakehouse-sqlpilot}
ENVIRONMENT=${2:-prod}  # prod, test, dev
SECRET_SCOPE="sqlpilot-secrets"

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}🔐 Automated Authentication Setup${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "App: $APP_NAME"
echo "Environment: $ENVIRONMENT"
echo "Secret Scope: $SECRET_SCOPE"
echo ""

# Function to check if secret exists
secret_exists() {
    local scope=$1
    local key=$2
    databricks secrets get --scope "$scope" --key "$key" &>/dev/null
    return $?
}

# Function to get or create service principal secret
get_or_create_sp_secret() {
    echo -e "${YELLOW}Step 1: Service Principal Secret${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Check if app exists
    if ! databricks apps get "$APP_NAME" &>/dev/null; then
        echo -e "${RED}❌ App '$APP_NAME' does not exist. Create it first:${NC}"
        echo "   databricks apps create $APP_NAME"
        exit 1
    fi
    
    # Get service principal ID
    SP_ID=$(databricks apps get "$APP_NAME" --output json | python3 -c "import json, sys; print(json.load(sys.stdin).get('service_principal_id', ''))")
    
    if [ -z "$SP_ID" ]; then
        echo -e "${RED}❌ Could not get service principal ID${NC}"
        exit 1
    fi
    
    echo "Service Principal ID: $SP_ID"
    
    # Check if secret already exists in scope
    if secret_exists "$SECRET_SCOPE" "databricks-token"; then
        echo -e "${GREEN}✅ Token already exists in secret scope${NC}"
        
        if [ "$ENVIRONMENT" = "test" ]; then
            # For test, we can reuse existing token
            echo "ℹ️  Using existing token for test environment"
            return 0
        else
            # For prod, ask if we should regenerate
            echo ""
            echo -e "${YELLOW}⚠️  Token already exists for production environment${NC}"
            read -p "Regenerate? This will invalidate the old token (y/n): " REGENERATE
            
            if [ "$REGENERATE" != "y" ] && [ "$REGENERATE" != "Y" ]; then
                echo "Using existing token"
                return 0
            fi
        fi
    fi
    
    # List existing secrets for service principal
    echo "Checking existing service principal secrets..."
    EXISTING_SECRETS=$(databricks service-principals list-secrets --service-principal-id "$SP_ID" --output json 2>/dev/null || echo '{"secrets":[]}')
    SECRET_COUNT=$(echo "$EXISTING_SECRETS" | python3 -c "import json, sys; print(len(json.load(sys.stdin).get('secrets', [])))")
    
    echo "Found $SECRET_COUNT existing secret(s)"
    
    # For test environment, use first secret if exists
    if [ "$ENVIRONMENT" = "test" ] && [ "$SECRET_COUNT" -gt 0 ]; then
        echo "Using existing service principal secret for test environment"
        # Note: We can't retrieve the actual secret value, so we assume it's already in the secret scope
        return 0
    fi
    
    # Generate new secret
    echo "Generating new service principal secret..."
    SECRET_RESPONSE=$(databricks service-principals create-secret \
        --service-principal-id "$SP_ID" \
        --output json 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to create secret:${NC}"
        echo "$SECRET_RESPONSE"
        exit 1
    fi
    
    SECRET_VALUE=$(echo "$SECRET_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('secret', ''))")
    
    if [ -z "$SECRET_VALUE" ]; then
        echo -e "${RED}❌ Could not extract secret value${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Service principal secret generated${NC}"
    
    # Upload to secret scope
    echo "Uploading to secret scope..."
    echo "$SECRET_VALUE" | databricks secrets put-secret "$SECRET_SCOPE" "databricks-token" || {
        echo -e "${RED}❌ Failed to upload secret${NC}"
        exit 1
    }
    
    echo -e "${GREEN}✅ Token uploaded to secret scope${NC}"
    
    # Save to .env for local development
    if [ "$ENVIRONMENT" != "prod" ]; then
        ENV_FILE=".env"
        if [ -f "$ENV_FILE" ]; then
            sed -i.tmp '/^DATABRICKS_TOKEN=/d' "$ENV_FILE" 2>/dev/null
            rm -f "${ENV_FILE}.tmp"
        fi
        echo "" >> "$ENV_FILE"
        echo "# Service Principal Token ($ENVIRONMENT - $(date))" >> "$ENV_FILE"
        echo "DATABRICKS_TOKEN=\"$SECRET_VALUE\"" >> "$ENV_FILE"
        echo "ℹ️  Saved to .env for local development"
    fi
    
    echo ""
}

# Function to setup Lakebase credentials
setup_lakebase() {
    echo -e "${YELLOW}Step 2: Lakebase Configuration${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Check if Lakebase is enabled
    LAKEBASE_ENABLED=$(grep -E "^LAKEBASE_ENABLED=" .env 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "false")
    
    if [ "$LAKEBASE_ENABLED" != "true" ]; then
        echo "ℹ️  Lakebase not enabled. Skipping."
        echo ""
        return 0
    fi
    
    # Get Lakebase host
    LAKEBASE_HOST=$(grep -E "^LAKEBASE_HOST=" .env 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "")
    
    if [ -z "$LAKEBASE_HOST" ]; then
        echo -e "${YELLOW}⚠️  LAKEBASE_HOST not set in .env${NC}"
        echo ""
        return 0
    fi
    
    echo "Lakebase Host: $LAKEBASE_HOST"
    
    # Upload Lakebase configuration to secrets
    echo "Uploading Lakebase configuration..."
    
    if ! secret_exists "$SECRET_SCOPE" "lakebase-host"; then
        echo "$LAKEBASE_HOST" | databricks secrets put-secret "$SECRET_SCOPE" "lakebase-host"
        echo "✅ lakebase-host uploaded"
    else
        echo "✅ lakebase-host already exists"
    fi
    
    # Get database name
    LAKEBASE_DATABASE=$(grep -E "^LAKEBASE_DATABASE=" .env 2>/dev/null | cut -d= -f2 | tr -d '"' || echo "databricks_postgres")
    echo "$LAKEBASE_DATABASE" | databricks secrets put-secret "$SECRET_SCOPE" "lakebase-database"
    echo "✅ lakebase-database uploaded"
    
    echo ""
    echo -e "${GREEN}✅ Lakebase configuration complete${NC}"
    echo "ℹ️  App will use Database Credential API with service principal token"
    echo ""
}

# Function to setup other required secrets
setup_databricks_config() {
    echo -e "${YELLOW}Step 3: Databricks Configuration${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # Get values from .env or environment
    DATABRICKS_HOST=${DATABRICKS_SERVER_HOSTNAME:-$(grep -E "^DATABRICKS_SERVER_HOSTNAME=" .env 2>/dev/null | cut -d= -f2 | tr -d '"')}
    WAREHOUSE_ID=${DATABRICKS_WAREHOUSE_ID:-$(grep -E "^DATABRICKS_WAREHOUSE_ID=" .env 2>/dev/null | cut -d= -f2 | tr -d '"')}
    CATALOG=${DATABRICKS_CATALOG:-$(grep -E "^DATABRICKS_CATALOG=" .env 2>/dev/null | cut -d= -f2 | tr -d '"')}
    SCHEMA=${DATABRICKS_SCHEMA:-$(grep -E "^DATABRICKS_SCHEMA=" .env 2>/dev/null | cut -d= -f2 | tr -d '"')}
    
    # Upload required secrets
    if [ -n "$DATABRICKS_HOST" ]; then
        if ! secret_exists "$SECRET_SCOPE" "databricks-host"; then
            echo "$DATABRICKS_HOST" | databricks secrets put-secret "$SECRET_SCOPE" "databricks-host"
            echo "✅ databricks-host uploaded"
        else
            echo "✅ databricks-host already exists"
        fi
    fi
    
    if [ -n "$WAREHOUSE_ID" ]; then
        if ! secret_exists "$SECRET_SCOPE" "databricks-warehouse-id"; then
            echo "$WAREHOUSE_ID" | databricks secrets put-secret "$SECRET_SCOPE" "databricks-warehouse-id"
            echo "✅ databricks-warehouse-id uploaded"
        else
            echo "✅ databricks-warehouse-id already exists"
        fi
    fi
    
    if [ -n "$CATALOG" ]; then
        if ! secret_exists "$SECRET_SCOPE" "databricks-catalog"; then
            echo "$CATALOG" | databricks secrets put-secret "$SECRET_SCOPE" "databricks-catalog"
            echo "✅ databricks-catalog uploaded"
        else
            echo "✅ databricks-catalog already exists"
        fi
    fi
    
    if [ -n "$SCHEMA" ]; then
        if ! secret_exists "$SECRET_SCOPE" "databricks-schema"; then
            echo "$SCHEMA" | databricks secrets put-secret "$SECRET_SCOPE" "databricks-schema"
            echo "✅ databricks-schema uploaded"
        else
            echo "✅ databricks-schema already exists"
        fi
    fi
    
    echo ""
}

# Function to verify setup
verify_setup() {
    echo -e "${YELLOW}Step 4: Verification${NC}"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    echo "Listing secrets in scope '$SECRET_SCOPE':"
    databricks secrets list --scope "$SECRET_SCOPE" --output json | python3 -c "
import json, sys
secrets = json.load(sys.stdin).get('secrets', [])
for s in secrets:
    print(f\"  ✅ {s['key']}\")
" || echo "Could not list secrets"
    
    echo ""
}

# Function to generate test token (short-lived)
generate_test_token() {
    echo -e "${YELLOW}Generating temporary test token...${NC}"
    
    # For test environment, we can use a personal access token with short expiry
    # Or reuse service principal secret
    # This is just for local testing, not for CI/CD
    
    echo "ℹ️  For test environment, using service principal secret"
    echo "   (Set up service principal secret first)"
}

# Main execution
main() {
    # Create secret scope if it doesn't exist
    if ! databricks secrets list-scopes 2>/dev/null | grep -q "$SECRET_SCOPE"; then
        echo "Creating secret scope '$SECRET_SCOPE'..."
        databricks secrets create-scope "$SECRET_SCOPE" || {
            echo -e "${RED}❌ Failed to create secret scope${NC}"
            exit 1
        }
        echo -e "${GREEN}✅ Secret scope created${NC}"
        echo ""
    fi
    
    # Run setup steps
    get_or_create_sp_secret
    setup_lakebase
    setup_databricks_config
    verify_setup
    
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✅ Authentication setup complete!${NC}"
    echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    
    if [ "$ENVIRONMENT" = "prod" ]; then
        echo -e "${BLUE}📋 Production Deployment${NC}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "All secrets are configured in '$SECRET_SCOPE' scope."
        echo "Your app will automatically access them during deployment."
        echo ""
        echo "Next step:"
        echo "  ./scripts/deployment/deploy_to_databricks.sh prod"
        echo ""
    else
        echo -e "${BLUE}📋 Test/Dev Environment${NC}"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Secrets configured in both:"
        echo "  - Secret scope: $SECRET_SCOPE (for deployed app)"
        echo "  - .env file (for local development)"
        echo ""
        echo "For local testing:"
        echo "  source .env"
        echo "  python -m pytest tests/"
        echo ""
        echo "For app testing:"
        echo "  ./scripts/deployment/deploy_to_databricks.sh test"
        echo ""
    fi
    
    echo -e "${YELLOW}💡 Authentication Flow:${NC}"
    echo "  1. App uses service principal token (from secret scope)"
    echo "  2. Service principal token → Databricks API calls"
    echo "  3. Service principal token → Database Credential API → Lakebase credentials"
    echo "  4. Lakebase credentials auto-refresh via Database Credential API"
    echo ""
    echo -e "${GREEN}No OAuth needed! 🎉${NC}"
    echo ""
}

# Run main
main


