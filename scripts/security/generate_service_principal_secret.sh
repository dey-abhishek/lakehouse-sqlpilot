#!/bin/bash

# generate_service_principal_secret.sh
# Generate a secret for the Databricks App's service principal

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔐 Service Principal Secret Generator"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Get app name
APP_NAME=${1:-lakehouse-sqlpilot}

echo "Getting service principal details for app: $APP_NAME"
echo ""

# Get app details
APP_DETAILS=$(databricks apps get "$APP_NAME" --output json 2>/dev/null)

if [ $? -ne 0 ]; then
    echo "❌ Could not find app: $APP_NAME"
    echo "Make sure the app exists: databricks apps get $APP_NAME"
    exit 1
fi

# Extract service principal ID
SP_ID=$(echo "$APP_DETAILS" | python3 -c "import json, sys; print(json.load(sys.stdin).get('service_principal_id', ''))")
SP_NAME=$(echo "$APP_DETAILS" | python3 -c "import json, sys; print(json.load(sys.stdin).get('service_principal_name', ''))")

if [ -z "$SP_ID" ]; then
    echo "❌ Could not extract service principal ID from app details"
    exit 1
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Service Principal Details"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Name: $SP_NAME"
echo "ID: $SP_ID"
echo ""

# List existing secrets
echo "Checking for existing secrets..."
EXISTING_SECRETS=$(databricks service-principals list-secrets --service-principal-id "$SP_ID" --output json 2>/dev/null || echo "[]")

SECRET_COUNT=$(echo "$EXISTING_SECRETS" | python3 -c "import json, sys; secrets = json.load(sys.stdin); print(len(secrets.get('secrets', [])))" 2>/dev/null || echo "0")

if [ "$SECRET_COUNT" -gt 0 ]; then
    echo ""
    echo "ℹ️  Found $SECRET_COUNT existing secret(s) for this service principal:"
    echo "$EXISTING_SECRETS" | python3 -m json.tool 2>/dev/null || echo "$EXISTING_SECRETS"
    echo ""
    read -p "Create a new secret anyway? (y/n): " CREATE_NEW
    
    if [ "$CREATE_NEW" != "y" ] && [ "$CREATE_NEW" != "Y" ]; then
        echo "Cancelled. Use an existing secret or delete old ones first."
        exit 0
    fi
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔑 Creating New Secret"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Create secret
SECRET_RESPONSE=$(databricks service-principals create-secret \
    --service-principal-id "$SP_ID" \
    --output json 2>&1)

if [ $? -ne 0 ]; then
    echo "❌ Failed to create secret:"
    echo "$SECRET_RESPONSE"
    exit 1
fi

# Extract secret value
SECRET_VALUE=$(echo "$SECRET_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('secret', ''))")
SECRET_ID=$(echo "$SECRET_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('id', ''))")

if [ -z "$SECRET_VALUE" ]; then
    echo "❌ Could not extract secret from response"
    echo "$SECRET_RESPONSE"
    exit 1
fi

echo "✅ Secret created successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📝 Your Service Principal Credentials"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Service Principal ID: $SP_ID"
echo "Secret ID: $SECRET_ID"
echo "Secret (Token): $SECRET_VALUE"
echo ""
echo "⚠️  SAVE THIS SECRET NOW - You won't be able to see it again!"
echo ""

# Offer to update .env file
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💾 Save to .env file?"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -p "Update .env file with this secret? (y/n): " UPDATE_ENV

if [ "$UPDATE_ENV" = "y" ] || [ "$UPDATE_ENV" = "Y" ]; then
    ENV_FILE=".env"
    
    # Create backup
    if [ -f "$ENV_FILE" ]; then
        cp "$ENV_FILE" "${ENV_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
        echo "✅ Backed up existing .env file"
    fi
    
    # Remove old DATABRICKS_TOKEN if exists
    if [ -f "$ENV_FILE" ]; then
        sed -i.tmp '/^DATABRICKS_TOKEN=/d' "$ENV_FILE"
        rm -f "${ENV_FILE}.tmp"
    fi
    
    # Append new token
    echo "" >> "$ENV_FILE"
    echo "# Service Principal Secret (Generated $(date))" >> "$ENV_FILE"
    echo "DATABRICKS_TOKEN=\"$SECRET_VALUE\"" >> "$ENV_FILE"
    
    echo "✅ Updated .env file"
    echo ""
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🚀 Next Steps"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "1. Upload secret to Databricks:"
echo "   source .env"
echo "   echo \"\$DATABRICKS_TOKEN\" | databricks secrets put-secret sqlpilot-secrets databricks-token"
echo ""
echo "2. This secret can be used for:"
echo "   ✅ Databricks API calls"
echo "   ✅ SQL Warehouse connections"
echo "   ✅ Unity Catalog operations"
echo "   ✅ Generating Lakebase credentials (via Database Credential API)"
echo ""
echo "3. Continue deployment:"
echo "   ./scripts/deployment/deploy_to_databricks.sh prod"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ Service Principal Secret generation complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "💡 TIP: This token can be used for everything - no OAuth needed!"
echo ""


