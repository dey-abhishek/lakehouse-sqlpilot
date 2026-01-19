#!/bin/bash

# generate_oauth_token.sh
# Helper script to generate OAuth refresh token for Databricks

set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔐 Databricks OAuth Token Generator"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Prompt for inputs
read -p "Enter Databricks Host (e.g., e2-demo-field-eng.cloud.databricks.com): " DATABRICKS_HOST
read -p "Enter OAuth Client ID: " CLIENT_ID
read -s -p "Enter OAuth Client Secret: " CLIENT_SECRET
echo ""

# Validate inputs
if [ -z "$DATABRICKS_HOST" ] || [ -z "$CLIENT_ID" ] || [ -z "$CLIENT_SECRET" ]; then
    echo "❌ All fields are required!"
    exit 1
fi

# Remove https:// if present
DATABRICKS_HOST=$(echo "$DATABRICKS_HOST" | sed 's|https://||' | sed 's|http://||')

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📋 Configuration"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Host: $DATABRICKS_HOST"
echo "Client ID: $CLIENT_ID"
echo "Client Secret: ${CLIENT_SECRET:0:10}..."
echo ""

# Generate authorization URL
AUTH_URL="https://${DATABRICKS_HOST}/oidc/v1/authorize?client_id=${CLIENT_ID}&redirect_uri=http://localhost:8020&response_type=code&scope=all-apis%20sql%20offline_access"

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🌐 Step 1: Authorize in Browser"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Opening browser for authorization..."
echo ""
echo "If browser doesn't open, visit this URL:"
echo "$AUTH_URL"
echo ""

# Try to open browser (works on macOS, Linux, WSL)
if command -v open &> /dev/null; then
    open "$AUTH_URL"
elif command -v xdg-open &> /dev/null; then
    xdg-open "$AUTH_URL"
elif command -v wsl-open &> /dev/null; then
    wsl-open "$AUTH_URL"
else
    echo "⚠️  Could not open browser automatically. Please open the URL above manually."
fi

echo ""
echo "After authorizing:"
echo "1. You'll be redirected to http://localhost:8020/?code=..."
echo "2. Copy the 'code' parameter from the URL"
echo ""

read -p "Enter the authorization code: " AUTH_CODE

if [ -z "$AUTH_CODE" ]; then
    echo "❌ Authorization code is required!"
    exit 1
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "🔄 Step 2: Exchanging Code for Tokens"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Exchange code for tokens
TOKEN_RESPONSE=$(curl -s -X POST "https://${DATABRICKS_HOST}/oidc/v1/token" \
    -H "Content-Type: application/x-www-form-urlencoded" \
    -d "grant_type=authorization_code" \
    -d "code=${AUTH_CODE}" \
    -d "client_id=${CLIENT_ID}" \
    -d "client_secret=${CLIENT_SECRET}" \
    -d "redirect_uri=http://localhost:8020")

# Check if request was successful
if echo "$TOKEN_RESPONSE" | grep -q "error"; then
    echo "❌ Token exchange failed:"
    echo "$TOKEN_RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$TOKEN_RESPONSE"
    exit 1
fi

# Extract tokens
ACCESS_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('access_token', ''))")
REFRESH_TOKEN=$(echo "$TOKEN_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('refresh_token', ''))")
EXPIRES_IN=$(echo "$TOKEN_RESPONSE" | python3 -c "import json, sys; print(json.load(sys.stdin).get('expires_in', ''))")

if [ -z "$REFRESH_TOKEN" ]; then
    echo "❌ Failed to get refresh token!"
    echo "$TOKEN_RESPONSE"
    exit 1
fi

echo "✅ Tokens obtained successfully!"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📝 Your OAuth Credentials"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "OAUTH_CLIENT_ID=\"$CLIENT_ID\""
echo "OAUTH_CLIENT_SECRET=\"$CLIENT_SECRET\""
echo "OAUTH_REFRESH_TOKEN=\"$REFRESH_TOKEN\""
echo ""
echo "Access Token (expires in ${EXPIRES_IN}s):"
echo "${ACCESS_TOKEN:0:50}..."
echo ""

# Offer to update .env file
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "💾 Save to .env file?"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
read -p "Update .env file with these credentials? (y/n): " UPDATE_ENV

if [ "$UPDATE_ENV" = "y" ] || [ "$UPDATE_ENV" = "Y" ]; then
    ENV_FILE=".env"
    
    # Create backup
    if [ -f "$ENV_FILE" ]; then
        cp "$ENV_FILE" "${ENV_FILE}.backup.$(date +%Y%m%d_%H%M%S)"
        echo "✅ Backed up existing .env file"
    fi
    
    # Remove old OAuth entries
    if [ -f "$ENV_FILE" ]; then
        sed -i.tmp '/^OAUTH_CLIENT_ID=/d' "$ENV_FILE"
        sed -i.tmp '/^OAUTH_CLIENT_SECRET=/d' "$ENV_FILE"
        sed -i.tmp '/^OAUTH_REFRESH_TOKEN=/d' "$ENV_FILE"
        rm -f "${ENV_FILE}.tmp"
    fi
    
    # Append new OAuth credentials
    echo "" >> "$ENV_FILE"
    echo "# OAuth Credentials (Generated $(date))" >> "$ENV_FILE"
    echo "OAUTH_CLIENT_ID=\"$CLIENT_ID\"" >> "$ENV_FILE"
    echo "OAUTH_CLIENT_SECRET=\"$CLIENT_SECRET\"" >> "$ENV_FILE"
    echo "OAUTH_REFRESH_TOKEN=\"$REFRESH_TOKEN\"" >> "$ENV_FILE"
    
    echo "✅ Updated .env file"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "🚀 Next Steps"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "1. Upload secrets to Databricks:"
    echo "   source .env"
    echo "   echo \"\$OAUTH_CLIENT_ID\" | databricks secrets put-secret sqlpilot-secrets oauth-client-id"
    echo "   echo \"\$OAUTH_CLIENT_SECRET\" | databricks secrets put-secret sqlpilot-secrets oauth-client-secret"
    echo "   echo \"\$OAUTH_REFRESH_TOKEN\" | databricks secrets put-secret sqlpilot-secrets oauth-refresh-token"
    echo ""
    echo "2. Continue deployment:"
    echo "   ./scripts/deployment/deploy_to_databricks.sh prod"
    echo ""
else
    echo ""
    echo "ℹ️  Credentials not saved. Copy them manually to your .env file."
    echo ""
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ OAuth token generation complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"


