#!/bin/bash
# OAuth Setup Script for Lakehouse SQLPilot
# Workspace: https://e2-demo-field-eng.cloud.databricks.com/browse?o=1444828305810485 

set -e

echo "======================================================================="
echo "  Lakehouse SQLPilot - OAuth Setup"
echo "======================================================================="
echo ""

# Configuration
WORKSPACE_HOST="e2-demo-field-eng.cloud.databricks.com"
WORKSPACE_ID="1444828305810485"
REDIRECT_URI="http://localhost:8020"
CLIENT_ID="databricks-cli"

echo "Workspace: https://${WORKSPACE_HOST}/?o=${WORKSPACE_ID}"
echo "Redirect URI: ${REDIRECT_URI}"
echo ""

# Step 1: Generate code verifier and challenge
echo "Step 1: Generating OAuth code verifier and challenge..."
echo ""

python3 << 'EOF'
import hashlib, base64, secrets, string

# Generate code verifier (43-128 characters)
allowed_chars = string.ascii_letters + string.digits + "-._~"
code_verifier = ''.join(secrets.choice(allowed_chars) for _ in range(64))

# Create SHA256 hash and base64-url-encode
sha256_hash = hashlib.sha256(code_verifier.encode()).digest()
code_challenge = base64.urlsafe_b64encode(sha256_hash).decode().rstrip("=")

print(f"Code Verifier:  {code_verifier}")
print(f"Code Challenge: {code_challenge}")
print("")

# Save to file for later use
with open('.oauth_verifier', 'w') as f:
    f.write(code_verifier)
print("✓ Code verifier saved to .oauth_verifier")
EOF

echo ""

# Step 2: Generate authorization URL
echo "Step 2: Authorization URL"
echo "======================================================================="
echo ""

CODE_CHALLENGE=$(python3 << 'EOF'
import hashlib, base64
with open('.oauth_verifier', 'r') as f:
    verifier = f.read().strip()
sha256_hash = hashlib.sha256(verifier.encode()).digest()
print(base64.urlsafe_b64encode(sha256_hash).decode().rstrip("="))
EOF
)

STATE=$(python3 -c "import secrets; print(secrets.token_urlsafe(16))")

AUTH_URL="https://${WORKSPACE_HOST}/oidc/v1/authorize?client_id=${CLIENT_ID}&redirect_uri=${REDIRECT_URI}&response_type=code&state=${STATE}&code_challenge=${CODE_CHALLENGE}&code_challenge_method=S256&scope=all-apis+offline_access"

echo "Open this URL in your browser to authorize:"
echo ""
echo "${AUTH_URL}"
echo ""
echo "Expected state value: ${STATE}"
echo ""

# Save state for verification
echo "${STATE}" > .oauth_state

echo "After authorization, you'll be redirected to:"
echo "${REDIRECT_URI}/?code=...&state=..."
echo ""
echo "======================================================================="
echo ""
read -p "Press Enter after you've been redirected and have the authorization code..."

echo ""
read -p "Enter the authorization code (the value after 'code=' in the URL): " AUTH_CODE
read -p "Enter the state value (the value after 'state=' in the URL): " RETURNED_STATE

# Verify state
EXPECTED_STATE=$(cat .oauth_state)
if [ "${RETURNED_STATE}" != "${EXPECTED_STATE}" ]; then
    echo "❌ ERROR: State mismatch! Possible CSRF attack."
    echo "   Expected: ${EXPECTED_STATE}"
    echo "   Received: ${RETURNED_STATE}"
    exit 1
fi

echo "✓ State verified"
echo ""

# Step 3: Exchange code for token
echo "Step 3: Exchanging authorization code for access token..."
echo ""

CODE_VERIFIER=$(cat .oauth_verifier)

TOKEN_RESPONSE=$(curl -s --request POST \
  "https://${WORKSPACE_HOST}/oidc/v1/token" \
  --data "client_id=${CLIENT_ID}" \
  --data "grant_type=authorization_code" \
  --data "scope=all-apis offline_access" \
  --data "redirect_uri=${REDIRECT_URI}" \
  --data "code_verifier=${CODE_VERIFIER}" \
  --data "code=${AUTH_CODE}")

echo "${TOKEN_RESPONSE}" | python3 -m json.tool

# Extract tokens
ACCESS_TOKEN=$(echo "${TOKEN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('access_token', ''))")
REFRESH_TOKEN=$(echo "${TOKEN_RESPONSE}" | python3 -c "import sys, json; print(json.load(sys.stdin).get('refresh_token', ''))")

if [ -z "${ACCESS_TOKEN}" ]; then
    echo ""
    echo "❌ ERROR: Failed to get access token"
    echo "Response: ${TOKEN_RESPONSE}"
    exit 1
fi

echo ""
echo "✓ Successfully obtained OAuth tokens!"
echo ""

# Step 4: Test the token
echo "Step 4: Testing access token..."
echo ""

CLUSTERS=$(curl -s --request GET \
  --header "Authorization: Bearer ${ACCESS_TOKEN}" \
  "https://${WORKSPACE_HOST}/api/2.0/clusters/list")

echo "${CLUSTERS}" | python3 -m json.tool | head -20

echo ""
echo "✓ OAuth token is working!"
echo ""

# Step 5: Update .env file
echo "Step 5: Updating .env file..."
echo ""

if [ ! -f .env ]; then
    echo "Creating .env file from env.example..."
    cp env.example .env
fi

# Update or add OAuth configuration
python3 << EOF
import os
from pathlib import Path

env_path = Path('.env')
lines = []

if env_path.exists():
    with open(env_path, 'r') as f:
        lines = f.readlines()

# Remove old OAuth entries
lines = [l for l in lines if not any(k in l for k in [
    'DATABRICKS_TOKEN=',
    'SQLPILOT_OAUTH_',
    'DATABRICKS_SERVER_HOSTNAME='
])]

# Add new OAuth configuration
new_lines = [
    '\n# OAuth Configuration (Generated)\n',
    f'DATABRICKS_SERVER_HOSTNAME=${WORKSPACE_HOST}\n',
    f'SQLPILOT_OAUTH_ENABLED=true\n',
    f'SQLPILOT_OAUTH_CLIENT_ID=${CLIENT_ID}\n',
    '\n# OAuth Tokens (from oauth_setup.sh)\n',
    f'DATABRICKS_TOKEN=${ACCESS_TOKEN}\n',
    f'DATABRICKS_REFRESH_TOKEN=${REFRESH_TOKEN}\n',
]

with open(env_path, 'w') as f:
    f.writelines(lines + new_lines)

print('✓ .env file updated with OAuth configuration')
EOF

echo ""

# Clean up temporary files
rm -f .oauth_verifier .oauth_state

echo "======================================================================="
echo "  OAuth Setup Complete! ✓"
echo "======================================================================="
echo ""
echo "Configuration saved to .env file"
echo ""
echo "Access Token (valid for 1 hour):"
echo "${ACCESS_TOKEN}" | head -c 50
echo "..."
echo ""
echo "Refresh Token (for token renewal):"
echo "${REFRESH_TOKEN}" | head -c 50
echo "..."
echo ""
echo "Next steps:"
echo "  1. Verify configuration: python check_env.py"
echo "  2. Run UAT tests: pytest tests/test_uat_end_to_end.py -v"
echo "  3. Start the application: python api/main.py"
echo ""
echo "Note: Access tokens expire after 1 hour. Use the refresh token to get"
echo "      a new access token without re-authorization."
echo ""

