#!/usr/bin/env python3
"""
Complete OAuth Token Exchange
Use this if the automatic script didn't complete
"""

import requests
import json
from pathlib import Path
from dotenv import set_key

# Your authorization details
AUTH_CODE = ""
STATE = ""

# Configuration
WORKSPACE_HOST = "e2-demo-field-eng.cloud.databricks.com"
REDIRECT_URI = "http://localhost:8020"
CLIENT_ID = "databricks-cli"

# Check if we have the code verifier saved
verifier_file = Path(".oauth_verifier")
if not verifier_file.exists():
    print("❌ Error: .oauth_verifier file not found")
    print("\nThe code verifier was not saved from the previous step.")
    print("You'll need to start over with: python setup_oauth.py")
    exit(1)

# Load code verifier
with open(verifier_file, 'r') as f:
    code_verifier = f.read().strip()

print("="*80)
print("  Completing OAuth Token Exchange")
print("="*80)
print()
print(f"Authorization Code: {AUTH_CODE}")
print(f"State: {STATE}")
print(f"Code Verifier: {code_verifier[:20]}...")
print()

# Exchange code for tokens
print("Exchanging authorization code for tokens...")

token_url = f"https://{WORKSPACE_HOST}/oidc/v1/token"

data = {
    "client_id": CLIENT_ID,
    "grant_type": "authorization_code",
    "scope": "all-apis offline_access",
    "redirect_uri": REDIRECT_URI,
    "code_verifier": code_verifier,
    "code": AUTH_CODE
}

response = requests.post(token_url, data=data)

if response.status_code != 200:
    print(f"❌ Token exchange failed!")
    print(f"Status: {response.status_code}")
    print(f"Response: {response.text}")
    exit(1)

tokens = response.json()

print("\n✓ Successfully obtained OAuth tokens!")
print(f"\nToken Response:")
print(json.dumps({
    "token_type": tokens.get("token_type"),
    "expires_in": tokens.get("expires_in"),
    "scope": tokens.get("scope"),
    "access_token": tokens.get("access_token", "")[:50] + "...",
    "refresh_token": tokens.get("refresh_token", "")[:50] + "..."
}, indent=2))

# Test the token
print("\nTesting access token...")
test_url = f"https://{WORKSPACE_HOST}/api/2.0/clusters/list"
headers = {"Authorization": f"Bearer {tokens['access_token']}"}

test_response = requests.get(test_url, headers=headers)

if test_response.status_code == 200:
    print("✓ OAuth token is working!")
    clusters = test_response.json()
    cluster_count = len(clusters.get("clusters", []))
    print(f"  Found {cluster_count} cluster(s) in workspace")
else:
    print(f"⚠️  Token test returned status {test_response.status_code}")

# Update .env file
print("\nUpdating .env file...")

env_path = Path(".env")

# Create .env from example if it doesn't exist
if not env_path.exists():
    example_path = Path("env.example")
    if example_path.exists():
        env_path.write_text(example_path.read_text())
    else:
        env_path.touch()

# Update configuration
set_key(env_path, "DATABRICKS_SERVER_HOSTNAME", WORKSPACE_HOST)
set_key(env_path, "DATABRICKS_TOKEN", tokens['access_token'])
set_key(env_path, "DATABRICKS_REFRESH_TOKEN", tokens.get('refresh_token', ''))
set_key(env_path, "SQLPILOT_OAUTH_ENABLED", "true")
set_key(env_path, "SQLPILOT_OAUTH_CLIENT_ID", CLIENT_ID)

print("✓ Configuration saved to .env")

# Clean up
verifier_file.unlink()
state_file = Path(".oauth_state")
if state_file.exists():
    state_file.unlink()

print()
print("="*80)
print("  OAuth Setup Complete! ✓")
print("="*80)
print()
print("Next steps:")
print("  1. Verify: python check_env.py")
print("  2. Test: pytest tests/test_uat_end_to_end.py -v")
print("  3. Validate: python validate_flagship.py")
print()

