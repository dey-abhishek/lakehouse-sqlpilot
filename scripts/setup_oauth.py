#!/usr/bin/env python3
"""
OAuth Setup for Lakehouse SQLPilot
Interactive script to set up OAuth authentication with Databricks workspace
Based on: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m
"""

import hashlib
import base64
import secrets
import string
import requests
import json
import webbrowser
from pathlib import Path
from dotenv import load_dotenv, set_key
import os

# Configuration
WORKSPACE_HOST = "e2-demo-field-eng.cloud.databricks.com"
WORKSPACE_ID = "1444828305810485"
REDIRECT_URI = "http://localhost:8020"
CLIENT_ID = "databricks-cli"

def print_header(title):
    """Print formatted header"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80 + "\n")

def generate_pkce_pair():
    """Generate PKCE code verifier and challenge"""
    print_header("Step 1: Generating PKCE Code Verifier and Challenge")
    
    # Generate code verifier (43-128 characters)
    allowed_chars = string.ascii_letters + string.digits + "-._~"
    code_verifier = ''.join(secrets.choice(allowed_chars) for _ in range(64))
    
    # Create SHA256 hash and base64-url-encode
    sha256_hash = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(sha256_hash).decode().rstrip("=")
    
    print(f"✓ Code Verifier:  {code_verifier}")
    print(f"✓ Code Challenge: {code_challenge}")
    
    return code_verifier, code_challenge

def get_authorization_code(code_challenge):
    """Get OAuth authorization code from user"""
    print_header("Step 2: Get Authorization Code")
    
    # Generate state for CSRF protection
    state = secrets.token_urlsafe(16)
    
    # Build authorization URL
    auth_url = (
        f"https://{WORKSPACE_HOST}/oidc/v1/authorize"
        f"?client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
        f"&response_type=code"
        f"&state={state}"
        f"&code_challenge={code_challenge}"
        f"&code_challenge_method=S256"
        f"&scope=all-apis+offline_access"
    )
    
    print(f"Workspace: https://{WORKSPACE_HOST}/?o={WORKSPACE_ID}")
    print(f"\nOpening authorization URL in your browser...")
    print(f"URL: {auth_url}\n")
    
    # Try to open in browser
    try:
        webbrowser.open(auth_url)
        print("✓ Browser opened")
    except:
        print("⚠️  Could not open browser automatically")
        print(f"\nPlease open this URL manually:\n{auth_url}\n")
    
    print(f"\nAfter authorization, you'll be redirected to:")
    print(f"{REDIRECT_URI}/?code=...&state=...\n")
    
    # Get authorization code from user
    input("Press Enter after you've been redirected...")
    
    auth_code = input("\nEnter the authorization code (value after 'code='): ").strip()
    returned_state = input("Enter the state value (value after 'state='): ").strip()
    
    # Verify state
    if returned_state != state:
        raise ValueError(
            f"State mismatch! Possible CSRF attack.\n"
            f"Expected: {state}\n"
            f"Received: {returned_state}"
        )
    
    print("✓ State verified - CSRF protection passed")
    
    return auth_code

def exchange_code_for_tokens(auth_code, code_verifier):
    """Exchange authorization code for access and refresh tokens"""
    print_header("Step 3: Exchange Code for Tokens")
    
    token_url = f"https://{WORKSPACE_HOST}/oidc/v1/token"
    
    data = {
        "client_id": CLIENT_ID,
        "grant_type": "authorization_code",
        "scope": "all-apis offline_access",
        "redirect_uri": REDIRECT_URI,
        "code_verifier": code_verifier,
        "code": auth_code
    }
    
    print(f"Requesting tokens from: {token_url}")
    
    response = requests.post(token_url, data=data)
    
    if response.status_code != 200:
        raise Exception(
            f"Token exchange failed!\n"
            f"Status: {response.status_code}\n"
            f"Response: {response.text}"
        )
    
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
    
    return tokens

def test_access_token(access_token):
    """Test the access token by calling a Databricks API"""
    print_header("Step 4: Testing Access Token")
    
    test_url = f"https://{WORKSPACE_HOST}/api/2.0/clusters/list"
    
    print(f"Testing token with: {test_url}")
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    response = requests.get(test_url, headers=headers)
    
    if response.status_code == 200:
        print("\n✓ OAuth token is working!")
        clusters = response.json()
        cluster_count = len(clusters.get("clusters", []))
        print(f"  Found {cluster_count} cluster(s) in workspace")
        return True
    else:
        print(f"\n❌ Token test failed!")
        print(f"Status: {response.status_code}")
        print(f"Response: {response.text}")
        return False

def update_env_file(access_token, refresh_token):
    """Update .env file with OAuth configuration"""
    print_header("Step 5: Updating Configuration")
    
    env_path = Path(".env")
    
    # Create .env from example if it doesn't exist
    if not env_path.exists():
        example_path = Path("env.example")
        if example_path.exists():
            print("Creating .env from env.example...")
            env_path.write_text(example_path.read_text())
        else:
            print("Creating new .env file...")
            env_path.touch()
    
    # Load existing .env
    load_dotenv(env_path)
    
    # Update OAuth configuration
    config_updates = {
        "DATABRICKS_SERVER_HOSTNAME": WORKSPACE_HOST,
        "DATABRICKS_TOKEN": access_token,
        "DATABRICKS_REFRESH_TOKEN": refresh_token,
        "SQLPILOT_OAUTH_ENABLED": "true",
        "SQLPILOT_OAUTH_CLIENT_ID": CLIENT_ID,
    }
    
    print("\nUpdating .env file with OAuth configuration...")
    for key, value in config_updates.items():
        set_key(env_path, key, value)
        print(f"  ✓ {key}")
    
    print(f"\n✓ Configuration saved to {env_path}")

def main():
    """Main OAuth setup flow"""
    print_header("Lakehouse SQLPilot - OAuth Setup")
    
    print(f"This script will set up OAuth authentication for:")
    print(f"  Workspace: https://{WORKSPACE_HOST}/?o={WORKSPACE_ID}")
    print(f"  Redirect URI: {REDIRECT_URI}")
    print(f"\nBased on: https://docs.databricks.com/aws/en/dev-tools/auth/oauth-u2m")
    print(f"\n⚠️  Note: You'll need to authorize SQLPilot to access your Databricks workspace.")
    
    input("\nPress Enter to continue...")
    
    try:
        # Step 1: Generate PKCE pair
        code_verifier, code_challenge = generate_pkce_pair()
        
        # Step 2: Get authorization code
        auth_code = get_authorization_code(code_challenge)
        
        # Step 3: Exchange code for tokens
        tokens = exchange_code_for_tokens(auth_code, code_verifier)
        
        access_token = tokens.get("access_token")
        refresh_token = tokens.get("refresh_token")
        
        if not access_token:
            raise Exception("No access token received!")
        
        # Step 4: Test the token
        if not test_access_token(access_token):
            raise Exception("Token validation failed!")
        
        # Step 5: Update .env file
        update_env_file(access_token, refresh_token)
        
        # Success!
        print_header("OAuth Setup Complete! ✓")
        
        print("Next steps:")
        print("  1. Verify configuration:")
        print("     python check_env.py")
        print("")
        print("  2. Run UAT tests:")
        print("     pytest tests/test_uat_end_to_end.py -v")
        print("")
        print("  3. Validate SCD2 flagship:")
        print("     python validate_flagship.py")
        print("")
        print("  4. Start the application:")
        print("     python api/main.py")
        print("")
        print("⚠️  Note: Access tokens expire after 1 hour.")
        print("   The refresh token can be used to get a new access token.")
        print("   See OAUTH_AUTHENTICATION.md for automatic token refresh setup.")
        print("")
        
    except KeyboardInterrupt:
        print("\n\n❌ Setup cancelled by user")
        return 1
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

