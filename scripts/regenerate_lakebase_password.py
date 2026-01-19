#!/usr/bin/env python3
"""
Regenerate Lakebase Password using Service Principal Authentication

This script uses OAuth M2M (machine-to-machine) authentication with
service principal credentials for production use.

Ref: https://docs.databricks.com/aws/en/oltp/instances/authentication

Authentication Methods:
1. Service Principal (RECOMMENDED for production)
   - Uses DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET
   - Long-lived credentials
   - Suitable for automation

2. PAT (For development only)
   - Uses DATABRICKS_TOKEN
   - Expires periodically
   - Not recommended for production
"""

import os
import sys
from datetime import datetime

print('🔄 Lakebase Password Regeneration (Service Principal Auth)')
print('=' * 70)
print()

# Load .env.dev
print('📂 Loading .env.dev...')
env_file = '.env.dev'
env_vars = {}

if os.path.exists(env_file):
    with open(env_file, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                value = value.strip().strip('"').strip("'")
                
                # Skip LAKEBASE_PASSWORD - we'll regenerate it
                if key != 'LAKEBASE_PASSWORD':
                    os.environ[key] = value
                    env_vars[key] = value
                else:
                    print(f'⏭️  Skipping old LAKEBASE_PASSWORD (will regenerate)')
    
    print(f'✅ Loaded {len(env_vars)} variables')
    print()
else:
    print('❌ .env.dev not found')
    sys.exit(1)

# Check configuration
db_host = os.getenv('DATABRICKS_SERVER_HOSTNAME')
client_id = os.getenv('DATABRICKS_CLIENT_ID')
client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
databricks_token = os.getenv('DATABRICKS_TOKEN')
lakebase_host = os.getenv('LAKEBASE_HOST')
lakebase_user = os.getenv('LAKEBASE_USER')

print('📋 Configuration:')
print('-' * 70)
print(f'Databricks Host:   {db_host}')
print(f'Lakebase Host:     {lakebase_host}')
print(f'Lakebase User:     {lakebase_user}')
print()

# Determine authentication method
if client_id and client_secret:
    auth_method = "service_principal"
    print('🔐 Authentication: Service Principal (OAuth M2M)')
    print(f'   Client ID: {client_id}')
    print(f'   ✅ RECOMMENDED for production')
elif databricks_token:
    auth_method = "pat"
    print('🔐 Authentication: Personal Access Token (PAT)')
    print(f'   Token: {databricks_token[:20]}...')
    print(f'   ⚠️  For development only - use service principal in production')
else:
    print('❌ Missing authentication credentials')
    print()
    print('Required (choose one):')
    print()
    print('Option 1 (RECOMMENDED):')
    print('  DATABRICKS_CLIENT_ID=<service_principal_client_id>')
    print('  DATABRICKS_CLIENT_SECRET=<service_principal_secret>')
    print()
    print('Option 2 (Development):')
    print('  DATABRICKS_TOKEN=dapi...')
    print()
    sys.exit(1)

print()

if not all([db_host, lakebase_host, lakebase_user]):
    print('❌ Missing required variables:')
    if not db_host:
        print('  - DATABRICKS_SERVER_HOSTNAME')
    if not lakebase_host:
        print('  - LAKEBASE_HOST')
    if not lakebase_user:
        print('  - LAKEBASE_USER')
    sys.exit(1)

# Extract instance name (use explicit NAME, not ID from host!)
instance_name = os.getenv('LAKEBASE_INSTANCE_NAME')
if not instance_name:
    # Fall back to extracting from host
    instance_name = lakebase_host.split('.')[0]
print(f'📦 Instance: {instance_name}')
print()

# Generate fresh token using SDK
print('🎟️  Generating fresh OAuth token using Databricks SDK...')
print('-' * 70)

try:
    from databricks.sdk import WorkspaceClient
    import uuid
    
    print('✅ Databricks SDK available')
    
    # Initialize client with appropriate auth method
    print(f'🔌 Connecting to: {db_host}')
    if auth_method == "service_principal":
        w = WorkspaceClient(
            host=f"https://{db_host}",
            client_id=client_id,
            client_secret=client_secret
        )
        print('✅ Connected (Service Principal)')
    else:
        w = WorkspaceClient(
            host=f"https://{db_host}",
            token=databricks_token
        )
        print('✅ Connected (PAT)')
    print()
    
    # Generate credentials
    print(f'🎟️  Calling: w.database.generate_database_credential()')
    print(f'   Request ID: {str(uuid.uuid4())[:8]}...')
    print(f'   Instance:   {instance_name}')
    print()
    
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )
    
    new_password = cred.token
    expiration = cred.expiration_time
    
    print('🎉 SUCCESS! Fresh token generated')
    print('=' * 70)
    print(f'New Password: {new_password[:60]}...')
    print(f'Expires:      {expiration}')
    
    if expiration:
        from datetime import timezone
        from dateutil import parser
        # Parse if it's a string
        exp_dt = parser.parse(expiration) if isinstance(expiration, str) else expiration
        ttl = (exp_dt - datetime.now(timezone.utc)).total_seconds()
        print(f'Valid for:    {int(ttl/60)} minutes (~1 hour)')
    print()
    
    # Update .env.dev with new password
    print('💾 Updating .env.dev with new password...')
    
    # Read current file
    with open(env_file, 'r') as f:
        lines = f.readlines()
    
    # Update LAKEBASE_PASSWORD or add it
    updated = False
    new_lines = []
    
    for line in lines:
        if line.strip().startswith('LAKEBASE_PASSWORD='):
            new_lines.append(f'LAKEBASE_PASSWORD="{new_password}"\n')
            updated = True
            print('✅ Updated existing LAKEBASE_PASSWORD')
        else:
            new_lines.append(line)
    
    # If not found, add it
    if not updated:
        # Find where to add it (after LAKEBASE_USER if present)
        for i, line in enumerate(new_lines):
            if line.strip().startswith('LAKEBASE_USER='):
                new_lines.insert(i + 1, f'LAKEBASE_PASSWORD="{new_password}"\n')
                print('✅ Added LAKEBASE_PASSWORD after LAKEBASE_USER')
                updated = True
                break
        
        # If still not added, append at end
        if not updated:
            new_lines.append(f'\nLAKEBASE_PASSWORD="{new_password}"\n')
            print('✅ Added LAKEBASE_PASSWORD at end of file')
    
    # Write back
    with open(env_file, 'w') as f:
        f.writelines(new_lines)
    
    print(f'✅ {env_file} updated')
    print()
    
    print('🎉 COMPLETE!')
    print('=' * 70)
    print()
    print('✅ New OAuth token generated and saved')
    print(f'✅ Valid for ~{int(ttl/60)} minutes')
    print()
    
    if auth_method == "service_principal":
        print('✅ Using Service Principal authentication')
        print('   → Suitable for production use')
        print('   → Long-lived credentials')
        print('   → Automated refresh supported')
    else:
        print('⚠️  Using PAT authentication')
        print('   → For development only')
        print('   → Consider using service principal for production')
    print()
    
    print('🧪 Next steps:')
    print('   1. source .env.dev')
    print('   2. pytest tests/test_lakebase_backend.py -v')
    print('   3. pytest tests/test_ui_to_sql_e2e.py -v')
    print()
    print('🔄 Token will be refreshed automatically by the fallback system')
    print('   when it expires (or run this script again)')
    print()
    
except ImportError:
    print('❌ Databricks SDK not installed')
    print()
    print('Install with:')
    print('  pip install databricks-sdk')
    print()
    sys.exit(1)
    
except AttributeError as e:
    print(f'❌ SDK Error: {e}')
    print()
    if 'database' in str(e):
        print('The database module is not available.')
        print('Your SDK might be too old.')
        print()
        print('Upgrade with:')
        print('  pip install --upgrade databricks-sdk')
    sys.exit(1)
    
except Exception as e:
    error_msg = str(e)
    print(f'❌ Error: {error_msg}')
    print()
    
    if 'Invalid access token' in error_msg or 'authentication' in error_msg.lower():
        if auth_method == "service_principal":
            print('⚠️  Service Principal credentials are invalid')
            print()
            print('Fix:')
            print('  1. Verify DATABRICKS_CLIENT_ID is correct')
            print('  2. Verify DATABRICKS_CLIENT_SECRET is correct')
            print('  3. Ensure service principal has access to the instance')
            print()
            print('How to create service principal:')
            print('  https://docs.databricks.com/en/dev-tools/service-principals.html')
        else:
            print('⚠️  Your DATABRICKS_TOKEN is invalid or expired')
            print()
            print('Fix:')
            print('  1. Go to Databricks workspace')
            print('  2. Settings → Developer → Access Tokens')
            print('  3. Generate New Token')
            print('  4. Update DATABRICKS_TOKEN in .env.dev')
            print('  5. Run this script again')
        
    elif 'RESOURCE_DOES_NOT_EXIST' in error_msg:
        print('⚠️  Instance not found')
        print(f'   Instance: {instance_name}')
        print()
        print('Check:')
        print('  1. Instance name is correct')
        print('  2. Instance exists in your workspace')
        print('  3. You have access to the instance')
        
    elif 'ENDPOINT_NOT_FOUND' in error_msg or '404' in error_msg:
        print('⚠️  API not available on your workspace')
        print()
        print('The database.generate_database_credential() API is not')
        print('available on your workspace yet (Public Preview in select regions).')
        print()
        print('✅ Fallback: Continue using existing static password')
        print('   Your tests will still work with current credentials')
        
    elif 'PERMISSION_DENIED' in error_msg:
        print('⚠️  Permission denied')
        print()
        if auth_method == "service_principal":
            print('Your service principal needs permission to access the database instance.')
        else:
            print('Your token needs permission to access the database instance.')
        print('Contact your Databricks admin for access.')
        
    else:
        print('Check:')
        print('  1. DATABRICKS_SERVER_HOSTNAME is correct')
        if auth_method == "service_principal":
            print('  2. DATABRICKS_CLIENT_ID is valid')
            print('  3. DATABRICKS_CLIENT_SECRET is valid')
        else:
            print('  2. DATABRICKS_TOKEN is valid')
        print('  4. LAKEBASE_HOST is correct')
        print('  5. You have permissions')
    
    sys.exit(1)
