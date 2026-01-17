#!/usr/bin/env python3
"""
Check Environment Configuration
Validates that configuration is properly set up for running tests and the application.
Supports both .env files and secrets manager backends.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import sys

# Try to import secrets manager from new location
try:
    scripts_security_path = Path(__file__).parent.parent.parent / "scripts" / "security"
    sys.path.insert(0, str(scripts_security_path))
    from secrets_manager import get_secret, get_secrets_manager
    secrets_available = True
except ImportError:
    secrets_available = False
    def get_secret(key, default=None):
        return os.getenv(key, default)


def check_secrets_backend():
    """Check if secrets manager is configured"""
    if not secrets_available:
        print("\n⚠️  Secrets manager not available (using environment variables only)")
        return None
    
    backend_type = os.getenv("SQLPILOT_SECRETS_BACKEND", "env")
    print(f"\n✓ Secrets backend: {backend_type}")
    
    if backend_type != "env":
        print(f"  Using {backend_type} for secure credential storage")
        return backend_type
    return None

def check_env_configuration():
    """Check if .env file exists and is properly configured"""
    
    # Load .env file
    env_path = Path(__file__).parent / '.env'
    env_example_path = Path(__file__).parent / 'env.example'
    
    print("="*80)
    print("  SQLPilot Environment Configuration Check")
    print("="*80)
    
    # Check secrets backend
    secrets_backend = check_secrets_backend()
    
    # Check if .env exists
    if not env_path.exists():
        print("\n❌ ERROR: .env file not found!")
        print(f"   Expected location: {env_path}")
        
        if env_example_path.exists():
            print(f"\n   Copy the example file to create your .env:")
            print(f"   cp env.example .env")
        
        print("\n   Then edit .env with your Databricks credentials.")
        return False
    
    print(f"\n✓ Found .env file: {env_path}")
    
    # Load environment variables
    load_dotenv(env_path)
    
    # Required variables
    required_vars = {
        'DATABRICKS_SERVER_HOSTNAME': 'Databricks workspace hostname',
        'DATABRICKS_WAREHOUSE_ID': 'SQL Warehouse ID',
        'DATABRICKS_CATALOG': 'Unity Catalog name',
        'DATABRICKS_SCHEMA': 'Schema name',
    }
    
    # Authentication (at least one required)
    auth_vars = {
        'DATABRICKS_TOKEN': 'Personal Access Token',
        'SQLPILOT_OAUTH_CLIENT_ID': 'OAuth Client ID',
    }
    
    # Optional but recommended
    optional_vars = {
        'SQLPILOT_SECRET_KEY': 'JWT secret key',
        'SQLPILOT_OAUTH_ENABLED': 'OAuth enabled flag',
    }
    
    print("\n" + "="*80)
    print("  Required Configuration")
    print("="*80)
    
    all_required_present = True
    for var, description in required_vars.items():
        # Try secrets manager first, then environment
        value = get_secret(var) or os.getenv(var)
        if value and not value.startswith('your-'):
            print(f"✓ {var}: {description}")
            print(f"  Value: {value[:30]}{'...' if len(value) > 30 else ''}")
            if secrets_backend and get_secret(var):
                print(f"  Source: secrets manager ({secrets_backend})")
        else:
            print(f"❌ {var}: {description}")
            print(f"   NOT CONFIGURED or using placeholder value")
            all_required_present = False
    
    print("\n" + "="*80)
    print("  Authentication Configuration")
    print("="*80)
    
    auth_configured = False
    for var, description in auth_vars.items():
        value = get_secret(var) or os.getenv(var)
        if value and not value.startswith('your-'):
            print(f"✓ {var}: {description}")
            print(f"  Configured: {'*' * min(len(value), 20)}")
            if secrets_backend and get_secret(var):
                print(f"  Source: secrets manager ({secrets_backend})")
            auth_configured = True
        else:
            print(f"⚠️  {var}: {description}")
            print(f"   Not configured")
    
    if not auth_configured:
        print("\n❌ ERROR: No authentication method configured!")
        print("   Configure either:")
        print("   1. DATABRICKS_TOKEN (for PAT authentication)")
        print("   2. SQLPILOT_OAUTH_CLIENT_ID + SQLPILOT_OAUTH_CLIENT_SECRET (for OAuth)")
    
    print("\n" + "="*80)
    print("  Optional Configuration")
    print("="*80)
    
    for var, description in optional_vars.items():
        value = os.getenv(var)
        if value and not value.startswith('your-') and not value.startswith('change-me'):
            print(f"✓ {var}: {description}")
        else:
            print(f"⚠️  {var}: {description} (using default)")
    
    print("\n" + "="*80)
    print("  Configuration Summary")
    print("="*80)
    
    if all_required_present and auth_configured:
        print("\n✅ Configuration is VALID!")
        print("   You can now run tests and start the application.")
        
        if secrets_backend:
            print(f"\n   Using secure secrets backend: {secrets_backend}")
            print("   See SECRETS_MANAGEMENT.md for details")
        
        print("\n   Run UAT tests:")
        print("   pytest tests/test_uat_end_to_end.py -v")
        print("\n   Validate SCD2 flagship:")
        print("   python validate_flagship.py")
        print("\n   Start the application:")
        print("   python api/main.py")
        return True
    else:
        print("\n❌ Configuration is INCOMPLETE!")
        if secrets_backend:
            print(f"   Please configure secrets in {secrets_backend} backend")
            print("   See SECRETS_MANAGEMENT.md for setup instructions")
        else:
            print("   Please edit .env and configure the missing values.")
            print("   Or use a secrets backend (see SECRETS_MANAGEMENT.md)")
        
        if not all_required_present:
            print("\n   Missing required configuration:")
            for var, desc in required_vars.items():
                value = os.getenv(var)
                if not value or value.startswith('your-'):
                    print(f"   - {var}")
        
        if not auth_configured:
            print("\n   Missing authentication configuration:")
            print("   - Configure either DATABRICKS_TOKEN or OAuth credentials")
        
        return False


if __name__ == '__main__':
    success = check_env_configuration()
    exit(0 if success else 1)

