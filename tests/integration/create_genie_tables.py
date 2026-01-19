#!/usr/bin/env python3
"""
Quick script to create Genie Space sample tables
"""

import os
import sys
from pathlib import Path

try:
    from databricks import sql
except ImportError:
    print("❌ Error: databricks-sql-connector not installed")
    print("   Run: pip install databricks-sql-connector")
    sys.exit(1)

def load_env():
    """Load environment variables from .env file"""
    env_file = Path(__file__).parent.parent.parent / '.env'
    
    if not env_file.exists():
        print("❌ Error: .env file not found")
        print(f"   Expected: {env_file}")
        print("   Run: ./setup_env.sh first")
        sys.exit(1)
    
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key] = value.strip()

def create_tables():
    """Create sample tables for Genie Space"""
    
    print("\n" + "="*80)
    print("CREATING GENIE SPACE SAMPLE TABLES")
    print("="*80)
    
    # Load environment
    load_env()
    
    host = os.getenv('DATABRICKS_HOST', '').replace('https://', '')
    token = os.getenv('DATABRICKS_TOKEN')
    warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')
    
    if not all([host, token, warehouse_id]):
        print("❌ Error: Missing environment variables")
        print("   Required: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID")
        sys.exit(1)
    
    print(f"\n📋 Configuration:")
    print(f"   Host: {host}")
    print(f"   Warehouse: {warehouse_id}")
    
    # Read SQL script
    sql_file = Path(__file__).parent / 'setup_genie_sample_tables.sql'
    
    if not sql_file.exists():
        print(f"\n❌ Error: SQL file not found: {sql_file}")
        sys.exit(1)
    
    print(f"\n📄 Reading SQL from: {sql_file.name}")
    
    with open(sql_file, 'r') as f:
        sql_content = f.read()
    
    # Split into statements
    statements = [
        s.strip() 
        for s in sql_content.split(';') 
        if s.strip() and not s.strip().startswith('--')
    ]
    
    print(f"   Found {len(statements)} SQL statements")
    
    # Connect to Databricks
    print(f"\n🔌 Connecting to Databricks...")
    
    try:
        connection = sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token
        )
        print("   ✅ Connected")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        sys.exit(1)
    
    # Execute statements
    print(f"\n⚡ Executing SQL statements...")
    
    cursor = connection.cursor()
    successful = 0
    failed = 0
    
    for i, statement in enumerate(statements, 1):
        # Show progress for long-running statements
        if 'CREATE TABLE' in statement.upper() or 'INSERT INTO' in statement.upper():
            action = 'CREATE' if 'CREATE TABLE' in statement.upper() else 'INSERT'
            table_name = statement.split('`')[-2] if '`' in statement else 'unknown'
            print(f"   [{i}/{len(statements)}] {action} {table_name}...", end=' ')
        else:
            print(f"   [{i}/{len(statements)}] Executing...", end=' ')
        
        try:
            cursor.execute(statement)
            
            # Fetch and display results if available
            if cursor.description:
                results = cursor.fetchall()
                for row in results:
                    print(f"\n      {row}", end='')
            
            print(" ✅")
            successful += 1
            
        except Exception as e:
            print(f" ❌")
            print(f"      Error: {e}")
            failed += 1
    
    cursor.close()
    connection.close()
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"   ✅ Successful: {successful}")
    if failed > 0:
        print(f"   ❌ Failed: {failed}")
    
    if failed == 0:
        print("\n🎉 All sample tables created successfully!")
        print("\n📋 Next Steps:")
        print("   1. Go to Databricks Genie")
        print("   2. Create a new Genie Space")
        print("   3. Add these tables to the space:")
        print("      • lakehouse-sqlpilot.lakehouse-sqlpilot-schema.customers")
        print("      • lakehouse-sqlpilot.lakehouse-sqlpilot-schema.products")
        print("      • lakehouse-sqlpilot.lakehouse-sqlpilot-schema.orders")
        print("      • lakehouse-sqlpilot.lakehouse-sqlpilot-schema.order_items")
        print("      • lakehouse-sqlpilot.lakehouse-sqlpilot-schema.sales_summary")
        print("   4. Copy the Genie Space ID")
        print("   5. Update .env with: DATABRICKS_GENIE_SPACE_ID=<your-space-id>")
        print("\n📖 Full guide: tests/integration/GENIE_SPACE_SETUP.md")
    else:
        print("\n⚠️  Some statements failed. Check errors above.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        create_tables()
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {e}")
        sys.exit(1)


