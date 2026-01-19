#!/usr/bin/env python3
"""
Initialize Lakebase Plan Registry Schema
Standalone script to create the plans table and related objects
"""

import os
import sys
import psycopg2
from pathlib import Path

def load_env():
    """Load environment variables from .env.dev"""
    env_file = Path(__file__).parent.parent.parent / '.env.dev'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes
                    value = value.strip('"').strip("'")
                    os.environ[key] = value

def get_connection_params():
    """Get database connection parameters from environment"""
    return {
        'host': os.getenv('LAKEBASE_HOST'),
        'port': int(os.getenv('LAKEBASE_PORT', '5432')),
        'database': os.getenv('LAKEBASE_DATABASE', 'databricks_postgres'),
        'user': os.getenv('LAKEBASE_USER'),
        'password': os.getenv('LAKEBASE_PASSWORD'),
        'sslmode': 'require'
    }

def verify_connection():
    """Test database connection"""
    params = get_connection_params()
    
    print("\n🔍 Verifying database connection...")
    print(f"   Host: {params['host']}")
    print(f"   Database: {params['database']}")
    print(f"   User: {params['user']}")
    
    if not all([params['host'], params['user'], params['password']]):
        print("\n❌ ERROR: Missing Lakebase credentials!")
        print("   Please ensure these are set in .env.dev:")
        print("   - LAKEBASE_HOST")
        print("   - LAKEBASE_USER")
        print("   - LAKEBASE_PASSWORD")
        return False
    
    try:
        conn = psycopg2.connect(**params)
        conn.close()
        print("   ✅ Connection successful!\n")
        return True
    except Exception as e:
        print(f"\n❌ ERROR: Connection failed: {e}\n")
        return False

def create_schema():
    """Create the plans table and related objects"""
    params = get_connection_params()
    
    # Read SQL schema file
    schema_file = Path(__file__).parent / 'schema' / 'lakebase_plans.sql'
    if not schema_file.exists():
        print(f"❌ ERROR: Schema file not found: {schema_file}")
        return False
    
    with open(schema_file) as f:
        schema_sql = f.read()
    
    try:
        print("📝 Creating schema...")
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        # Execute schema creation
        cursor.execute(schema_sql)
        conn.commit()
        
        # Verify table was created
        cursor.execute("""
            SELECT 
                table_name,
                table_type
            FROM information_schema.tables 
            WHERE table_name = 'plans'
        """)
        result = cursor.fetchone()
        
        if result:
            print(f"   ✅ Table 'plans' created successfully!")
            
            # Check indexes
            cursor.execute("""
                SELECT COUNT(*) 
                FROM pg_indexes 
                WHERE tablename = 'plans'
            """)
            index_count = cursor.fetchone()[0]
            print(f"   ✅ Created {index_count} indexes")
            
            # Check triggers
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.triggers 
                WHERE event_object_table = 'plans'
            """)
            trigger_count = cursor.fetchone()[0]
            print(f"   ✅ Created {trigger_count} trigger(s)")
            
            # Check views
            cursor.execute("""
                SELECT COUNT(*) 
                FROM information_schema.views 
                WHERE table_name LIKE 'v_plan%'
            """)
            view_count = cursor.fetchone()[0]
            print(f"   ✅ Created {view_count} view(s)")
        else:
            print("   ⚠️  Table creation status unknown")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: Schema creation failed: {e}\n")
        return False

def verify_schema():
    """Verify schema is working correctly"""
    params = get_connection_params()
    
    print("\n🔍 Verifying schema...")
    
    try:
        conn = psycopg2.connect(**params)
        cursor = conn.cursor()
        
        # Check table structure
        cursor.execute("""
            SELECT 
                column_name,
                data_type,
                is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'plans'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        
        print(f"   ✅ Table has {len(columns)} columns")
        
        # Test insert (will rollback)
        cursor.execute("""
            INSERT INTO plans 
            (plan_id, plan_name, owner, pattern_type, plan_json, status)
            VALUES (
                gen_random_uuid(),
                'test_verification',
                'test@example.com',
                'TEST',
                '{"test": true}'::jsonb,
                'draft'
            )
            RETURNING plan_id
        """)
        test_id = cursor.fetchone()[0]
        print(f"   ✅ Test insert successful (ID: {test_id})")
        
        # Rollback test insert
        conn.rollback()
        print(f"   ✅ Test data rolled back")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: Schema verification failed: {e}\n")
        return False

def show_summary():
    """Show summary and next steps"""
    print("\n" + "="*70)
    print("✅ Plan Registry Schema Initialized Successfully!")
    print("="*70)
    print("\n📊 What was created:")
    print("   • plans table (main storage)")
    print("   • Indexes for fast queries (owner, pattern_type, status, etc.)")
    print("   • JSONB GIN indexes for flexible JSON queries")
    print("   • Auto-update trigger for updated_at timestamp")
    print("   • Helper views (v_active_plans, v_plan_stats_*)")
    
    print("\n🚀 Next Steps:")
    print("   1. Restart your backend:")
    print("      uvicorn api.main:app --host 0.0.0.0 --port 8001 --reload")
    print("\n   2. Test plan save in UI:")
    print("      http://localhost:5173")
    print("\n   3. Query plans in psql:")
    print("      SELECT * FROM v_active_plans;")
    
    print("\n📝 Useful Queries:")
    print("   • List all plans:        SELECT plan_name, owner, status FROM plans;")
    print("   • Active plans:          SELECT * FROM v_active_plans;")
    print("   • Stats by owner:        SELECT * FROM v_plan_stats_by_owner;")
    print("   • Stats by pattern:      SELECT * FROM v_plan_stats_by_pattern;")
    print("\n" + "="*70 + "\n")

def main():
    """Main execution"""
    print("\n" + "="*70)
    print("🗄️  Lakehouse SQLPilot - Plan Registry Schema Initialization")
    print("="*70)
    
    # Load environment
    print("\n📁 Loading environment variables...")
    load_env()
    
    # Verify connection
    if not verify_connection():
        print("\n💡 TIP: Generate a fresh Lakebase password:")
        print("   curl -X POST \\")
        print("     https://e2-demo-field-eng.cloud.databricks.com/api/2.0/database/generate-credential \\")
        print("     -H 'Authorization: Bearer $DATABRICKS_TOKEN' \\")
        print("     -H 'Content-Type: application/json' \\")
        print("     -d '{\"database_name\": \"databricks_postgres\"}' | jq -r '.password'")
        print("\n   Then add to .env.dev: LAKEBASE_PASSWORD=\"<password>\"")
        sys.exit(1)
    
    # Create schema
    if not create_schema():
        sys.exit(1)
    
    # Verify schema
    if not verify_schema():
        print("⚠️  Schema created but verification had issues")
        sys.exit(1)
    
    # Show summary
    show_summary()

if __name__ == '__main__':
    main()


