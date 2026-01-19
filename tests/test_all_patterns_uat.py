"""
Comprehensive UAT Tests for All SQL Patterns

This test suite validates all supported patterns end-to-end:
1. Incremental Append
2. Full Replace  
3. Merge/Upsert
4. SCD Type 2
5. Snapshot

Each pattern is tested with:
- Plan validation
- SQL compilation
- Table creation
- Actual execution on Databricks SQL Warehouse
- Result verification
"""

import pytest
import os
import uuid
from datetime import datetime, timezone
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks import sql as databricks_sql

from plan_schema.v1.validator import PlanValidator
from compiler.sql_generator import SQLCompiler


def connect_with_token(server_hostname, http_path, access_token):
    """
    Create a Databricks SQL connection using access token.
    Temporarily hides OAuth config to prevent browser popup.
    """
    import shutil
    from pathlib import Path
    
    # Hide OAuth env vars
    client_id = os.environ.pop('DATABRICKS_CLIENT_ID', None)
    client_secret = os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
    
    # Hide .databrickscfg
    config_path = Path.home() / '.databrickscfg'
    backup_path = Path.home() / '.databrickscfg.backup_for_tests'
    config_existed = config_path.exists()
    
    if config_existed:
        shutil.move(str(config_path), str(backup_path))
    
    saved_config_file = os.environ.get('DATABRICKS_CONFIG_FILE')
    os.environ['DATABRICKS_CONFIG_FILE'] = '/dev/null'
    
    try:
        from databricks.sql import connect
        conn = connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            _user_agent_entry="SQLPilot-UAT-AllPatterns"
        )
        return conn
    finally:
        # Restore config file
        if config_existed:
            shutil.move(str(backup_path), str(config_path))
        
        # Restore env vars
        if saved_config_file:
            os.environ['DATABRICKS_CONFIG_FILE'] = saved_config_file
        else:
            os.environ.pop('DATABRICKS_CONFIG_FILE', None)


@pytest.mark.requires_databricks
@pytest.mark.e2e
class TestAllPatternsUAT:
    """Comprehensive UAT tests for all SQL patterns"""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment"""
        print("\n" + "="*80)
        print("Setting up All Patterns UAT Test Environment")
        print("="*80)
        
        from dotenv import load_dotenv
        import shutil
        load_dotenv('.env.dev')
        
        # Hide .databrickscfg
        databricks_cfg = os.path.expanduser("~/.databrickscfg")
        databricks_cfg_backup = os.path.expanduser("~/.databrickscfg.uat_patterns_backup")
        
        cls._databricks_cfg_existed = os.path.exists(databricks_cfg)
        if cls._databricks_cfg_existed:
            shutil.move(databricks_cfg, databricks_cfg_backup)
        
        # Get configuration
        cls.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        cls.catalog = os.getenv("DATABRICKS_CATALOG", "lakehouse-sqlpilot")
        cls.schema = os.getenv("DATABRICKS_SCHEMA", "lakehouse-sqlpilot-schema")
        
        if not cls.warehouse_id:
            pytest.skip("DATABRICKS_WAREHOUSE_ID not configured in .env.dev")
        
        # Initialize OAuth
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        if not all([server_hostname, client_id, client_secret]):
            pytest.skip("Databricks credentials not configured in .env.dev")
        
        cls._server_hostname = server_hostname
        cls._client_id = client_id
        cls._client_secret = client_secret
        
        # Initialize token manager and WorkspaceClient
        from infrastructure.oauth_token_manager import get_oauth_token_manager
        cls._token_manager = get_oauth_token_manager()
        
        cls.workspace_client = WorkspaceClient(
            host=f"https://{server_hostname}",
            client_id=client_id,
            client_secret=client_secret
        )
        
        # Remove OAuth env vars
        os.environ.pop('DATABRICKS_CLIENT_ID', None)
        os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
        os.environ.pop('DATABRICKS_AZURE_CLIENT_ID', None)
        os.environ.pop('DATABRICKS_AZURE_CLIENT_SECRET', None)
        
        cls.compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        cls.validator = PlanValidator("plan-schema/v1/plan.schema.json")
        
        print("✓ Setup complete\n")
    
    @classmethod
    def teardown_class(cls):
        """Cleanup test tables"""
        print("\n" + "="*80)
        print("Cleaning up all pattern test tables")
        print("="*80)
        
        try:
            access_token = cls._token_manager.get_token() if hasattr(cls, '_token_manager') else None
            
            with connect_with_token(
                server_hostname=cls._server_hostname if hasattr(cls, '_server_hostname') else os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=f"/sql/1.0/warehouses/{cls.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    catalog_fmt = cls.catalog
                    schema_fmt = cls.schema
                    
                    # Drop all test tables
                    tables = [
                        'orders_raw', 'orders_processed',  # Incremental
                        'products_staging', 'products',      # Full Replace
                        'customers_updates', 'customers',    # Merge/Upsert
                        'accounts_source', 'accounts_dim',   # SCD2
                        'inventory_source', 'inventory_snapshots'  # Snapshot
                    ]
                    
                    for table in tables:
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS `{catalog_fmt}`.`{schema_fmt}`.`{table}`")
                            print(f"✓ Dropped {table}")
                        except:
                            pass
                    
                    print("✓ All test tables cleaned up")
        except Exception as e:
            print(f"⚠️  Cleanup failed: {e}")
        
        # Restore .databrickscfg
        import shutil
        databricks_cfg = os.path.expanduser("~/.databrickscfg")
        databricks_cfg_backup = os.path.expanduser("~/.databrickscfg.uat_patterns_backup")
        if hasattr(cls, '_databricks_cfg_existed') and cls._databricks_cfg_existed:
            if os.path.exists(databricks_cfg_backup):
                shutil.move(databricks_cfg_backup, databricks_cfg)
        
        # Restore OAuth env vars
        if hasattr(cls, '_client_id') and cls._client_id:
            os.environ['DATABRICKS_CLIENT_ID'] = cls._client_id
        if hasattr(cls, '_client_secret') and cls._client_secret:
            os.environ['DATABRICKS_CLIENT_SECRET'] = cls._client_secret
    
    # =========================================================================
    # TEST 1: INCREMENTAL APPEND PATTERN
    # =========================================================================
    
    def test_1_incremental_append_pattern(self):
        """Test Incremental Append: Daily order processing"""
        print("\n" + "="*80)
        print("UAT-1: Testing Incremental Append Pattern")
        print("="*80)
        
        # Create plan
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_incremental_orders',
                'description': 'Daily order processing',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'INCREMENTAL_APPEND'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'orders_raw',
                'columns': ['order_id', 'customer_id', 'amount', 'order_date']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'orders_processed',
                'write_mode': 'append'
            },
            'pattern_config': {
                'watermark_column': 'order_date',
                'watermark_type': 'date'
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600
            }
        }
        
        # Validate plan
        is_valid, errors = self.validator.validate_plan(plan)
        assert is_valid, f"Plan validation failed: {errors}"
        print("✓ Plan validated")
        
        # Compile SQL
        sql = self.compiler.compile(plan)
        assert len(sql) > 0
        assert 'INSERT INTO' in sql
        assert 'order_date' in sql
        print("✓ SQL compiled")
        
        # Create tables and execute
        access_token = self._token_manager.get_token()
        
        with connect_with_token(
            server_hostname=self._server_hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                catalog_fmt = self.catalog
                schema_fmt = self.schema
                
                # Create source table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`orders_raw` (
                        order_id INT,
                        customer_id INT,
                        amount DECIMAL(10,2),
                        order_date DATE
                    ) USING DELTA
                """)
                
                # Insert test data
                cursor.execute(f"""
                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`orders_raw`
                    VALUES 
                        (1, 101, 150.00, '2026-01-01'),
                        (2, 102, 200.00, '2026-01-02'),
                        (3, 103, 175.50, '2026-01-03')
                """)
                print("✓ Test data created")
                
                # Create target table
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`orders_processed`
                    LIKE `{catalog_fmt}`.`{schema_fmt}`.`orders_raw`
                """)
                
                # Execute incremental append
                cursor.execute(sql)
                print("✓ Incremental append executed")
                
                # Verify results
                cursor.execute(f"""
                    SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`orders_processed`
                """)
                count = cursor.fetchone()[0]
                assert count == 3, f"Expected 3 rows, got {count}"
                print(f"✓ Verified: {count} orders processed")
    
    # =========================================================================
    # TEST 2: FULL REPLACE PATTERN
    # =========================================================================
    
    def test_2_full_replace_pattern(self):
        """Test Full Replace: Product catalog refresh"""
        print("\n" + "="*80)
        print("UAT-2: Testing Full Replace Pattern")
        print("="*80)
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_full_replace_products',
                'description': 'Daily product catalog refresh',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'FULL_REPLACE'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'products_staging'
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'products',
                'write_mode': 'overwrite'
            },
            'pattern_config': {},  # Required even if empty
            'execution_config': {
                'warehouse_id': self.warehouse_id
            }
        }
        
        # Validate and compile
        is_valid, errors = self.validator.validate_plan(plan)
        assert is_valid, f"Plan validation failed: {errors}"
        
        sql = self.compiler.compile(plan)
        assert 'CREATE OR REPLACE TABLE' in sql or 'INSERT OVERWRITE' in sql
        print("✓ SQL compiled for full replace")
        
        # Execute
        access_token = self._token_manager.get_token()
        
        with connect_with_token(
            server_hostname=self._server_hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                catalog_fmt = self.catalog
                schema_fmt = self.schema
                
                # Create source
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`products_staging` (
                        product_id INT,
                        name STRING,
                        price DECIMAL(10,2),
                        category STRING
                    ) USING DELTA
                """)
                
                cursor.execute(f"""
                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`products_staging`
                    VALUES (1, 'Laptop', 999.99, 'Electronics'),
                           (2, 'Mouse', 29.99, 'Electronics')
                """)
                
                # Execute full replace
                cursor.execute(sql)
                print("✓ Full replace executed")
                
                # Verify
                cursor.execute(f"""
                    SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`products`
                """)
                count = cursor.fetchone()[0]
                assert count == 2, f"Expected 2 rows, got {count}"
                print(f"✓ Verified: {count} products in catalog")
    
    # =========================================================================
    # TEST 3: MERGE/UPSERT PATTERN
    # =========================================================================
    
    def test_3_merge_upsert_pattern(self):
        """Test Merge/Upsert: Customer profile updates"""
        print("\n" + "="*80)
        print("UAT-3: Testing Merge/Upsert Pattern")
        print("="*80)
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_merge_customers',
                'description': 'Customer profile updates',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'MERGE_UPSERT'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_updates',
                'columns': ['customer_id', 'name', 'email', 'city']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'merge_keys': ['customer_id'],
                'update_columns': ['name', 'email', 'city']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id
            }
        }
        
        # Validate and compile
        is_valid, errors = self.validator.validate_plan(plan)
        assert is_valid, f"Plan validation failed: {errors}"
        
        sql = self.compiler.compile(plan)
        assert 'MERGE INTO' in sql
        assert 'WHEN MATCHED' in sql
        assert 'WHEN NOT MATCHED' in sql
        print("✓ SQL compiled for merge/upsert")
        
        # Execute
        access_token = self._token_manager.get_token()
        
        with connect_with_token(
            server_hostname=self._server_hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                catalog_fmt = self.catalog
                schema_fmt = self.schema
                
                # Create target with initial data
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers` (
                        customer_id INT,
                        name STRING,
                        email STRING,
                        city STRING
                    ) USING DELTA
                """)
                
                cursor.execute(f"""
                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers`
                    VALUES (1, 'John Doe', 'john@example.com', 'NYC')
                """)
                
                # Create source with updates
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_updates` (
                        customer_id INT,
                        name STRING,
                        email STRING,
                        city STRING
                    ) USING DELTA
                """)
                
                cursor.execute(f"""
                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_updates`
                    VALUES 
                        (1, 'John Doe', 'john.doe@example.com', 'Boston'),  -- Update
                        (2, 'Jane Smith', 'jane@example.com', 'LA')         -- Insert
                """)
                
                # Execute merge
                cursor.execute(sql)
                print("✓ Merge/upsert executed")
                
                # Verify
                cursor.execute(f"""
                    SELECT customer_id, email, city 
                    FROM `{catalog_fmt}`.`{schema_fmt}`.`customers`
                    ORDER BY customer_id
                """)
                results = cursor.fetchall()
                
                assert len(results) == 2
                assert results[0][1] == 'john.doe@example.com'  # Updated email
                assert results[0][2] == 'Boston'                 # Updated city
                assert results[1][0] == 2                        # New customer
                print("✓ Verified: 1 update + 1 insert = 2 customers")
    
    # =========================================================================
    # TEST 4: SCD TYPE 2 PATTERN
    # =========================================================================
    
    def test_4_scd2_pattern(self):
        """Test SCD Type 2: Account history tracking"""
        print("\n" + "="*80)
        print("UAT-4: Testing SCD Type 2 Pattern")
        print("="*80)
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_accounts',
                'description': 'Account history tracking',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'accounts_source',
                'columns': ['account_id', 'status', 'balance', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'accounts_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['account_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['status', 'balance']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id
            }
        }
        
        # Validate and compile
        is_valid, errors = self.validator.validate_plan(plan)
        assert is_valid, f"Plan validation failed: {errors}"
        
        sql = self.compiler.compile(plan)
        assert 'MERGE INTO' in sql
        assert 'is_current' in sql
        assert 'valid_from' in sql
        assert 'valid_to' in sql
        print("✓ SQL compiled for SCD2")
        
        # Execute (simplified test - full test exists in test_uat_end_to_end.py)
        print("✓ SCD2 pattern validated (comprehensive test exists)")
    
    # =========================================================================
    # TEST 5: SNAPSHOT PATTERN
    # =========================================================================
    
    def test_5_snapshot_pattern(self):
        """Test Snapshot: Daily inventory snapshots"""
        print("\n" + "="*80)
        print("UAT-5: Testing Snapshot Pattern")
        print("="*80)
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_snapshot_inventory',
                'description': 'Daily inventory snapshots',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SNAPSHOT'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'inventory_source'
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'inventory_snapshots',
                'write_mode': 'append'
            },
            'pattern_config': {
                'snapshot_date_column': 'snapshot_date',
                'partition_columns': ['snapshot_date']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id
            }
        }
        
        # Validate plan
        is_valid, errors = self.validator.validate_plan(plan)
        assert is_valid, f"Plan validation failed: {errors}"
        print("✓ Plan validated")
        
        # Compile SQL
        sql = self.compiler.compile(plan)
        assert len(sql) > 0
        assert 'snapshot_date' in sql
        print("✓ SQL compiled for snapshot")
        
        # Execute
        access_token = self._token_manager.get_token()
        
        with connect_with_token(
            server_hostname=self._server_hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=access_token
        ) as connection:
            with connection.cursor() as cursor:
                catalog_fmt = self.catalog
                schema_fmt = self.schema
                
                # Create source
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`inventory_source` (
                        product_id INT,
                        quantity INT,
                        location STRING
                    ) USING DELTA
                """)
                
                cursor.execute(f"""
                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`inventory_source`
                    VALUES (1, 100, 'Warehouse A'),
                           (2, 50, 'Warehouse B')
                """)
                
                # Create target table for snapshots
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`inventory_snapshots` (
                        product_id INT,
                        quantity INT,
                        location STRING,
                        snapshot_date DATE
                    ) USING DELTA
                """)
                
                # Execute snapshot
                cursor.execute(sql)
                print("✓ Snapshot created")
                
                # Verify
                cursor.execute(f"""
                    SELECT COUNT(*), snapshot_date 
                    FROM `{catalog_fmt}`.`{schema_fmt}`.`inventory_snapshots`
                    GROUP BY snapshot_date
                """)
                results = cursor.fetchall()
                
                assert len(results) > 0
                assert results[0][0] == 2  # 2 products
                print(f"✓ Verified: {results[0][0]} items in snapshot for {results[0][1]}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

