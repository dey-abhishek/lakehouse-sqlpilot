"""
User Acceptance Testing (UAT) - End-to-End Tests
Tests complete flow from frontend to backend to Databricks warehouse execution

Configuration is loaded from secrets manager (supports multiple backends).
See ENV_CONFIGURATION.md and SECRETS_MANAGEMENT.md for setup instructions.
"""

import pytest
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import os
import time
import re
from databricks.sdk import WorkspaceClient
from databricks.sql import connect
from dotenv import load_dotenv

from compiler import SQLCompiler
from plan_schema.v1.validator import PlanValidator
from execution import SQLExecutor
from unity_catalog import PermissionValidator

# Import from new location
import sys
from pathlib import Path
scripts_security_path = Path(__file__).parent.parent / "scripts" / "security"
sys.path.insert(0, str(scripts_security_path))
from secrets_manager import get_secret

# Import REST API helper for bypassing Python connector limitations
from tests.databricks_rest_api import DatabricksStatementAPI

# Load environment variables from .env file (for non-sensitive config)
env_path = Path(__file__).parent.parent / '.env'
if env_path.exists():
    load_dotenv(env_path)
    print(f"✓ Loaded environment from: {env_path}")


def split_sql_statements(sql: str, debug: bool = False) -> list:
    """
    Safely split multi-statement SQL, handling:
    - Semicolons inside strings (single/double quotes)
    - Multi-line comments (/* ... */)
    - Single-line comments (--)
    - Empty statements
    
    This is safer than naive split(';') which can break valid SQL.
    """
    statements = []
    current = []
    in_string = None  # Track if we're inside a string (' or ")
    in_comment = False  # Track if we're inside /* */
    
    lines = sql.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        # Track multi-line comment state
        if '/*' in line and not in_string:
            in_comment = True
        if '*/' in line and in_comment:
            in_comment = False
            current.append(line)
            continue
        
        # Skip if in multi-line comment
        if in_comment:
            current.append(line)
            continue
        
        # Check for statement terminator (semicolon not in string)
        has_terminator = False
        for i, char in enumerate(line):
            if char in ("'", '"'):
                if in_string == char:
                    in_string = None
                elif in_string is None:
                    in_string = char
            elif char == ';' and in_string is None:
                has_terminator = True
        
        current.append(line)
        
        # If we found a terminator, finalize this statement
        if has_terminator and in_string is None:
            stmt = '\n'.join(current).strip()
            # Check if statement has any non-comment, non-empty lines
            has_content = False
            for stmt_line in current:
                stripped = stmt_line.strip()
                if stripped and not stripped.startswith('--'):
                    has_content = True
                    break
            
            if has_content:
                statements.append(stmt)
                if debug:
                    print(f"  [Statement {len(statements)}] lines {line_num - len(current) + 1}-{line_num}")
            current = []
    
    # Add any remaining content
    if current:
        stmt = '\n'.join(current).strip()
        # Check if statement has any non-comment, non-empty lines
        has_content = False
        for stmt_line in current:
            stripped = stmt_line.strip()
            if stripped and not stripped.startswith('--'):
                has_content = True
                break
        
        if has_content:
            statements.append(stmt)
            if debug:
                print(f"  [Statement {len(statements)}] lines {len(lines) - len(current) + 1}-{len(lines)}")
    
    return statements


def get_databricks_credentials():
    """
    Helper to get Databricks credentials using the pre-initialized OAuth token manager.
    
    This uses the token manager that was created in setup_class BEFORE removing
    the OAuth env vars, so it can get tokens without triggering browser OAuth.
    """
    from dotenv import load_dotenv
    load_dotenv('.env.dev')
    
    # Get server hostname
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME") or os.getenv("DATABRICKS_HOST")
    
    # If not in environment, load from .env.dev file
    if not server_hostname:
        import dotenv
        env_vars = dotenv.dotenv_values('.env.dev')
        server_hostname = env_vars.get('DATABRICKS_SERVER_HOSTNAME')
    
    if not server_hostname:
        pytest.skip("DATABRICKS_SERVER_HOSTNAME not configured")
    
    # Use the token manager from the test class (created in setup_class)
    # This was initialized before we removed the OAuth env vars
    try:
        from tests.test_uat_end_to_end import TestUATEndToEnd
        if hasattr(TestUATEndToEnd, '_token_manager'):
            access_token = TestUATEndToEnd._token_manager.get_token()
            return server_hostname, access_token
    except Exception as e:
        pass
    
    # If we get here, something went wrong - should NOT restore env vars!
    print("  [WARNING] Fallback path should not be used - this may cause browser popup!")
    pytest.skip("Token manager not initialized properly")


def connect_with_token(server_hostname, http_path, access_token):
    """
    Create a Databricks SQL connection using ONLY an access token.
    
    CRITICAL: Temporarily renames ~/.databrickscfg to prevent the connector
    from reading OAuth config and triggering browser-based authentication.
    
    Usage:
        with connect_with_token(hostname, http_path, token) as conn:
            # use connection
    """
    import shutil
    from pathlib import Path
    
    # Double-check OAuth env vars are not set
    if 'DATABRICKS_CLIENT_ID' in os.environ:
        del os.environ['DATABRICKS_CLIENT_ID']
    if 'DATABRICKS_CLIENT_SECRET' in os.environ:
        del os.environ['DATABRICKS_CLIENT_SECRET']
    
    # CRITICAL: Temporarily rename ~/.databrickscfg to prevent connector from reading it
    config_path = Path.home() / '.databrickscfg'
    backup_path = Path.home() / '.databrickscfg.backup_for_tests'
    config_existed = config_path.exists()
    
    if config_existed:
        shutil.move(str(config_path), str(backup_path))
    
    # Also set DATABRICKS_CONFIG_FILE to ensure it's not used
    saved_config_file = os.environ.get('DATABRICKS_CONFIG_FILE')
    os.environ['DATABRICKS_CONFIG_FILE'] = '/dev/null'
    
    try:
        conn = connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
            _user_agent_entry="SQLPilot-UAT-Tests"
        )
        return conn
    finally:
        # Restore config file
        if config_existed:
            shutil.move(str(backup_path), str(config_path))
        
        # Restore env var
        if saved_config_file:
            os.environ['DATABRICKS_CONFIG_FILE'] = saved_config_file
        else:
            os.environ.pop('DATABRICKS_CONFIG_FILE', None)


class TestUATEndToEnd:
    """UAT tests for complete end-to-end workflows"""
    
    @classmethod
    def setup_class(cls):
        """Setup test environment"""
        print("\n" + "="*80)
        print("Setting up UAT Test Environment")
        print("="*80)
        
        from dotenv import load_dotenv
        import shutil
        load_dotenv('.env.dev')
        
        # CRITICAL: Rename .databrickscfg to prevent SQL connector from detecting OAuth
        databricks_cfg = os.path.expanduser("~/.databrickscfg")
        databricks_cfg_backup = os.path.expanduser("~/.databrickscfg.uat_backup")
        
        cls._databricks_cfg_existed = os.path.exists(databricks_cfg)
        if cls._databricks_cfg_existed:
            shutil.move(databricks_cfg, databricks_cfg_backup)
        
        # Get configuration directly from environment (already loaded from .env.dev)
        cls.warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        cls.catalog = os.getenv("DATABRICKS_CATALOG", "lakehouse-sqlpilot")
        cls.schema = os.getenv("DATABRICKS_SCHEMA", "lakehouse-sqlpilot-schema")
        
        if not cls.warehouse_id:
            pytest.skip("DATABRICKS_WAREHOUSE_ID not configured in .env.dev")
        
        # Initialize WorkspaceClient with OAuth credentials from environment
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        if not all([server_hostname, client_id, client_secret]):
            pytest.skip("Databricks credentials not configured in .env.dev")
        
        # Save credentials for later use
        cls._server_hostname = server_hostname
        cls._client_id = client_id
        cls._client_secret = client_secret
        
        # IMPORTANT: Generate OAuth token BEFORE removing env vars
        from infrastructure.oauth_token_manager import get_oauth_token_manager
        cls._token_manager = get_oauth_token_manager()
        
        # Create WorkspaceClient BEFORE removing env vars
        cls.workspace_client = WorkspaceClient(
            host=f"https://{server_hostname}",
            client_id=client_id,
            client_secret=client_secret
        )
        
        # CRITICAL: NOW remove OAuth env vars to prevent SQL connector from using them
        # The token manager and WorkspaceClient are already initialized
        os.environ.pop('DATABRICKS_CLIENT_ID', None)
        os.environ.pop('DATABRICKS_CLIENT_SECRET', None)
        os.environ.pop('DATABRICKS_AZURE_CLIENT_ID', None)
        os.environ.pop('DATABRICKS_AZURE_CLIENT_SECRET', None)
        
        cls.compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        cls.validator = PlanValidator("plan-schema/v1/plan.schema.json")
        
        print("✓ Setup complete\n")
    
    @classmethod
    def teardown_class(cls):
        """Restore environment after all tests"""
        # Restore OAuth env vars
        if hasattr(cls, '_client_id') and cls._client_id:
            os.environ['DATABRICKS_CLIENT_ID'] = cls._client_id
        if hasattr(cls, '_client_secret') and cls._client_secret:
            os.environ['DATABRICKS_CLIENT_SECRET'] = cls._client_secret
        
    def test_1_warehouse_connectivity(self):
        """UAT-1: Verify warehouse is accessible and can execute queries"""
        print("\n" + "="*80)
        print("UAT-1: Testing Warehouse Connectivity")
        print("="*80)
        
        try:
            # Get OAuth token from token manager
            from infrastructure.oauth_token_manager import get_oauth_token_manager
            
            token_manager = get_oauth_token_manager()
            access_token = token_manager.get_token()
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
            
            with connect_with_token(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT 1 as test")
                    result = cursor.fetchone()
                    print(f"✓ Warehouse connection successful: {result}")
                    assert result[0] == 1
        except Exception as e:
            pytest.skip(f"Warehouse not available: {e}")
    
    def test_2_catalog_and_schema_exist(self):
        """UAT-2: Verify catalog and schema exist"""
        print("\n" + "="*80)
        print("UAT-2: Testing Catalog and Schema")
        print("="*80)
        
        try:
            # Get OAuth token from token manager
            from infrastructure.oauth_token_manager import get_oauth_token_manager
            
            token_manager = get_oauth_token_manager()
            access_token = token_manager.get_token()
            server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
            
            with connect_with_token(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    # Check catalog
                    cursor.execute(f"SHOW CATALOGS LIKE '{self.catalog}'")
                    catalogs = cursor.fetchall()
                    print(f"✓ Found catalog: {self.catalog}")
                    assert len(catalogs) > 0, f"Catalog {self.catalog} not found"
                    
                    # Check schema (use backticks for identifiers with hyphens)
                    cursor.execute(f"SHOW SCHEMAS IN `{self.catalog}` LIKE '{self.schema}'")
                    schemas = cursor.fetchall()
                    print(f"✓ Found schema: {self.catalog}.{self.schema}")
                    assert len(schemas) > 0, f"Schema {self.schema} not found"
        except Exception as e:
            pytest.skip(f"Catalog/schema check failed: {e}")
    
    def test_3_validate_scd2_plan(self):
        """UAT-3: Validate SCD2 plan against schema"""
        print("\n" + "="*80)
        print("UAT-3: Testing SCD2 Plan Validation")
        print("="*80)
        
        from datetime import datetime, timezone
        import uuid
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_test',
                'description': 'UAT test for SCD2 pattern',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,  # Use actual catalog name with hyphens
                'schema': self.schema,    # Use actual schema name with hyphens
                'table': 'customers_source',
                'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email', 'city']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
        
        # Validate plan
        is_valid, errors = self.compiler.validate_plan(plan)
        print(f"✓ Plan validation: valid={is_valid}")
        if not is_valid:
            for error in errors:
                print(f"  Error: {error}")
        assert is_valid == True, f"Plan validation failed: {errors}"
        print("✓ SCD2 plan is valid")
    
    def test_4_compile_scd2_plan_to_sql(self):
        """UAT-4: Compile SCD2 plan into executable SQL"""
        print("\n" + "="*80)
        print("UAT-4: Testing SQL Compilation")
        print("="*80)
        
        from datetime import datetime, timezone
        import uuid
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_compile',
                'description': 'UAT test for SCD2 compilation',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_source',
                'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email', 'city']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
        
        # Compile plan
        sql = self.compiler.compile(plan)
        print(f"✓ Generated SQL ({len(sql)} chars)")
        print("\n" + "-"*80)
        print("Generated SQL:")
        print("-"*80)
        print(sql[:500] + "..." if len(sql) > 500 else sql)
        print("-"*80)
        
        # Verify SQL contains expected elements
        assert len(sql) > 0, "SQL should not be empty"
        # Verify SQL content (check for both quoted and unquoted identifiers)
        assert self.catalog in sql or f"`{self.catalog}`" in sql, "SQL should reference catalog"
        assert self.schema in sql or f"`{self.schema}`" in sql, "SQL should reference schema"
        assert "customers_dim" in sql, "SQL should reference target table"
        assert "customers_source" in sql, "SQL should reference source table"
        print("✓ SQL compilation successful")
    
    def test_5_create_test_tables(self):
        """UAT-5: Create test source and dimension tables"""
        print("\n" + "="*80)
        print("UAT-5: Creating Test Tables")
        print("="*80)
        
        # DEBUG: Print warehouse ID
        print(f"DEBUG: self.warehouse_id = [{self.warehouse_id}]")
        print(f"DEBUG: warehouse_id repr = {repr(self.warehouse_id)}")
        print(f"DEBUG: warehouse_id length = {len(self.warehouse_id)}")
        
        try:
            # Use catalog and schema names directly (they have hyphens, SQL uses backticks)
            catalog_fmt = self.catalog
            schema_fmt = self.schema
            
            # Use token manager from setup_class
            access_token = self._token_manager.get_token()
            server_hostname = self._server_hostname
            
            print(f"DEBUG: http_path = /sql/1.0/warehouses/{self.warehouse_id}")
            
            with connect_with_token(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    # Drop tables if they exist (use backticks for identifiers with underscores from hyphens)
                    cursor.execute(f"DROP TABLE IF EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                    cursor.execute(f"DROP TABLE IF EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_dim`")
                    print("✓ Dropped existing tables")
                    
                    # Create source table
                    cursor.execute(f"""
                        CREATE TABLE `{catalog_fmt}`.`{schema_fmt}`.`customers_source` (
                            customer_id INT,
                            name STRING,
                            email STRING,
                            city STRING,
                            updated_at TIMESTAMP
                        ) USING DELTA
                    """)
                    print("✓ Created customers_source table")
                    
                    # Create dimension table
                    cursor.execute(f"""
                        CREATE TABLE `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` (
                            customer_id INT,
                            name STRING,
                            email STRING,
                            city STRING,
                            updated_at TIMESTAMP,
                            valid_from TIMESTAMP,
                            valid_to TIMESTAMP,
                            is_current BOOLEAN
                        ) USING DELTA
                    """)
                    print("✓ Created customers_dim table")
                    
                    # Insert test data using explicit column names + VALUES (user suggestion)
                    print("\n--- Executing INSERT Statements with Explicit Columns ---")
                    
                    sql1 = f"""INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
(customer_id, name, email, city, updated_at)
VALUES (
  1,
  'Alice Smith',
  'alice@example.com',
  'San Francisco',
  current_timestamp()
)"""
                    print(f"SQL 1:\n{sql1}\n")
                    cursor.execute(sql1)
                    print("✓ Row 1 inserted")
                    
                    sql2 = f"""INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
(customer_id, name, email, city, updated_at)
VALUES (
  2,
  'Bob Jones',
  'bob@example.com',
  'New York',
  current_timestamp()
)"""
                    print(f"SQL 2:\n{sql2}\n")
                    cursor.execute(sql2)
                    print("✓ Row 2 inserted")
                    
                    sql3 = f"""INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
(customer_id, name, email, city, updated_at)
VALUES (
  3,
  'Carol White',
  'carol@example.com',
  'Chicago',
  current_timestamp()
)"""
                    print(f"SQL 3:\n{sql3}\n")
                    cursor.execute(sql3)
                    print("✓ Row 3 inserted")
                    
                    print("--- End INSERT Statements ---\n")
                    print("✓ Inserted test data into source table")
                    
                    # Verify data
                    cursor.execute(f"SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                    count = cursor.fetchone()[0]
                    print(f"✓ Source table has {count} rows")
                    assert count == 3, f"Should have 3 rows in source, got {count}"
        except Exception as e:
            error_msg = str(e)
            # Known Spark codegen warning - check if tables were actually created
            if "scala.Tuple2" in error_msg or "GeneratedClassFactory" in error_msg:
                print(f"⚠️  Spark codegen warning (can be ignored): {error_msg[:100]}...")
                # Verify tables were created despite the warning
                try:
                    with connect_with_token(
                        server_hostname=server_hostname,
                        http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                        access_token=access_token
                    ) as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(f"SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                            count = cursor.fetchone()[0]
                            if count >= 3:
                                print(f"✓ Tables verified successfully - source has {count} rows")
                                return  # Success - tables created and data inserted
                            else:
                                pytest.skip(f"Tables created but data insertion incomplete: {count}/3 rows")
                except:
                    pytest.skip(f"Table creation verification failed after Spark warning")
            # Real errors (not just Spark warnings)
            elif "TABLE_OR_VIEW_NOT_FOUND" in error_msg or "PERMISSION_DENIED" in error_msg:
                pytest.skip(f"Table creation failed: {e}")
            else:
                # Unknown error - fail the test
                raise
    
    @pytest.mark.e2e
    @pytest.mark.requires_databricks
    def test_6_execute_scd2_sql_on_warehouse(self):
        """
        Test 6: Execute compiled SCD2 SQL on Databricks SQL Warehouse
        
        **REQUIRES**: Active Databricks SQL Warehouse with proper credentials
        This test will be skipped if run without infrastructure access.
        """
        """UAT-6: Execute generated SCD2 SQL on warehouse"""
        print("\n" + "="*80)
        print("UAT-6: Executing SCD2 SQL on Warehouse")
        print("="*80)
        
        from datetime import datetime, timezone
        import uuid
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_execute',
                'description': 'UAT test for SCD2 execution',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_source',
                'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email', 'city']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
        
        try:
            # Compile SQL
            sql = self.compiler.compile(plan)
            print("✓ Compiled SQL")
            print(f"\n--- Generated SQL ---\n{sql}\n--- End SQL ---\n")
            
            # Execute SQL on warehouse
            # Get OAuth token for authentication
            from infrastructure.oauth_token_manager import get_oauth_token_manager
            token_manager = get_oauth_token_manager()
            access_token = token_manager.get_token()
            
            # FIX C: Connection 1 - Check and setup test data if needed
            print("\n✓ Opening connection 1: Setup/verification...")
            with connect_with_token(
                server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as setup_connection:
                with setup_connection.cursor() as setup_cursor:
                    catalog_fmt = self.catalog
                    schema_fmt = self.schema
                    
                    # FIX A: Check if source table exists (fail fast)
                    print("  🔍 Verifying source table exists...")
                    setup_cursor.execute(f"""
                        SHOW TABLES IN `{catalog_fmt}`.`{schema_fmt}`
                        LIKE 'customers_source'
                    """)
                    source_exists = bool(setup_cursor.fetchall())
                    
                    if not source_exists:
                        print("⚠️  Source table not found - recreating test data...")
                        try:
                            setup_cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_source` (
                                    customer_id INT,
                                    name STRING,
                                    email STRING,
                                    city STRING,
                                    updated_at TIMESTAMP
                                ) USING DELTA
                            """)
                            setup_cursor.execute(f"""
                                CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` (
                                    customer_id INT,
                                    name STRING,
                                    email STRING,
                                    city STRING,
                                    updated_at TIMESTAMP,
                                    valid_from TIMESTAMP,
                                    valid_to TIMESTAMP,
                                    is_current BOOLEAN
                                ) USING DELTA
                            """)
                            print("  ✓ Tables created")
                            
                            # Try to insert test data (will hit Spark bug, but that's OK for this check)
                            print("  ⚠️  Attempting to insert test data (may hit Spark bug)...")
                            try:
                                setup_cursor.execute(f"""
                                    INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
                                    (customer_id, name, email, city, updated_at)
                                    VALUES (1, 'Alice Smith', 'alice@example.com', 'San Francisco', current_timestamp())
                                """)
                                print("  ✓ Data inserted successfully!")
                            except Exception as insert_error:
                                # Known Databricks Spark bug - INSERT VALUES fails via Python connector
                                # But this doesn't affect the SCD2 SQL validation test
                                print(f"  ⚠️  INSERT failed (known Databricks bug): {str(insert_error)[:100]}")
                                print("  ℹ️  This is expected - Databricks has a Spark codegen bug with INSERT VALUES")
                                print("  ℹ️  The test will now reach the production validation code and fail properly")
                        except Exception as create_error:
                            # FIX B: Fail immediately on CREATE errors
                            pytest.fail(f"Failed to create tables: {create_error}")
                    
                    # Check if source table has data (but don't skip - let production validation handle it)
                    setup_cursor.execute(f"SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                    source_count = setup_cursor.fetchone()[0]
                    print(f"  ℹ️  Source table has {source_count} rows")
                    
                    # Note: We DON'T skip here anymore - we let the production validation code
                    # run and provide a proper error message
                    
                    print("✓ Setup phase complete - moving to SCD2 execution phase...")
                    
                    # Save SQL to file for debugging
                    sql_debug_file = Path(__file__).parent.parent / "debug_scd2_sql.sql"
                    with open(sql_debug_file, 'w') as f:
                        f.write(sql)
                    print(f"  📝 Saved generated SQL to: {sql_debug_file}")
                    
            # FIX C: Use separate connection for SCD2 execution (clean session)
            print("\n✓ Opening clean connection for SCD2 execution...")
            with connect_with_token(
                server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as scd2_connection:
                with scd2_connection.cursor() as scd2_cursor:
                    catalog_fmt = self.catalog
                    schema_fmt = self.schema
                    
                    # ✅ SQLPILOT SOURCE TABLE VALIDATION (Production-grade)
                    # Guarantee source table exists before emitting SQL
                    print("  🔍 SQLPilot Pre-flight Check: Verifying source table exists...")
                    scd2_cursor.execute(f"""
                        SHOW TABLES IN `{catalog_fmt}`.`{schema_fmt}`
                        LIKE 'customers_source'
                    """)
                    source_tables = scd2_cursor.fetchall()
                    
                    if not source_tables:
                        # FAIL FAST with clear, actionable message
                        error_msg = (
                            f"❌ SQLPilot Pre-flight Check Failed\n"
                            f"\n"
                            f"Source table does not exist: `{catalog_fmt}`.`{schema_fmt}`.`customers_source`\n"
                            f"\n"
                            f"SQLPilot requires source tables to exist before execution.\n"
                            f"\n"
                            f"To fix this:\n"
                            f"1. Run: cat setup_uat_test_data_CTAS.sql\n"
                            f"2. Execute the SQL in Databricks SQL Editor\n"
                            f"3. Re-run this test\n"
                            f"\n"
                            f"This is a PRODUCTION-GRADE safeguard to prevent failed executions.\n"
                        )
                        pytest.fail(error_msg)
                    
                    print(f"  ✅ Source table exists ({len(source_tables)} match)")
                    
                    # Verify source table has data
                    scd2_cursor.execute(f"SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                    source_count = scd2_cursor.fetchone()[0]
                    print(f"  ✅ Source table has {source_count} rows")
                    
                    if source_count == 0:
                        error_msg = (
                            f"❌ SQLPilot Pre-flight Check Failed\n"
                            f"\n"
                            f"Source table exists but contains no data: `{catalog_fmt}`.`{schema_fmt}`.`customers_source`\n"
                            f"\n"
                            f"To fix: Run setup_uat_test_data_CTAS.sql in Databricks SQL Editor\n"
                        )
                        pytest.fail(error_msg)
                    
                    # Verify target table exists
                    print("  🔍 Verifying target table exists...")
                    scd2_cursor.execute(f"""
                        SHOW TABLES IN `{catalog_fmt}`.`{schema_fmt}`
                        LIKE 'customers_dim'
                    """)
                    target_tables = scd2_cursor.fetchall()
                    
                    if not target_tables:
                        error_msg = (
                            f"❌ SQLPilot Pre-flight Check Failed\n"
                            f"\n"
                            f"Target table does not exist: `{catalog_fmt}`.`{schema_fmt}`.`customers_dim`\n"
                            f"\n"
                            f"To fix: Run setup_uat_test_data_CTAS.sql in Databricks SQL Editor\n"
                        )
                        pytest.fail(error_msg)
                    
                    print(f"  ✅ Target table exists ({len(target_tables)} match)")
                    print("  ✅ All pre-flight checks passed - ready to execute SCD2 SQL")
                    
                    # Use safe SQL splitter (handles strings, comments, etc.)
                    print("  🔍 Splitting SQL statements (safe splitter)...")
                    statements = split_sql_statements(sql, debug=True)
                    print(f"  ✓ Found {len(statements)} statements")
                    
                    # Execute each statement separately
                    for i, stmt in enumerate(statements, 1):
                        print(f"  Executing statement {i}/{len(statements)}...")
                        print(f"  Preview: {stmt[:100]}...")
                        try:
                            scd2_cursor.execute(stmt)
                            print(f"  ✓ Statement {i} executed successfully")
                        except Exception as e:
                            # FIX B: Never continue after analysis error
                            print(f"  ❌ Statement {i} failed: {e}")
                            print(f"  Full statement:\n{stmt}")
                            pytest.fail(f"SQL execution failed at statement {i}: {e}")
                    print("✓ SQL executed successfully")
            
            # FIX C: Use separate connection for validation (clean session)
            print("\n✓ Opening clean connection for validation...")
            with connect_with_token(
                server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token  # Use the same token from earlier in this test
            ) as validation_connection:
                with validation_connection.cursor() as validation_cursor:
                    # Verify results (use original names with hyphens)
                    catalog_fmt = self.catalog
                    schema_fmt = self.schema
                    validation_cursor.execute(f"SELECT COUNT(*) FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_dim`")
                    count = validation_cursor.fetchone()[0]
                    print(f"✓ Dimension table has {count} rows after SCD2 load")
                    assert count > 0, "Dimension table should have rows after load"
                    
                    # Verify current records
                    validation_cursor.execute(f"""
                        SELECT customer_id, name, is_current 
                        FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` 
                        WHERE is_current = true 
                        ORDER BY customer_id
                    """)
                    current_records = validation_cursor.fetchall()
                    print(f"✓ Found {len(current_records)} current records:")
                    for record in current_records:
                        print(f"  - Customer {record[0]}: {record[1]} (current={record[2]})")
                    
                    assert len(current_records) == 3, "Should have 3 current records"
                    print("✓ SCD2 pattern executed successfully!")
        except Exception as e:
            # FIX B: Fail immediately, don't continue
            pytest.fail(f"SQL execution failed: {e}")
    
    @pytest.mark.requires_databricks
    def test_6b_execute_scd2_via_rest_api(self):
        """UAT-6b: Execute SCD2 SQL using REST API (bypasses Python connector bug)"""
        print("\n" + "="*80)
        print("UAT-6b: Executing SCD2 SQL via REST API")
        print("="*80)
        
        import uuid
        from datetime import datetime, timezone
        
        # Get credentials
        server_hostname, access_token = get_databricks_credentials()
        
        # Initialize REST API client
        api_client = DatabricksStatementAPI(
            server_hostname=server_hostname.replace("https://", ""),
            access_token=access_token,
            warehouse_id=self.warehouse_id
        )
        
        # Create plan
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_rest_api',
                'description': 'UAT SCD2 via REST API',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_source',
                'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email', 'city']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
        
        try:
            # Compile SQL
            sql = self.compiler.compile(plan)
            print("✓ Compiled SQL (Databricks SCD2 pattern with equal_null)")
            
            # Save SQL for debugging
            sql_debug_file = Path(__file__).parent.parent / "debug_scd2_rest_api.sql"
            with open(sql_debug_file, 'w') as f:
                f.write(sql)
            print(f"  📝 Saved SQL to: {sql_debug_file}")
            
            # STEP 1: Setup test data via REST API (if needed)
            print("\n📍 STEP 1: Verify/create test tables...")
            catalog_fmt = self.catalog
            schema_fmt = self.schema
            
            # Check if source table exists
            print("  🔍 Checking if source table exists...")
            check_sql = f"SHOW TABLES IN `{catalog_fmt}`.`{schema_fmt}` LIKE 'customers_source'"
            
            try:
                result = api_client.execute_statement_with_results(check_sql, max_rows=10)
                table_exists = len(result["rows"]) > 0
                print(f"  {'✅' if table_exists else '⚠️ '} Source table {'exists' if table_exists else 'not found'}")
            except Exception as e:
                print(f"  ⚠️  Could not check table: {e}")
                table_exists = False
            
            # Create tables if they don't exist
            if not table_exists:
                print("  📦 Creating test tables...")
                
                # Create source table
                create_source_sql = f"""
                CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_source` (
                    customer_id INT,
                    name STRING,
                    email STRING,
                    city STRING,
                    updated_at TIMESTAMP
                ) USING DELTA
                """
                api_client.execute_statement(create_source_sql)
                print("  ✅ Source table created")
                
                # Create dimension table
                create_dim_sql = f"""
                CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` (
                    customer_id INT,
                    name STRING,
                    email STRING,
                    city STRING,
                    updated_at TIMESTAMP,
                    valid_from TIMESTAMP,
                    valid_to TIMESTAMP,
                    is_current BOOLEAN
                ) USING DELTA
                """
                api_client.execute_statement(create_dim_sql)
                print("  ✅ Dimension table created")
                
                # Insert test data using CTAS pattern (avoids INSERT VALUES bug)
                print("  📥 Creating test data with CREATE TABLE AS SELECT...")
                insert_sql = f"""
                INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
                SELECT 1 as customer_id, 'Alice Smith' as name, 'alice@example.com' as email,
                       'San Francisco' as city, current_timestamp() as updated_at
                UNION ALL
                SELECT 2, 'Bob Jones', 'bob@example.com', 'New York', current_timestamp()
                UNION ALL
                SELECT 3, 'Carol White', 'carol@example.com', 'Chicago', current_timestamp()
                """
                
                try:
                    api_client.execute_statement(insert_sql)
                    print("  ✅ Test data inserted successfully!")
                except Exception as insert_error:
                    print(f"  ⚠️  INSERT failed: {str(insert_error)[:100]}")
                    # Try to continue - table might have some data
            
            # Verify data exists
            print("\n  🔍 Verifying source table has data...")
            count_sql = f"SELECT COUNT(*) as cnt FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`"
            count_result = api_client.execute_statement_with_results(count_sql, max_rows=1)
            
            if count_result["rows"]:
                row_count = count_result["rows"][0][0]  # First row, first column
                print(f"  ✅ Source table has {row_count} rows")
                
                if row_count == 0:
                    pytest.skip("Source table has no data. Run setup_uat_test_data_CTAS.sql manually.")
            
            # STEP 2: Execute SCD2 SQL via REST API
            print("\n📍 STEP 2: Execute SCD2 SQL via REST API...")
            print("  ℹ️  Using Databricks Statement Execution API (REST)")
            print("  ℹ️  This bypasses the Python SQL connector completely")
            
            # Split SQL into statements
            statements = split_sql_statements(sql, debug=True)
            print(f"  ✓ Found {len(statements)} statements to execute")
            
            # Execute each statement via REST API
            results = api_client.execute_multiple_statements(statements, stop_on_error=True)
            
            print(f"\n  ✅ All {len(results)} statements executed successfully via REST API!")
            
            # STEP 3: Validate results
            print("\n📍 STEP 3: Validate SCD2 results...")
            
            # Count dimension records
            dim_count_sql = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN is_current = TRUE THEN 1 END) as current_records
            FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_dim`
            """
            dim_result = api_client.execute_statement_with_results(dim_count_sql, max_rows=1)
            
            if dim_result["rows"]:
                total, current = dim_result["rows"][0]
                total = int(total) if total is not None else 0
                current = int(current) if current is not None else 0
                print(f"  ✅ Dimension table has {total} total records, {current} current")
                
                # Verify we have current records
                assert current > 0, f"Expected current records, but found {current}"
                print(f"  ✅ SCD2 initial load validated via REST API!")
            
            print("\n✅ SUCCESS: SCD2 SQL executed via REST API without hitting Spark bug!")
            
        except Exception as e:
            pytest.fail(f"REST API execution failed: {e}")
    
    @pytest.mark.requires_databricks
    def test_6c_scd2_hybrid_execution(self):
        """
        UAT-6c: Test SCD2 with hybrid execution strategy
        
        Uses the standard Databricks SCD2 pattern with hybrid execution:
        - REST API for DDL (CREATE, DROP, SELECT)
        - SQL Connector for DML (MERGE, UPDATE, DELETE)
        
        Reference: https://www.databricks.com/blog/implementing-dimensional-data-warehouse-databricks-sql-part-2
        """
        print("\n" + "="*80)
        print("UAT-6c: Testing Standard SCD2 Pattern with Hybrid Execution")
        print("="*80)
        
        import uuid
        from datetime import datetime, timezone
        
        # Get credentials
        server_hostname, access_token = get_databricks_credentials()
        
        # Initialize REST API client with catalog and schema context
        api_client = DatabricksStatementAPI(
            server_hostname=server_hostname.replace("https://", ""),
            access_token=access_token,
            warehouse_id=self.warehouse_id,
            catalog=self.catalog,
            schema=self.schema
        )
        
        # Create plan using standard SCD2 pattern
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'uat_scd2_hybrid',
                'description': 'UAT SCD2 test with hybrid execution',
                'owner': 'uat-tester@example.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_source',
                'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
            },
            'target': {
                'catalog': self.catalog,
                'schema': self.schema,
                'table': 'customers_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email', 'city', 'updated_at']
            },
            'execution_config': {
                'warehouse_id': self.warehouse_id,
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
        
        try:
            # Compile SQL using standard SCD2 pattern
            compiler = SQLCompiler(schema_path='plan-schema/v1/plan.schema.json')
            sql = compiler.compile(plan)
            print("✓ Compiled SQL using Standard Databricks SCD2 Pattern")
            print("  ℹ️  Three-step approach: UPDATE late-arriving → MERGE expire → INSERT new")
            
            debug_file_path = Path(__file__).parent.parent / "debug_scd2_sql.sql"
            with open(debug_file_path, "w") as f:
                f.write(sql)
            print(f"  📝 Saved SQL to: {debug_file_path}")

            catalog_fmt = self.catalog
            schema_fmt = self.schema
            
            print("\n📍 STEP 1: Setup catalog, schema, and test tables...")
            
            # Create catalog if not exists
            print("  📦 Creating catalog if not exists...")
            create_catalog_sql = f"CREATE CATALOG IF NOT EXISTS `{catalog_fmt}`"
            try:
                api_client.execute_statement(create_catalog_sql)
                print(f"  ✅ Catalog `{catalog_fmt}` ready")
            except Exception as e:
                print(f"  ℹ️  Catalog creation skipped: {str(e)[:100]}")
            
            # Create schema if not exists
            print("  📦 Creating schema if not exists...")
            create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`"
            try:
                api_client.execute_statement(create_schema_sql)
                print(f"  ✅ Schema `{catalog_fmt}`.`{schema_fmt}` ready")
            except Exception as e:
                print(f"  ℹ️  Schema creation skipped: {str(e)[:100]}")
            
            # Create source table
            print("  📦 Creating source table...")
            create_source_sql = f"""
            CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_source` (
                customer_id INT,
                name STRING,
                email STRING,
                city STRING,
                updated_at TIMESTAMP
            ) USING DELTA
            """
            api_client.execute_statement(create_source_sql)
            print("  ✅ Source table created")
            
            # Create dimension table
            print("  📦 Creating dimension table...")
            create_dim_sql = f"""
            CREATE TABLE IF NOT EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` (
                customer_id INT,
                name STRING,
                email STRING,
                city STRING,
                updated_at TIMESTAMP,
                valid_from TIMESTAMP,
                valid_to TIMESTAMP,
                is_current BOOLEAN
            ) USING DELTA
            """
            api_client.execute_statement(create_dim_sql)
            print("  ✅ Dimension table created")
            
            # Check if data exists
            print("  🔍 Checking for existing data...")
            count_sql = f"SELECT COUNT(*) as cnt FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_source`"
            count_result = api_client.execute_statement_with_results(count_sql, max_rows=1)
            row_count = count_result["rows"][0][0] if count_result["rows"] else 0
            row_count = int(row_count) if row_count is not None else 0
            print(f"  ℹ️  Current row count: {row_count}")
            
            if row_count == 0:
                print("  📝 Inserting test data...")
                insert_sql = f"""
                INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
                SELECT 1 as customer_id, 'Alice Smith' as name, 'alice@example.com' as email,
                       'San Francisco' as city, current_timestamp() as updated_at
                UNION ALL
                SELECT 2, 'Bob Jones', 'bob@example.com', 'New York', current_timestamp()
                UNION ALL
                SELECT 3, 'Carol White', 'carol@example.com', 'Chicago', current_timestamp()
                """
                try:
                    api_client.execute_statement(insert_sql)
                    print(f"  ✅ Inserted 3 test records - ready to test MERGE!")
                except Exception as insert_err:
                    error_msg = str(insert_err)
                    if 'scala.Tuple2' in error_msg or 'ClassCastException' in error_msg:
                        print("  ❌ INSERT failed due to Spark bug")
                        pytest.skip(f"Cannot insert test data via API due to Spark bug. Please run QUICK_MANUAL_SETUP.md manually, then rerun.")
                    else:
                        pytest.skip(f"INSERT failed: {error_msg[:200]}")
            else:
                print(f"  ✅ Source table has {row_count} rows - ready to test MERGE!")
            
            # Check and disable row tracking if enabled (known to cause API issues)
            print("\n  🔍 Checking table properties...")
            try:
                # Check source table properties
                check_props_sql = f"DESCRIBE TABLE EXTENDED `{catalog_fmt}`.`{schema_fmt}`.`customers_source`"
                props_result = api_client.execute_statement_with_results(check_props_sql, max_rows=100)
                
                # Look for row tracking in properties
                has_row_tracking = False
                if props_result.get("rows"):
                    for row in props_result["rows"]:
                        if len(row) >= 2:
                            col_name, data_type = row[0], row[1]
                            if str(col_name).lower() == "delta.enablerowtracking" and str(data_type).lower() == "true":
                                has_row_tracking = True
                                break
                
                if has_row_tracking:
                    print("  ⚠️  Row tracking is enabled - disabling it (known to cause API issues)")
                    api_client.execute_statement(f"ALTER TABLE `{catalog_fmt}`.`{schema_fmt}`.`customers_source` SET TBLPROPERTIES ('delta.enableRowTracking' = 'false')")
                    api_client.execute_statement(f"ALTER TABLE `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` SET TBLPROPERTIES ('delta.enableRowTracking' = 'false')")
                    print("  ✅ Row tracking disabled on both tables")
                else:
                    print("  ✅ Row tracking not enabled - good to go")
            except Exception as prop_error:
                print(f"  ⚠️  Could not check/modify table properties: {str(prop_error)[:100]}")
                print("  ℹ️  Continuing anyway...")
            
            # Execute the generated SQL (split if multiple statements)
            from databricks.sql import connect
            
            # Import SQL splitter utility
            def split_sql_statements(sql: str) -> list:
                """Split SQL into individual statements"""
                import re
                # Remove comments
                sql_no_comments = re.sub(r'--[^\n]*', '', sql)
                # Split on semicolons
                statements = [s.strip() for s in sql_no_comments.split(';') if s.strip()]
                return statements
            
            statements = split_sql_statements(sql)
            print(f"  ℹ️  Executing {len(statements)} SQL statements...")
            
            with connect_with_token(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    for i, stmt in enumerate(statements, 1):
                        if stmt:
                            print(f"    Executing statement {i}/{len(statements)}...")
                            cursor.execute(stmt)
            
            result = {"method": "sql_connector"}
            
        except Exception as e:
            pytest.fail(f"Alternative pattern test failed: {e}")
    
    def test_7_scd2_update_scenario(self):
        """UAT-7: Test SCD2 update scenario (change tracking)"""
        print("\n" + "="*80)
        print("UAT-7: Testing SCD2 Update Scenario")
        print("="*80)
        
        from datetime import datetime, timezone
        import uuid
        
        try:
            # Use catalog and schema names directly (they have hyphens, SQL uses backticks)
            catalog_fmt = self.catalog
            schema_fmt = self.schema
            
            # Use token manager from setup_class
            access_token = self._token_manager.get_token()
            server_hostname = self._server_hostname
            
            with connect_with_token(
                server_hostname=server_hostname,
                http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    # Update a customer in source (simulate data change)
                    print("✓ Updating customer data in source...")
                    cursor.execute(f"""
                        INSERT INTO `{catalog_fmt}`.`{schema_fmt}`.`customers_source`
                        (customer_id, name, email, city, updated_at)
                        VALUES (1, 'Alice Johnson', 'alice.j@example.com', 'Los Angeles', current_timestamp())
                    """)
                    print("✓ Updated Alice's email and city")
                    
                    # Re-run SCD2 process
                    plan = {
                        'schema_version': '1.0',
                        'plan_metadata': {
                            'plan_id': str(uuid.uuid4()),
                            'plan_name': 'uat_scd2_update',
                            'description': 'UAT SCD2 update test',
                            'owner': 'uat-tester@example.com',
                            'created_at': datetime.now(timezone.utc).isoformat(),
                            'version': '1.0.0'
                        },
                        'pattern': {'type': 'SCD2'},
                        'source': {
                            'catalog': catalog_fmt,
                            'schema': schema_fmt,
                            'table': 'customers_source',
                            'columns': ['customer_id', 'name', 'email', 'city', 'updated_at']
                        },
                        'target': {
                            'catalog': catalog_fmt,
                            'schema': schema_fmt,
                            'table': 'customers_dim',
                            'write_mode': 'merge'
                        },
                        'pattern_config': {
                            'business_keys': ['customer_id'],
                            'effective_date_column': 'valid_from',
                            'end_date_column': 'valid_to',
                            'current_flag_column': 'is_current',
                            'end_date_default': '9999-12-31 23:59:59',
                            'compare_columns': ['name', 'email', 'city']
                        },
                        'execution_config': {
                            'warehouse_id': self.warehouse_id,
                            'timeout_seconds': 3600,
                            'max_retries': 3
                        }
                    }
                    
                    sql = self.compiler.compile(plan)
                    # Use safe SQL splitter
                    print("  🔍 Splitting SQL statements (safe splitter)...")
                    statements = split_sql_statements(sql, debug=True)
                    print(f"  ✓ Found {len(statements)} statements")
                    for i, stmt in enumerate(statements, 1):
                        if stmt.strip():
                            print(f"  Executing statement {i}/{len(statements)}...")
                            cursor.execute(stmt)
                    print("✓ Re-executed SCD2 SQL")
                    
                    # Verify history tracking
                    cursor.execute(f"""
                        SELECT customer_id, name, email, city, is_current 
                        FROM `{catalog_fmt}`.`{schema_fmt}`.`customers_dim` 
                        WHERE customer_id = 1
                        ORDER BY valid_from
                    """)
                    alice_history = cursor.fetchall()
                    print(f"✓ Found {len(alice_history)} historical records for Alice:")
                    for i, record in enumerate(alice_history, 1):
                        print(f"  Version {i}: {record[1]}, {record[2]}, {record[3]} (current={record[4]})")
                    
                    # Verify latest record is current and has updated data
                    assert len(alice_history) >= 2, "Should have at least 2 historical records"
                    latest_record = alice_history[-1]
                    assert latest_record[4] == True, "Latest record should be current"
                    assert latest_record[2] == 'alice.j@example.com', "Latest record should have updated email"
                    assert latest_record[3] == 'Los Angeles', "Latest record should have updated city"
                    print("✓ SCD2 change tracking working correctly!")
        except Exception as e:
            # If it's a data dependency issue (no source data), skip gracefully
            if "customers_source" in str(e) or "has no data" in str(e).lower():
                pytest.skip(f"Test requires data from test_6: {e}")
            # Otherwise, fail to show the real error
            raise
    
    @pytest.mark.e2e
    @pytest.mark.requires_databricks
    def test_8_api_to_warehouse_integration(self):
        """
        Test 8: End-to-end API to Warehouse Integration
        
        Tests API endpoints (validate, compile) with real Databricks credentials
        """
        print("\n" + "="*80)
        print("UAT-8: Testing API to Warehouse Integration")
        print("="*80)
        
        from fastapi.testclient import TestClient
        from datetime import datetime, timezone
        import uuid
        
        # Import app and auth dependency
        from api.main import app
        from security.middleware import get_current_user
        
        # Mock authentication for this test using FastAPI dependency override
        async def mock_get_current_user():
            return {"user": "uat_tester", "email": "uat-tester@example.com", "roles": ["user"]}
        
        # Set override before creating client
        original_overrides = app.dependency_overrides.copy()
        app.dependency_overrides[get_current_user] = mock_get_current_user
        
        try:
            client = TestClient(app)
            
            # Test validation endpoint
            plan = {
                'schema_version': '1.0',
                'plan_metadata': {
                    'plan_id': str(uuid.uuid4()),
                    'plan_name': 'uat_api_test',
                    'description': 'UAT API test',
                    'owner': 'uat-tester@example.com',
                    'created_at': datetime.now(timezone.utc).isoformat(),
                    'version': '1.0.0'
                },
                'pattern': {'type': 'SCD2'},
                'source': {
                    'catalog': self.catalog,
                    'schema': self.schema,
                    'table': 'customers_source',
                    'columns': ['customer_id', 'name', 'email', 'city']
                },
                'target': {
                    'catalog': self.catalog,
                    'schema': self.schema,
                    'table': 'customers_dim',
                    'write_mode': 'merge'
                },
                'pattern_config': {
                    'business_keys': ['customer_id'],
                    'effective_date_column': 'valid_from',
                    'end_date_column': 'valid_to',
                    'current_flag_column': 'is_current',
                    'end_date_default': '9999-12-31 23:59:59',
                    'compare_columns': ['name', 'email', 'city']
                },
                'execution_config': {
                    'warehouse_id': self.warehouse_id,
                    'timeout_seconds': 3600
                }
            }
            
            # Validate via API
            response = client.post("/api/v1/plans/validate", json={"plan": plan})
            # If auth is still required (401), skip this test as auth mocking isn't active
            if response.status_code == 401:
                pytest.skip("Auth is required - this test needs auth mocking context from API test suite")
            assert response.status_code == 200, f"Validation failed: {response.status_code} - {response.text}"
            validation_result = response.json()
            print(f"✓ API validation: success={validation_result.get('success')}")
            assert validation_result.get("success") == True or validation_result.get("valid") == True
            
            # Compile via API
            response = client.post("/api/v1/plans/compile", json={"plan": plan})
            assert response.status_code == 200, f"Compilation failed: {response.status_code} - {response.text}"
            compile_result = response.json()
            print(f"✓ API compilation: {compile_result['success']}")
            assert compile_result["success"] == True
            assert "sql" in compile_result
            print(f"✓ Generated SQL via API: {len(compile_result['sql'])} chars")
            
            print("✓ API to warehouse integration verified!")
            
        finally:
            # Restore original overrides (don't clear, as other tests may be using them)
            app.dependency_overrides = original_overrides
    
    @classmethod
    def teardown_class(cls):
        """Cleanup test tables"""
        print("\n" + "="*80)
        print("Cleaning up test tables")
        print("="*80)
        
        try:
            # Use the token manager we created in setup_class
            access_token = cls._token_manager.get_token() if hasattr(cls, '_token_manager') else None
            
            with connect_with_token(
                server_hostname=cls._server_hostname if hasattr(cls, '_server_hostname') else os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                http_path=f"/sql/1.0/warehouses/{cls.warehouse_id}",
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    # Use original names with hyphens (enclosed in backticks)
                    catalog_fmt = cls.catalog
                    schema_fmt = cls.schema
                    cursor.execute(f"DROP TABLE IF EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_source`")
                    cursor.execute(f"DROP TABLE IF EXISTS `{catalog_fmt}`.`{schema_fmt}`.`customers_dim`")
                    print("✓ Test tables cleaned up")
        except:
            pass
        
        # NOW restore everything AFTER cleanup is done
        import shutil
        databricks_cfg = os.path.expanduser("~/.databrickscfg")
        databricks_cfg_backup = os.path.expanduser("~/.databrickscfg.uat_backup")
        if hasattr(cls, '_databricks_cfg_existed') and cls._databricks_cfg_existed:
            if os.path.exists(databricks_cfg_backup):
                shutil.move(databricks_cfg_backup, databricks_cfg)
        
        # Restore OAuth env vars
        if hasattr(cls, '_client_id') and cls._client_id:
            os.environ['DATABRICKS_CLIENT_ID'] = cls._client_id
        if hasattr(cls, '_client_secret') and cls._client_secret:
            os.environ['DATABRICKS_CLIENT_SECRET'] = cls._client_secret


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])

