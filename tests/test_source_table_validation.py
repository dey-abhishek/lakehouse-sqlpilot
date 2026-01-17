"""
Test source table existence validation
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from plan_schema.v1.validator import PlanValidator
from compiler.sql_generator import SQLCompiler, CompilationError
from execution.executor import SQLExecutor, ExecutionError


class TestSourceTableValidation:
    """Test source table existence checks at multiple layers"""
    
    @pytest.fixture
    def sample_scd2_plan(self):
        """Sample SCD2 plan for testing"""
        import uuid
        from datetime import datetime, timezone
        return {
            "schema_version": "1.0",
            "plan_metadata": {
                "plan_id": str(uuid.uuid4()),
                "plan_name": "test_scd2",
                "description": "Test SCD2 plan",
                "owner": "test@databricks.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "version": "1.0.0"
            },
            "pattern": {
                "type": "SCD2"
            },
            "source": {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "table": "customers_source",
                "columns": ["customer_id", "name", "email", "city", "updated_at"]
            },
            "target": {
                "catalog": "test_catalog",
                "schema": "test_schema",
                "table": "customers_dim",
                "write_mode": "merge"
            },
            "pattern_config": {
                "business_keys": ["customer_id"],
                "effective_date_column": "valid_from",
                "end_date_column": "valid_to",
                "current_flag_column": "is_current"
            },
            "execution_config": {
                "warehouse_id": "test-warehouse-id",
                "timeout_seconds": 3600,
                "max_retries": 3
            },
            "schedule": {
                "type": "manual"
            }
        }
    
    @pytest.fixture
    def mock_workspace_client_with_table(self):
        """Mock workspace client that returns a table"""
        mock_client = Mock()
        mock_table = Mock()
        mock_table.name = "customers_source"
        mock_client.tables.list.return_value = [mock_table]
        return mock_client
    
    @pytest.fixture
    def mock_workspace_client_without_table(self):
        """Mock workspace client that returns no tables"""
        mock_client = Mock()
        mock_client.tables.list.return_value = []
        return mock_client
    
    # ========================================
    # Validator Layer Tests
    # ========================================
    
    def test_validator_passes_when_table_exists(self, sample_scd2_plan, mock_workspace_client_with_table):
        """Validator should pass when source table exists"""
        validator = PlanValidator('plan-schema/v1/plan.schema.json')
        
        is_valid, errors = validator.validate_with_runtime_checks(
            sample_scd2_plan,
            mock_workspace_client_with_table
        )
        
        assert is_valid, f"Validation should pass when table exists. Errors: {errors}"
        assert len(errors) == 0
    
    def test_validator_fails_when_table_missing(self, sample_scd2_plan, mock_workspace_client_without_table):
        """Validator should fail when source table does not exist"""
        validator = PlanValidator('plan-schema/v1/plan.schema.json')
        
        is_valid, errors = validator.validate_with_runtime_checks(
            sample_scd2_plan,
            mock_workspace_client_without_table
        )
        
        assert not is_valid, "Validation should fail when table is missing"
        assert len(errors) > 0
        assert any("does not exist" in err.lower() for err in errors), \
            f"Error message should mention table doesn't exist. Got: {errors}"
        assert any("customers_source" in err for err in errors), \
            f"Error should mention the table name. Got: {errors}"
    
    def test_validator_without_runtime_check_skips_table_validation(self, sample_scd2_plan):
        """Validator without runtime check should skip table existence validation"""
        validator = PlanValidator('plan-schema/v1/plan.schema.json')
        
        # Standard validation (no runtime check)
        is_valid, errors = validator.validate_plan(sample_scd2_plan)
        
        # Should pass because it only checks schema/semantic rules, not table existence
        assert is_valid, f"Standard validation should pass. Errors: {errors}"
    
    # ========================================
    # Compiler Layer Tests
    # ========================================
    
    def test_compiler_with_runtime_validation_passes_when_table_exists(
        self, sample_scd2_plan, mock_workspace_client_with_table
    ):
        """Compiler with runtime validation should pass when table exists"""
        compiler = SQLCompiler('plan-schema/v1/plan.schema.json')
        
        sql = compiler.compile_with_runtime_validation(
            sample_scd2_plan,
            mock_workspace_client_with_table
        )
        
        assert sql is not None
        assert "MERGE INTO" in sql
        assert "INSERT INTO" in sql
    
    def test_compiler_with_runtime_validation_fails_when_table_missing(
        self, sample_scd2_plan, mock_workspace_client_without_table
    ):
        """Compiler with runtime validation should fail when table is missing"""
        compiler = SQLCompiler('plan-schema/v1/plan.schema.json')
        
        with pytest.raises(CompilationError) as exc_info:
            compiler.compile_with_runtime_validation(
                sample_scd2_plan,
                mock_workspace_client_without_table
            )
        
        error_msg = str(exc_info.value)
        assert "does not exist" in error_msg.lower(), \
            f"Error should mention table doesn't exist. Got: {error_msg}"
        assert "customers_source" in error_msg, \
            f"Error should mention the table name. Got: {error_msg}"
    
    def test_compiler_without_runtime_validation_succeeds_even_if_table_missing(
        self, sample_scd2_plan
    ):
        """Regular compile method should succeed without runtime validation"""
        compiler = SQLCompiler('plan-schema/v1/plan.schema.json')
        
        # Regular compile (no runtime validation)
        sql = compiler.compile(sample_scd2_plan)
        
        # Should succeed because it doesn't check table existence
        assert sql is not None
        assert "MERGE INTO" in sql
    
    # ========================================
    # Executor Layer Tests
    # ========================================
    
    @patch('execution.executor.sql.connect')
    def test_executor_with_preflight_check_passes_when_table_exists(
        self, mock_connect, sample_scd2_plan
    ):
        """Executor with pre-flight check should pass when table exists"""
        # Mock SQL connection
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [('customers_source',)]  # Table exists
        mock_cursor.rowcount = 1
        mock_cursor.description = None
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        
        mock_connect.return_value = mock_conn
        
        # Mock workspace client
        mock_workspace_client = Mock()
        mock_workspace_client.config.host = "test.cloud.databricks.com"
        mock_workspace_client.config.token = "test-token"
        
        # Mock execution tracker
        mock_tracker = Mock()
        mock_record = Mock()
        mock_tracker.create_execution.return_value = mock_record
        
        # Create executor
        executor = SQLExecutor(
            workspace_client=mock_workspace_client,
            execution_tracker=mock_tracker
        )
        
        # Execute with pre-flight check
        result = executor.execute(
            plan_id="test-plan-123",
            plan_version="1.0.0",
            sql="SELECT 1",
            warehouse_id="test-warehouse",
            executor_user="test@example.com",
            source_table_fqn="test_catalog.test_schema.customers_source"
        )
        
        assert result is not None
        assert result['state'] == 'SUCCESS'
    
    @patch('execution.executor.sql.connect')
    def test_executor_with_preflight_check_fails_when_table_missing(
        self, mock_connect
    ):
        """Executor with pre-flight check should fail when table is missing"""
        # Mock SQL connection
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []  # Table does not exist
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        
        mock_connect.return_value = mock_conn
        
        # Mock workspace client
        mock_workspace_client = Mock()
        mock_workspace_client.config.host = "test.cloud.databricks.com"
        mock_workspace_client.config.token = "test-token"
        
        # Mock execution tracker
        mock_tracker = Mock()
        
        # Create executor
        executor = SQLExecutor(
            workspace_client=mock_workspace_client,
            execution_tracker=mock_tracker
        )
        
        # Execute with pre-flight check (should fail)
        with pytest.raises(ExecutionError) as exc_info:
            executor.execute(
                plan_id="test-plan-123",
                plan_version="1.0.0",
                sql="SELECT 1",
                warehouse_id="test-warehouse",
                executor_user="test@example.com",
                source_table_fqn="test_catalog.test_schema.missing_table"
            )
        
        error_msg = str(exc_info.value)
        assert "does not exist" in error_msg.lower(), \
            f"Error should mention table doesn't exist. Got: {error_msg}"
        assert "Pre-flight check failed" in error_msg or "Source table does not exist" in error_msg, \
            f"Error should mention pre-flight check. Got: {error_msg}"
    
    @patch('execution.executor.sql.connect')
    def test_executor_without_preflight_check_succeeds(
        self, mock_connect
    ):
        """Executor without pre-flight check should succeed (backward compatibility)"""
        # Mock SQL connection
        mock_cursor = Mock()
        mock_cursor.rowcount = 0
        mock_cursor.description = None
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=False)
        
        mock_connect.return_value = mock_conn
        
        # Mock workspace client
        mock_workspace_client = Mock()
        mock_workspace_client.config.host = "test.cloud.databricks.com"
        mock_workspace_client.config.token = "test-token"
        
        # Mock execution tracker
        mock_tracker = Mock()
        mock_record = Mock()
        mock_tracker.create_execution.return_value = mock_record
        
        # Create executor
        executor = SQLExecutor(
            workspace_client=mock_workspace_client,
            execution_tracker=mock_tracker
        )
        
        # Execute without pre-flight check (no source_table_fqn)
        result = executor.execute(
            plan_id="test-plan-123",
            plan_version="1.0.0",
            sql="SELECT 1",
            warehouse_id="test-warehouse",
            executor_user="test@example.com"
            # No source_table_fqn parameter
        )
        
        assert result is not None
        assert result['state'] == 'SUCCESS'

