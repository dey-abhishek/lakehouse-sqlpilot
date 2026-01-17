"""
End-to-End Integration Tests

Tests the complete flow from plan creation to execution.
"""

import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock, MagicMock, patch

from compiler import SQLCompiler
from execution import SQLExecutor, ExecutionTracker, ExecutionState
from unity_catalog import PermissionValidator, LineageTracker
from preview import PreviewEngine
from agents import PlanSuggestionAgent


@pytest.fixture
def mock_workspace_client():
    """Mock Databricks workspace client"""
    client = Mock()
    
    # Mock catalogs
    client.catalogs.get = Mock(return_value=Mock(name='lakehouse-sqlpilot'))
    
    # Mock schemas
    client.schemas.get = Mock(return_value=Mock(full_name='lakehouse-sqlpilot.lakehouse-sqlpilot-schema'))
    
    # Mock tables
    mock_table = Mock()
    mock_table.name = 'test_table'
    
    # Create column mocks with properly set string attributes
    mock_col_id = Mock()
    mock_col_id.name = 'id'
    mock_col_id.type_name = 'STRING'
    mock_col_id.nullable = False
    
    mock_col_created = Mock()
    mock_col_created.name = 'created_at'
    mock_col_created.type_name = 'TIMESTAMP'
    mock_col_created.nullable = False
    
    mock_table.columns = [mock_col_id, mock_col_created]
    client.tables.get = Mock(return_value=mock_table)
    client.tables.list = Mock(return_value=[mock_table])
    
    # Mock grants - Return grants for all permission checks
    def mock_get_grants(securable_type, full_name):
        """Mock grants response for any securable"""
        mock_privilege_assignment = Mock()
        mock_privilege_assignment.principal = 'test@databricks.com'
        
        # Mock privilege objects with proper value attribute
        select_priv = Mock()
        select_priv.value = 'SELECT'
        
        modify_priv = Mock()
        modify_priv.value = 'MODIFY'
        
        use_catalog_priv = Mock()
        use_catalog_priv.value = 'USE_CATALOG'
        
        use_schema_priv = Mock()
        use_schema_priv.value = 'USE_SCHEMA'
        
        # Return all privileges for testing
        mock_privilege_assignment.privileges = [select_priv, modify_priv, use_catalog_priv, use_schema_priv]
        
        mock_grants = Mock()
        mock_grants.privilege_assignments = [mock_privilege_assignment]
        return mock_grants
    
    client.grants.get = Mock(side_effect=mock_get_grants)
    
    return client


@pytest.fixture
def test_plan():
    """Test plan for end-to-end testing"""
    return {
        'schema_version': '1.0',
        'plan_metadata': {
            'plan_id': str(uuid.uuid4()),
            'plan_name': 'e2e_test_plan',
            'description': 'End-to-end test plan',
            'owner': 'test@databricks.com',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'version': '1.0.0'
        },
        'pattern': {'type': 'INCREMENTAL_APPEND'},
        'source': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'events_raw'
        },
        'target': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'events_processed',
            'write_mode': 'append'
        },
        'pattern_config': {
            'watermark_column': 'created_at',
            'watermark_type': 'timestamp'
        },
        'execution_config': {
            'warehouse_id': 'test_warehouse',
            'timeout_seconds': 3600,
            'max_retries': 3
        }
    }


class TestEndToEndPlanExecution:
    """Test complete plan execution flow"""
    
    def test_plan_to_sql_to_preview_flow(self, test_plan, mock_workspace_client):
        """Test flow: Plan -> Validation -> Compilation -> Preview"""
        
        # 1. Validate plan
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        is_valid, errors = compiler.validate_plan(test_plan)
        assert is_valid, f"Plan validation failed: {errors}"
        
        # 2. Compile to SQL
        sql = compiler.compile(test_plan)
        assert sql is not None
        assert 'INSERT INTO' in sql
        
        # 3. Validate permissions
        perm_validator = PermissionValidator(mock_workspace_client)
        has_perms, violations = perm_validator.validate_plan_permissions(
            test_plan, 
            'test@databricks.com'
        )
        assert has_perms, f"Permission check failed: {violations}"
        
        # 4. Generate preview
        mock_executor = Mock()
        mock_executor.preview_sql = Mock(return_value={
            'columns': ['id', 'created_at'],
            'rows': [['1', '2026-01-16']],
            'row_count': 1
        })
        
        preview_engine = PreviewEngine(compiler, perm_validator, mock_executor)
        preview = preview_engine.preview_plan(
            test_plan,
            'test@databricks.com',
            'test_warehouse'
        )
        
        assert preview['validation']['is_valid'] is True
        
        # Check compilation
        if not preview['compilation']['success']:
            raise AssertionError(f"Compilation failed: {preview['compilation'].get('error')}")
        
        assert preview['compilation']['sql'] is not None
        assert preview['permissions']['has_permissions'] is True
    
    @patch('execution.executor.sql.connect')
    def test_plan_execution_with_tracking(self, mock_sql_connect, test_plan, mock_workspace_client):
        """Test plan execution with state tracking"""
        
        # Mock SQL connection and cursor
        mock_cursor = Mock()
        mock_cursor.execute = Mock()
        mock_cursor.rowcount = 100
        mock_cursor.description = None
        mock_cursor.query_id = 'query-123'
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=False)
        
        mock_connection = Mock()
        mock_connection.cursor = Mock(return_value=mock_cursor)
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        mock_sql_connect.return_value = mock_connection
        
        # Mock storage
        mock_storage = Mock()
        
        # Create a mock execution record object with attributes
        class MockExecutionRecord:
            def __init__(self, execution_id, plan_id):
                self.execution_id = execution_id
                self.state = ExecutionState.RUNNING.value
                self.plan_id = plan_id
                self.query_id = None
                self.error_message = None
                self.rows_affected = None
                self.started_at = None
                self.completed_at = None
        
        # Storage needs to return execution record when queried
        def mock_get_execution(execution_id):
            return MockExecutionRecord(execution_id, test_plan['plan_metadata']['plan_id'])
        
        mock_storage.save_execution = Mock()
        mock_storage.get_execution = Mock(side_effect=mock_get_execution)
        mock_storage.update_execution = Mock()
        
        # Create tracker and executor
        tracker = ExecutionTracker(mock_storage)
        executor = SQLExecutor(mock_workspace_client, tracker)
        
        # Compile SQL
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        sql = compiler.compile(test_plan)
        
        # Execute
        result = executor.execute(
            plan_id=test_plan['plan_metadata']['plan_id'],
            plan_version=test_plan['plan_metadata']['version'],
            sql=sql,
            warehouse_id='test_warehouse',
            executor_user='test@databricks.com'
        )
        
        assert result['state'] == ExecutionState.SUCCESS.value
        assert result['rows_affected'] == 100
        assert mock_storage.save_execution.called


class TestAgentIntegration:
    """Test agent integration in workflows"""
    
    def test_plan_suggestion_agent(self, mock_workspace_client):
        """Test plan suggestion agent generates valid plan"""
        
        agent = PlanSuggestionAgent(mock_workspace_client)
        
        input_data = {
            'request_type': 'suggest_plan',
            'user_intent': 'Load customer events incrementally every day',
            'source_table': 'lakehouse-sqlpilot.lakehouse-sqlpilot-schema.events_raw',
            'target_table': 'lakehouse-sqlpilot.lakehouse-sqlpilot-schema.events_processed'
        }
        
        result = agent.process(input_data)
        
        assert result['success'] is True
        assert result['recommended_pattern'] == 'INCREMENTAL_APPEND'
        assert 'suggested_plan' in result
        
        # Validate suggested plan
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        is_valid, errors = compiler.validate_plan(result['suggested_plan'])
        assert is_valid, f"Agent generated invalid plan: {errors}"


class TestSCD2EndToEnd:
    """Test SCD2 pattern end-to-end"""
    
    @pytest.fixture
    def scd2_plan(self):
        """SCD2 test plan"""
        return {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'e2e_scd2_test',
                'description': 'SCD2 end-to-end test',
                'owner': 'test@databricks.com',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {'type': 'SCD2'},
            'source': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'customer_current',
                'columns': ['customer_id', 'name', 'email']
            },
            'target': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'customer_dim',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'business_keys': ['customer_id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current',
                'end_date_default': '9999-12-31 23:59:59',
                'compare_columns': ['name', 'email']
            },
            'execution_config': {
                'warehouse_id': 'test_warehouse',
                'timeout_seconds': 3600,
                'max_retries': 3
            }
        }
    
    def test_scd2_compilation_and_validation(self, scd2_plan):
        """Test SCD2 plan validates and compiles"""
        
        # Validate
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        is_valid, errors = compiler.validate_plan(scd2_plan)
        assert is_valid, f"SCD2 validation failed: {errors}"
        
        # Compile
        sql = compiler.compile(scd2_plan)
        assert '-- STEP 2: Expire changed records' in sql
        assert '-- STEP 3: Insert new versions and new records' in sql
        assert 'MERGE INTO' in sql
        assert 'INSERT INTO' in sql
    
    def test_scd2_preview_shows_changes(self, scd2_plan, mock_workspace_client):
        """Test SCD2 preview shows records to close and insert"""
        
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        perm_validator = PermissionValidator(mock_workspace_client)
        
        mock_executor = Mock()
        mock_executor.preview_sql = Mock(return_value={
            'columns': ['customer_id', 'name', 'preview_action'],
            'rows': [
                ['C001', 'John', 'WILL_BE_CLOSED'],
                ['C001', 'John Updated', 'NEW_VERSION']
            ],
            'row_count': 2
        })
        
        preview_engine = PreviewEngine(compiler, perm_validator, mock_executor)
        preview = preview_engine.preview_plan(
            scd2_plan,
            'test@databricks.com',
            'test_warehouse',
            include_sample_data=True
        )
        
        assert preview['validation']['is_valid'] is True
        assert preview['impact_analysis']['operation_type'] == 'SCD2_HISTORY_TRACKING'
        assert preview['sample_data']['success'] is True


class TestIdempotencyValidation:
    """Test idempotency of SQL execution"""
    
    def test_incremental_append_idempotent(self, test_plan):
        """Test incremental append can be re-run safely"""
        
        compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
        
        # Generate SQL twice with same context
        context = {
            'execution_id': 'test-123',
            'generated_at': '2026-01-16T10:00:00Z',
            'execution_date': '2026-01-16',
            'execution_timestamp': '2026-01-16T10:00:00Z',
            'variables': {}
        }
        
        sql1 = compiler.compile(test_plan, context)
        sql2 = compiler.compile(test_plan, context)
        
        # Same plan + same context = same SQL
        assert sql1 == sql2
        
        # SQL uses MAX watermark, so re-running with no new data does nothing
        assert 'MAX(' in sql1
        assert 'COALESCE' in sql1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

