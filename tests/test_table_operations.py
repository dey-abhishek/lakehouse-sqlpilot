"""
Test cases for table operations and coordinated execution
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from api.main import app

client = TestClient(app)


class TestTableCheckEndpoint:
    """Test /api/v1/tables/check endpoint"""
    
    @patch('api.main.get_workspace_client')
    def test_table_exists(self, mock_ws_client):
        """Test checking a table that exists"""
        # Mock successful DESCRIBE TABLE response
        mock_result = Mock()
        mock_result.status.state.value = "SUCCEEDED"
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result
        
        response = client.post("/api/v1/tables/check", json={
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "test_table",
            "warehouse_id": "test_warehouse"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["exists"] is True
        assert "test_catalog" in data["table"]
        assert "test_schema" in data["table"]
        assert "test_table" in data["table"]
    
    @patch('api.main.get_workspace_client')
    def test_table_not_found(self, mock_ws_client):
        """Test checking a table that doesn't exist"""
        # Mock TABLE_OR_VIEW_NOT_FOUND error
        mock_ws_client.return_value.statement_execution.execute_statement.side_effect = Exception(
            "TABLE_OR_VIEW_NOT_FOUND: The table cannot be found"
        )
        
        response = client.post("/api/v1/tables/check", json={
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "nonexistent_table",
            "warehouse_id": "test_warehouse"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["exists"] is False
    
    def test_missing_required_fields(self):
        """Test with missing required fields"""
        response = client.post("/api/v1/tables/check", json={
            "catalog": "test_catalog",
            # Missing schema, table, warehouse_id
        })
        
        assert response.status_code == 400
        assert "Missing required fields" in response.json()["detail"]
    
    @patch('api.main.get_workspace_client')
    def test_unexpected_error(self, mock_ws_client):
        """Test handling of unexpected errors"""
        mock_ws_client.return_value.statement_execution.execute_statement.side_effect = Exception(
            "Unexpected database error"
        )
        
        response = client.post("/api/v1/tables/check", json={
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "test_table",
            "warehouse_id": "test_warehouse"
        })
        
        assert response.status_code == 500


class TestTableCreateEndpoint:
    """Test /api/v1/tables/create endpoint"""
    
    @patch('api.main.get_plan_registry')
    @patch('api.main.get_workspace_client')
    def test_create_scd2_table(self, mock_ws_client, mock_registry):
        """Test creating a table for SCD2 pattern"""
        # Mock plan fetch
        mock_plan = {
            "source": {
                "catalog": "src_catalog",
                "schema": "src_schema",
                "table": "src_table"
            },
            "target": {
                "catalog": "tgt_catalog",
                "schema": "tgt_schema",
                "table": "tgt_table"
            },
            "pattern": {"type": "SCD2"},
            "source": {
                "catalog": "src_catalog",
                "schema": "src_schema",
                "table": "src_table",
                "columns": ["id", "name", "email"]
            },
            "pattern_config": {
                "effective_date_column": "eff_date",
                "end_date_column": "end_date",
                "current_flag_column": "is_current"
            }
        }
        mock_registry.return_value.get_plan.return_value = mock_plan
        
        # Mock successful CREATE TABLE
        mock_result = Mock()
        mock_result.statement_id = "stmt_123"
        mock_result.status.state.value = "SUCCEEDED"
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result
        
        response = client.post("/api/v1/tables/create", json={
            "plan_id": "plan_123",
            "warehouse_id": "warehouse_123"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "tgt_table" in data["table"]
        assert data["statement_id"] == "stmt_123"
        
        # Verify CREATE TABLE SQL includes SCD2 columns
        call_args = mock_ws_client.return_value.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "eff_date" in sql
        assert "end_date" in sql
        assert "is_current" in sql
    
    @patch('api.main.get_plan_registry')
    @patch('api.main.get_workspace_client')
    def test_create_regular_table(self, mock_ws_client, mock_registry):
        """Test creating a table for non-SCD2 pattern"""
        mock_plan = {
            "source": {
                "catalog": "src_catalog",
                "schema": "src_schema",
                "table": "src_table"
            },
            "target": {
                "catalog": "tgt_catalog",
                "schema": "tgt_schema",
                "table": "tgt_table"
            },
            "pattern": {"type": "INCREMENTAL_APPEND"}
        }
        mock_registry.return_value.get_plan.return_value = mock_plan
        
        mock_result = Mock()
        mock_result.statement_id = "stmt_456"
        mock_result.status.state.value = "SUCCEEDED"
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result
        
        response = client.post("/api/v1/tables/create", json={
            "plan_id": "plan_456",
            "warehouse_id": "warehouse_456"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        
        # Verify CREATE TABLE uses LIKE syntax for non-SCD2
        call_args = mock_ws_client.return_value.statement_execution.execute_statement.call_args
        sql = call_args.kwargs["statement"]
        assert "LIKE" in sql
    
    @patch('api.main.get_plan_registry')
    def test_plan_not_found(self, mock_registry):
        """Test creating table when plan doesn't exist"""
        mock_registry.return_value.get_plan.return_value = None
        
        response = client.post("/api/v1/tables/create", json={
            "plan_id": "nonexistent_plan",
            "warehouse_id": "warehouse_123"
        })
        
        assert response.status_code == 404
        assert "not found" in response.json()["detail"]
    
    def test_missing_required_fields(self):
        """Test with missing required fields"""
        response = client.post("/api/v1/tables/create", json={
            "plan_id": "plan_123"
            # Missing warehouse_id
        })
        
        assert response.status_code == 400


class TestCoordinatedExecution:
    """Test coordinated multi-statement execution"""
    
    @patch('api.main.get_workspace_client')
    def test_sequential_execution_both_succeed(self, mock_ws_client):
        """Test MERGE then INSERT, both succeed"""
        # Mock two successful statement executions
        mock_result_1 = Mock()
        mock_result_1.statement_id = "stmt_merge_123"
        mock_result_1.status.state.value = "SUCCEEDED"
        
        mock_result_2 = Mock()
        mock_result_2.statement_id = "stmt_insert_456"
        mock_result_2.status.state.value = "SUCCEEDED"
        
        mock_ws_client.return_value.statement_execution.execute_statement.side_effect = [
            mock_result_1,
            mock_result_2
        ]
        
        sql = "MERGE INTO table1 USING table2 ON key = key WHEN MATCHED THEN UPDATE; INSERT INTO table1 SELECT * FROM table2;"
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "plan_123",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_123",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["total_statements"] == 2
        assert len(data["execution_ids"]) == 2
        
        # Verify both statements executed
        assert data["execution_ids"][0]["statement_number"] == 1
        assert data["execution_ids"][0]["status"] == "SUCCEEDED"
        assert data["execution_ids"][1]["statement_number"] == 2
        assert data["execution_ids"][1]["status"] == "SUCCEEDED"
    
    @pytest.mark.lakebase
    @patch('api.main.get_workspace_client')
    def test_merge_fails_insert_never_runs(self, mock_ws_client):
        """Test MERGE fails, INSERT should not execute"""
        # Mock MERGE failure with proper error structure
        mock_result_1 = Mock()
        mock_result_1.statement_id = "stmt_merge_fail"
        mock_result_1.status.state.value = "FAILED"
        mock_result_1.status.error.error_code = "SEMANTIC_ERROR"
        mock_result_1.status.error.message = "Table not found"
        
        # INSERT should never be called
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result_1
        
        sql = "MERGE INTO table1; INSERT INTO table1;"
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "plan_123",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_123",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 500
        # Check for the actual error message format
        detail = response.json()["detail"]
        assert ("Statement 1 failed" in detail or "did not complete successfully" in detail)
        
        # Verify only one statement was attempted
        assert mock_ws_client.return_value.statement_execution.execute_statement.call_count == 1
    
    @patch('api.main.get_workspace_client')
    def test_merge_succeeds_insert_fails(self, mock_ws_client):
        """Test MERGE succeeds but INSERT fails"""
        mock_result_1 = Mock()
        mock_result_1.statement_id = "stmt_merge_123"
        mock_result_1.status.state.value = "SUCCEEDED"
        
        # Mock INSERT execution error
        mock_ws_client.return_value.statement_execution.execute_statement.side_effect = [
            mock_result_1,
            Exception("INSERT failed: syntax error")
        ]
        
        sql = "MERGE INTO table1; INSERT INTO table1;"
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "plan_123",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_123",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 500
        assert "Statement 2 failed" in response.json()["detail"]
        
        # Verify both statements were attempted
        assert mock_ws_client.return_value.statement_execution.execute_statement.call_count == 2
    
    @pytest.mark.lakebase
    @patch('api.main.get_workspace_client')
    def test_120_second_timeout(self, mock_ws_client):
        """Test that statements use 50-second timeout (Databricks max)"""
        mock_result = Mock()
        mock_result.statement_id = "stmt_123"
        mock_result.status.state.value = "SUCCEEDED"
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result
        
        sql = "SELECT * FROM table1;"
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "plan_123",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_123",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 200
        
        # Verify timeout is 50s (Databricks max)
        call_args = mock_ws_client.return_value.statement_execution.execute_statement.call_args
        assert call_args.kwargs["wait_timeout"] == "50s"
    
    @patch('api.main.get_workspace_client')
    def test_single_statement_execution(self, mock_ws_client):
        """Test execution with single statement (no semicolon split)"""
        mock_result = Mock()
        mock_result.statement_id = "stmt_single_123"
        mock_result.status.state.value = "SUCCEEDED"
        mock_ws_client.return_value.statement_execution.execute_statement.return_value = mock_result
        
        sql = "INSERT INTO table1 SELECT * FROM table2"
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "plan_123",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_123",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["total_statements"] == 1
        assert len(data["execution_ids"]) == 1


class TestExecutionIntegration:
    """Integration tests for full execution flow"""
    
    @patch('api.main.get_workspace_client')
    def test_scd2_full_flow(self, mock_ws_client):
        """Test full SCD2 execution: MERGE expires, INSERT adds new versions"""
        # Mock both statements succeeding
        mock_merge = Mock()
        mock_merge.statement_id = "merge_001"
        mock_merge.status.state.value = "SUCCEEDED"
        
        mock_insert = Mock()
        mock_insert.statement_id = "insert_001"
        mock_insert.status.state.value = "SUCCEEDED"
        
        mock_ws_client.return_value.statement_execution.execute_statement.side_effect = [
            mock_merge,
            mock_insert
        ]
        
        # Realistic SCD2 SQL
        sql = """
        MERGE INTO target AS t
        USING source AS s
        ON t.id = s.id AND t.is_current = TRUE
        WHEN MATCHED THEN UPDATE SET t.is_current = FALSE, t.end_date = CURRENT_TIMESTAMP();
        
        INSERT INTO target
        SELECT *, CURRENT_TIMESTAMP() AS eff_date, TRUE AS is_current
        FROM source;
        """
        
        response = client.post("/api/v1/plans/execute", json={
            "plan_id": "scd2_plan",
            "plan_version": "1.0.0",
            "sql": sql,
            "warehouse_id": "warehouse_scd2",
            "executor_user": "test@example.com"
        })
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert data["total_statements"] == 2
        
        # Verify execution order
        calls = mock_ws_client.return_value.statement_execution.execute_statement.call_args_list
        assert "MERGE" in calls[0].kwargs["statement"]
        assert "INSERT" in calls[1].kwargs["statement"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

