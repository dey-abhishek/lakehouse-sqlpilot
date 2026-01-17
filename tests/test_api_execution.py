"""
API Execution Endpoint Tests
Tests the execution and status endpoints for plan execution
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.main import app

@pytest.fixture
def client():
    """Create test client for FastAPI app"""
    return TestClient(app)


class TestExecutionEndpoints:
    """Test plan execution endpoints"""
    
    @pytest.mark.skip(reason="Async execution requires live database connection")
    @patch('api.main.SQLExecutor')
    @patch('api.main.ExecutionTracker')
    @patch('os.getenv')
    def test_execute_plan_success(self, mock_getenv, mock_tracker_class, mock_executor_class, client):
        """Test successful plan execution"""
        # This test would require real async execution setup
        # Skipped for now - execution endpoints are tested in integration tests
        pass
    
    @patch('os.getenv')
    def test_execute_plan_missing_credentials(self, mock_getenv, client):
        """Test execution fails with missing credentials"""
        mock_getenv.return_value = ""
        
        request = {
            "plan_id": "plan-123",
            "plan_version": "1.0.0",
            "sql": "INSERT INTO table SELECT * FROM source",
            "warehouse_id": "wh-123",
            "executor_user": "test@databricks.com"
        }
        
        response = client.post("/api/v1/plans/execute", json=request)
        
        assert response.status_code == 500
        data = response.json()
        assert "credentials" in data["detail"].lower()
    
    @patch('api.main.ExecutionTracker')
    @patch('os.getenv')
    def test_get_execution_status_success(self, mock_getenv, mock_tracker_class, client):
        """Test getting execution status"""
        # Mock environment variables
        def getenv_side_effect(key, default=""):
            env_vars = {
                "DATABRICKS_HOST": "https://test.databricks.com",
                "DATABRICKS_TOKEN": "test_token",
                "DATABRICKS_WAREHOUSE_ID": "wh-123"
            }
            return env_vars.get(key, default)
        
        mock_getenv.side_effect = getenv_side_effect
        
        # Mock execution record
        mock_execution = type('ExecutionRecord', (), {
            'state': 'SUCCEEDED',
            'query_id': 'query-123',
            'error_message': None,
            'rows_affected': 100,
            'started_at': None,
            'completed_at': None
        })()
        
        # Mock tracker
        mock_tracker = Mock()
        mock_tracker.get_execution.return_value = mock_execution
        mock_tracker_class.return_value = mock_tracker
        
        response = client.get("/api/v1/executions/exec-123")
        
        assert response.status_code == 200
        data = response.json()
        assert data["execution_id"] == "exec-123"
        assert data["state"] == "SUCCEEDED"
        assert data["query_id"] == "query-123"
        assert data["rows_affected"] == 100
    
    @patch('api.main.ExecutionTracker')
    @patch('os.getenv')
    def test_get_execution_status_not_found(self, mock_getenv, mock_tracker_class, client):
        """Test getting status for non-existent execution"""
        # Mock environment variables
        def getenv_side_effect(key, default=""):
            env_vars = {
                "DATABRICKS_HOST": "https://test.databricks.com",
                "DATABRICKS_TOKEN": "test_token",
                "DATABRICKS_WAREHOUSE_ID": "wh-123"
            }
            return env_vars.get(key, default)
        
        mock_getenv.side_effect = getenv_side_effect
        
        # Mock tracker returning None
        mock_tracker = Mock()
        mock_tracker.get_execution.return_value = None
        mock_tracker_class.return_value = mock_tracker
        
        response = client.get("/api/v1/executions/nonexistent")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()


class TestAgentEndpoints:
    """Test agent suggestion endpoints"""
    
    @patch('agents.plan_suggestion_agent.PlanSuggestionAgent')
    @patch('api.main.workspace_client')
    def test_agent_suggest_plan(self, mock_workspace_client, mock_agent_class, client):
        """Test agent plan suggestion"""
        # Mock agent
        mock_agent = Mock()
        mock_agent.suggest_plan.return_value = {
            "plan": {
                "plan_name": "suggested_plan",
                "pattern": "incremental_append"
            },
            "confidence": 0.85,
            "explanation": "Based on your intent, this pattern is recommended",
            "warnings": []
        }
        mock_agent_class.return_value = mock_agent
        
        request = {
            "intent": "I want to incrementally load customer data daily",
            "user": "test@databricks.com",
            "context": {"source": "customers_raw", "target": "customers_curated"}
        }
        
        response = client.post("/api/v1/agent/suggest", json=request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "suggested_plan" in data
        assert data["confidence"] == 0.85
        assert "explanation" in data


class TestPlanCRUDEndpoints:
    """Test plan CRUD endpoints"""
    
    @patch('api.main.compiler')
    def test_save_plan_success(self, mock_compiler, client):
        """Test saving a valid plan"""
        # Mock validation
        mock_compiler.validate_plan.return_value = (True, [])
        
        request = {
            "plan": {
                "plan_name": "test_plan",
                "version": "1.0.0",
                "owner": "test@databricks.com",
                "pattern": "incremental_append"
            },
            "user": "test@databricks.com"
        }
        
        response = client.post("/api/v1/plans", json=request)
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
        assert "plan_id" in data
    
    @patch('api.main.compiler')
    def test_save_invalid_plan_fails(self, mock_compiler, client):
        """Test saving an invalid plan fails"""
        # Mock validation failure
        mock_compiler.validate_plan.return_value = (False, ["Invalid plan_name format"])
        
        request = {
            "plan": {
                "plan_name": "Invalid Plan Name!",
                "version": "1.0.0",
                "owner": "test@databricks.com",
                "pattern": "incremental_append"
            },
            "user": "test@databricks.com"
        }
        
        response = client.post("/api/v1/plans", json=request)
        
        assert response.status_code == 400
        data = response.json()
        assert "validation failed" in data["detail"]["message"].lower()
    
    def test_list_plans_no_filters(self, client):
        """Test listing all plans"""
        response = client.get("/api/v1/plans")
        
        assert response.status_code == 200
        data = response.json()
        assert "plans" in data
        assert "total" in data
        assert isinstance(data["plans"], list)
    
    def test_list_plans_with_filters(self, client):
        """Test listing plans with filters"""
        response = client.get("/api/v1/plans?owner=data-team@company.com&pattern_type=INCREMENTAL_APPEND")
        
        assert response.status_code == 200
        data = response.json()
        assert "plans" in data
        # All returned plans should match filters (when real backend is implemented)
    
    def test_get_plan_by_id(self, client):
        """Test getting a specific plan"""
        response = client.get("/api/v1/plans/1")
        
        assert response.status_code == 200
        data = response.json()
        assert data["plan_id"] == "1"
        assert "plan_name" in data
        assert "pattern_type" in data
    
    def test_get_nonexistent_plan(self, client):
        """Test getting a non-existent plan"""
        response = client.get("/api/v1/plans/999999")
        
        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

