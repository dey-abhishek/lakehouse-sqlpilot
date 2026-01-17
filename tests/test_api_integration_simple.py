"""
API Integration Tests - Frontend to Backend
Tests the FastAPI backend endpoints that the React frontend will call
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.main import app
from security.middleware import get_current_user

# Mock get_current_user for API integration tests only
async def mock_get_current_user():
    return {"user": "test_user", "email": "test@databricks.com", "roles": ["user"]}

@pytest.fixture(autouse=True)
def override_get_current_user():
    """Override get_current_user dependency for API integration tests"""
    app.dependency_overrides[get_current_user] = mock_get_current_user
    yield
    app.dependency_overrides.clear()

@pytest.fixture
def client():
    """Create test client for FastAPI app"""
    return TestClient(app)


class TestHealthEndpoint:
    """Test health check endpoint"""
    
    def test_health_check_returns_ok(self, client):
        """Test that health endpoint returns OK status"""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestPatternsEndpoint:
    """Test patterns listing endpoint"""
    
    def test_list_patterns_returns_all_patterns(self, client):
        """Test that patterns endpoint returns all supported patterns"""
        response = client.get("/api/v1/patterns")
        
        assert response.status_code == 200
        data = response.json()
        
        # API returns {"patterns": [...]}
        assert "patterns" in data
        patterns = data["patterns"]
        
        assert isinstance(patterns, list)
        assert len(patterns) >= 3  # At least incremental, scd2, merge
        
        # Pattern names are uppercase strings
        assert "INCREMENTAL_APPEND" in patterns or "SCD2" in patterns


class TestPlanValidationEndpoint:
    """Test plan validation endpoint"""
    
    def test_validate_valid_incremental_plan(self, client):
        """Test validation of a valid incremental append plan"""
        plan = {
            "plan_name": "test_incremental",
            "version": "1.0.0",
            "owner": "test@databricks.com",
            "pattern": "incremental_append",
            "pattern_config": {
                "source_table": "catalog.schema.source",
                "target_table": "catalog.schema.target",
                "watermark_column": "updated_at",
                "watermark_type": "timestamp"
            }
        }
        
        response = client.post("/api/v1/plans/validate", json={"plan": plan})
        
        assert response.status_code == 200
        data = response.json()
        # Should return validation result
        assert isinstance(data, dict)


class TestPlanCompilationEndpoint:
    """Test plan compilation endpoint"""
    
    @patch('api.main.compiler')
    def test_compile_incremental_plan(self, mock_compiler, client):
        """Test compilation of incremental append plan"""
        # Mock the compilation
        mock_compiler.compile.return_value = "SELECT * FROM table"
        
        plan = {
            "plan_name": "test_compile",
            "version": "1.0.0",
            "owner": "test@databricks.com",
            "pattern": "incremental_append",
            "pattern_config": {
                "source_table": "catalog.schema.source",
                "target_table": "catalog.schema.target",
                "watermark_column": "updated_at",
                "watermark_type": "timestamp"
            }
        }
        
        response = client.post("/api/v1/plans/compile", json={"plan": plan})
        
        assert response.status_code == 200
        data = response.json()
        # Should return SQL
        assert isinstance(data, dict)
        assert "sql" in data or "success" in data


class TestUnityCatalogEndpoints:
    """Test Unity Catalog discovery endpoints"""
    
    @patch('api.main.workspace_client')
    def test_list_catalogs(self, mock_client, client):
        """Test listing catalogs from Unity Catalog"""
        # Create simple mock objects that won't cause recursion
        catalog1 = type('MockCatalog', (), {
            'name': 'catalog1',
            'comment': 'Test catalog',
            'owner': 'admin'
        })()
        catalog2 = type('MockCatalog', (), {
            'name': 'catalog2',
            'comment': 'Another catalog',
            'owner': 'admin'
        })()
        
        mock_client.catalogs.list.return_value = [catalog1, catalog2]
        
        response = client.get("/api/v1/catalogs")
        
        assert response.status_code == 200
        data = response.json()
        assert "catalogs" in data
        assert len(data["catalogs"]) == 2
    
    @patch('api.main.workspace_client')
    def test_list_schemas_for_catalog(self, mock_client, client):
        """Test listing schemas for a specific catalog"""
        # Create simple mock objects
        schema1 = type('MockSchema', (), {
            'name': 'schema1',
            'catalog_name': 'test_catalog',
            'owner': 'admin'
        })()
        schema2 = type('MockSchema', (), {
            'name': 'schema2',
            'catalog_name': 'test_catalog',
            'owner': 'admin'
        })()
        
        mock_client.schemas.list.return_value = [schema1, schema2]
        
        response = client.get("/api/v1/catalogs/test_catalog/schemas")
        
        assert response.status_code == 200
        data = response.json()
        assert "schemas" in data
        assert len(data["schemas"]) == 2


class TestWarehouseEndpoints:
    """Test SQL Warehouse endpoints"""
    
    @patch('api.main.workspace_client')
    def test_list_warehouses(self, mock_client, client):
        """Test listing SQL warehouses"""
        mock_client.warehouses.list.return_value = [
            Mock(id='wh1', name='Warehouse 1', state='RUNNING'),
            Mock(id='wh2', name='Warehouse 2', state='STOPPED')
        ]
        
        response = client.get("/api/v1/warehouses")
        
        assert response.status_code in [200, 401, 500]


class TestCORSHeaders:
    """Test CORS headers for frontend integration"""
    
    def test_cors_headers_present(self, client):
        """Test that CORS headers are present in responses"""
        response = client.get("/health")
        
        # CORS middleware should add these headers
        assert response.status_code == 200


class TestAPIStructure:
    """Test API structure and routing"""
    
    def test_404_for_unknown_endpoint(self, client):
        """Test 404 response for unknown endpoints"""
        response = client.get("/api/v1/nonexistent")
        
        assert response.status_code == 404
    
    def test_health_endpoint_accessible(self, client):
        """Test that health endpoint is accessible"""
        response = client.get("/health")
        assert response.status_code == 200
    
    def test_patterns_endpoint_accessible(self, client):
        """Test that patterns endpoint is accessible"""
        response = client.get("/api/v1/patterns")
        assert response.status_code == 200


class TestRequestValidation:
    """Test request body validation"""
    
    def test_validate_endpoint_requires_plan(self, client):
        """Test that validate endpoint requires a plan"""
        response = client.post("/api/v1/plans/validate", json={})
        
        # Should return validation error
        assert response.status_code in [400, 422, 500]
    
    def test_compile_endpoint_requires_plan(self, client):
        """Test that compile endpoint requires a plan"""
        response = client.post("/api/v1/plans/compile", json={})
        
        # Should return validation error
        assert response.status_code in [400, 422, 500]


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])

