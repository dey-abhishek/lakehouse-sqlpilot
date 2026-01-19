"""
Test Suite for Lakebase Plan Registry
Tests plan persistence, retrieval, and CRUD operations
"""

import pytest
import os
import uuid
import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock


def is_lakebase_enabled():
    """Check if Lakebase is enabled (evaluated at runtime, not import time)"""
    return os.getenv("LAKEBASE_ENABLED", "false").lower() == "true"


@pytest.fixture
def sample_plan():
    """Sample valid plan for testing"""
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": str(uuid.uuid4()),
            "plan_name": "test_customer_scd2",
            "owner": "test@example.com",
            "description": "Test SCD2 plan for customer dimension",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
            "tags": {"env": "test", "team": "data-engineering"}
        },
        "pattern": {
            "type": "SCD2"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "customers_source"
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "test_schema",
            "table": "customers_dim",
            "write_mode": "merge"
        },
        "pattern_config": {
            "business_keys": ["customer_id"],
            "compare_columns": ["name", "email", "city"],
            "effective_date_column": "valid_from",
            "end_date_column": "valid_to",
            "current_flag_column": "is_current"
        },
        "execution_config": {
            "warehouse_id": "test-warehouse-123"
        }
    }


@pytest.fixture
def mock_lakebase_backend():
    """Mock LakebaseBackend for testing without real database"""
    from contextlib import contextmanager
    
    mock_backend = Mock()
    
    # Mock connection and cursor
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Setup cursor context manager
    mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = Mock(return_value=None)
    mock_conn.commit = Mock()
    
    # Setup connection context manager using contextmanager decorator
    @contextmanager
    def mock_get_connection():
        yield mock_conn
    
    # Assign the context manager to the mock
    mock_backend._get_connection = mock_get_connection
    
    return mock_backend, mock_cursor


class TestLakebasePlanRegistryIntegration:
    """Integration tests with real Lakebase (only run when LAKEBASE_ENABLED=true)"""
    
    @pytest.fixture(autouse=True)
    def check_lakebase_enabled(self):
        """Auto-check if Lakebase is enabled before each integration test"""
        enabled = is_lakebase_enabled()
        print(f"\n[DEBUG] check_lakebase_enabled called: LAKEBASE_ENABLED={os.getenv('LAKEBASE_ENABLED')}, enabled={enabled}")
        if not enabled:
            pytest.skip("Lakebase not enabled (set LAKEBASE_ENABLED=true in .env.dev)")
    
    @pytest.fixture(autouse=True)
    def setup_registry(self):
        """Setup plan registry for each test"""
        from plan_registry import get_plan_registry
        
        # Get or create registry
        self.registry = get_plan_registry()
        
        # Track created plan IDs for cleanup
        self.created_plan_ids = []
        
        yield
        
        # Cleanup: delete test plans
        for plan_id in self.created_plan_ids:
            try:
                self.registry.delete_plan(plan_id)
            except:
                pass
    
    def test_save_new_plan(self, sample_plan):
        """Test saving a new plan to Lakebase"""
        result = self.registry.save_plan(sample_plan)
        
        # Track for cleanup
        self.created_plan_ids.append(result["plan_id"])
        
        # Assertions
        assert result["success"] is True
        assert "plan_id" in result
        assert "created successfully" in result["message"]
        assert "created_at" in result
        assert "updated_at" in result
    
    def test_get_plan_by_id(self, sample_plan):
        """Test retrieving a plan by ID"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        self.created_plan_ids.append(plan_id)
        
        # Retrieve plan
        retrieved_plan = self.registry.get_plan(plan_id)
        
        # Assertions
        assert retrieved_plan is not None
        assert retrieved_plan["plan_metadata"]["plan_id"] == plan_id
        assert retrieved_plan["plan_metadata"]["plan_name"] == sample_plan["plan_metadata"]["plan_name"]
        assert retrieved_plan["pattern"]["type"] == sample_plan["pattern"]["type"]
        assert "_metadata" in retrieved_plan
        assert retrieved_plan["_metadata"]["status"] == "active"
    
    def test_get_nonexistent_plan(self):
        """Test retrieving a non-existent plan returns None"""
        fake_id = str(uuid.uuid4())
        result = self.registry.get_plan(fake_id)
        assert result is None
    
    def test_update_existing_plan(self, sample_plan):
        """Test updating an existing plan"""
        # Save initial plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        self.created_plan_ids.append(plan_id)
        
        # Update plan
        sample_plan["plan_metadata"]["description"] = "Updated description"
        sample_plan["plan_metadata"]["version"] = "1.1.0"
        
        update_result = self.registry.save_plan(sample_plan)
        
        # Assertions
        assert update_result["success"] is True
        assert "updated successfully" in update_result["message"]
        
        # Verify update
        retrieved = self.registry.get_plan(plan_id)
        assert retrieved["plan_metadata"]["description"] == "Updated description"
        assert retrieved["plan_metadata"]["version"] == "1.1.0"
    
    def test_list_plans_no_filters(self, sample_plan):
        """Test listing all plans"""
        # Create test plans
        for i in range(3):
            plan = sample_plan.copy()
            plan["plan_metadata"] = sample_plan["plan_metadata"].copy()
            plan["plan_metadata"]["plan_id"] = str(uuid.uuid4())
            plan["plan_metadata"]["plan_name"] = f"test_plan_{i}"
            
            result = self.registry.save_plan(plan)
            self.created_plan_ids.append(result["plan_id"])
        
        # List plans
        result = self.registry.list_plans(limit=100)
        
        # Assertions
        assert "plans" in result
        assert "total" in result
        assert len(result["plans"]) >= 3  # At least our 3 test plans
        assert result["total"] >= 3
    
    def test_list_plans_filter_by_owner(self, sample_plan):
        """Test listing plans filtered by owner"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        self.created_plan_ids.append(save_result["plan_id"])
        
        # List by owner
        result = self.registry.list_plans(owner="test@example.com")
        
        # Assertions
        assert len(result["plans"]) >= 1
        for plan in result["plans"]:
            assert plan["owner"] == "test@example.com"
    
    def test_list_plans_filter_by_pattern_type(self, sample_plan):
        """Test listing plans filtered by pattern type"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        self.created_plan_ids.append(save_result["plan_id"])
        
        # List by pattern type
        result = self.registry.list_plans(pattern_type="SCD2")
        
        # Assertions
        assert len(result["plans"]) >= 1
        for plan in result["plans"]:
            assert plan["pattern_type"] == "SCD2"
    
    def test_list_plans_filter_by_status(self, sample_plan):
        """Test listing plans filtered by status"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        self.created_plan_ids.append(save_result["plan_id"])
        
        # List active plans
        result = self.registry.list_plans(status="active")
        
        # Assertions
        assert len(result["plans"]) >= 1
        for plan in result["plans"]:
            assert plan["status"] == "active"
    
    def test_list_plans_pagination(self, sample_plan):
        """Test plan listing with pagination"""
        # Create multiple test plans
        for i in range(5):
            plan = sample_plan.copy()
            plan["plan_metadata"] = sample_plan["plan_metadata"].copy()
            plan["plan_metadata"]["plan_id"] = str(uuid.uuid4())
            plan["plan_metadata"]["plan_name"] = f"test_pagination_{i}"
            
            result = self.registry.save_plan(plan)
            self.created_plan_ids.append(result["plan_id"])
        
        # Get first page
        page1 = self.registry.list_plans(limit=2, offset=0)
        assert len(page1["plans"]) == 2
        assert page1["limit"] == 2
        assert page1["offset"] == 0
        
        # Get second page
        page2 = self.registry.list_plans(limit=2, offset=2)
        assert len(page2["plans"]) == 2
        assert page2["offset"] == 2
        
        # Plans should be different
        page1_ids = [p["plan_id"] for p in page1["plans"]]
        page2_ids = [p["plan_id"] for p in page2["plans"]]
        assert len(set(page1_ids) & set(page2_ids)) == 0  # No overlap
    
    def test_delete_plan(self, sample_plan):
        """Test soft-deleting a plan"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        
        # Delete plan
        delete_result = self.registry.delete_plan(plan_id)
        
        # Assertions
        assert delete_result["success"] is True
        assert "archived successfully" in delete_result["message"]
        
        # Verify plan is archived
        retrieved = self.registry.get_plan(plan_id)
        assert retrieved["_metadata"]["status"] == "archived"
    
    def test_update_plan_status(self, sample_plan):
        """Test updating plan status"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        self.created_plan_ids.append(plan_id)
        
        # Update status to deprecated
        update_result = self.registry.update_plan_status(plan_id, "deprecated")
        
        # Assertions
        assert update_result["success"] is True
        
        # Verify status changed
        retrieved = self.registry.get_plan(plan_id)
        assert retrieved["_metadata"]["status"] == "deprecated"
    
    def test_invalid_status_raises_error(self, sample_plan):
        """Test updating to invalid status raises ValueError"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        self.created_plan_ids.append(plan_id)
        
        # Try invalid status
        with pytest.raises(ValueError, match="Invalid status"):
            self.registry.update_plan_status(plan_id, "invalid_status")
    
    def test_jsonb_query_source_catalog(self, sample_plan):
        """Test querying plans by JSONB fields (source catalog)"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        self.created_plan_ids.append(save_result["plan_id"])
        
        # Query using raw SQL (demonstrates JSONB querying capability)
        with self.registry.backend._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT plan_id, plan_name 
                    FROM plans 
                    WHERE plan_json->'source'->>'catalog' = %s
                """, ("test_catalog",))
                
                results = cursor.fetchall()
                assert len(results) >= 1
    
    def test_timestamp_auto_update(self, sample_plan):
        """Test that updated_at timestamp auto-updates"""
        # Save plan
        save_result = self.registry.save_plan(sample_plan)
        plan_id = save_result["plan_id"]
        self.created_plan_ids.append(plan_id)
        
        # Get initial timestamps
        initial = self.registry.get_plan(plan_id)
        initial_updated_at = initial["_metadata"]["updated_at"]
        
        # Wait a moment and update
        import time
        time.sleep(1)
        
        sample_plan["plan_metadata"]["description"] = "Updated"
        self.registry.save_plan(sample_plan)
        
        # Get updated timestamps
        updated = self.registry.get_plan(plan_id)
        updated_updated_at = updated["_metadata"]["updated_at"]
        
        # Assertion: updated_at should have changed
        assert updated_updated_at > initial_updated_at


class TestPlanRegistryUnit:
    """Unit tests with mocked Lakebase backend"""
    
    @patch('plan_registry.plan_storage.PlanRegistry._initialize_schema')
    def test_save_plan_insert(self, mock_init_schema, mock_lakebase_backend, sample_plan):
        """Test save_plan performs INSERT for new plan"""
        from plan_registry.plan_storage import PlanRegistry
        
        mock_backend, mock_cursor = mock_lakebase_backend
        
        # Mock: Plan doesn't exist
        mock_cursor.fetchone.side_effect = [
            None,  # SELECT to check if exists
            (sample_plan["plan_metadata"]["plan_id"], datetime.now(timezone.utc), datetime.now(timezone.utc))  # INSERT RETURNING
        ]
        
        registry = PlanRegistry(mock_backend)
        result = registry.save_plan(sample_plan)
        
        # Verify INSERT was called
        assert mock_cursor.execute.call_count >= 2
        insert_call = [call for call in mock_cursor.execute.call_args_list if "INSERT" in str(call)]
        assert len(insert_call) > 0
        
        assert result["success"] is True
        assert "created successfully" in result["message"]
    
    @patch('plan_registry.plan_storage.PlanRegistry._initialize_schema')
    def test_save_plan_update(self, mock_init_schema, mock_lakebase_backend, sample_plan):
        """Test save_plan performs UPDATE for existing plan"""
        from plan_registry.plan_storage import PlanRegistry
        
        mock_backend, mock_cursor = mock_lakebase_backend
        
        # Mock: Plan exists
        mock_cursor.fetchone.side_effect = [
            (sample_plan["plan_metadata"]["plan_id"],),  # SELECT to check if exists
            (sample_plan["plan_metadata"]["plan_id"], datetime.now(timezone.utc), datetime.now(timezone.utc))  # UPDATE RETURNING
        ]
        
        registry = PlanRegistry(mock_backend)
        result = registry.save_plan(sample_plan)
        
        # Verify UPDATE was called
        assert mock_cursor.execute.call_count >= 2
        update_call = [call for call in mock_cursor.execute.call_args_list if "UPDATE" in str(call)]
        assert len(update_call) > 0
        
        assert result["success"] is True
        assert "updated successfully" in result["message"]
    
    @patch('plan_registry.plan_storage.PlanRegistry._initialize_schema')
    def test_get_plan_returns_none_if_not_found(self, mock_init_schema, mock_lakebase_backend):
        """Test get_plan returns None for non-existent plan"""
        from plan_registry.plan_storage import PlanRegistry
        
        mock_backend, mock_cursor = mock_lakebase_backend
        mock_cursor.fetchone.return_value = None
        
        registry = PlanRegistry(mock_backend)
        result = registry.get_plan(str(uuid.uuid4()))
        
        assert result is None
    
    @patch('plan_registry.plan_storage.PlanRegistry._initialize_schema')
    def test_list_plans_builds_correct_query(self, mock_init_schema, mock_lakebase_backend):
        """Test list_plans builds correct SQL with filters"""
        from plan_registry.plan_storage import PlanRegistry
        
        mock_backend, mock_cursor = mock_lakebase_backend
        mock_cursor.fetchone.return_value = (0,)  # Count
        mock_cursor.fetchall.return_value = []  # Plans
        
        registry = PlanRegistry(mock_backend)
        registry.list_plans(owner="test@example.com", pattern_type="SCD2", status="active")
        
        # Verify query includes filters
        calls = mock_cursor.execute.call_args_list
        assert len(calls) >= 2  # Count + List queries
        
        # Check that filters were applied
        count_query = str(calls[0])
        assert "owner" in count_query or len(calls[0][0]) > 1  # Parameters passed


@pytest.mark.parametrize("invalid_plan", [
    {},  # Empty plan
    {"schema_version": "1.0"},  # Missing plan_metadata
    {"schema_version": "1.0", "plan_metadata": {"plan_name": "test"}},  # Missing required fields
])
def test_api_rejects_invalid_plans(invalid_plan):
    """Test that API validation rejects invalid plans"""
    from compiler import SQLCompiler
    
    compiler = SQLCompiler("plan-schema/v1/plan.schema.json")
    is_valid, errors = compiler.validate_plan(invalid_plan)
    
    assert is_valid is False
    assert len(errors) > 0


def test_plan_registry_not_available_when_lakebase_disabled():
    """Test plan registry raises error when Lakebase is disabled"""
    # Clear singleton
    import plan_registry.plan_storage
    plan_registry.plan_storage._plan_registry = None
    
    with patch.dict(os.environ, {"LAKEBASE_ENABLED": "false"}, clear=False):
        from plan_registry import get_plan_registry
        
        with pytest.raises(RuntimeError, match="Lakebase is not enabled"):
            get_plan_registry()
    
    # Reset singleton to avoid affecting other tests
    plan_registry.plan_storage._plan_registry = None


def test_plan_registry_singleton():
    """Test that get_plan_registry returns singleton instance"""
    if not is_lakebase_enabled():
        pytest.skip("Lakebase not enabled")
    
    from plan_registry import get_plan_registry
    
    registry1 = get_plan_registry()
    registry2 = get_plan_registry()
    
    assert registry1 is registry2


@pytest.mark.skipif(not is_lakebase_enabled(), reason="Lakebase not enabled")
def test_schema_initialization_is_idempotent():
    """Test that schema initialization can be run multiple times safely"""
    from infrastructure.lakebase_backend import LakebaseBackend
    from plan_registry.plan_storage import PlanRegistry
    
    lakebase = LakebaseBackend(
        database=os.getenv("LAKEBASE_DATABASE", "databricks_postgres")
    )
    
    # Initialize schema twice
    registry1 = PlanRegistry(lakebase)
    registry2 = PlanRegistry(lakebase)
    
    # Should not raise any errors
    assert registry1 is not None
    assert registry2 is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

