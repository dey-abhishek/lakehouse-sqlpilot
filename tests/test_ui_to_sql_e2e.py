#!/usr/bin/env python3
"""
End-to-End Integration Test: UI → Backend API → Lakebase → SQL Generation

Tests the complete workflow:
1. Save plan to Lakebase via API
2. Retrieve plan from Lakebase via API
3. Compile retrieved plan to SQL
4. Verify SQL correctness

Requires:
- LAKEBASE_ENABLED=true
- LAKEBASE_HOST, LAKEBASE_USER, LAKEBASE_PASSWORD configured
"""

import os
import pytest
import uuid
from datetime import datetime, timezone
from fastapi.testclient import TestClient


def is_lakebase_enabled():
    """Check if Lakebase is enabled"""
    return os.getenv("LAKEBASE_ENABLED", "false").lower() == "true"


# Skip if Lakebase not enabled
pytestmark = pytest.mark.skipif(
    not is_lakebase_enabled(),
    reason="Requires LAKEBASE_ENABLED=true in environment"
)


@pytest.fixture
def client():
    """FastAPI test client"""
    from api.main import app
    return TestClient(app)


@pytest.fixture
def sample_plan():
    """Sample plan for testing"""
    plan_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": plan_id,
            "plan_name": "e2e_incremental_test",
            "version": "1.0.0",
            "description": "End-to-end test plan for incremental append",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": {"env": "test", "type": "e2e", "pattern": "incremental"}
        },
        "pattern": {
            "type": "INCREMENTAL_APPEND"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "raw",
            "table": "events_source"
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "curated",
            "table": "events_target",
            "write_mode": "append"
        },
        "pattern_config": {
            "watermark_column": "event_timestamp",
            "watermark_type": "timestamp"
        },
        "execution_config": {
            "warehouse_id": "test_warehouse",
            "batch_size": 1000,
            "timeout_seconds": 300
        }
    }


@pytest.fixture
def scd2_plan():
    """SCD2 plan for testing"""
    plan_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": plan_id,
            "plan_name": "e2e_scd2_test",
            "version": "1.0.0",
            "description": "End-to-end test plan for SCD2",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": {"env": "test", "type": "e2e", "pattern": "scd2"}
        },
        "pattern": {
            "type": "SCD2"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "raw",
            "table": "customers_source",
            "columns": ["customer_id", "name", "email", "city", "updated_at"]
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "curated",
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
            "warehouse_id": "test_warehouse",
            "batch_size": 1000,
            "timeout_seconds": 300
        }
    }


class TestEndToEndWorkflow:
    """Test complete workflow from UI to SQL generation"""
    
    def test_save_retrieve_compile_incremental(self, client, sample_plan):
        """
        Test: Save plan → Retrieve plan → Compile to SQL
        Pattern: INCREMENTAL_APPEND
        """
        plan_id = sample_plan["plan_metadata"]["plan_id"]
        
        # Step 1: Save plan via API (simulating UI save)
        print(f"\n[E2E] Step 1: Saving plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"Save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        assert save_data["plan_id"] == plan_id
        # Plan was saved successfully (message may vary)
        assert "created" in save_data.get("message", "").lower() or "saved" in save_data.get("message", "").lower()
        
        print(f"[E2E] ✅ Plan saved: {save_data['message']}")
        
        # Step 2: Retrieve plan via API (simulating UI load)
        print(f"[E2E] Step 2: Retrieving plan {plan_id}...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200, f"Retrieve failed: {get_response.text}"
        retrieved_plan = get_response.json()
        
        # Verify plan structure
        assert retrieved_plan["plan_metadata"]["plan_id"] == plan_id
        assert retrieved_plan["plan_metadata"]["plan_name"] == "e2e_incremental_test"
        assert retrieved_plan["pattern"]["type"] == "INCREMENTAL_APPEND"
        assert retrieved_plan["source"]["table"] == "events_source"
        assert retrieved_plan["target"]["table"] == "events_target"
        
        # Check metadata
        assert "_metadata" in retrieved_plan
        assert "created_at" in retrieved_plan["_metadata"]
        assert "updated_at" in retrieved_plan["_metadata"]
        assert retrieved_plan["_metadata"]["status"] == "active"
        
        print(f"[E2E] ✅ Plan retrieved from Lakebase")
        
        # Step 3: Compile plan to SQL
        print(f"[E2E] Step 3: Compiling plan to SQL...")
        
        # Remove metadata before compilation
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200, f"Compile failed: {compile_response.text}"
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert sql is not None
        assert len(sql) > 0
        
        print(f"[E2E] ✅ SQL generated ({len(sql)} characters)")
        
        # Step 4: Verify SQL correctness for INCREMENTAL_APPEND
        assert "-- LAKEHOUSE SQLPILOT GENERATED SQL" in sql
        assert "INSERT INTO" in sql
        assert "`test_catalog`.`curated`.`events_target`" in sql or "test_catalog.curated.events_target" in sql
        assert "`test_catalog`.`raw`.`events_source`" in sql or "test_catalog.raw.events_source" in sql
        assert "event_timestamp" in sql  # watermark column
        
        print(f"[E2E] ✅ SQL validated for INCREMENTAL_APPEND pattern")
        print(f"\n[E2E] SUCCESS: Full workflow completed for plan {plan_id}\n")
    
    def test_save_retrieve_compile_scd2(self, client, scd2_plan):
        """
        Test: Save plan → Retrieve plan → Compile to SQL
        Pattern: SCD2
        """
        plan_id = scd2_plan["plan_metadata"]["plan_id"]
        
        # Step 1: Save plan
        print(f"\n[E2E] Step 1: Saving SCD2 plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": scd2_plan,
            "user": "test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"SCD2 save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        
        print(f"[E2E] ✅ SCD2 plan saved")
        
        # Step 2: Retrieve plan
        print(f"[E2E] Step 2: Retrieving SCD2 plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        
        assert retrieved_plan["pattern"]["type"] == "SCD2"
        assert "business_keys" in retrieved_plan["pattern_config"]
        assert retrieved_plan["pattern_config"]["business_keys"] == ["customer_id"]
        
        print(f"[E2E] ✅ SCD2 plan retrieved")
        
        # Step 3: Compile to SQL
        print(f"[E2E] Step 3: Compiling SCD2 to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        sql = compile_data["sql"]
        
        print(f"[E2E] ✅ SCD2 SQL generated")
        
        # Step 4: Verify SCD2 SQL correctness
        assert "MERGE INTO" in sql
        assert "`test_catalog`.`curated`.`customers_dim`" in sql or "test_catalog.curated.customers_dim" in sql
        assert "`test_catalog`.`raw`.`customers_source`" in sql or "test_catalog.raw.customers_source" in sql
        assert "customer_id" in sql  # business key
        assert "is_current" in sql
        assert "valid_from" in sql
        assert "valid_to" in sql
        
        print(f"[E2E] ✅ SCD2 SQL validated")
        print(f"\n[E2E] SUCCESS: SCD2 workflow completed for plan {plan_id}\n")
    
    def test_list_plans_includes_saved_plan(self, client, sample_plan):
        """Test that saved plans appear in list endpoint"""
        plan_id = sample_plan["plan_metadata"]["plan_id"]
        
        # Save plan
        save_response = client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        assert save_response.status_code == 200
        
        # List all plans
        list_response = client.get("/api/v1/plans")
        assert list_response.status_code == 200
        
        list_data = list_response.json()
        assert "plans" in list_data
        
        # Verify saved plan is in list
        plan_ids = [p["plan_id"] for p in list_data["plans"]]
        assert plan_id in plan_ids
        
        print(f"[E2E] ✅ Saved plan appears in list endpoint")
    
    def test_filter_plans_by_pattern_type(self, client, sample_plan, scd2_plan):
        """Test filtering plans by pattern type"""
        # Save both plans
        client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        client.post("/api/v1/plans", json={
            "plan": scd2_plan,
            "user": "test@databricks.com"
        })
        
        # Filter by INCREMENTAL_APPEND
        response = client.get("/api/v1/plans?pattern_type=INCREMENTAL_APPEND")
        assert response.status_code == 200
        data = response.json()
        
        incremental_plans = [p for p in data["plans"] if p["pattern_type"] == "INCREMENTAL_APPEND"]
        assert len(incremental_plans) > 0
        
        # Verify no SCD2 plans in results
        scd2_plans = [p for p in data["plans"] if p["pattern_type"] == "SCD2"]
        assert len(scd2_plans) == 0
        
        print(f"[E2E] ✅ Pattern filtering works")
    
    def test_update_plan_workflow(self, client, sample_plan):
        """Test updating an existing plan"""
        plan_id = sample_plan["plan_metadata"]["plan_id"]
        
        # Save initial plan
        save_response = client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        assert save_response.status_code == 200
        
        # Update plan
        sample_plan["plan_metadata"]["version"] = "2.0.0"
        sample_plan["plan_metadata"]["description"] = "Updated description"
        
        update_response = client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        assert update_response.status_code == 200
        update_data = update_response.json()
        assert "updated" in update_data.get("message", "").lower()
        
        # Retrieve updated plan
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        assert get_response.status_code == 200
        retrieved = get_response.json()
        
        assert retrieved["plan_metadata"]["version"] == "2.0.0"
        assert retrieved["plan_metadata"]["description"] == "Updated description"
        
        print(f"[E2E] ✅ Plan update workflow works")
    
    def test_compile_nonexistent_plan_returns_404(self, client):
        """Test that compiling a non-existent plan returns 404"""
        fake_plan_id = str(uuid.uuid4())
        
        get_response = client.get(f"/api/v1/plans/{fake_plan_id}")
        assert get_response.status_code == 404
        
        print(f"[E2E] ✅ Non-existent plan returns 404")
    
    def test_invalid_plan_validation_fails(self, client):
        """Test that invalid plans are rejected during save"""
        invalid_plan = {
            "plan_metadata": {
                "plan_id": str(uuid.uuid4()),
                # Missing required fields
            },
            "pattern": {
                "type": "INVALID_PATTERN"
            }
        }
        
        save_response = client.post("/api/v1/plans", json={
            "plan": invalid_plan,
            "user": "test@databricks.com"
        })
        
        # Should fail validation
        assert save_response.status_code in [400, 422]
        
        print(f"[E2E] ✅ Invalid plan validation works")


class TestEndToEndPerformance:
    """Performance tests for the full workflow"""
    
    @pytest.mark.slow
    @pytest.mark.skip(reason="Performance test - run manually with --run-slow")
    def test_multiple_plans_save_retrieve_compile(self, client):
        """Test saving, retrieving, and compiling multiple plans"""
        import time
        
        num_plans = 10
        plan_ids = []
        
        start_time = time.time()
        
        # Create and save multiple plans
        for i in range(num_plans):
            plan = {
                "schema_version": "1.0",
                "plan_metadata": {
                    "plan_id": str(uuid.uuid4()),
                    "plan_name": f"perf_test_plan_{i}",
                    "version": "1.0.0",
                    "owner": "test@databricks.com",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "tags": {"env": "test", "type": "performance"}
                },
                "pattern": {"type": "INCREMENTAL_APPEND"},
                "source": {"catalog": "test", "schema": "raw", "table": f"source_{i}"},
                "target": {
                    "catalog": "test",
                    "schema": "curated",
                    "table": f"target_{i}",
                    "write_mode": "append"
                },
                "pattern_config": {
                    "watermark_column": "timestamp"
                },
                "execution_config": {
                    "batch_size": 1000,
                    "timeout_seconds": 300
                }
            }
            
            response = client.post("/api/v1/plans", json={
                "plan": plan,
                "user": "test@databricks.com"
            })
            assert response.status_code == 200
            plan_ids.append(plan["plan_metadata"]["plan_id"])
        
        save_time = time.time() - start_time
        
        # Retrieve all plans
        retrieve_start = time.time()
        for plan_id in plan_ids:
            response = client.get(f"/api/v1/plans/{plan_id}")
            assert response.status_code == 200
        retrieve_time = time.time() - retrieve_start
        
        # Compile all plans
        compile_start = time.time()
        for plan_id in plan_ids:
            get_response = client.get(f"/api/v1/plans/{plan_id}")
            plan = get_response.json()
            compile_plan = {k: v for k, v in plan.items() if k != "_metadata"}
            
            compile_response = client.post("/api/v1/plans/compile", json={"plan": compile_plan, "context": {}})
            assert compile_response.status_code == 200
        compile_time = time.time() - compile_start
        
        total_time = time.time() - start_time
        
        print(f"\n[E2E Performance]")
        print(f"  Plans: {num_plans}")
        print(f"  Save time: {save_time:.2f}s ({save_time/num_plans:.3f}s per plan)")
        print(f"  Retrieve time: {retrieve_time:.2f}s ({retrieve_time/num_plans:.3f}s per plan)")
        print(f"  Compile time: {compile_time:.2f}s ({compile_time/num_plans:.3f}s per plan)")
        print(f"  Total time: {total_time:.2f}s")
        
        # Performance assertions
        assert save_time / num_plans < 2.0, "Save operation too slow"
        assert retrieve_time / num_plans < 1.0, "Retrieve operation too slow"
        assert compile_time / num_plans < 1.0, "Compile operation too slow"


if __name__ == "__main__":
    """Run tests directly"""
    import sys
    sys.exit(pytest.main([__file__, "-v", "--run-lakebase", "-s"]))

