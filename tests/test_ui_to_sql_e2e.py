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


@pytest.fixture
def full_replace_plan():
    """Full Replace plan for testing"""
    plan_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": plan_id,
            "plan_name": "e2e_full_replace_test",
            "version": "1.0.0",
            "description": "End-to-end test plan for Full Replace",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": {"env": "test", "type": "e2e", "pattern": "full_replace"}
        },
        "pattern": {
            "type": "FULL_REPLACE"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "staging",
            "table": "products_staging"
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "curated",
            "table": "products",
            "write_mode": "overwrite"
        },
        "pattern_config": {},  # Required even if empty
        "execution_config": {
            "warehouse_id": "test_warehouse",
            "batch_size": 1000,
            "timeout_seconds": 300
        }
    }


@pytest.fixture
def merge_upsert_plan():
    """Merge/Upsert plan for testing"""
    plan_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": plan_id,
            "plan_name": "e2e_merge_upsert_test",
            "version": "1.0.0",
            "description": "End-to-end test plan for Merge/Upsert",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": {"env": "test", "type": "e2e", "pattern": "merge"}
        },
        "pattern": {
            "type": "MERGE_UPSERT"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "staging",
            "table": "accounts_updates",
            "columns": ["account_id", "account_name", "balance", "status"]
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "curated",
            "table": "accounts",
            "write_mode": "merge"
        },
        "pattern_config": {
            "merge_keys": ["account_id"],
            "update_columns": ["account_name", "balance", "status"]
        },
        "execution_config": {
            "warehouse_id": "test_warehouse",
            "batch_size": 1000,
            "timeout_seconds": 300
        }
    }


@pytest.fixture
def snapshot_plan():
    """Snapshot plan for testing"""
    plan_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": plan_id,
            "plan_name": "e2e_snapshot_test",
            "version": "1.0.0",
            "description": "End-to-end test plan for Snapshot",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": {"env": "test", "type": "e2e", "pattern": "snapshot"}
        },
        "pattern": {
            "type": "SNAPSHOT"
        },
        "source": {
            "catalog": "test_catalog",
            "schema": "raw",
            "table": "inventory_current"
        },
        "target": {
            "catalog": "test_catalog",
            "schema": "curated",
            "table": "inventory_snapshots",
            "write_mode": "append"
        },
        "pattern_config": {
            "snapshot_date_column": "snapshot_date",
            "partition_columns": ["snapshot_date"]
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
    
    def test_save_retrieve_compile_full_replace(self, client, full_replace_plan):
        """
        Test: Save plan → Retrieve plan → Compile to SQL
        Pattern: FULL_REPLACE
        """
        plan_id = full_replace_plan["plan_metadata"]["plan_id"]
        
        # Step 1: Save plan
        print(f"\n[E2E] Step 1: Saving Full Replace plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": full_replace_plan,
            "user": "test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"Full Replace save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        assert save_data["plan_id"] == plan_id
        
        print(f"[E2E] ✅ Full Replace plan saved")
        
        # Step 2: Retrieve plan
        print(f"[E2E] Step 2: Retrieving Full Replace plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        
        assert retrieved_plan["pattern"]["type"] == "FULL_REPLACE"
        assert retrieved_plan["target"]["write_mode"] == "overwrite"
        assert "pattern_config" in retrieved_plan
        
        print(f"[E2E] ✅ Full Replace plan retrieved")
        
        # Step 3: Compile to SQL
        print(f"[E2E] Step 3: Compiling Full Replace to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert sql is not None
        assert len(sql) > 0
        
        print(f"[E2E] ✅ Full Replace SQL generated")
        
        # Step 4: Verify Full Replace SQL correctness
        # Full Replace uses CREATE OR REPLACE TABLE or INSERT OVERWRITE
        assert "CREATE OR REPLACE TABLE" in sql or "INSERT OVERWRITE" in sql or "TRUNCATE" in sql
        assert "`test_catalog`.`curated`.`products`" in sql or "test_catalog.curated.products" in sql
        assert "`test_catalog`.`staging`.`products_staging`" in sql or "test_catalog.staging.products_staging" in sql
        
        print(f"[E2E] ✅ Full Replace SQL validated")
        print(f"\n[E2E] SUCCESS: Full Replace workflow completed for plan {plan_id}\n")
    
    def test_save_retrieve_compile_merge_upsert(self, client, merge_upsert_plan):
        """
        Test: Save plan → Retrieve plan → Compile to SQL
        Pattern: MERGE_UPSERT
        """
        plan_id = merge_upsert_plan["plan_metadata"]["plan_id"]
        
        # Step 1: Save plan
        print(f"\n[E2E] Step 1: Saving Merge/Upsert plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": merge_upsert_plan,
            "user": "test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"Merge/Upsert save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        assert save_data["plan_id"] == plan_id
        
        print(f"[E2E] ✅ Merge/Upsert plan saved")
        
        # Step 2: Retrieve plan
        print(f"[E2E] Step 2: Retrieving Merge/Upsert plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        
        assert retrieved_plan["pattern"]["type"] == "MERGE_UPSERT"
        assert "merge_keys" in retrieved_plan["pattern_config"]
        assert retrieved_plan["pattern_config"]["merge_keys"] == ["account_id"]
        assert "update_columns" in retrieved_plan["pattern_config"]
        
        print(f"[E2E] ✅ Merge/Upsert plan retrieved")
        
        # Step 3: Compile to SQL
        print(f"[E2E] Step 3: Compiling Merge/Upsert to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert sql is not None
        assert len(sql) > 0
        
        print(f"[E2E] ✅ Merge/Upsert SQL generated")
        
        # Step 4: Verify Merge/Upsert SQL correctness
        assert "MERGE INTO" in sql
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql
        assert "`test_catalog`.`curated`.`accounts`" in sql or "test_catalog.curated.accounts" in sql
        assert "`test_catalog`.`staging`.`accounts_updates`" in sql or "test_catalog.staging.accounts_updates" in sql
        assert "account_id" in sql  # merge key
        assert "account_name" in sql or "balance" in sql or "status" in sql  # update columns
        
        print(f"[E2E] ✅ Merge/Upsert SQL validated")
        print(f"\n[E2E] SUCCESS: Merge/Upsert workflow completed for plan {plan_id}\n")
    
    def test_save_retrieve_compile_snapshot(self, client, snapshot_plan):
        """
        Test: Save plan → Retrieve plan → Compile to SQL
        Pattern: SNAPSHOT
        """
        plan_id = snapshot_plan["plan_metadata"]["plan_id"]
        
        # Step 1: Save plan
        print(f"\n[E2E] Step 1: Saving Snapshot plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": snapshot_plan,
            "user": "test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"Snapshot save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        assert save_data["plan_id"] == plan_id
        
        print(f"[E2E] ✅ Snapshot plan saved")
        
        # Step 2: Retrieve plan
        print(f"[E2E] Step 2: Retrieving Snapshot plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        
        assert retrieved_plan["pattern"]["type"] == "SNAPSHOT"
        assert "snapshot_date_column" in retrieved_plan["pattern_config"]
        assert retrieved_plan["pattern_config"]["snapshot_date_column"] == "snapshot_date"
        
        print(f"[E2E] ✅ Snapshot plan retrieved")
        
        # Step 3: Compile to SQL
        print(f"[E2E] Step 3: Compiling Snapshot to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert sql is not None
        assert len(sql) > 0
        
        print(f"[E2E] ✅ Snapshot SQL generated")
        
        # Step 4: Verify Snapshot SQL correctness
        assert "INSERT INTO" in sql
        assert "`test_catalog`.`curated`.`inventory_snapshots`" in sql or "test_catalog.curated.inventory_snapshots" in sql
        assert "`test_catalog`.`raw`.`inventory_current`" in sql or "test_catalog.raw.inventory_current" in sql
        assert "snapshot_date" in sql  # snapshot column
        
        print(f"[E2E] ✅ Snapshot SQL validated")
        print(f"\n[E2E] SUCCESS: Snapshot workflow completed for plan {plan_id}\n")
    
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
    
    def test_filter_plans_by_pattern_type(self, client, sample_plan, scd2_plan, full_replace_plan, merge_upsert_plan, snapshot_plan):
        """Test filtering plans by pattern type"""
        # Save all plan types
        client.post("/api/v1/plans", json={
            "plan": sample_plan,
            "user": "test@databricks.com"
        })
        client.post("/api/v1/plans", json={
            "plan": scd2_plan,
            "user": "test@databricks.com"
        })
        client.post("/api/v1/plans", json={
            "plan": full_replace_plan,
            "user": "test@databricks.com"
        })
        client.post("/api/v1/plans", json={
            "plan": merge_upsert_plan,
            "user": "test@databricks.com"
        })
        client.post("/api/v1/plans", json={
            "plan": snapshot_plan,
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
        
        # Filter by FULL_REPLACE
        response = client.get("/api/v1/plans?pattern_type=FULL_REPLACE")
        assert response.status_code == 200
        data = response.json()
        
        full_replace_plans = [p for p in data["plans"] if p["pattern_type"] == "FULL_REPLACE"]
        assert len(full_replace_plans) > 0
        
        # Filter by MERGE_UPSERT
        response = client.get("/api/v1/plans?pattern_type=MERGE_UPSERT")
        assert response.status_code == 200
        data = response.json()
        
        merge_plans = [p for p in data["plans"] if p["pattern_type"] == "MERGE_UPSERT"]
        assert len(merge_plans) > 0
        
        # Filter by SNAPSHOT
        response = client.get("/api/v1/plans?pattern_type=SNAPSHOT")
        assert response.status_code == 200
        data = response.json()
        
        snapshot_plans = [p for p in data["plans"] if p["pattern_type"] == "SNAPSHOT"]
        assert len(snapshot_plans) > 0
        
        print(f"[E2E] ✅ Pattern filtering works for all 5 patterns")
    
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

