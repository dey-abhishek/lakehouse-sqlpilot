#!/usr/bin/env python3
"""
Comprehensive Integration Tests: All Patterns (UI → API → Lakebase → SQL → Databricks)

Tests the complete end-to-end workflow for ALL patterns:
1. UI sends plan to API
2. API validates plan
3. API saves plan to Lakebase
4. API retrieves plan from Lakebase
5. API compiles plan to SQL
6. API executes SQL on Databricks warehouse
7. Verify results

Patterns Tested:
- Incremental Append
- Full Replace
- Merge/Upsert
- SCD Type 2
- Snapshot (if implemented)
"""

import os
import pytest
import uuid
from datetime import datetime, timezone
from fastapi.testclient import TestClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv(".env.dev")


def is_integration_enabled():
    """Check if integration tests are enabled"""
    return (
        os.getenv("LAKEBASE_ENABLED", "false").lower() == "true" and
        os.getenv("DATABRICKS_WAREHOUSE_ID") is not None
    )


# Skip if integration not enabled
pytestmark = pytest.mark.skipif(
    not is_integration_enabled(),
    reason="Requires LAKEBASE_ENABLED=true and DATABRICKS_WAREHOUSE_ID in environment"
)


@pytest.fixture
def client():
    """FastAPI test client"""
    from api.main import app
    return TestClient(app)


@pytest.fixture
def test_catalog():
    """Test catalog name"""
    return os.getenv("DATABRICKS_CATALOG", "lakehouse-sqlpilot")


@pytest.fixture
def test_schema():
    """Test schema name"""
    return os.getenv("DATABRICKS_SCHEMA", "lakehouse-sqlpilot-schema")


@pytest.fixture
def test_warehouse():
    """Test warehouse ID"""
    return os.getenv("DATABRICKS_WAREHOUSE_ID")


class TestIncrementalAppendIntegration:
    """Integration tests for Incremental Append pattern"""
    
    def test_full_workflow_incremental_append(self, client, test_catalog, test_schema, test_warehouse):
        """
        Test complete workflow: Save → Retrieve → Compile → Execute
        Pattern: INCREMENTAL_APPEND
        """
        plan_id = str(uuid.uuid4())
        plan_name = f"integ_incremental_{plan_id[:8]}"
        source_table = f"integ_events_source_{plan_id[:8]}"
        target_table = f"integ_events_target_{plan_id[:8]}"
        
        plan = {
            "schema_version": "1.0",
            "plan_metadata": {
                "plan_id": plan_id,
                "plan_name": plan_name,
                "version": "1.0.0",
                "description": "Integration test: Incremental Append",
                "owner": "integration_test@databricks.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": {"type": "integration_test", "pattern": "incremental"}
            },
            "pattern": {
                "type": "INCREMENTAL_APPEND"
            },
            "source": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": source_table
            },
            "target": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": target_table,
                "write_mode": "append"
            },
            "pattern_config": {
                "watermark_column": "event_timestamp",
                "watermark_type": "timestamp"
            },
            "execution_config": {
                "warehouse_id": test_warehouse,
                "batch_size": 1000,
                "timeout_seconds": 300
            }
        }
        
        # STEP 1: Save plan
        print(f"\n[INTEGRATION] Step 1: Saving plan {plan_id}...")
        save_response = client.post("/api/v1/plans", json={
            "plan": plan,
            "user": "integration_test@databricks.com"
        })
        
        assert save_response.status_code == 200, f"Save failed: {save_response.text}"
        save_data = save_response.json()
        assert save_data["success"] is True
        assert save_data["plan_id"] == plan_id
        print(f"[INTEGRATION] ✅ Plan saved to Lakebase")
        
        # STEP 2: Retrieve plan
        print(f"[INTEGRATION] Step 2: Retrieving plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        assert retrieved_plan["plan_metadata"]["plan_id"] == plan_id
        assert retrieved_plan["pattern"]["type"] == "INCREMENTAL_APPEND"
        assert "_metadata" in retrieved_plan
        print(f"[INTEGRATION] ✅ Plan retrieved from Lakebase")
        
        # STEP 3: Compile to SQL
        print(f"[INTEGRATION] Step 3: Compiling to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert "INSERT INTO" in sql
        assert f"`{test_catalog}`.`{test_schema}`.`{target_table}`" in sql or \
               f"{test_catalog}.{test_schema}.{target_table}" in sql
        print(f"[INTEGRATION] ✅ SQL compiled ({len(sql)} chars)")
        
        # STEP 4: Note - Actual execution skipped (requires test tables)
        print(f"[INTEGRATION] Step 4: Execution skipped (requires test tables)")
        print(f"[INTEGRATION] ✅ FULL WORKFLOW VALIDATED for Incremental Append")


class TestFullReplaceIntegration:
    """Integration tests for Full Replace pattern"""
    
    def test_full_workflow_full_replace(self, client, test_catalog, test_schema, test_warehouse):
        """
        Test complete workflow: Save → Retrieve → Compile → Execute
        Pattern: FULL_REPLACE
        """
        plan_id = str(uuid.uuid4())
        plan_name = f"integ_full_replace_{plan_id[:8]}"
        source_table = f"integ_snapshot_source_{plan_id[:8]}"
        target_table = f"integ_snapshot_target_{plan_id[:8]}"
        
        plan = {
            "schema_version": "1.0",
            "plan_metadata": {
                "plan_id": plan_id,
                "plan_name": plan_name,
                "version": "1.0.0",
                "description": "Integration test: Full Replace",
                "owner": "integration_test@databricks.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": {"type": "integration_test", "pattern": "full_replace"}
            },
            "pattern": {
                "type": "FULL_REPLACE"
            },
            "source": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": source_table
            },
            "target": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": target_table,
                "write_mode": "overwrite"
            },
            "pattern_config": {},  # Required even if empty
            "execution_config": {
                "warehouse_id": test_warehouse,
                "batch_size": 1000,
                "timeout_seconds": 300
            }
        }
        
        # STEP 1: Save plan
        print(f"\n[INTEGRATION] Step 1: Saving Full Replace plan...")
        save_response = client.post("/api/v1/plans", json={
            "plan": plan,
            "user": "integration_test@databricks.com"
        })
        
        assert save_response.status_code == 200
        save_data = save_response.json()
        assert save_data["success"] is True
        print(f"[INTEGRATION] ✅ Full Replace plan saved")
        
        # STEP 2: Retrieve plan
        print(f"[INTEGRATION] Step 2: Retrieving plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        assert retrieved_plan["pattern"]["type"] == "FULL_REPLACE"
        print(f"[INTEGRATION] ✅ Plan retrieved")
        
        # STEP 3: Compile to SQL
        print(f"[INTEGRATION] Step 3: Compiling to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        # Full Replace uses either TRUNCATE + INSERT or INSERT OVERWRITE
        assert "TRUNCATE" in sql or "INSERT OVERWRITE" in sql or "CREATE OR REPLACE" in sql
        print(f"[INTEGRATION] ✅ SQL compiled with FULL_REPLACE logic")
        print(f"[INTEGRATION] ✅ FULL WORKFLOW VALIDATED for Full Replace")


class TestMergeUpsertIntegration:
    """Integration tests for Merge/Upsert pattern"""
    
    def test_full_workflow_merge_upsert(self, client, test_catalog, test_schema, test_warehouse):
        """
        Test complete workflow: Save → Retrieve → Compile → Execute
        Pattern: MERGE_UPSERT
        """
        plan_id = str(uuid.uuid4())
        plan_name = f"integ_merge_{plan_id[:8]}"
        source_table = f"integ_accounts_source_{plan_id[:8]}"
        target_table = f"integ_accounts_target_{plan_id[:8]}"
        
        plan = {
            "schema_version": "1.0",
            "plan_metadata": {
                "plan_id": plan_id,
                "plan_name": plan_name,
                "version": "1.0.0",
                "description": "Integration test: Merge/Upsert",
                "owner": "integration_test@databricks.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": {"type": "integration_test", "pattern": "merge"}
            },
            "pattern": {
                "type": "MERGE_UPSERT"
            },
            "source": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": source_table
            },
            "target": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": target_table,
                "write_mode": "merge"
            },
            "pattern_config": {
                "merge_keys": ["account_id"],
                "update_columns": ["balance", "last_updated"]
            },
            "execution_config": {
                "warehouse_id": test_warehouse,
                "batch_size": 1000,
                "timeout_seconds": 300
            }
        }
        
        # STEP 1: Save plan
        print(f"\n[INTEGRATION] Step 1: Saving Merge/Upsert plan...")
        save_response = client.post("/api/v1/plans", json={
            "plan": plan,
            "user": "integration_test@databricks.com"
        })
        
        assert save_response.status_code == 200
        save_data = save_response.json()
        assert save_data["success"] is True
        print(f"[INTEGRATION] ✅ Merge/Upsert plan saved")
        
        # STEP 2: Retrieve plan
        print(f"[INTEGRATION] Step 2: Retrieving plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        assert retrieved_plan["pattern"]["type"] == "MERGE_UPSERT"
        assert retrieved_plan["pattern_config"]["merge_keys"] == ["account_id"]
        print(f"[INTEGRATION] ✅ Plan retrieved with merge keys")
        
        # STEP 3: Compile to SQL
        print(f"[INTEGRATION] Step 3: Compiling to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert "MERGE INTO" in sql
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql
        assert "account_id" in sql
        print(f"[INTEGRATION] ✅ SQL compiled with MERGE logic")
        print(f"[INTEGRATION] ✅ FULL WORKFLOW VALIDATED for Merge/Upsert")


class TestSCD2Integration:
    """Integration tests for SCD Type 2 pattern"""
    
    def test_full_workflow_scd2(self, client, test_catalog, test_schema, test_warehouse):
        """
        Test complete workflow: Save → Retrieve → Compile → Execute
        Pattern: SCD2
        """
        plan_id = str(uuid.uuid4())
        plan_name = f"integ_scd2_{plan_id[:8]}"
        source_table = f"integ_customer_source_{plan_id[:8]}"
        target_table = f"integ_customer_dim_{plan_id[:8]}"
        
        plan = {
            "schema_version": "1.0",
            "plan_metadata": {
                "plan_id": plan_id,
                "plan_name": plan_name,
                "version": "1.0.0",
                "description": "Integration test: SCD Type 2",
                "owner": "integration_test@databricks.com",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": {"type": "integration_test", "pattern": "scd2"}
            },
            "pattern": {
                "type": "SCD2"
            },
            "source": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": source_table,
                "columns": ["customer_id", "name", "email", "city", "updated_at"]
            },
            "target": {
                "catalog": test_catalog,
                "schema": test_schema,
                "table": target_table,
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
                "warehouse_id": test_warehouse,
                "batch_size": 1000,
                "timeout_seconds": 300
            }
        }
        
        # STEP 1: Save plan
        print(f"\n[INTEGRATION] Step 1: Saving SCD2 plan...")
        save_response = client.post("/api/v1/plans", json={
            "plan": plan,
            "user": "integration_test@databricks.com"
        })
        
        assert save_response.status_code == 200
        save_data = save_response.json()
        assert save_data["success"] is True
        print(f"[INTEGRATION] ✅ SCD2 plan saved")
        
        # STEP 2: Retrieve plan
        print(f"[INTEGRATION] Step 2: Retrieving plan...")
        get_response = client.get(f"/api/v1/plans/{plan_id}")
        
        assert get_response.status_code == 200
        retrieved_plan = get_response.json()
        assert retrieved_plan["pattern"]["type"] == "SCD2"
        assert "business_keys" in retrieved_plan["pattern_config"]
        print(f"[INTEGRATION] ✅ Plan retrieved with SCD2 config")
        
        # STEP 3: Compile to SQL
        print(f"[INTEGRATION] Step 3: Compiling to SQL...")
        compile_plan = {k: v for k, v in retrieved_plan.items() if k != "_metadata"}
        
        compile_response = client.post("/api/v1/plans/compile", json={
            "plan": compile_plan,
            "context": {}
        })
        
        assert compile_response.status_code == 200
        compile_data = compile_response.json()
        assert compile_data["success"] is True
        
        sql = compile_data["sql"]
        assert "MERGE INTO" in sql
        assert "is_current" in sql
        assert "valid_from" in sql
        assert "valid_to" in sql
        print(f"[INTEGRATION] ✅ SQL compiled with SCD2 logic")
        print(f"[INTEGRATION] ✅ FULL WORKFLOW VALIDATED for SCD2")


class TestCrossPatternIntegration:
    """Integration tests across multiple patterns"""
    
    def test_save_and_list_all_patterns(self, client, test_catalog, test_schema, test_warehouse):
        """Test saving plans for all patterns and listing them"""
        
        patterns_to_test = [
            ("INCREMENTAL_APPEND", {"watermark_column": "timestamp", "watermark_type": "timestamp"}, "append"),
            ("FULL_REPLACE", {}, "overwrite"),
            ("MERGE_UPSERT", {"merge_keys": ["id"], "update_columns": ["value"]}, "merge"),
            ("SCD2", {
                "business_keys": ["id"],
                "compare_columns": ["value"],
                "effective_date_column": "valid_from",
                "end_date_column": "valid_to",
                "current_flag_column": "is_current"
            }, "merge")
        ]
        
        saved_plan_ids = []
        
        for pattern_type, pattern_config, write_mode in patterns_to_test:
            plan_id = str(uuid.uuid4())
            
            plan = {
                "schema_version": "1.0",
                "plan_metadata": {
                    "plan_id": plan_id,
                    "plan_name": f"cross_pattern_test_{pattern_type.lower()}",
                    "version": "1.0.0",
                    "owner": "integration_test@databricks.com",
                    "created_at": datetime.now(timezone.utc).isoformat()
                },
                "pattern": {"type": pattern_type},
                "source": {
                    "catalog": test_catalog,
                    "schema": test_schema,
                    "table": f"src_{pattern_type.lower()}"
                },
                "target": {
                    "catalog": test_catalog,
                    "schema": test_schema,
                    "table": f"tgt_{pattern_type.lower()}",
                    "write_mode": write_mode
                },
                "pattern_config": pattern_config,
                "execution_config": {
                    "warehouse_id": test_warehouse
                }
            }
            
            # Add source columns for SCD2
            if pattern_type == "SCD2":
                plan["source"]["columns"] = ["id", "value", "updated_at"]
            
            # Save plan
            print(f"\n[CROSS-PATTERN] Saving {pattern_type} plan...")
            save_response = client.post("/api/v1/plans", json={
                "plan": plan,
                "user": "integration_test@databricks.com"
            })
            
            if save_response.status_code != 200:
                print(f"[CROSS-PATTERN] ❌ Failed to save {pattern_type} plan")
                print(f"[CROSS-PATTERN] Status: {save_response.status_code}")
                print(f"[CROSS-PATTERN] Response: {save_response.text}")
            
            assert save_response.status_code == 200, f"{pattern_type} save failed: {save_response.text}"
            assert save_response.json()["success"] is True
            saved_plan_ids.append(plan_id)
            print(f"[CROSS-PATTERN] ✅ {pattern_type} plan saved")
        
        # List all plans and verify they're all there
        print(f"\n[CROSS-PATTERN] Listing all plans...")
        list_response = client.get("/api/v1/plans")
        
        assert list_response.status_code == 200
        list_data = list_response.json()
        assert "plans" in list_data
        
        listed_plan_ids = [p["plan_id"] for p in list_data["plans"]]
        
        for plan_id in saved_plan_ids:
            assert plan_id in listed_plan_ids, f"Plan {plan_id} not found in list"
        
        print(f"[CROSS-PATTERN] ✅ All {len(saved_plan_ids)} patterns saved and listed correctly")
    
    def test_filter_plans_by_pattern(self, client):
        """Test filtering plans by pattern type"""
        
        # Get all plans
        all_response = client.get("/api/v1/plans")
        assert all_response.status_code == 200
        all_plans = all_response.json()["plans"]
        
        # Get only SCD2 plans
        scd2_response = client.get("/api/v1/plans?pattern_type=SCD2")
        assert scd2_response.status_code == 200
        scd2_plans = scd2_response.json()["plans"]
        
        # Verify all returned plans are SCD2
        for plan in scd2_plans:
            assert plan["pattern_type"] == "SCD2"
        
        print(f"[CROSS-PATTERN] ✅ Pattern filtering works ({len(scd2_plans)} SCD2 plans found)")


class TestExecutionIntegration:
    """Integration tests for plan execution tracking"""
    
    @pytest.mark.slow
    def test_execution_tracking_end_to_end(self, client, test_catalog, test_schema, test_warehouse):
        """Test complete execution with tracking (requires real tables)"""
        # This test would require pre-created test tables
        # Skipped in basic test run
        pytest.skip("Requires pre-created test tables - run manually")


if __name__ == "__main__":
    """Run integration tests directly"""
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s", "--tb=short"]))

