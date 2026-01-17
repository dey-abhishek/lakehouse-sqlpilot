#!/usr/bin/env python3
"""
Lakehouse SQLPilot - SCD2 Integration Test Runner

Runs comprehensive integration tests for the SCD2 flagship pattern.
Tests the complete lifecycle: Initial Load → Updates → Validation
"""

import os
import sys
import json
import yaml
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from databricks import sql
from databricks.sdk import WorkspaceClient
from plan_schema.v1.validator import PlanValidator
from compiler.sql_generator import SQLCompiler
from execution.executor import SQLExecutor
from execution.tracker import ExecutionTracker


class SCD2IntegrationTester:
    """Runs end-to-end integration tests for SCD2 pattern"""
    
    def __init__(self, workspace_url: str, token: str, warehouse_id: str, catalog: str, schema: str):
        self.workspace_url = workspace_url
        self.token = token
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        
        # Initialize components
        self.workspace_client = WorkspaceClient(host=workspace_url, token=token)
        self.validator = PlanValidator(self.workspace_client)
        self.compiler = SQLCompiler()
        self.executor = SQLExecutor(
            host=workspace_url,
            token=token,
            warehouse_id=warehouse_id,
            catalog=catalog,
            schema=schema
        )
        self.tracker = ExecutionTracker(self.executor)
        
        self.test_run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
    def setup_tables(self) -> bool:
        """Create test tables and load initial data"""
        print("\n" + "="*80)
        print("STEP 1: Setting up test tables")
        print("="*80)
        
        setup_sql_file = Path(__file__).parent / "setup_scd2_tables.sql"
        
        if not setup_sql_file.exists():
            print(f"❌ Setup SQL file not found: {setup_sql_file}")
            return False
        
        print(f"📄 Reading setup SQL from: {setup_sql_file}")
        
        with open(setup_sql_file, 'r') as f:
            sql_content = f.read()
        
        # Split SQL statements and execute
        statements = [s.strip() for s in sql_content.split(';') if s.strip() and not s.strip().startswith('--')]
        
        connection = sql.connect(
            server_hostname=self.workspace_url.replace('https://', ''),
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=self.token
        )
        
        try:
            cursor = connection.cursor()
            
            for i, stmt in enumerate(statements):
                if stmt.strip():
                    print(f"  Executing statement {i+1}/{len(statements)}...")
                    cursor.execute(stmt)
                    
                    # Fetch results if available
                    if cursor.description:
                        results = cursor.fetchall()
                        for row in results:
                            print(f"    {row}")
            
            cursor.close()
            print("✅ Tables created and initial data loaded")
            return True
            
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            return False
        finally:
            connection.close()
    
    def run_initial_load(self) -> dict:
        """Test: Initial load of SCD2 dimension"""
        print("\n" + "="*80)
        print("STEP 2: Initial Load - First-time population of SCD2 table")
        print("="*80)
        
        # Create plan for initial load
        plan = {
            "plan_name": "customer_scd2_initial_load",
            "version": "1.0.0",
            "owner": "integration.test@databricks.com",
            "pattern": "scd2",
            "schedule": None,
            "pattern_config": {
                "source_table": f"{self.catalog}.{self.schema}.customer_dim_source",
                "target_table": f"{self.catalog}.{self.schema}.customer_dim_scd2",
                "business_keys": ["customer_id"],
                "tracked_columns": [
                    "customer_name",
                    "email",
                    "phone",
                    "address",
                    "city",
                    "state",
                    "country",
                    "customer_segment",
                    "account_status"
                ],
                "effective_timestamp": "last_updated_at",
                "valid_from_column": "valid_from",
                "valid_to_column": "valid_to",
                "current_flag_column": "is_current"
            }
        }
        
        print("\n📋 Plan:")
        print(json.dumps(plan, indent=2))
        
        # Validate plan
        print("\n🔍 Validating plan...")
        validation_result = self.validator.validate(plan)
        
        if not validation_result.is_valid:
            print(f"❌ Validation failed: {validation_result.errors}")
            return {"status": "FAILED", "phase": "validation", "errors": validation_result.errors}
        
        print("✅ Plan validated")
        
        # Compile to SQL
        print("\n🔨 Compiling to SQL...")
        compilation = self.compiler.generate(plan)
        
        if not compilation.success:
            print(f"❌ Compilation failed: {compilation.errors}")
            return {"status": "FAILED", "phase": "compilation", "errors": compilation.errors}
        
        print("✅ SQL generated")
        print("\n📝 Generated SQL:")
        print("-" * 80)
        print(compilation.sql)
        print("-" * 80)
        
        # Execute
        print("\n⚡ Executing SQL...")
        execution_id = self.tracker.save_execution(
            plan_name=plan["plan_name"],
            plan_version=plan["version"],
            sql=compilation.sql,
            state="running"
        )
        
        try:
            result = self.executor.execute_plan(
                plan_name=plan["plan_name"],
                sql=compilation.sql,
                execution_id=execution_id
            )
            
            print(f"✅ Execution completed")
            print(f"   Rows affected: {result.get('rows_affected', 0)}")
            
            # Verify results
            verification = self._verify_scd2_state(
                expected_current_records=5,
                expected_total_records=5,
                test_phase="initial_load"
            )
            
            return {
                "status": "PASSED" if verification["passed"] else "FAILED",
                "phase": "initial_load",
                "execution_id": execution_id,
                "rows_affected": result.get('rows_affected', 0),
                "verification": verification
            }
            
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            self.tracker.save_execution(
                plan_name=plan["plan_name"],
                plan_version=plan["version"],
                sql=compilation.sql,
                state="failed",
                error_message=str(e),
                execution_id=execution_id
            )
            return {"status": "FAILED", "phase": "execution", "error": str(e)}
    
    def run_update_test(self) -> dict:
        """Test: Process updates and track history"""
        print("\n" + "="*80)
        print("STEP 3: Update Test - Process changes and track history")
        print("="*80)
        
        # Load Day 2 data
        print("📊 Loading Day 2 data updates...")
        update_sql_file = Path(__file__).parent / "update_scd2_data.sql"
        
        with open(update_sql_file, 'r') as f:
            update_sql = f.read()
        
        connection = sql.connect(
            server_hostname=self.workspace_url.replace('https://', ''),
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=self.token
        )
        
        try:
            cursor = connection.cursor()
            for stmt in [s.strip() for s in update_sql.split(';') if s.strip() and not s.strip().startswith('--') and '/*' not in s]:
                cursor.execute(stmt)
            cursor.close()
            print("✅ Day 2 data loaded")
        finally:
            connection.close()
        
        # Create plan for update (same as initial load plan)
        plan = {
            "plan_name": "customer_scd2_update",
            "version": "1.0.0",
            "owner": "integration.test@databricks.com",
            "pattern": "scd2",
            "schedule": None,
            "pattern_config": {
                "source_table": f"{self.catalog}.{self.schema}.customer_dim_source",
                "target_table": f"{self.catalog}.{self.schema}.customer_dim_scd2",
                "business_keys": ["customer_id"],
                "tracked_columns": [
                    "customer_name",
                    "email",
                    "phone",
                    "address",
                    "city",
                    "state",
                    "country",
                    "customer_segment",
                    "account_status"
                ],
                "effective_timestamp": "last_updated_at",
                "valid_from_column": "valid_from",
                "valid_to_column": "valid_to",
                "current_flag_column": "is_current"
            }
        }
        
        # Validate, compile, execute (same flow as initial load)
        validation_result = self.validator.validate(plan)
        if not validation_result.is_valid:
            return {"status": "FAILED", "phase": "validation", "errors": validation_result.errors}
        
        compilation = self.compiler.generate(plan)
        if not compilation.success:
            return {"status": "FAILED", "phase": "compilation", "errors": compilation.errors}
        
        print("\n📝 Generated SQL:")
        print("-" * 80)
        print(compilation.sql)
        print("-" * 80)
        
        execution_id = self.tracker.save_execution(
            plan_name=plan["plan_name"],
            plan_version=plan["version"],
            sql=compilation.sql,
            state="running"
        )
        
        try:
            result = self.executor.execute_plan(
                plan_name=plan["plan_name"],
                sql=compilation.sql,
                execution_id=execution_id
            )
            
            print(f"✅ Execution completed")
            print(f"   Rows affected: {result.get('rows_affected', 0)}")
            
            # Verify: Should have 6 current records (5 existing + 1 new)
            # Should have historical records for the 4 customers that changed
            verification = self._verify_scd2_state(
                expected_current_records=6,
                expected_min_total_records=10,  # At least 6 current + 4 historical
                test_phase="update_test"
            )
            
            return {
                "status": "PASSED" if verification["passed"] else "FAILED",
                "phase": "update_test",
                "execution_id": execution_id,
                "rows_affected": result.get('rows_affected', 0),
                "verification": verification
            }
            
        except Exception as e:
            print(f"❌ Execution failed: {e}")
            return {"status": "FAILED", "phase": "execution", "error": str(e)}
    
    def _verify_scd2_state(self, expected_current_records: int, expected_total_records: int = None, 
                           expected_min_total_records: int = None, test_phase: str = "unknown") -> dict:
        """Verify SCD2 table state"""
        print("\n🔍 Verifying SCD2 table state...")
        
        connection = sql.connect(
            server_hostname=self.workspace_url.replace('https://', ''),
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=self.token
        )
        
        verification = {"passed": True, "checks": []}
        
        try:
            cursor = connection.cursor()
            
            # Check 1: Total records
            cursor.execute(f"""
                SELECT COUNT(*) as total_records
                FROM `{self.catalog}`.`{self.schema}`.`customer_dim_scd2`
            """)
            total_records = cursor.fetchone()[0]
            
            if expected_total_records is not None:
                check1 = total_records == expected_total_records
                verification["checks"].append({
                    "name": "Total records",
                    "expected": expected_total_records,
                    "actual": total_records,
                    "passed": check1
                })
                if check1:
                    print(f"  ✅ Total records: {total_records}")
                else:
                    print(f"  ❌ Total records: expected {expected_total_records}, got {total_records}")
                    verification["passed"] = False
            elif expected_min_total_records is not None:
                check1 = total_records >= expected_min_total_records
                verification["checks"].append({
                    "name": "Total records (minimum)",
                    "expected": f">= {expected_min_total_records}",
                    "actual": total_records,
                    "passed": check1
                })
                if check1:
                    print(f"  ✅ Total records: {total_records} (>= {expected_min_total_records})")
                else:
                    print(f"  ❌ Total records: expected >= {expected_min_total_records}, got {total_records}")
                    verification["passed"] = False
            
            # Check 2: Current records
            cursor.execute(f"""
                SELECT COUNT(*) as current_records
                FROM `{self.catalog}`.`{self.schema}`.`customer_dim_scd2`
                WHERE is_current = TRUE
            """)
            current_records = cursor.fetchone()[0]
            
            check2 = current_records == expected_current_records
            verification["checks"].append({
                "name": "Current records",
                "expected": expected_current_records,
                "actual": current_records,
                "passed": check2
            })
            
            if check2:
                print(f"  ✅ Current records: {current_records}")
            else:
                print(f"  ❌ Current records: expected {expected_current_records}, got {current_records}")
                verification["passed"] = False
            
            # Check 3: Historical records
            historical_records = total_records - current_records
            verification["checks"].append({
                "name": "Historical records",
                "expected": "N/A",
                "actual": historical_records,
                "passed": True
            })
            print(f"  ℹ️  Historical records: {historical_records}")
            
            # Check 4: No duplicate current records per business key
            cursor.execute(f"""
                SELECT customer_id, COUNT(*) as cnt
                FROM `{self.catalog}`.`{self.schema}`.`customer_dim_scd2`
                WHERE is_current = TRUE
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            
            check4 = len(duplicates) == 0
            verification["checks"].append({
                "name": "No duplicate current records",
                "expected": 0,
                "actual": len(duplicates),
                "passed": check4
            })
            
            if check4:
                print(f"  ✅ No duplicate current records")
            else:
                print(f"  ❌ Found duplicate current records: {duplicates}")
                verification["passed"] = False
            
            # Check 5: Sample current records
            cursor.execute(f"""
                SELECT customer_id, customer_name, email, customer_segment, account_status, is_current
                FROM `{self.catalog}`.`{self.schema}`.`customer_dim_scd2`
                WHERE is_current = TRUE
                ORDER BY customer_id
                LIMIT 10
            """)
            
            print("\n  📊 Sample current records:")
            for row in cursor.fetchall():
                print(f"    {row}")
            
            cursor.close()
            
        except Exception as e:
            print(f"  ❌ Verification error: {e}")
            verification["passed"] = False
            verification["error"] = str(e)
        finally:
            connection.close()
        
        return verification
    
    def run_all_tests(self) -> dict:
        """Run complete integration test suite"""
        print("\n" + "="*80)
        print("LAKEHOUSE SQLPILOT - SCD2 INTEGRATION TEST SUITE")
        print("="*80)
        print(f"Test Run ID: {self.test_run_id}")
        print(f"Workspace: {self.workspace_url}")
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"Warehouse: {self.warehouse_id}")
        
        results = {
            "test_run_id": self.test_run_id,
            "timestamp": datetime.now().isoformat(),
            "tests": []
        }
        
        # Step 1: Setup
        if not self.setup_tables():
            results["status"] = "FAILED"
            results["reason"] = "Table setup failed"
            return results
        
        # Step 2: Initial Load
        initial_load_result = self.run_initial_load()
        results["tests"].append(initial_load_result)
        
        if initial_load_result["status"] != "PASSED":
            results["status"] = "FAILED"
            results["reason"] = "Initial load failed"
            return results
        
        # Step 3: Update Test
        update_result = self.run_update_test()
        results["tests"].append(update_result)
        
        # Final status
        all_passed = all(t["status"] == "PASSED" for t in results["tests"])
        results["status"] = "PASSED" if all_passed else "FAILED"
        
        # Print summary
        print("\n" + "="*80)
        print("TEST SUMMARY")
        print("="*80)
        
        for test in results["tests"]:
            status_icon = "✅" if test["status"] == "PASSED" else "❌"
            print(f"{status_icon} {test['phase']}: {test['status']}")
            
            if "verification" in test:
                for check in test["verification"]["checks"]:
                    check_icon = "✅" if check["passed"] else "❌"
                    print(f"  {check_icon} {check['name']}: {check['actual']}")
        
        print("\n" + "="*80)
        if results["status"] == "PASSED":
            print("🎉 ALL TESTS PASSED!")
        else:
            print("❌ SOME TESTS FAILED")
        print("="*80)
        
        return results


def main():
    """Main entry point"""
    
    # Load environment variables
    workspace_url = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "592f1f39793f7795")
    catalog = os.getenv("DATABRICKS_CATALOG", "lakehouse-sqlpilot")
    schema = os.getenv("DATABRICKS_SCHEMA", "lakehouse-sqlpilot-schema")
    
    if not workspace_url or not token:
        print("❌ Error: DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        print("\nSet environment variables:")
        print("  export DATABRICKS_HOST='https://e2-dogfood.staging.cloud.databricks.com/?o=6051921418418893'")
        print("  export DATABRICKS_TOKEN='your-token-here'")
        sys.exit(1)
    
    # Run tests
    tester = SCD2IntegrationTester(
        workspace_url=workspace_url,
        token=token,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema
    )
    
    results = tester.run_all_tests()
    
    # Save results to file
    results_file = Path(__file__).parent / f"test_results_{tester.test_run_id}.json"
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Results saved to: {results_file}")
    
    # Exit with appropriate code
    sys.exit(0 if results["status"] == "PASSED" else 1)


if __name__ == "__main__":
    main()

