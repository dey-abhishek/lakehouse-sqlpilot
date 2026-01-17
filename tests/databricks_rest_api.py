"""
Databricks SQL Statement Execution API Helper

Provides REST API-based SQL execution to bypass Python connector limitations.
Uses the official Databricks SQL Statement Execution API.

HYBRID APPROACH: Automatically switches between REST API and SQL connector
- MERGE/UPDATE/DELETE → SQL connector (avoids Spark bug)
- SELECT/CREATE/DROP/etc → REST API (faster, no connection overhead)

Reference: https://docs.databricks.com/api/workspace/statementexecution
"""

import time
import requests
from typing import Dict, Any, Optional, List
from datetime import datetime
import re


def _requires_sql_connector(statement: str) -> bool:
    """
    Determine if a statement should use SQL connector instead of REST API
    
    MERGE, UPDATE, DELETE operations hit a Spark codegen bug via REST API
    but work fine via SQL connector or SQL Editor.
    
    Args:
        statement: SQL statement to analyze
        
    Returns:
        True if should use SQL connector, False if REST API is safe
    """
    # Remove comments and normalize whitespace
    sql_upper = re.sub(r'--.*$', '', statement, flags=re.MULTILINE)  # Remove line comments
    sql_upper = re.sub(r'/\*.*?\*/', '', sql_upper, flags=re.DOTALL)  # Remove block comments
    sql_upper = sql_upper.upper().strip()
    
    # Check for problematic operations
    problematic_keywords = ['MERGE', 'UPDATE', 'DELETE']
    
    for keyword in problematic_keywords:
        # Look for keyword at start of statement or after semicolon
        if re.search(rf'\b{keyword}\b', sql_upper):
            return True
    
    return False


class DatabricksStatementAPI:
    """
    Databricks SQL Statement Execution API client
    
    This uses REST APIs instead of the Python SQL connector to:
    - Bypass Spark codegen bugs in the Python connector
    - Provide more reliable test execution
    - Enable better error handling
    """
    
    def __init__(self, server_hostname: str, access_token: str, warehouse_id: str, 
                 catalog: str = None, schema: str = None):
        """
        Initialize the API client
        
        Args:
            server_hostname: Databricks workspace hostname (without https://)
            access_token: Personal access token or OAuth token
            warehouse_id: SQL Warehouse ID for execution
            catalog: Default catalog for SQL execution (optional)
            schema: Default schema for SQL execution (optional)
        """
        self.base_url = f"https://{server_hostname}"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
    
    def execute_statement(
        self,
        statement: str,
        timeout_seconds: int = 3600,
        wait_timeout: str = "30s",
        on_wait_timeout: str = "CONTINUE",
        force_connector: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement using the optimal method
        
        HYBRID APPROACH:
        - MERGE/UPDATE/DELETE → SQL connector (avoids Spark bug)
        - Other operations → REST API (faster, simpler)
        
        Args:
            statement: SQL statement to execute
            timeout_seconds: Maximum time to wait for completion
            wait_timeout: Server-side wait timeout (e.g., "30s", "5m")
            on_wait_timeout: Action when wait timeout reached ("CONTINUE" or "CANCEL")
            force_connector: Force use of SQL connector regardless of statement type
            
        Returns:
            Dict with execution results
            
        Raises:
            Exception: If execution fails or times out
        """
        # Determine execution method
        use_connector = force_connector or _requires_sql_connector(statement)
        
        if use_connector:
            print(f"    🔄 Using SQL connector (MERGE/UPDATE/DELETE operation)")
            return self._execute_via_connector(statement, timeout_seconds)
        else:
            print(f"    🌐 Using REST API (standard operation)")
            return self._execute_via_rest_api(statement, timeout_seconds, wait_timeout, on_wait_timeout)
    
    def _execute_via_connector(self, statement: str, timeout_seconds: int) -> Dict[str, Any]:
        """
        Execute SQL via Databricks SQL connector (for MERGE/UPDATE/DELETE)
        
        This method uses the Python SQL connector which uses a different
        execution path that doesn't hit the Spark codegen bug.
        """
        from databricks import sql
        
        print(f"    📤 Connecting to warehouse via SQL connector...")
        
        # Extract hostname without https://
        hostname = self.base_url.replace("https://", "")
        
        start_time = time.time()
        
        with sql.connect(
            server_hostname=hostname,
            http_path=f"/sql/1.0/warehouses/{self.warehouse_id}",
            access_token=self.headers["Authorization"].replace("Bearer ", ""),
            catalog=self.catalog,
            schema=self.schema
        ) as connection:
            with connection.cursor() as cursor:
                print(f"    ⚡ Executing SQL via connector...")
                cursor.execute(statement)
                
                # Try to fetch results if available
                try:
                    rows = cursor.fetchall()
                    row_count = len(rows) if rows else 0
                except Exception:
                    rows = []
                    row_count = 0
                
                execution_time = int((time.time() - start_time) * 1000)
                
                print(f"    ✅ Statement completed successfully via connector")
                print(f"    ⏱️  Execution time: {execution_time}ms")
                
                return {
                    "statement_id": f"connector-{int(time.time() * 1000)}",
                    "status": "SUCCEEDED",
                    "row_count": row_count,
                    "execution_time_ms": execution_time,
                    "method": "sql_connector"
                }
    
    def _execute_via_rest_api(
        self,
        statement: str,
        timeout_seconds: int,
        wait_timeout: str,
        on_wait_timeout: str
    ) -> Dict[str, Any]:
        """
        Execute SQL via REST API (original method, for non-problematic operations)
        """
        # Submit statement
        submit_url = f"{self.base_url}/api/2.0/sql/statements"
        
        # Build payload according to official Databricks API spec
        # Reference: https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial
        payload = {
            "warehouse_id": self.warehouse_id,
            "statement": statement,
            "wait_timeout": wait_timeout,
            "on_wait_timeout": on_wait_timeout
        }
        
        # Add catalog and schema if specified (sets execution context)
        if self.catalog:
            payload["catalog"] = self.catalog
        if self.schema:
            payload["schema"] = self.schema
        
        print(f"    📤 Submitting SQL statement via REST API...")
        print(f"       Warehouse ID: {self.warehouse_id}")
        if self.catalog:
            print(f"       Catalog: {self.catalog}")
        if self.schema:
            print(f"       Schema: {self.schema}")
        print(f"       Statement length: {len(statement)} characters")
        print(f"       API endpoint: {submit_url}")
        response = requests.post(submit_url, headers=self.headers, json=payload)
        
        if response.status_code != 200:
            raise Exception(f"Failed to submit statement: {response.status_code} - {response.text}")
        
        result = response.json()
        statement_id = result["statement_id"]
        status = result["status"]["state"]
        
        print(f"    ✅ Statement submitted: {statement_id}")
        print(f"    ⏳ Initial status: {status}")
        
        # Poll for completion
        start_time = time.time()
        while status in ["PENDING", "RUNNING"]:
            if time.time() - start_time > timeout_seconds:
                self.cancel_statement(statement_id)
                raise Exception(f"Statement execution timeout after {timeout_seconds}s")
            
            time.sleep(2)  # Poll every 2 seconds
            
            status_url = f"{self.base_url}/api/2.0/sql/statements/{statement_id}"
            response = requests.get(status_url, headers=self.headers)
            
            if response.status_code != 200:
                raise Exception(f"Failed to get status: {response.status_code} - {response.text}")
            
            result = response.json()
            status = result["status"]["state"]
            
            elapsed = int(time.time() - start_time)
            print(f"    ⏳ Status: {status} (elapsed: {elapsed}s)")
        
        # Check final status
        if status == "FAILED":
            error = result["status"].get("error", {})
            error_msg = error.get("message", "Unknown error")
            error_code = error.get("error_code", "UNKNOWN")
            
            # Log full error details for debugging
            print(f"    ❌ Statement failed!")
            print(f"       Error code: {error_code}")
            print(f"       Error message: {error_msg[:200]}")
            
            # Also log the full result for debugging
            import json
            print(f"    🔍 Full error response:")
            print(json.dumps(result, indent=2)[:500])
            
            raise Exception(f"Statement execution failed: {error_msg}")
        
        if status == "CANCELED":
            raise Exception("Statement execution was canceled")
        
        # Success!
        print(f"    ✅ Statement completed successfully")
        
        # Extract result summary
        manifest = result.get("manifest", {})
        result_summary = {
            "statement_id": statement_id,
            "status": status,
            "row_count": manifest.get("total_row_count", 0),
            "chunk_count": manifest.get("total_chunk_count", 0),
            "schema": manifest.get("schema", {}),
            "execution_time_ms": result["status"].get("execution_time_ms", 0),
            "method": "rest_api"
        }
        
        return result_summary
    
    def execute_statement_with_results(
        self,
        statement: str,
        timeout_seconds: int = 3600,
        max_rows: int = 1000
    ) -> Dict[str, Any]:
        """
        Execute a SQL statement and fetch results
        
        Args:
            statement: SQL statement to execute
            timeout_seconds: Maximum time to wait
            max_rows: Maximum number of rows to fetch
            
        Returns:
            Dict with execution results and data
        """
        result_summary = self.execute_statement(statement, timeout_seconds)
        statement_id = result_summary["statement_id"]
        
        # Fetch results if available
        if result_summary["chunk_count"] > 0:
            print(f"    📥 Fetching results (chunks: {result_summary['chunk_count']})...")
            
            all_rows = []
            for chunk_idx in range(result_summary["chunk_count"]):
                chunk_url = f"{self.base_url}/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_idx}"
                response = requests.get(chunk_url, headers=self.headers)
                
                if response.status_code != 200:
                    print(f"    ⚠️  Failed to fetch chunk {chunk_idx}")
                    continue
                
                chunk_data = response.json()
                rows = chunk_data.get("data_array", [])
                all_rows.extend(rows)
                
                if len(all_rows) >= max_rows:
                    break
            
            result_summary["rows"] = all_rows[:max_rows]
            print(f"    ✅ Fetched {len(result_summary['rows'])} rows")
        else:
            result_summary["rows"] = []
        
        return result_summary
    
    def cancel_statement(self, statement_id: str) -> None:
        """
        Cancel a running statement
        
        Args:
            statement_id: Statement ID to cancel
        """
        cancel_url = f"{self.base_url}/api/2.0/sql/statements/{statement_id}/cancel"
        response = requests.post(cancel_url, headers=self.headers)
        
        if response.status_code == 200:
            print(f"    🛑 Statement {statement_id} canceled")
        else:
            print(f"    ⚠️  Failed to cancel statement: {response.status_code}")
    
    def execute_multiple_statements(
        self,
        statements: List[str],
        stop_on_error: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Execute multiple SQL statements in sequence
        
        Args:
            statements: List of SQL statements
            stop_on_error: Stop execution if a statement fails
            
        Returns:
            List of execution results
        """
        results = []
        
        for i, stmt in enumerate(statements, 1):
            print(f"\n  📍 Executing statement {i}/{len(statements)}...")
            print(f"     Preview: {stmt[:80]}...")
            
            try:
                result = self.execute_statement(stmt)
                results.append({
                    "index": i,
                    "success": True,
                    "result": result
                })
                print(f"  ✅ Statement {i} completed")
                
            except Exception as e:
                print(f"  ❌ Statement {i} failed: {str(e)[:100]}")
                results.append({
                    "index": i,
                    "success": False,
                    "error": str(e)
                })
                
                if stop_on_error:
                    raise Exception(f"Statement {i} failed: {e}")
        
        return results

