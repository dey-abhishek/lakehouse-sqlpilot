"""
SQL Executor - Executes SQL on Databricks SQL Warehouse
"""

import uuid
from typing import Dict, Any, Optional
from datetime import datetime
import time

from databricks import sql
from databricks.sdk import WorkspaceClient

from .tracker import ExecutionTracker, ExecutionState
from .retry_handler import RetryHandler


class ExecutionError(Exception):
    """Exception raised during SQL execution"""
    pass


class SQLExecutor:
    """Executes SQL on Databricks SQL Warehouse with tracking and retry"""
    
    def __init__(self,
                 workspace_client: WorkspaceClient,
                 execution_tracker: ExecutionTracker,
                 retry_handler: Optional[RetryHandler] = None):
        """
        Initialize SQL executor
        
        Args:
            workspace_client: Databricks workspace client
            execution_tracker: Execution tracker for persistence
            retry_handler: Optional retry handler (creates default if not provided)
        """
        self.workspace_client = workspace_client
        self.tracker = execution_tracker
        self.retry_handler = retry_handler or RetryHandler()
    
    def execute(self,
                plan_id: str,
                plan_version: str,
                sql: str,
                warehouse_id: str,
                executor_user: str,
                timeout_seconds: int = 3600,
                variables: Optional[Dict[str, Any]] = None,
                metadata: Optional[Dict[str, Any]] = None,
                source_table_fqn: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute SQL with tracking and retry
        
        Args:
            plan_id: Plan ID
            plan_version: Plan version
            sql: SQL to execute
            warehouse_id: Databricks SQL Warehouse ID
            executor_user: User executing the SQL
            timeout_seconds: Execution timeout
            variables: Optional variables for parameterization
            metadata: Optional metadata to store with execution
            source_table_fqn: Optional source table FQN for pre-flight validation
            
        Returns:
            Execution result dictionary
            
        Raises:
            ExecutionError: If execution fails or pre-flight checks fail
        """
        execution_id = str(uuid.uuid4())
        
        # PRE-FLIGHT CHECK: Verify source table exists (if provided)
        if source_table_fqn:
            try:
                self._verify_source_table_exists(source_table_fqn, warehouse_id)
            except Exception as e:
                raise ExecutionError(
                    f"Pre-flight check failed - Source table does not exist: {source_table_fqn}. "
                    f"SQLPilot requires source tables to exist before execution. Error: {str(e)}"
                )
        
        # Create execution record
        record = self.tracker.create_execution(
            execution_id=execution_id,
            plan_id=plan_id,
            plan_version=plan_version,
            sql_text=sql,
            warehouse_id=warehouse_id,
            executor_user=executor_user,
            metadata=metadata or {}
        )
        
        try:
            # Execute with retry
            result = self.retry_handler.execute_with_retry(
                self._execute_sql,
                execution_id=execution_id,
                sql_statement=sql,
                warehouse_id=warehouse_id,
                timeout_seconds=timeout_seconds,
                variables=variables,
                on_retry=lambda attempt, error: self._on_retry(execution_id, attempt, error)
            )
            
            return result
            
        except Exception as e:
            # Mark as failed
            self.tracker.update_state(
                execution_id=execution_id,
                new_state=ExecutionState.FAILED,
                error_message=str(e)
            )
            raise ExecutionError(f"Execution failed: {str(e)}")
    
    def _execute_sql(self,
                     execution_id: str,
                     sql_statement: str,
                     warehouse_id: str,
                     timeout_seconds: int,
                     variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute SQL (internal method called by retry handler)
        
        Args:
            execution_id: Execution ID
            sql: SQL to execute
            warehouse_id: Warehouse ID
            timeout_seconds: Timeout
            variables: Optional variables
            
        Returns:
            Execution result
        """
        # Update state to RUNNING
        self.tracker.update_state(
            execution_id=execution_id,
            new_state=ExecutionState.RUNNING
        )
        
        # Get connection details from workspace client
        host = self.workspace_client.config.host
        token = self.workspace_client.config.token
        
        # Connect to SQL Warehouse
        with sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token
        ) as connection:
            
            with connection.cursor() as cursor:
                # Set timeout
                cursor.execute(f"SET statement_timeout = {timeout_seconds}")
                
                # Execute SQL
                start_time = time.time()
                cursor.execute(sql_statement, parameters=variables or {})
                execution_time = time.time() - start_time
                
                # Get query ID from cursor (if available)
                query_id = getattr(cursor, 'query_id', None)
                
                # Get rows affected (if applicable)
                rows_affected = cursor.rowcount if cursor.rowcount >= 0 else None
                
                # Fetch results for SELECT queries
                results = None
                if cursor.description:
                    # This was a SELECT query
                    results = cursor.fetchall()
                
                # Update state to SUCCESS
                self.tracker.update_state(
                    execution_id=execution_id,
                    new_state=ExecutionState.SUCCESS,
                    query_id=query_id,
                    rows_affected=rows_affected
                )
                
                return {
                    'execution_id': execution_id,
                    'state': ExecutionState.SUCCESS.value,
                    'query_id': query_id,
                    'rows_affected': rows_affected,
                    'execution_time_seconds': execution_time,
                    'results': results
                }
    
    def _on_retry(self, execution_id: str, attempt: int, error: Exception) -> None:
        """
        Callback invoked before retry
        
        Args:
            execution_id: Execution ID
            attempt: Retry attempt number
            error: Error that triggered retry
        """
        # Increment retry counter
        self.tracker.increment_retry(execution_id)
        
        # Update state back to PENDING
        self.tracker.update_state(
            execution_id=execution_id,
            new_state=ExecutionState.PENDING,
            error_message=f"Retry {attempt + 1}: {str(error)}"
        )
    
    def _verify_source_table_exists(self, table_fqn: str, warehouse_id: str) -> None:
        """
        Verify source table exists before execution (fail-fast check)
        
        Args:
            table_fqn: Fully qualified table name (catalog.schema.table)
            warehouse_id: Warehouse ID to use for verification
            
        Raises:
            ExecutionError: If table does not exist
        """
        # Parse FQN
        parts = table_fqn.replace('`', '').split('.')
        if len(parts) != 3:
            raise ExecutionError(f"Invalid table FQN format: {table_fqn}. Expected: catalog.schema.table")
        
        catalog, schema, table = parts
        
        # Connect and verify
        host = self.workspace_client.config.host
        token = self.workspace_client.config.token
        
        with sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token
        ) as connection:
            with connection.cursor() as cursor:
                # Use SHOW TABLES (more reliable than SELECT COUNT)
                cursor.execute(f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}'")
                results = cursor.fetchall()
                
                if not results:
                    raise ExecutionError(
                        f"Source table `{catalog}`.`{schema}`.`{table}` does not exist. "
                        f"Create the table before executing this plan."
                    )
    
    def cancel_execution(self, execution_id: str) -> None:
        """
        Cancel running execution
        
        Args:
            execution_id: Execution ID to cancel
        """
        record = self.tracker.get_execution(execution_id)
        
        if not record:
            raise ValueError(f"Execution {execution_id} not found")
        
        if record.state != ExecutionState.RUNNING:
            raise ValueError(f"Execution {execution_id} is not running (state: {record.state.value})")
        
        # Cancel query in Databricks
        if record.query_id:
            try:
                # Use SQL to cancel query
                host = self.workspace_client.config.host
                token = self.workspace_client.config.token
                
                with sql.connect(
                    server_hostname=host,
                    http_path=f"/sql/1.0/warehouses/{record.warehouse_id}",
                    access_token=token
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"CANCEL QUERY '{record.query_id}'")
            except Exception as e:
                # Log error but continue to update state
                pass
        
        # Update state to CANCELLED
        self.tracker.update_state(
            execution_id=execution_id,
            new_state=ExecutionState.CANCELLED
        )
    
    def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """
        Get execution status
        
        Args:
            execution_id: Execution ID
            
        Returns:
            Status dictionary
        """
        record = self.tracker.get_execution(execution_id)
        
        if not record:
            raise ValueError(f"Execution {execution_id} not found")
        
        return record.to_dict()
    
    def preview_sql(self,
                   sql: str,
                   warehouse_id: str,
                   limit: int = 10) -> Dict[str, Any]:
        """
        Preview SQL results without persisting execution
        
        Args:
            sql: SQL to preview
            warehouse_id: Warehouse ID
            limit: Number of rows to return
            
        Returns:
            Preview results
        """
        # Wrap in SELECT with LIMIT for safety
        preview_sql = f"""
{sql.rstrip(';')}
LIMIT {limit};
"""
        
        host = self.workspace_client.config.host
        token = self.workspace_client.config.token
        
        with sql.connect(
            server_hostname=host,
            http_path=f"/sql/1.0/warehouses/{warehouse_id}",
            access_token=token
        ) as connection:
            
            with connection.cursor() as cursor:
                # Set short timeout for preview
                cursor.execute("SET statement_timeout = 60")
                
                # Execute preview
                cursor.execute(preview_sql)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                # Fetch limited results
                results = cursor.fetchmany(limit)
                
                return {
                    'columns': columns,
                    'rows': results,
                    'row_count': len(results)
                }

