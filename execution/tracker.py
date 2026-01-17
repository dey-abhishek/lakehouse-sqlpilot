"""
Execution Tracker - Tracks SQL execution state and history
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from enum import Enum


class ExecutionState(Enum):
    """Execution states"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


class ExecutionRecord:
    """Represents a single execution record"""
    
    def __init__(self,
                 execution_id: str,
                 plan_id: str,
                 plan_version: str,
                 sql_text: str,
                 warehouse_id: str,
                 executor_user: str):
        self.execution_id = execution_id
        self.plan_id = plan_id
        self.plan_version = plan_version
        self.state = ExecutionState.PENDING
        self.sql_text = sql_text
        self.query_id = None
        self.started_at = None
        self.completed_at = None
        self.rows_affected = None
        self.error_message = None
        self.retries = 0
        self.executor_user = executor_user
        self.warehouse_id = warehouse_id
        self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'execution_id': self.execution_id,
            'plan_id': self.plan_id,
            'plan_version': self.plan_version,
            'state': self.state.value,
            'sql_text': self.sql_text,
            'query_id': self.query_id,
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'rows_affected': self.rows_affected,
            'error_message': self.error_message,
            'retries': self.retries,
            'executor_user': self.executor_user,
            'warehouse_id': self.warehouse_id,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionRecord':
        """Create from dictionary"""
        record = cls(
            execution_id=data['execution_id'],
            plan_id=data['plan_id'],
            plan_version=data['plan_version'],
            sql_text=data['sql_text'],
            warehouse_id=data['warehouse_id'],
            executor_user=data['executor_user']
        )
        record.state = ExecutionState(data['state'])
        record.query_id = data.get('query_id')
        record.rows_affected = data.get('rows_affected')
        record.error_message = data.get('error_message')
        record.retries = data.get('retries', 0)
        record.metadata = data.get('metadata', {})
        
        if data.get('started_at'):
            record.started_at = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            record.completed_at = datetime.fromisoformat(data['completed_at'])
        
        return record


class ExecutionTracker:
    """Tracks execution records in storage"""
    
    def __init__(self, storage_backend: 'StorageBackend'):
        """
        Initialize execution tracker
        
        Args:
            storage_backend: Backend for persisting execution records
        """
        self.storage = storage_backend
    
    def create_execution(self,
                        execution_id: str,
                        plan_id: str,
                        plan_version: str,
                        sql_text: str,
                        warehouse_id: str,
                        executor_user: str,
                        metadata: Optional[Dict[str, Any]] = None) -> ExecutionRecord:
        """
        Create new execution record
        
        Args:
            execution_id: Unique execution ID
            plan_id: Plan ID
            plan_version: Plan version
            sql_text: Generated SQL
            warehouse_id: Databricks warehouse ID
            executor_user: User executing the plan
            metadata: Additional metadata
            
        Returns:
            ExecutionRecord instance
        """
        record = ExecutionRecord(
            execution_id=execution_id,
            plan_id=plan_id,
            plan_version=plan_version,
            sql_text=sql_text,
            warehouse_id=warehouse_id,
            executor_user=executor_user
        )
        
        if metadata:
            record.metadata = metadata
        
        # Persist to storage
        self.storage.save_execution(record)
        
        return record
    
    def update_state(self,
                    execution_id: str,
                    new_state: ExecutionState,
                    query_id: Optional[str] = None,
                    error_message: Optional[str] = None,
                    rows_affected: Optional[int] = None) -> None:
        """
        Update execution state
        
        Args:
            execution_id: Execution ID
            new_state: New state
            query_id: Databricks query ID
            error_message: Error message if failed
            rows_affected: Number of rows affected
        """
        record = self.storage.get_execution(execution_id)
        
        if not record:
            raise ValueError(f"Execution {execution_id} not found")
        
        record.state = new_state
        
        if query_id:
            record.query_id = query_id
        
        if error_message:
            record.error_message = error_message
        
        if rows_affected is not None:
            record.rows_affected = rows_affected
        
        # Update timestamps
        if new_state == ExecutionState.RUNNING and not record.started_at:
            record.started_at = datetime.now(timezone.utc)
        
        if new_state in [ExecutionState.SUCCESS, ExecutionState.FAILED, ExecutionState.TIMEOUT, ExecutionState.CANCELLED]:
            record.completed_at = datetime.now(timezone.utc)
        
        # Persist update
        self.storage.save_execution(record)
    
    def increment_retry(self, execution_id: str) -> int:
        """
        Increment retry counter
        
        Args:
            execution_id: Execution ID
            
        Returns:
            New retry count
        """
        record = self.storage.get_execution(execution_id)
        
        if not record:
            raise ValueError(f"Execution {execution_id} not found")
        
        record.retries += 1
        self.storage.save_execution(record)
        
        return record.retries
    
    def get_execution(self, execution_id: str) -> Optional[ExecutionRecord]:
        """Get execution record by ID"""
        return self.storage.get_execution(execution_id)
    
    def list_executions(self,
                       plan_id: Optional[str] = None,
                       state: Optional[ExecutionState] = None,
                       limit: int = 100) -> List[ExecutionRecord]:
        """
        List execution records
        
        Args:
            plan_id: Filter by plan ID
            state: Filter by state
            limit: Maximum records to return
            
        Returns:
            List of execution records
        """
        return self.storage.list_executions(plan_id, state, limit)
    
    def get_execution_history(self, plan_id: str, limit: int = 50) -> List[ExecutionRecord]:
        """
        Get execution history for a plan
        
        Args:
            plan_id: Plan ID
            limit: Maximum records to return
            
        Returns:
            List of execution records sorted by started_at desc
        """
        return self.storage.list_executions(plan_id=plan_id, limit=limit)


class StorageBackend:
    """Abstract storage backend for execution records"""
    
    def save_execution(self, record: ExecutionRecord) -> None:
        """Save execution record"""
        raise NotImplementedError
    
    def get_execution(self, execution_id: str) -> Optional[ExecutionRecord]:
        """Get execution record by ID"""
        raise NotImplementedError
    
    def list_executions(self,
                       plan_id: Optional[str] = None,
                       state: Optional[ExecutionState] = None,
                       limit: int = 100) -> List[ExecutionRecord]:
        """List execution records"""
        raise NotImplementedError


class DeltaTableStorage(StorageBackend):
    """Storage backend using Delta table"""
    
    def __init__(self, table_fqn: str, spark_session):
        """
        Initialize Delta table storage
        
        Args:
            table_fqn: Fully qualified table name (catalog.schema.table)
            spark_session: Spark session for Delta operations
        """
        self.table_fqn = table_fqn
        self.spark = spark_session
        self._ensure_table_exists()
    
    def _ensure_table_exists(self) -> None:
        """Ensure execution log table exists"""
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_fqn} (
          execution_id STRING NOT NULL,
          plan_id STRING NOT NULL,
          plan_version STRING NOT NULL,
          state STRING NOT NULL,
          sql_text STRING NOT NULL,
          query_id STRING,
          started_at TIMESTAMP,
          completed_at TIMESTAMP,
          rows_affected BIGINT,
          error_message STRING,
          retries INT DEFAULT 0,
          executor_user STRING NOT NULL,
          warehouse_id STRING NOT NULL,
          metadata STRING,
          PRIMARY KEY (execution_id)
        ) 
        TBLPROPERTIES (
          delta.enableChangeDataFeed = true,
          delta.columnMapping.mode = 'name'
        );
        """
        self.spark.sql(create_sql)
    
    def save_execution(self, record: ExecutionRecord) -> None:
        """Save execution record to Delta table"""
        import json
        
        # Convert to dict for insertion
        data = record.to_dict()
        data['metadata'] = json.dumps(data['metadata'])
        
        # Escape strings for SQL
        sql_text_escaped = data['sql_text'].replace("'", "''")
        error_message_escaped = data['error_message'].replace("'", "''") if data['error_message'] else None
        
        # Build field values
        query_id_value = f"'{data['query_id']}'" if data['query_id'] else 'NULL'
        started_at_value = f"TIMESTAMP '{data['started_at']}'" if data['started_at'] else 'NULL'
        completed_at_value = f"TIMESTAMP '{data['completed_at']}'" if data['completed_at'] else 'NULL'
        rows_affected_value = data['rows_affected'] if data['rows_affected'] is not None else 'NULL'
        error_message_value = f"'{error_message_escaped}'" if error_message_escaped else 'NULL'
        
        # Use MERGE for upsert
        merge_sql = f"""
        MERGE INTO {self.table_fqn} AS target
        USING (SELECT 
          '{data['execution_id']}' AS execution_id,
          '{data['plan_id']}' AS plan_id,
          '{data['plan_version']}' AS plan_version,
          '{data['state']}' AS state,
          '{sql_text_escaped}' AS sql_text,
          {query_id_value} AS query_id,
          {started_at_value} AS started_at,
          {completed_at_value} AS completed_at,
          {rows_affected_value} AS rows_affected,
          {error_message_value} AS error_message,
          {data['retries']} AS retries,
          '{data['executor_user']}' AS executor_user,
          '{data['warehouse_id']}' AS warehouse_id,
          '{data['metadata']}' AS metadata
        ) AS source
        ON target.execution_id = source.execution_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        
        self.spark.sql(merge_sql)
    
    def get_execution(self, execution_id: str) -> Optional[ExecutionRecord]:
        """Get execution record from Delta table"""
        import json
        
        query = f"""
        SELECT * FROM {self.table_fqn}
        WHERE execution_id = '{execution_id}'
        """
        
        result = self.spark.sql(query).collect()
        
        if not result:
            return None
        
        row = result[0].asDict()
        row['metadata'] = json.loads(row['metadata']) if row['metadata'] else {}
        
        return ExecutionRecord.from_dict(row)
    
    def list_executions(self,
                       plan_id: Optional[str] = None,
                       state: Optional[ExecutionState] = None,
                       limit: int = 100) -> List[ExecutionRecord]:
        """List execution records from Delta table"""
        import json
        
        where_clauses = []
        if plan_id:
            where_clauses.append(f"plan_id = '{plan_id}'")
        if state:
            where_clauses.append(f"state = '{state.value}'")
        
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        
        query = f"""
        SELECT * FROM {self.table_fqn}
        {where_sql}
        ORDER BY started_at DESC
        LIMIT {limit}
        """
        
        results = self.spark.sql(query).collect()
        
        records = []
        for row in results:
            row_dict = row.asDict()
            row_dict['metadata'] = json.loads(row_dict['metadata']) if row_dict['metadata'] else {}
            records.append(ExecutionRecord.from_dict(row_dict))
        
        return records

