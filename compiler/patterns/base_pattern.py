"""
Base Pattern - Abstract base class for all SQL patterns
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime, timezone


class BasePattern(ABC):
    """Abstract base class for SQL generation patterns"""
    
    def __init__(self, plan: Dict[str, Any]):
        """
        Initialize pattern with plan
        
        Args:
            plan: Validated plan dictionary
        """
        self.plan = plan
        self.plan_metadata = plan.get('plan_metadata', {})
        self.pattern_type = plan.get('pattern', {}).get('type')
        self.source = plan.get('source', {})
        self.target = plan.get('target', {})
        self.pattern_config = plan.get('pattern_config', {})
        self.execution_config = plan.get('execution_config', {})
    
    @abstractmethod
    def validate_config(self) -> List[str]:
        """
        Validate pattern-specific configuration
        
        Returns:
            List of validation errors (empty if valid)
        """
        pass
    
    @abstractmethod
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """
        Generate SQL for this pattern
        
        Args:
            context: Execution context with variables
            
        Returns:
            Generated SQL string
        """
        pass
    
    @abstractmethod
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """
        Generate preview queries (read-only)
        
        Args:
            context: Execution context
            
        Returns:
            List of preview SQL queries
        """
        pass
    
    def get_source_fqn(self) -> str:
        """Get fully qualified source table name"""
        return f"`{self.source['catalog']}`.`{self.source['schema']}`.`{self.source['table']}`"
    
    def get_target_fqn(self) -> str:
        """Get fully qualified target table name"""
        return f"`{self.target['catalog']}`.`{self.target['schema']}`.`{self.target['table']}`"
    
    def generate_sql_header(self, context: Dict[str, Any]) -> str:
        """Generate SQLPilot header for all generated SQL"""
        header = f"""-- LAKEHOUSE SQLPILOT GENERATED SQL
-- Generated: {context.get('generated_at', datetime.now(timezone.utc).isoformat())}
-- plan_id: {self.plan_metadata.get('plan_id')}
-- plan_name: {self.plan_metadata.get('plan_name')}
-- plan_version: {self.plan_metadata.get('version')}
-- pattern: {self.pattern_type}
-- execution_id: {context.get('execution_id', 'preview')}
--
-- ⚠️  DO NOT MODIFY THIS SQL MANUALLY
-- This SQL was deterministically generated from a validated plan.
-- To make changes, update the plan and regenerate.
--
"""
        return header
    
    def get_column_list(self, columns: List[str] = None) -> str:
        """Get comma-separated column list"""
        if columns is None:
            columns = self.source.get('columns', [])
        
        if not columns:
            return '*'
        
        return ', '.join([f"`{col}`" for col in columns])
