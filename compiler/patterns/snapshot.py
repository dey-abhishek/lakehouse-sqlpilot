"""
Snapshot Pattern - Point-in-time data snapshots
"""

from typing import Dict, Any, List
from .base_pattern import BasePattern


class SnapshotPattern(BasePattern):
    """
    Snapshot pattern for capturing point-in-time data.
    
    Use Case:
    - Daily inventory snapshots
    - End-of-day account balances
    - Historical state tracking
    - Point-in-time reporting
    
    Strategy:
    - Capture complete dataset at specific point in time
    - Partition by snapshot date for efficient querying
    - Each snapshot is APPENDED (accumulates over time)
    - Historical snapshots retained for time-travel queries
    
    Important Notes:
    - This pattern uses INSERT INTO to accumulate snapshots
    - Target table schema must match source columns + snapshot_date_column
    - If you need to replace all snapshots, use FULL_REPLACE pattern instead
    - For schema changes, you must recreate the target table manually
    """
    
    def validate_config(self) -> List[str]:
        """Validate snapshot configuration"""
        errors = []
        
        if not self.pattern_config.get('snapshot_date_column'):
            errors.append("Snapshot pattern requires 'snapshot_date_column' in pattern_config")
        
        # Check if snapshot_date_column is in source columns (should NOT be)
        source_columns = self.source.get('columns', [])
        snapshot_col = self.pattern_config.get('snapshot_date_column', '')
        
        if source_columns and snapshot_col in source_columns:
            errors.append(
                f"snapshot_date_column '{snapshot_col}' should NOT be in source columns. "
                f"It will be added automatically by the pattern."
            )
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate snapshot SQL"""
        snapshot_col = self.pattern_config['snapshot_date_column']
        partition_cols = self.pattern_config.get('partition_columns', [snapshot_col])
        
        sql = self.generate_sql_header(context)
        
        # Get snapshot date from context or use CURRENT_DATE
        snapshot_date = context.get('snapshot_date', 'CURRENT_DATE()')
        
        # Get source columns, excluding snapshot_date_column if it already exists
        source_columns = self.source.get('columns', [])
        
        # Filter out the snapshot column if it's already in source
        select_columns = [col for col in source_columns if col != snapshot_col]
        
        # Build the column list for SELECT
        if select_columns:
            # Explicit columns specified
            column_list = ',\n    '.join([f'`{col}`' for col in select_columns])
        else:
            # No columns specified, use * (source doesn't have snapshot column)
            column_list = '*'
        
        sql += f"""
-- Snapshot Pattern: Point-in-Time Data Capture
-- Snapshot Column: {snapshot_col}
-- Partitions: {', '.join(partition_cols)}
-- Strategy: Append complete dataset with snapshot timestamp

INSERT INTO {self.get_target_fqn()}
SELECT 
    {column_list},
    {snapshot_date} AS `{snapshot_col}`
FROM {self.get_source_fqn()};
"""
        return sql
    
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """Generate preview queries"""
        snapshot_col = self.pattern_config['snapshot_date_column']
        
        queries = [
            # Count of records to snapshot
            f"""
            SELECT 
                'Records to Snapshot' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()}
            """,
            # Latest snapshot info
            f"""
            SELECT 
                'Latest Snapshot Date' as metric,
                MAX({snapshot_col}) as value
            FROM {self.get_target_fqn()}
            WHERE {snapshot_col} IS NOT NULL
            """
        ]
        
        return queries

