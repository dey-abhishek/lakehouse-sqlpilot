"""
Full Replace Pattern - Atomically replace entire table
"""

from typing import Dict, Any, List
from .base_pattern import BasePattern


class FullReplacePattern(BasePattern):
    """Full replace pattern using CREATE OR REPLACE"""
    
    def validate_config(self) -> List[str]:
        """Validate full replace configuration"""
        # No specific config required for full replace
        return []
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate full replace SQL"""
        sql = self.generate_sql_header(context)
        
        sql += f"""
-- Full Replace: Atomically replace entire table
-- Uses CREATE OR REPLACE for transactional replacement

CREATE OR REPLACE TABLE {self.get_target_fqn()}
AS
SELECT {self.get_column_list()}
FROM {self.get_source_fqn()};
"""
        return sql
    
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """Generate preview queries"""
        queries = [
            # Current row count
            f"""
            SELECT 
                'Current Rows in Target' as metric,
                COUNT(*) as value
            FROM {self.get_target_fqn()}
            """,
            # New row count
            f"""
            SELECT 
                'New Rows from Source' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()}
            """
        ]
        
        return queries
