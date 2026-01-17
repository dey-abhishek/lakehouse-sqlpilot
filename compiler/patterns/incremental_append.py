"""
Incremental Append Pattern - Load new records based on watermark
"""

from typing import Dict, Any, List
from .base_pattern import BasePattern


class IncrementalAppendPattern(BasePattern):
    """Incremental append pattern using watermark column"""
    
    def validate_config(self) -> List[str]:
        """Validate incremental append configuration"""
        errors = []
        
        if not self.pattern_config.get('watermark_column'):
            errors.append("Incremental append requires 'watermark_column' in pattern_config")
        
        if not self.pattern_config.get('watermark_type'):
            errors.append("Incremental append requires 'watermark_type' in pattern_config")
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate incremental append SQL"""
        watermark_col = self.pattern_config['watermark_column']
        
        sql = self.generate_sql_header(context)
        
        sql += f"""
-- Incremental Append: Load new records based on watermark
-- Watermark Column: {watermark_col}

INSERT INTO {self.get_target_fqn()}
SELECT {self.get_column_list()}
FROM {self.get_source_fqn()}
WHERE `{watermark_col}` > (
    SELECT COALESCE(MAX(`{watermark_col}`), CAST('1900-01-01' AS TIMESTAMP))
    FROM {self.get_target_fqn()}
);
"""
        return sql
    
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """Generate preview queries"""
        watermark_col = self.pattern_config['watermark_column']
        
        queries = [
            # Current max watermark
            f"""
            SELECT 
                'Current Max Watermark' as metric,
                MAX(`{watermark_col}`) as value
            FROM {self.get_target_fqn()}
            """,
            # Count of new records
            f"""
            SELECT 
                'New Records to Load' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()}
            WHERE `{watermark_col}` > (
                SELECT COALESCE(MAX(`{watermark_col}`), CAST('1900-01-01' AS TIMESTAMP))
                FROM {self.get_target_fqn()}
            )
            """
        ]
        
        return queries
