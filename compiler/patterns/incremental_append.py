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
        
        # Validate write_mode
        write_mode = self.target.get('write_mode', 'append')
        if write_mode not in ['append', 'merge', 'overwrite']:
            errors.append(f"Incremental append supports write_mode 'append', 'merge', or 'overwrite', got '{write_mode}'")
        
        # If merge mode, require match_columns
        if write_mode == 'merge':
            if not self.pattern_config.get('match_columns'):
                errors.append("Incremental append with write_mode='merge' requires 'match_columns' in pattern_config")
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate incremental append SQL"""
        watermark_col = self.pattern_config['watermark_column']
        write_mode = self.target.get('write_mode', 'append')
        
        sql = self.generate_sql_header(context)
        
        if write_mode == 'merge':
            # MERGE mode: Update existing records and insert new ones
            match_columns = self.pattern_config.get('match_columns', [])
            
            # Build match condition
            match_conditions = [f"target.`{col}` = source.`{col}`" for col in match_columns]
            match_condition = " AND ".join(match_conditions)
            
            # Build update set clause
            all_columns = self.get_column_list().split(', ')
            update_set = ", ".join([f"target.`{col.strip('`')}` = source.`{col.strip('`')}`" for col in all_columns if col.strip('`') not in match_columns])
            
            # Build insert columns
            insert_columns = ", ".join([f"`{col.strip('`')}`" for col in all_columns])
            insert_values = ", ".join([f"source.`{col.strip('`')}`" for col in all_columns])
            
            sql += f"""
-- Incremental Append (MERGE mode): Upsert new records based on watermark
-- Watermark Column: {watermark_col}
-- Match Columns: {', '.join(match_columns)}

MERGE INTO {self.get_target_fqn()} AS target
USING (
    SELECT {self.get_column_list()}
    FROM {self.get_source_fqn()}
    WHERE `{watermark_col}` > (
        SELECT COALESCE(MAX(`{watermark_col}`), CAST('1900-01-01' AS TIMESTAMP))
        FROM {self.get_target_fqn()}
    )
) AS source
ON {match_condition}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_columns})
    VALUES ({insert_values});
"""
        elif write_mode == 'overwrite':
            # OVERWRITE mode: Replace table with incremental slice
            sql += f"""
-- Incremental Append (OVERWRITE mode): Replace table with new records based on watermark
-- Watermark Column: {watermark_col}
-- Note: This replaces the entire table with only new records

CREATE OR REPLACE TABLE {self.get_target_fqn()}
AS
SELECT {self.get_column_list()}
FROM {self.get_source_fqn()}
WHERE `{watermark_col}` > (
    -- Get max watermark from current table before replacement
    SELECT COALESCE(MAX(`{watermark_col}`), CAST('1900-01-01' AS TIMESTAMP))
    FROM {self.get_target_fqn()}
);
"""
        else:
            # APPEND mode: Simple INSERT INTO
            sql += f"""
-- Incremental Append (APPEND mode): Load new records based on watermark
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
