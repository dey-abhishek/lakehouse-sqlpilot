"""
Merge/Upsert Pattern - Update existing and insert new records
"""

from typing import Dict, Any, List
from .base_pattern import BasePattern


class MergeUpsertPattern(BasePattern):
    """Merge/Upsert pattern for updating and inserting"""
    
    def validate_config(self) -> List[str]:
        """Validate merge configuration"""
        errors = []
        
        if not self.pattern_config.get('merge_keys'):
            errors.append("Merge/upsert requires 'merge_keys' in pattern_config")
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate merge/upsert SQL"""
        merge_keys = self.pattern_config['merge_keys']
        update_columns = self.pattern_config.get('update_columns', self.source.get('columns', []))
        
        # Remove merge keys from update columns
        update_columns = [c for c in update_columns if c not in merge_keys]
        
        sql = self.generate_sql_header(context)
        
        # Build merge key matching
        key_match = ' AND '.join([f"target.`{key}` = source.`{key}`" for key in merge_keys])
        
        # Build update SET clause
        update_set = ', '.join([f"target.`{col}` = source.`{col}`" for col in update_columns])
        
        # Get all columns for INSERT
        all_columns = self.source.get('columns', [])
        
        sql += f"""
-- Merge/Upsert: Update existing records and insert new ones
-- Merge Keys: {', '.join(merge_keys)}

MERGE INTO {self.get_target_fqn()} AS target
USING {self.get_source_fqn()} AS source
ON {key_match}
WHEN MATCHED THEN
    UPDATE SET {update_set}
WHEN NOT MATCHED THEN
    INSERT ({', '.join([f'`{col}`' for col in all_columns])})
    VALUES ({', '.join([f'source.`{col}`' for col in all_columns])});
"""
        return sql
    
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """Generate preview queries"""
        merge_keys = self.pattern_config['merge_keys']
        key_match = ' AND '.join([f"target.`{key}` = source.`{key}`" for key in merge_keys])
        
        queries = [
            # Records to update
            f"""
            SELECT 
                'Records to Update' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()} AS source
            JOIN {self.get_target_fqn()} AS target
                ON {key_match}
            """,
            # Records to insert
            f"""
            SELECT 
                'Records to Insert' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()} AS source
            LEFT JOIN {self.get_target_fqn()} AS target
                ON {key_match}
            WHERE target.{merge_keys[0]} IS NULL
            """
        ]
        
        return queries
