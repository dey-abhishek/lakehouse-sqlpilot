"""
SCD Type 2 Pattern - Slowly Changing Dimension Type 2
"""

from typing import Dict, Any, List
from .base_pattern import BasePattern


class SCD2Pattern(BasePattern):
    """SCD Type 2 pattern with history tracking"""
    
    def validate_config(self) -> List[str]:
        """Validate SCD2 configuration"""
        errors = []
        
        # Required fields
        if not self.pattern_config.get('business_keys'):
            errors.append("SCD2 requires 'business_keys' in pattern_config")
        
        if not self.pattern_config.get('effective_date_column'):
            errors.append("SCD2 requires 'effective_date_column' in pattern_config")
        
        if not self.pattern_config.get('end_date_column'):
            errors.append("SCD2 requires 'end_date_column' in pattern_config")
        
        if not self.pattern_config.get('current_flag_column'):
            errors.append("SCD2 requires 'current_flag_column' in pattern_config")
        
        # Write mode must be merge
        if self.target.get('write_mode') != 'merge':
            errors.append("SCD2 requires write_mode='merge'")
        
        # Must have explicit columns
        if not self.source.get('columns'):
            errors.append("SCD2 requires explicit columns in source")
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """
        Generate SCD2 three-step SQL following Databricks best practices
        
        Based on: https://www.databricks.com/blog/implementing-dimensional-data-warehouse-databricks-sql-part-2
        
        Step 1: UPDATE late-arriving records (if applicable)
        Step 2: MERGE to expire changed records (set end_date, current_flag = FALSE)
        Step 3: INSERT new versions and new records
        """
        business_keys = self.pattern_config['business_keys']
        compare_columns = self.pattern_config.get('compare_columns', self.source.get('columns', []))
        effective_col = self.pattern_config['effective_date_column']
        end_col = self.pattern_config['end_date_column']
        current_flag = self.pattern_config['current_flag_column']
        end_date_default = self.pattern_config.get('end_date_default', '9999-12-31 23:59:59')
        
        # Remove business keys and SCD metadata from compare columns
        compare_columns = [c for c in compare_columns 
                          if c not in business_keys 
                          and c not in [effective_col, end_col, current_flag]]
        
        sql = self.generate_sql_header(context)
        
        # Build business key matching condition
        key_match = ' AND '.join([f"target.`{key}` = source.`{key}`" for key in business_keys])
        
        # Build change detection using equal_null() for NULL-safe comparison
        # See: https://docs.databricks.com/en/sql/language-manual/functions/equal_null.html
        change_conditions = []
        for col in compare_columns:
            # Use NOT equal_null() to detect changes (including NULL changes)
            change_conditions.append(f"NOT equal_null(target.`{col}`, source.`{col}`)")
        change_detection = ' OR '.join(change_conditions) if change_conditions else 'FALSE'
        
        # Get source columns for INSERT
        source_columns = self.source.get('columns', [])
        
        # STEP 1: Update late-arriving records (optional, typically for fact-driven dimensions)
        # This is commented out by default but can be enabled if late-arriving logic is needed
        sql += f"""
-- STEP 1: Update late-arriving records (if applicable)
-- This step is typically used when facts reference dimension keys before dimension records arrive
-- Uncomment and configure if your workflow requires late-arriving dimension handling
-- 
-- UPDATE {self.get_target_fqn()} AS target
-- SET target.column = source.column, ...
--     target.is_late_arriving = FALSE
-- FROM {self.get_source_fqn()} AS source
-- WHERE {key_match}
--   AND target.is_late_arriving = TRUE;

"""
        
        # STEP 2: Expire changed records using MERGE
        # This follows the Databricks pattern of using MERGE to update existing records
        sql += f"""-- STEP 2: Expire changed records
-- Mark existing current records as historical when changes are detected
-- Uses equal_null() for NULL-safe comparison as per Databricks best practices

MERGE INTO {self.get_target_fqn()} AS target
USING {self.get_source_fqn()} AS source
ON {key_match}
   AND target.`{current_flag}` = TRUE
WHEN MATCHED AND ({change_detection})
THEN UPDATE SET
    target.`{end_col}` = CURRENT_TIMESTAMP(),
    target.`{current_flag}` = FALSE;

"""
        
        # STEP 3: Insert new versions and new records
        sql += f"""-- STEP 3: Insert new versions and new records
-- Insert new records for:
--   a) Brand new business keys (WHERE target.key IS NULL)
--   b) Changed records that were expired in Step 2 (WHERE change detected)

INSERT INTO {self.get_target_fqn()}
SELECT 
    {', '.join([f'source.`{col}`' for col in source_columns])},
    CURRENT_TIMESTAMP() AS `{effective_col}`,
    CAST('{end_date_default}' AS TIMESTAMP) AS `{end_col}`,
    TRUE AS `{current_flag}`
FROM {self.get_source_fqn()} AS source
LEFT JOIN {self.get_target_fqn()} AS target
    ON {key_match}
    AND target.`{current_flag}` = TRUE
WHERE target.{business_keys[0]} IS NULL
   OR ({change_detection});
"""
        return sql
    
    def get_preview_queries(self, context: Dict[str, Any]) -> List[str]:
        """Generate SCD2 preview queries"""
        business_keys = self.pattern_config['business_keys']
        compare_columns = self.pattern_config.get('compare_columns', self.source.get('columns', []))
        current_flag = self.pattern_config['current_flag_column']
        
        # Remove business keys from compare columns
        compare_columns = [c for c in compare_columns if c not in business_keys]
        
        key_match = ' AND '.join([f"target.`{key}` = source.`{key}`" for key in business_keys])
        
        change_conditions = []
        for col in compare_columns:
            change_conditions.append(f"target.`{col}` != source.`{col}`")
        change_detection = ' OR '.join(change_conditions)
        
        queries = [
            # Records to be closed
            f"""
            SELECT 
                'Records to Close' as metric,
                COUNT(*) as value
            FROM {self.get_target_fqn()} AS target
            JOIN {self.get_source_fqn()} AS source
                ON {key_match}
            WHERE target.`{current_flag}` = TRUE
              AND ({change_detection})
            """,
            # New records to insert
            f"""
            SELECT 
                'New Records to Insert' as metric,
                COUNT(*) as value
            FROM {self.get_source_fqn()} AS source
            LEFT JOIN {self.get_target_fqn()} AS target
                ON {key_match}
                AND target.`{current_flag}` = TRUE
            WHERE target.{business_keys[0]} IS NULL
            """
        ]
        
        return queries
