"""
Full Replace Pattern - Atomically replace entire table with advanced options

Supports:
- Direct replace or staging table approach
- In-place refresh (source = target)
- Partitioning and Liquid Clustering
- Table properties (Delta/Iceberg)
- Data filtering during refresh
- Schema evolution and optimization
"""

from typing import Dict, Any, List
import json
from .base_pattern import BasePattern


class FullReplacePattern(BasePattern):
    """Full replace pattern with advanced table maintenance features"""
    
    def validate_config(self) -> List[str]:
        """Validate full replace configuration"""
        errors = []
        
        # Validate refresh_mode
        refresh_mode = self.pattern_config.get('refresh_mode', 'direct')
        if refresh_mode not in ['direct', 'staging']:
            errors.append(f"Invalid refresh_mode '{refresh_mode}'. Must be 'direct' or 'staging'")
        
        # Validate table_format
        table_format = self.pattern_config.get('table_format', 'delta')
        if table_format not in ['delta', 'iceberg']:
            errors.append(f"Invalid table_format '{table_format}'. Must be 'delta' or 'iceberg'")
        
        # Validate in-place refresh cannot change table format
        refresh_inplace = self.pattern_config.get('refresh_inplace', False)
        if refresh_inplace:
            # Note: We can't validate the actual table format here without querying the metastore
            # This validation is a reminder - the user should ensure they're not changing formats
            # The UI should prevent this, but we add a warning in the SQL
            pass
        
        # Validate table_properties if provided
        if 'table_properties' in self.pattern_config:
            props = self.pattern_config['table_properties']
            if isinstance(props, str):
                try:
                    json.loads(props)
                except json.JSONDecodeError as e:
                    errors.append(f"Invalid table_properties JSON: {e}")
            elif not isinstance(props, dict):
                errors.append("table_properties must be a JSON string or dict")
        
        # Validate clustering and partitioning aren't both specified
        if self.pattern_config.get('cluster_columns') and self.pattern_config.get('partition_columns'):
            errors.append("Cannot specify both cluster_columns (Liquid Clustering) and partition_columns. Choose one.")
        
        return errors
    
    def generate_sql(self, context: Dict[str, Any]) -> str:
        """Generate full replace SQL with advanced options"""
        sql = self.generate_sql_header(context)
        
        refresh_mode = self.pattern_config.get('refresh_mode', 'direct')
        refresh_inplace = self.pattern_config.get('refresh_inplace', False)
        table_format = self.pattern_config.get('table_format', 'delta').upper()
        
        # Determine source and target
        source_fqn = self.get_source_fqn()
        target_fqn = self.get_target_fqn()
        
        # Check if this is an in-place upgrade (same table name, property/version changes only)
        if self._is_inplace_upgrade():
            sql += self._generate_alter_table_upgrade(source_fqn, target_fqn, table_format)
            return sql
        
        # Check if this is truly in-place (source = target)
        if refresh_inplace and source_fqn == target_fqn:
            sql += f"""
-- ⚠️  IN-PLACE REFRESH: Source and target are the same table
-- This will read from the table and rewrite it with new properties/structure
-- ⚠️  WARNING: Ensure the existing table format matches '{table_format}'
-- Changing table format (Delta ↔ Iceberg) requires a different target table name
"""
        elif refresh_inplace and source_fqn != target_fqn:
            # refresh_inplace is checked but source != target - likely a format conversion
            sql += f"""
-- FORMAT CONVERSION: Reading from {source_fqn} and writing to {target_fqn}
-- Table Format: Converting to {table_format}
"""
        else:
            # Normal mode (refresh_inplace not checked)
            sql += f"""
-- FULL REPLACE: Source → Target table replacement
"""
        
        # Build clauses
        cluster_clause = self._build_cluster_clause()
        partition_clause = self._build_partition_clause()
        table_props = self._build_table_properties()
        where_clause = self._build_where_clause()
        
        # Add warning for Iceberg v2 + Liquid Clustering
        cluster_columns = self.pattern_config.get('cluster_columns', [])
        version = self.pattern_config.get('iceberg_version', '2')
        if table_format == 'ICEBERG' and version == '2' and cluster_columns:
            sql += f"""
-- ⚠️  WARNING: ICEBERG v2 + LIQUID CLUSTERING LIMITATION
-- Row-level concurrency is NOT supported on Iceberg v2 with Liquid Clustering.
-- Deletion vectors and row tracking are disabled (required for LC).
-- This may cause write conflicts for concurrent operations.
-- Consider using Iceberg v3 if you need row-level concurrency.
-- Reference: https://docs.databricks.com/aws/en/delta/clustering#override-default-feature-enablement-optional
"""
        
        if refresh_mode == 'staging':
            sql += self._generate_staging_sql(
                source_fqn, target_fqn, table_format, 
                cluster_clause, partition_clause, table_props, where_clause
            )
        else:
            sql += self._generate_direct_sql(
                source_fqn, target_fqn, table_format,
                cluster_clause, partition_clause, table_props, where_clause
            )
        
        return sql
    
    def _generate_direct_sql(self, source_fqn: str, target_fqn: str, 
                            table_format: str, cluster_clause: str, 
                            partition_clause: str, table_props: str, 
                            where_clause: str) -> str:
        """Generate direct replacement SQL"""
        refresh_inplace = self.pattern_config.get('refresh_inplace', False)
        
        # Debug logging
        import structlog
        logger = structlog.get_logger()
        logger.info("full_replace_direct_sql_generation",
                   refresh_inplace=refresh_inplace,
                   source_fqn=source_fqn,
                   target_fqn=target_fqn,
                   table_format=table_format,
                   source_equals_target=source_fqn == target_fqn)
        
        # Check if this requires DROP + CREATE approach:
        # 1. Different source and target tables (format conversion or new table)
        # 2. Same table but format conversion (e.g., Delta → Iceberg on same table name)
        #    - This is indicated by refresh_inplace=True but we can't use CREATE OR REPLACE
        #      for format changes, so we use DROP + CREATE
        # 
        # Use DROP + CREATE instead of CREATE OR REPLACE to avoid errors like:
        # - "Managed Iceberg tables do not support REPLACE with different providers"
        # - Format conversion limitations
        requires_drop_create = source_fqn != target_fqn
        
        logger.info("full_replace_drop_create_check",
                   requires_drop_create=requires_drop_create,
                   refresh_inplace=refresh_inplace,
                   different_tables=source_fqn != target_fqn)
        
        if requires_drop_create:
            return f"""
-- Full Replace: Format Conversion (requires DROP + CREATE)
-- ⚠️  WARNING: This will DROP the target table and recreate it with new format
-- Source: {source_fqn} → Target: {target_fqn}
-- Table Format: {table_format}
{self._get_feature_summary()}

-- STEP 1: Drop existing target table (if exists)
DROP TABLE IF EXISTS {target_fqn};

-- STEP 2: Create new table with desired format
CREATE TABLE {target_fqn}
USING {table_format}
{cluster_clause}
{partition_clause}
{table_props}
AS
SELECT {self.get_column_list()}
FROM {source_fqn}
{where_clause};
"""
        else:
            # True in-place refresh (source = target, no format change)
            # Use CREATE OR REPLACE for atomic operation
            # Note: If you need to change the format, use different table names
            return f"""
-- Full Replace: Direct atomic replacement
-- Table Format: {table_format}
{self._get_feature_summary()}

-- ℹ️  Note: For format conversion (Delta ↔ Iceberg), use different source/target table names
-- ℹ️  For version upgrades (Iceberg v2 → v3), use ALTER TABLE SET TBLPROPERTIES after creation

CREATE OR REPLACE TABLE {target_fqn}
USING {table_format}
{cluster_clause}
{partition_clause}
{table_props}
AS
SELECT {self.get_column_list()}
FROM {source_fqn}
{where_clause};
"""
    
    def _generate_staging_sql(self, source_fqn: str, target_fqn: str,
                              table_format: str, cluster_clause: str,
                              partition_clause: str, table_props: str,
                              where_clause: str) -> str:
        """Generate staging table approach SQL"""
        # Strip backticks from target_fqn to build staging/old table names
        target_base = target_fqn.replace('`', '')
        
        # Split the FQN into parts
        parts = target_base.split('.')
        if len(parts) == 3:
            catalog, schema, table = parts
            staging_table = f"`{catalog}`.`{schema}`.`{table}_staging`"
            old_table = f"`{catalog}`.`{schema}`.`{table}_old`"
        else:
            # Fallback for non-standard FQN
            staging_table = f"`{target_base}_staging`"
            old_table = f"`{target_base}_old`"
        
        # For validation, compare staging with the ORIGINAL production table
        # The staging approach always compares with target_fqn (the current production table)
        comparison_table = target_fqn
        
        # Check if this is a format conversion scenario (different table names/formats)
        # In staging mode, source != target typically means format conversion or new target
        is_format_conversion = source_fqn != target_fqn
        
        swap_instructions = ""
        if is_format_conversion:
            # Format conversion or new table: Target might not exist yet or has different format
            # Must use DROP + RENAME
            swap_instructions = f"""
-- ⚠️  FORMAT CONVERSION OR NEW TABLE
-- If target table exists with a different format, it must be dropped first
-- DROP TABLE IF EXISTS {target_fqn};

-- Promote staging to production
-- ALTER TABLE {staging_table} RENAME TO {target_fqn};
"""
        else:
            # Normal swap: Backup old, promote staging
            # This is for replacing an existing table with same format
            swap_instructions = f"""
-- Rename production table to backup
-- ALTER TABLE {target_fqn} RENAME TO {old_table};

-- Promote staging to production
-- ALTER TABLE {staging_table} RENAME TO {target_fqn};

-- Optional: Drop old table after confirming new table works
-- DROP TABLE IF EXISTS {old_table};
"""
        
        format_warning = ""
        if is_format_conversion:
            format_warning = f"""
-- ⚠️  FORMAT CONVERSION DETECTED
-- Source: {source_fqn} → Target: {target_fqn} (format: {table_format})
-- This operation will change the table format and requires dropping the target table
"""
        
        return f"""
-- Full Replace: Staging Table Approach (Zero Downtime)
-- Table Format: {table_format}
{self._get_feature_summary()}
{format_warning}

-- ═══════════════════════════════════════════════════════════════
-- STEP 1: Create staging table with new configuration
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE {staging_table}
USING {table_format}
{cluster_clause}
{partition_clause}
{table_props}
AS
SELECT {self.get_column_list()}
FROM {source_fqn}
{where_clause};

-- ═══════════════════════════════════════════════════════════════
-- STEP 2: Validate staging table (MANUAL - run these queries)
-- ═══════════════════════════════════════════════════════════════

-- Check row counts
-- SELECT COUNT(*) as row_count FROM {staging_table};
-- SELECT COUNT(*) as row_count FROM {comparison_table};

-- Check data quality (replace column_name with actual columns)
-- SELECT COUNT(*) as total_rows, COUNT(DISTINCT primary_key_column) as unique_keys FROM {staging_table};
-- SELECT MIN(your_timestamp_column), MAX(your_timestamp_column) FROM {staging_table};

-- ═══════════════════════════════════════════════════════════════
-- STEP 3: Atomic swap (MANUAL - run after validation)
-- ═══════════════════════════════════════════════════════════════
{swap_instructions}
"""
    
    def _build_cluster_clause(self) -> str:
        """Build CLUSTER BY clause for Liquid Clustering"""
        cluster_cols = self.pattern_config.get('cluster_columns', [])
        if not cluster_cols:
            return ""
        
        if isinstance(cluster_cols, str):
            cluster_cols = [c.strip() for c in cluster_cols.split(',')]
        
        cols = ", ".join([f"`{col}`" for col in cluster_cols])
        return f"CLUSTER BY ({cols})"
    
    def _build_partition_clause(self) -> str:
        """Build PARTITIONED BY clause"""
        partition_cols = self.pattern_config.get('partition_columns', [])
        if not partition_cols:
            return ""
        
        if isinstance(partition_cols, str):
            partition_cols = [c.strip() for c in partition_cols.split(',')]
        
        cols = ", ".join([f"`{col}`" for col in partition_cols])
        return f"PARTITIONED BY ({cols})"
    
    def _build_table_properties(self) -> str:
        """Build TBLPROPERTIES clause"""
        props = self.pattern_config.get('table_properties', {})
        
        # Handle JSON string
        if isinstance(props, str):
            if not props.strip():
                props = {}
            else:
                try:
                    props = json.loads(props)
                except json.JSONDecodeError:
                    props = {}
        
        if not isinstance(props, dict):
            props = {}
        
        # Make a copy to avoid modifying the original
        props = dict(props)
        
        # Auto-add properties based on table format and features
        table_format = self.pattern_config.get('table_format', 'delta').lower()
        cluster_columns = self.pattern_config.get('cluster_columns', [])
        refresh_inplace = self.pattern_config.get('refresh_inplace', False)
        version = self.pattern_config.get('iceberg_version', '2')  # '2' = standard, '3' = enhanced (UniForm for Delta, v3 for Iceberg)
        
        # === ICEBERG TABLE PROPERTIES ===
        if table_format == 'iceberg':
            # Liquid Clustering with Iceberg: Must disable deletion vectors AND row tracking
            if cluster_columns:
                # Use the selected version (v2 or v3)
                props['format-version'] = version
                
                # CRITICAL: LC requires BOTH deletion vectors AND row tracking to be disabled
                # These must override any user settings
                props['iceberg.enableDeletionVectors'] = 'false'
                props['iceberg.enableRowTracking'] = 'false'
                
                # LIMITATION: Apache Iceberg v2 with LC does NOT support row-level concurrency
                # "Row-level concurrency is not supported on managed Iceberg tables with Apache 
                # Iceberg v2, as deletion vectors and row tracking are not supported"
            else:
                # Without LC, use the selected version with default features
                if 'format-version' not in props:
                    props['format-version'] = version
                # For v3 without LC, deletion vectors are enabled by default
                # For v2, deletion vectors don't exist
        
        # === DELTA TABLE PROPERTIES ===
        elif table_format == 'delta':
            # Check if UniForm is explicitly enabled
            enable_uniform = self.pattern_config.get('enable_uniform', False)
            
            if enable_uniform:
                # Version '3' = Enable UniForm with Iceberg v3 (deletion vectors supported)
                # Version '2' = Enable UniForm with Iceberg v2 (no deletion vectors)
                if version == '3':
                    # Delta UniForm with Iceberg v3 compatibility
                    # CRITICAL: These properties are REQUIRED for IcebergCompatV3 and must override user settings
                    props['delta.universalFormat.enabledFormats'] = 'iceberg'
                    props['delta.enableIcebergCompatV3'] = 'true'
                    props['delta.enableIcebergCompatV2'] = 'false'  # Explicitly disable V2
                    props['delta.columnMapping.mode'] = 'name'
                    props['delta.enableRowTracking'] = 'true'  # REQUIRED for IcebergCompatV3 - always true
                    
                    # Handle Liquid Clustering with UniForm v3
                    if cluster_columns:
                        # Liquid Clustering requires deletion vectors to be disabled
                        # Row tracking stays enabled (required for v3)
                        props['delta.enableDeletionVectors'] = 'false'
                    else:
                        # Without LC, enable deletion vectors (default for v3)
                        if 'delta.enableDeletionVectors' not in props:
                            props['delta.enableDeletionVectors'] = 'true'
                
                elif version == '2':
                    # Delta UniForm with Iceberg v2 compatibility (no deletion vectors)
                    # CRITICAL: These properties are REQUIRED for IcebergCompatV2 and must override user settings
                    props['delta.universalFormat.enabledFormats'] = 'iceberg'
                    props['delta.enableIcebergCompatV2'] = 'true'
                    props['delta.columnMapping.mode'] = 'name'
                    # Note: IcebergCompatV2 does NOT support deletion vectors or row tracking
            # else: Standard Delta (no UniForm) - no special properties needed
        
        if not props:
            return ""
        
        prop_lines = [f"  '{k}' = '{v}'" for k, v in props.items()]
        return "TBLPROPERTIES (\n" + ",\n".join(prop_lines) + "\n)"
    
    def _build_where_clause(self) -> str:
        """Build WHERE clause for data filtering"""
        filter_condition = self.pattern_config.get('filter_condition', '')
        if not filter_condition or not filter_condition.strip():
            return ""
        return f"WHERE {filter_condition}"
    
    def _get_feature_summary(self) -> str:
        """Generate a summary comment of applied features"""
        features = []
        
        table_format = self.pattern_config.get('table_format', 'delta').lower()
        cluster_columns = self.pattern_config.get('cluster_columns', [])
        version = self.pattern_config.get('iceberg_version', '2')
        enable_uniform = self.pattern_config.get('enable_uniform', False)
        props = self.pattern_config.get('table_properties', {})
        
        # Parse props if it's a string
        if isinstance(props, str) and props.strip():
            try:
                props = json.loads(props)
            except json.JSONDecodeError:
                props = {}
        
        # Table format and version
        if table_format == 'iceberg':
            if version == '3':
                if cluster_columns:
                    features.append("✓ Iceberg v3 + LC (deletion vectors disabled, row-level concurrency supported)")
                else:
                    features.append("✓ Iceberg v3 (deletion vectors + row tracking)")
            else:
                if cluster_columns:
                    features.append("✓ Iceberg v2 + LC (⚠️ NO row-level concurrency, NO deletion vectors)")
                else:
                    features.append("✓ Iceberg v2")
        elif table_format == 'delta':
            if enable_uniform:
                if version == '3':
                    features.append("✓ Delta + UniForm v3 (dual Delta/Iceberg, deletion vectors enabled)")
                    if cluster_columns:
                        features.append("✓ LC enabled (deletion vectors disabled)")
                elif version == '2':
                    features.append("✓ Delta + UniForm v2 (dual Delta/Iceberg, no deletion vectors)")
            else:
                features.append("✓ Delta (standard)")
        
        if cluster_columns and not enable_uniform and table_format == 'delta':
            features.append(f"✓ Liquid Clustering on: {', '.join(cluster_columns)}")
        
        if self.pattern_config.get('partition_columns'):
            features.append("✓ Partitioning enabled")
        if self.pattern_config.get('table_properties'):
            features.append("✓ Custom table properties")
        if self.pattern_config.get('filter_condition'):
            features.append("✓ Data filtering applied")
        if self.pattern_config.get('refresh_inplace'):
            features.append("✓ In-place refresh")
        
        if not features:
            return ""
        
        return "-- Features: " + ", ".join(features)
    
    def _is_inplace_upgrade(self) -> bool:
        """
        Check if this is an in-place upgrade (same table, property/version changes only).
        
        Returns True ONLY for scenarios like:
        - Iceberg v2 → v3 upgrade (same table, same format)
        - Delta → Delta + UniForm (same table, same format)
        
        Returns False for format conversions:
        - Delta → Iceberg (different format)
        - Iceberg → Delta (different format)
        """
        source_fqn = self.get_source_fqn()
        target_fqn = self.get_target_fqn()
        refresh_inplace = self.pattern_config.get('refresh_inplace', False)
        table_format = self.pattern_config.get('table_format', 'delta').lower()
        
        # Must be same table name and refresh_inplace checked
        if not (refresh_inplace and source_fqn == target_fqn):
            return False
        
        # Now check if format is changing - we need to detect the SOURCE table format
        # For now, if source = target, we assume it's the same format (true in-place)
        # Format conversion should have different source/target names
        
        # True in-place upgrade: same table, same format, just property changes
        return True
    
    def _generate_alter_table_upgrade(self, source_fqn: str, target_fqn: str, table_format: str) -> str:
        """
        Generate ALTER TABLE statements for in-place upgrades.
        
        Handles:
        - Iceberg v2 → v3
        - Delta → Delta + UniForm  
        - Delta + UniForm property changes
        """
        sql = f"""
-- IN-PLACE UPGRADE: Modifying table properties without recreating the table
-- Target: {target_fqn}
{self._get_feature_summary()}

"""
        
        version = self.pattern_config.get('iceberg_version', '2')
        cluster_columns = self.pattern_config.get('cluster_columns', [])
        table_format_lower = self.pattern_config.get('table_format', 'delta').lower()
        
        # Build the properties to set
        # Start with custom properties, then override with system-required properties
        custom_props = self.pattern_config.get('table_properties', {})
        if isinstance(custom_props, str) and custom_props.strip():
            try:
                custom_props = json.loads(custom_props)
            except json.JSONDecodeError:
                custom_props = {}
        
        props = {}
        if isinstance(custom_props, dict):
            props.update(custom_props)  # Start with custom props
        
        # Now set system-required properties that MUST override user settings
        if table_format_lower == 'iceberg':
            # Iceberg v2 → v3 upgrade
            if version == '3':
                props['format-version'] = '3'  # Override
                if cluster_columns:
                    # Liquid Clustering: disable deletion vectors and row tracking
                    props['iceberg.enableDeletionVectors'] = 'false'  # Override
                    props['iceberg.enableRowTracking'] = 'false'  # Override
            else:
                # Iceberg v2 (no special properties needed for downgrade, not recommended)
                props['format-version'] = '2'  # Override
        
        elif table_format_lower == 'delta':
            # Check if UniForm is enabled
            enable_uniform = self.pattern_config.get('enable_uniform', False)
            
            if enable_uniform:
                # Delta → Delta + UniForm upgrade
                if version == '3':
                    # CRITICAL: These properties are REQUIRED and MUST override user settings
                    props['delta.universalFormat.enabledFormats'] = 'iceberg'
                    props['delta.enableIcebergCompatV3'] = 'true'
                    props['delta.enableIcebergCompatV2'] = 'false'
                    props['delta.columnMapping.mode'] = 'name'
                    props['delta.enableRowTracking'] = 'true'  # MUST be true for v3
                    
                    if cluster_columns:
                        # LC requires deletion vectors to be disabled
                        props['delta.enableDeletionVectors'] = 'false'
                    else:
                        # Enable deletion vectors (default for v3)
                        props['delta.enableDeletionVectors'] = 'true'
                
                elif version == '2':
                    # CRITICAL: These properties are REQUIRED and MUST override user settings
                    props['delta.universalFormat.enabledFormats'] = 'iceberg'
                    props['delta.enableIcebergCompatV2'] = 'true'
                    props['delta.columnMapping.mode'] = 'name'
                    # Note: IcebergCompatV2 does NOT support deletion vectors
        
        # Generate ALTER TABLE statements
        if props:
            prop_lines = [f"  '{k}' = '{v}'" for k, v in props.items()]
            sql += f"""ALTER TABLE {target_fqn}
SET TBLPROPERTIES (
{',\n'.join(prop_lines)}
);

"""
        
        # Handle Liquid Clustering (if specified)
        if cluster_columns:
            cluster_cols = ', '.join(cluster_columns)
            sql += f"""-- Enable Liquid Clustering
ALTER TABLE {target_fqn}
CLUSTER BY ({cluster_cols});

"""
        
        sql += f"""-- ✅ In-place upgrade complete
-- The table structure and data remain unchanged, only properties were modified
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
