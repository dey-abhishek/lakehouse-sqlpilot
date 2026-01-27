"""
Preview Mode - Safe preview of plan execution without side effects
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
import uuid

from compiler import SQLCompiler
from unity_catalog import PermissionValidator
from execution import SQLExecutor


class PreviewError(Exception):
    """Exception raised during preview"""
    pass


class PreviewEngine:
    """Generates safe previews of plan execution"""
    
    def __init__(self,
                 sql_compiler: SQLCompiler,
                 permission_validator: PermissionValidator,
                 sql_executor: SQLExecutor):
        """
        Initialize preview engine
        
        Args:
            sql_compiler: SQL compiler
            permission_validator: Permission validator
            sql_executor: SQL executor
        """
        self.compiler = sql_compiler
        self.permission_validator = permission_validator
        self.executor = sql_executor
    
    def preview_plan(self,
                    plan: Dict[str, Any],
                    user: str,
                    warehouse_id: str,
                    include_sample_data: bool = True,
                    sample_limit: int = 10) -> Dict[str, Any]:
        """
        Generate comprehensive preview of plan execution
        
        Args:
            plan: Plan dictionary
            user: User requesting preview
            warehouse_id: Warehouse ID for sample data execution
            include_sample_data: Whether to execute and return sample data
            sample_limit: Number of sample rows to return
            
        Returns:
            Preview result dictionary
        """
        preview_result = {
            'plan_id': plan.get('plan_metadata', {}).get('plan_id'),
            'plan_name': plan.get('plan_metadata', {}).get('plan_name'),
            'plan_version': plan.get('plan_metadata', {}).get('version'),
            'pattern_type': plan.get('pattern', {}).get('type'),
            'preview_timestamp': datetime.now(timezone.utc).isoformat(),
            'preview_id': str(uuid.uuid4()),
            'validation': {},
            'compilation': {},
            'permissions': {},
            'sample_data': {},
            'impact_analysis': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            # Step 1: Validate plan
            preview_result['validation'] = self._validate_plan(plan)
            
            # Step 2: Compile to SQL
            preview_result['compilation'] = self._compile_plan(plan)
            
            # If compilation failed, stop here
            if not preview_result['compilation']['success']:
                return preview_result
            
            sql = preview_result['compilation']['sql']
            
            # Step 3: Validate permissions
            preview_result['permissions'] = self._validate_permissions(plan, user)
            
            # Step 4: Analyze impact
            preview_result['impact_analysis'] = self._analyze_impact(plan, sql)
            
            # Step 5: Get sample data (if requested and permissions OK)
            if include_sample_data and preview_result['permissions']['has_permissions']:
                preview_result['sample_data'] = self._get_sample_data(
                    sql, 
                    warehouse_id, 
                    sample_limit
                )
            
            # Step 6: Generate warnings
            preview_result['warnings'] = self._generate_warnings(plan, sql)
            
            # Step 7: Validate table format for Full Replace in-place mode
            if plan.get('pattern', {}).get('type') == 'FULL_REPLACE':
                format_validation = self._validate_table_format_for_inplace(plan, warehouse_id)
                if format_validation.get('error'):
                    preview_result['errors'].append(format_validation['error'])
                if format_validation.get('warning'):
                    preview_result['warnings'].append(format_validation['warning'])
            
        except Exception as e:
            preview_result['errors'].append(f"Preview error: {str(e)}")
        
        return preview_result
    
    def _validate_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Validate plan against schema and semantic rules"""
        is_valid, errors = self.compiler.validate_plan(plan)
        
        return {
            'is_valid': is_valid,
            'errors': errors
        }
    
    def _compile_plan(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Compile plan to SQL"""
        try:
            # Create preview context
            context = {
                'execution_id': 'preview',
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'execution_date': datetime.now(timezone.utc).date().isoformat(),
            'execution_timestamp': datetime.now(timezone.utc).isoformat(),
                'variables': {}
            }
            
            sql = self.compiler.compile(plan, context)
            
            return {
                'success': True,
                'sql': sql,
                'formatted_sql': self._format_sql(sql)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'sql': None
            }
    
    def _validate_permissions(self, plan: Dict[str, Any], user: str) -> Dict[str, Any]:
        """Validate Unity Catalog permissions"""
        try:
            is_valid, violations = self.permission_validator.validate_plan_permissions(plan, user)
            
            return {
                'has_permissions': is_valid,
                'violations': violations,
                'required_permissions': self._get_required_permissions(plan)
            }
            
        except Exception as e:
            return {
                'has_permissions': False,
                'violations': [str(e)],
                'required_permissions': []
            }
    
    def _analyze_impact(self, plan: Dict[str, Any], sql: str) -> Dict[str, Any]:
        """Analyze potential impact of execution"""
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        write_mode = target.get('write_mode')
        
        # Determine operation type based on pattern
        if pattern_type == 'SCD2':
            operation = 'MERGE'
            operation_type = 'SCD2_HISTORY_TRACKING'
            destructive = False
        elif write_mode == 'overwrite':
            operation = 'REPLACE'
            operation_type = 'FULL_REPLACE'
            destructive = True
        elif write_mode == 'merge':
            operation = 'MERGE'
            operation_type = 'MERGE_UPSERT'
            destructive = False
        else:
            operation = 'APPEND'
            operation_type = 'INCREMENTAL_APPEND'
            destructive = False
        
        return {
            'source_table': f"{source['catalog']}.{source['schema']}.{source['table']}",
            'target_table': f"{target['catalog']}.{target['schema']}.{target['table']}",
            'operation': operation,
            'operation_type': operation_type,
            'is_destructive': destructive,
            'pattern': pattern_type,
            'estimated_risk': 'low' if not destructive else 'medium',
            'affects_production': target.get('catalog') == 'prod_catalog'
        }
    
    def _get_sample_data(self, sql: str, warehouse_id: str, limit: int) -> Dict[str, Any]:
        """Execute SQL and get sample data"""
        try:
            # Convert INSERT/MERGE/CREATE to SELECT for preview
            preview_sql = self._convert_to_select(sql)
            
            result = self.executor.preview_sql(
                sql=preview_sql,
                warehouse_id=warehouse_id,
                limit=limit
            )
            
            return {
                'success': True,
                'columns': result['columns'],
                'rows': result['rows'],
                'row_count': result['row_count'],
                'note': f'Showing first {limit} rows that would be affected'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'columns': [],
                'rows': []
            }
    
    def _convert_to_select(self, sql: str) -> str:
        """
        Convert INSERT/MERGE/CREATE SQL to SELECT for preview
        
        Args:
            sql: Original SQL
            
        Returns:
            SELECT equivalent
        """
        # Remove SQLPilot header
        lines = sql.split('\n')
        sql_lines = [l for l in lines if not l.strip().startswith('--')]
        sql_clean = '\n'.join(sql_lines).strip()
        
        # Handle INSERT INTO ... SELECT
        if 'INSERT INTO' in sql_clean.upper():
            # Extract SELECT portion
            select_start = sql_clean.upper().find('SELECT')
            if select_start >= 0:
                return sql_clean[select_start:]
        
        # Handle CREATE OR REPLACE TABLE ... AS SELECT
        if 'CREATE OR REPLACE TABLE' in sql_clean.upper():
            as_pos = sql_clean.upper().find(' AS ')
            if as_pos >= 0:
                return sql_clean[as_pos + 4:].strip()
        
        # Handle MERGE (more complex, extract source query)
        if 'MERGE INTO' in sql_clean.upper():
            using_pos = sql_clean.upper().find('USING')
            on_pos = sql_clean.upper().find('ON ')
            if using_pos >= 0 and on_pos >= 0:
                # Extract USING clause
                using_clause = sql_clean[using_pos + 5:on_pos].strip()
                # If it's a subquery, extract it
                if using_clause.startswith('('):
                    return using_clause.strip('()')
                else:
                    return f"SELECT * FROM {using_clause}"
        
        # Default: return as-is
        return sql_clean
    
    def _get_required_permissions(self, plan: Dict[str, Any]) -> List[Dict[str, str]]:
        """Get list of required permissions"""
        source = plan.get('source', {})
        target = plan.get('target', {})
        write_mode = target.get('write_mode')
        
        permissions = []
        
        # Source permissions
        permissions.append({
            'table': f"{source['catalog']}.{source['schema']}.{source['table']}",
            'permission': 'SELECT'
        })
        
        # Target permissions
        target_perms = ['SELECT', 'MODIFY']
        for perm in target_perms:
            permissions.append({
                'table': f"{target['catalog']}.{target['schema']}.{target['table']}",
                'permission': perm
            })
        
        return permissions
    
    def _generate_warnings(self, plan: Dict[str, Any], sql: str) -> List[str]:
        """Generate warnings for potential issues"""
        warnings = []
        
        target = plan.get('target', {})
        write_mode = target.get('write_mode')
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Warning for overwrite mode
        if write_mode == 'overwrite':
            warnings.append("This operation will REPLACE all existing data in the target table")
        
        # Warning for production catalog
        if target.get('catalog') in ['prod_catalog', 'production', 'prod']:
            warnings.append("This operation affects a PRODUCTION catalog")
        
        # Warning for no partitioning on large tables
        if not target.get('partition_by') and pattern_type in ['INCREMENTAL_APPEND', 'SNAPSHOT']:
            warnings.append("Consider partitioning the target table for better performance")
        
        # Warning for SCD2 complexity
        if pattern_type == 'SCD2':
            warnings.append("SCD2 pattern involves multiple steps and may take longer to execute")
        
        return warnings
    
    def _validate_table_format_for_inplace(self, plan: Dict[str, Any], warehouse_id: str) -> Dict[str, Any]:
        """
        Validate table format when using Full Replace with in-place refresh
        
        For format conversions (Delta → Iceberg), allows different source/target names.
        
        Returns:
            Dict with 'error' or 'warning' keys if there are issues
        """
        result = {}
        
        pattern_config = plan.get('pattern_config', {})
        refresh_inplace = pattern_config.get('refresh_inplace', False)
        
        # Only validate if in-place mode is enabled
        if not refresh_inplace:
            return result
        
        source = plan.get('source', {})
        target = plan.get('target', {})
        target_format = pattern_config.get('table_format', 'delta').lower()
        
        source_catalog = source.get('catalog')
        source_schema = source.get('schema')
        source_table = source.get('table')
        target_catalog = target.get('catalog')
        target_schema = target.get('schema')
        target_table = target.get('table')
        
        if not all([source_catalog, source_schema, source_table, target_catalog, target_schema, target_table]):
            return result
        
        source_fqn = f"{source_catalog}.{source_schema}.{source_table}"
        target_fqn = f"{target_catalog}.{target_schema}.{target_table}"
        
        # Check if source and target are different
        if source_fqn != target_fqn:
            # Different tables - this is OK ONLY if it's a format conversion
            # Let's check the source table format
            try:
                from databricks.sdk import WorkspaceClient
                if hasattr(self.permission_validator, 'workspace_client'):
                    ws = self.permission_validator.workspace_client
                else:
                    ws = WorkspaceClient()
                
                source_table_info = ws.tables.get(full_name=source_fqn)
                
                if source_table_info.data_source_format:
                    source_format = source_table_info.data_source_format.value.lower() if hasattr(source_table_info.data_source_format, 'value') else str(source_table_info.data_source_format).lower()
                    
                    if source_format == target_format:
                        # Same format but different table names - this is suspicious
                        result['warning'] = (
                            f"⚠️  'Refresh table in-place' is enabled, but source and target are different tables with the SAME format ({source_format.upper()}).\n\n"
                            f"  Source: {source_fqn} ({source_format.upper()})\n"
                            f"  Target: {target_fqn} ({target_format.upper()})\n\n"
                            f"This is unusual. 'In-place refresh' typically means source = target for compaction/clustering.\n"
                            f"If you're just copying data, consider unchecking 'Refresh table in-place'."
                        )
                    else:
                        # Different formats - this is a format conversion, which is valid!
                        result['warning'] = (
                            f"✅ Format conversion detected: {source_format.upper()} → {target_format.upper()}\n\n"
                            f"  Source: {source_fqn} ({source_format.upper()})\n"
                            f"  Target: {target_fqn} ({target_format.upper()})\n\n"
                            f"Note: 'Refresh table in-place' is a bit misleading here since you're creating a new table.\n"
                            f"This will read from the source and create a new target table with the specified format."
                        )
            
            except Exception as e:
                error_str = str(e)
                if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str:
                    result['error'] = (
                        f"❌ Source table '{source_fqn}' does not exist.\n"
                        f"Cannot read from a non-existent table."
                    )
        
        else:
            # Source = Target (true in-place refresh)
            # Validate that the format matches the existing table
            try:
                from databricks.sdk import WorkspaceClient
                if hasattr(self.permission_validator, 'workspace_client'):
                    ws = self.permission_validator.workspace_client
                else:
                    ws = WorkspaceClient()
                
                table_info = ws.tables.get(full_name=target_fqn)
                
                if table_info.data_source_format:
                    existing_format = table_info.data_source_format.value.lower() if hasattr(table_info.data_source_format, 'value') else str(table_info.data_source_format).lower()
                    
                    if existing_format != target_format:
                        result['error'] = (
                            f"❌ In-place refresh format mismatch: "
                            f"Existing table '{target_fqn}' is {existing_format.upper()}, "
                            f"but you're trying to refresh it as {target_format.upper()}. "
                            f"You CANNOT change table format with true in-place refresh (source = target). "
                            f"Either: (1) Change 'Table Format' to {existing_format.upper()} to match the existing table, "
                            f"or (2) Set a different target table name for format conversion."
                        )
            
            except Exception as e:
                error_str = str(e)
                if "TABLE_OR_VIEW_NOT_FOUND" in error_str or "does not exist" in error_str:
                    result['warning'] = (
                        f"⚠️  In-place refresh mode enabled, but table '{target_fqn}' does not exist yet. "
                        f"This will create a new table (not truly in-place). "
                        f"Consider unchecking 'Refresh table in-place' for clarity."
                    )
        
        return result
    
    def _format_sql(self, sql: str) -> str:
        """Format SQL for readability"""
        # Basic formatting - in production, use sqlparse or similar
        import sqlparse
        return sqlparse.format(sql, reindent=True, keyword_case='upper')

