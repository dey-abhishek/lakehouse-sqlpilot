"""
Plan Suggestion Agent - Helps users create plans from intent
"""

from typing import Dict, Any, List
from datetime import datetime, timezone
import uuid
from databricks.sdk import WorkspaceClient
from .base_agent import BaseAgent, AgentBoundaryViolation


class PlanSuggestionAgent(BaseAgent):
    """Suggests plan configurations based on user intent"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        super().__init__(
            name="PlanSuggestionAgent",
            description="Helps users create plans from business intent"
        )
        self.workspace_client = workspace_client
    
    def process(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process plan suggestion request
        
        Input format:
        {
            'request_type': 'suggest_plan',
            'user_intent': 'Load customer data incrementally every day',
            'source_table': 'catalog.schema.table',
            'target_table': 'catalog.schema.table',
            'additional_context': {...}
        }
        
        Output format:
        {
            'success': True/False,
            'suggested_plan': {...},  # Plan YAML structure
            'recommended_pattern': 'INCREMENTAL_APPEND',
            'configuration_notes': [],
            'warnings': []
        }
        """
        # Validate input
        is_valid, errors = self.validate_input(input_data)
        if not is_valid:
            return {'success': False, 'errors': errors}
        
        # Enforce boundaries - agent cannot execute SQL
        self.enforce_boundaries('read_actual_data')  # Should only read metadata
        
        user_intent = input_data.get('user_intent', '')
        source_table = input_data.get('source_table')
        target_table = input_data.get('target_table')
        
        # Analyze intent and suggest pattern
        suggested_pattern = self._analyze_intent(user_intent)
        
        # Get table metadata (NOT data)
        source_metadata = self._get_table_metadata(source_table) if source_table else {}
        target_metadata = self._get_table_metadata(target_table) if target_table else {}
        
        # Generate plan suggestion
        suggested_plan = self._generate_plan_suggestion(
            pattern=suggested_pattern,
            user_intent=user_intent,
            source_table=source_table,
            target_table=target_table,
            source_metadata=source_metadata,
            target_metadata=target_metadata
        )
        
        # Generate configuration notes
        notes = self._generate_configuration_notes(suggested_pattern, source_metadata, target_metadata)
        
        # Generate warnings
        warnings = self._generate_warnings(suggested_pattern, source_table, target_table)
        
        result = {
            'success': True,
            'suggested_plan': suggested_plan,
            'recommended_pattern': suggested_pattern,
            'configuration_notes': notes,
            'warnings': warnings
        }
        
        # Log interaction
        self.log_interaction(input_data, result)
        
        return result
    
    def get_allowed_inputs(self) -> List[str]:
        """Allowed input types"""
        return ['suggest_plan', 'refine_plan', 'validate_intent']
    
    def get_allowed_outputs(self) -> List[str]:
        """Allowed output types"""
        return ['suggested_plan', 'configuration_notes', 'warnings']
    
    def _analyze_intent(self, user_intent: str) -> str:
        """Analyze user intent and suggest pattern"""
        intent_lower = user_intent.lower()
        
        # Pattern detection keywords
        if any(word in intent_lower for word in ['incremental', 'new', 'append', 'delta']):
            return 'INCREMENTAL_APPEND'
        
        if any(word in intent_lower for word in ['replace', 'full', 'refresh', 'rebuild']):
            return 'FULL_REPLACE'
        
        if any(word in intent_lower for word in ['merge', 'upsert', 'update', 'sync']):
            return 'MERGE_UPSERT'
        
        if any(word in intent_lower for word in ['history', 'scd', 'track changes', 'slowly changing']):
            return 'SCD2'
        
        if any(word in intent_lower for word in ['snapshot', 'point in time', 'daily copy']):
            return 'SNAPSHOT'
        
        if any(word in intent_lower for word in ['aggregate', 'summarize', 'rollup']):
            return 'AGGREGATE_REFRESH'
        
        # Default to incremental append
        return 'INCREMENTAL_APPEND'
    
    def _get_table_metadata(self, table_fqn: str) -> Dict[str, Any]:
        """
        Get table metadata from Unity Catalog (NOT actual data)
        
        Args:
            table_fqn: Fully qualified table name
            
        Returns:
            Metadata dictionary
        """
        try:
            parts = table_fqn.split('.')
            if len(parts) != 3:
                return {}
            
            catalog, schema, table = parts
            
            # Get table info from Unity Catalog
            table_info = self.workspace_client.tables.get(f"{catalog}.{schema}.{table}")
            
            # Extract metadata (NO actual data)
            metadata = {
                'name': table_info.name,
                'catalog': catalog,
                'schema': schema,
                'table': table,
                'columns': [
                    {
                        'name': col.name,
                        'type': col.type_name,
                        'nullable': col.nullable
                    }
                    for col in (table_info.columns or [])
                ],
                'partitions': table_info.storage_location,
                'table_type': table_info.table_type,
            }
            
            return metadata
            
        except Exception as e:
            return {'error': str(e)}
    
    def _generate_plan_suggestion(self,
                                  pattern: str,
                                  user_intent: str,
                                  source_table: str,
                                  target_table: str,
                                  source_metadata: Dict,
                                  target_metadata: Dict) -> Dict[str, Any]:
        """Generate suggested plan structure"""
        import uuid
        from datetime import datetime
        
        source_parts = source_table.split('.') if source_table else ['', '', '']
        target_parts = target_table.split('.') if target_table else ['', '', '']
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': self._generate_plan_name(user_intent),
                'description': user_intent,
                'owner': 'user@company.com',  # Placeholder
                'created_at': datetime.now(timezone.utc).isoformat(),
                'version': '1.0.0'
            },
            'pattern': {
                'type': pattern
            },
            'source': {
                'catalog': source_parts[0],
                'schema': source_parts[1],
                'table': source_parts[2]
            },
            'target': {
                'catalog': target_parts[0],
                'schema': target_parts[1],
                'table': target_parts[2],
                'write_mode': self._get_write_mode(pattern)
            },
            'pattern_config': self._generate_pattern_config(pattern, source_metadata),
            'execution_config': {
                'warehouse_id': 'YOUR_WAREHOUSE_ID',  # Placeholder
                'timeout_seconds': 3600,
                'max_retries': 3
            },
            'schedule': {
                'type': 'manual'
            }
        }
        
        return plan
    
    def _generate_plan_name(self, user_intent: str) -> str:
        """Generate plan name from intent"""
        # Simple name generation - take first few words, lowercase, underscore
        words = user_intent.lower().split()[:4]
        name = '_'.join(w for w in words if w.isalnum())
        return name[:64]  # Limit length
    
    def _get_write_mode(self, pattern: str) -> str:
        """Get write mode for pattern"""
        mode_map = {
            'INCREMENTAL_APPEND': 'append',
            'FULL_REPLACE': 'overwrite',
            'MERGE_UPSERT': 'merge',
            'SCD2': 'merge',
            'SNAPSHOT': 'append',
            'AGGREGATE_REFRESH': 'overwrite',
        }
        return mode_map.get(pattern, 'append')
    
    def _generate_pattern_config(self, pattern: str, source_metadata: Dict) -> Dict[str, Any]:
        """Generate pattern-specific configuration"""
        config = {}
        
        columns = source_metadata.get('columns', [])
        
        if pattern == 'INCREMENTAL_APPEND':
            # Suggest timestamp column for watermark
            timestamp_cols = [c['name'] for c in columns if 'timestamp' in c['type'].lower() or 'date' in c['type'].lower()]
            config = {
                'watermark_column': timestamp_cols[0] if timestamp_cols else 'created_at',
                'watermark_type': 'timestamp'
            }
        
        elif pattern == 'MERGE_UPSERT':
            # Suggest ID columns for merge keys
            id_cols = [c['name'] for c in columns if 'id' in c['name'].lower()]
            config = {
                'merge_keys': id_cols[:1] if id_cols else ['id']
            }
        
        elif pattern == 'SCD2':
            id_cols = [c['name'] for c in columns if 'id' in c['name'].lower()]
            config = {
                'business_keys': id_cols[:1] if id_cols else ['id'],
                'effective_date_column': 'valid_from',
                'end_date_column': 'valid_to',
                'current_flag_column': 'is_current'
            }
        
        return config
    
    def _generate_configuration_notes(self, pattern: str, source_metadata: Dict, target_metadata: Dict) -> List[str]:
        """Generate configuration notes for user"""
        notes = []
        
        notes.append(f"Recommended pattern: {pattern}")
        notes.append("Review and customize the suggested plan before deployment")
        notes.append("Ensure you have the necessary permissions on source and target tables")
        
        if pattern == 'INCREMENTAL_APPEND':
            notes.append("Configure watermark_column to match your incremental logic")
        
        if pattern == 'MERGE_UPSERT':
            notes.append("Specify merge_keys that uniquely identify rows")
        
        if pattern == 'SCD2':
            notes.append("SCD2 requires additional columns in target: valid_from, valid_to, is_current")
        
        return notes
    
    def _generate_warnings(self, pattern: str, source_table: str, target_table: str) -> List[str]:
        """Generate warnings"""
        warnings = []
        
        if 'prod' in (target_table or '').lower():
            warnings.append("Target table appears to be in production - exercise caution")
        
        if pattern == 'FULL_REPLACE':
            warnings.append("FULL_REPLACE will delete all existing data in target table")
        
        return warnings

