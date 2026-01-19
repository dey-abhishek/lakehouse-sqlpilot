"""
Genie Handoff Workflow - Structured handoff from Genie to SQLPilot
"""

from typing import Dict, Any, List, Optional
from databricks.sdk import WorkspaceClient


class GenieHandoffContext:
    """Context object for Genie to SQLPilot handoff"""
    
    def __init__(self):
        self.genie_session_id: Optional[str] = None
        self.exploration_queries: List[Dict[str, Any]] = []
        self.validated_tables: List[str] = []
        self.validated_columns: Dict[str, List[str]] = {}
        self.user_intent: Optional[str] = None
        self.metadata_cache: Dict[str, Any] = {}
    
    def add_exploration_query(self, query: str, result_summary: str) -> None:
        """Add a query from Genie exploration"""
        self.exploration_queries.append({
            'query': query,
            'result_summary': result_summary
        })
    
    def add_validated_table(self, table_fqn: str, columns: List[str]) -> None:
        """Add a table that was validated in Genie"""
        if table_fqn not in self.validated_tables:
            self.validated_tables.append(table_fqn)
        self.validated_columns[table_fqn] = columns
    
    def set_user_intent(self, intent: str) -> None:
        """Set the production intent expressed by user"""
        self.user_intent = intent
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            'genie_session_id': self.genie_session_id,
            'exploration_queries': self.exploration_queries,
            'validated_tables': self.validated_tables,
            'validated_columns': self.validated_columns,
            'user_intent': self.user_intent,
            'metadata_cache': self.metadata_cache
        }


class GenieHandoffWorkflow:
    """Manages handoff from Genie exploration to SQLPilot production"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        self.workspace_client = workspace_client
    
    def create_handoff_context(self, genie_session_id: str) -> GenieHandoffContext:
        """
        Create handoff context from Genie session
        
        Args:
            genie_session_id: Genie session ID
            
        Returns:
            GenieHandoffContext with exploration data
        """
        context = GenieHandoffContext()
        context.genie_session_id = genie_session_id
        
        # In production, this would fetch actual Genie session data
        # For now, we create an empty context that users fill manually
        
        return context
    
    def validate_handoff_readiness(self, context: GenieHandoffContext) -> tuple[bool, List[str]]:
        """
        Validate if handoff context is ready for SQLPilot
        
        Args:
            context: Handoff context
            
        Returns:
            Tuple of (is_ready, list_of_issues)
        """
        issues = []
        
        # Check user intent is set
        if not context.user_intent:
            issues.append("Production intent not specified")
        
        # Check at least one table is validated
        if not context.validated_tables:
            issues.append("No tables validated in Genie")
        
        # Check tables exist in Unity Catalog
        for table_fqn in context.validated_tables:
            if not self._verify_table_exists(table_fqn):
                issues.append(f"Table {table_fqn} not found in Unity Catalog")
        
        return len(issues) == 0, issues
    
    def generate_plan_from_handoff(self,
                                   context: GenieHandoffContext,
                                   suggested_pattern: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate SQLPilot plan from Genie handoff context
        
        Args:
            context: Handoff context from Genie
            suggested_pattern: Optional pattern override
            
        Returns:
            Initial plan dictionary
        """
        import uuid
        from datetime import datetime
        
        # Determine pattern from intent if not specified
        if not suggested_pattern:
            suggested_pattern = self._infer_pattern_from_intent(context.user_intent or '')
        
        # Assume first table is source, second is target (if available)
        source_table = context.validated_tables[0] if len(context.validated_tables) > 0 else None
        target_table = context.validated_tables[1] if len(context.validated_tables) > 1 else None
        
        source_parts = source_table.split('.') if source_table else ['', '', '']
        target_parts = target_table.split('.') if target_table else ['', '', '']
        
        plan = {
            'schema_version': '1.0',
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': self._generate_plan_name(context.user_intent or 'genie_handoff'),
                'description': context.user_intent or 'Plan generated from Genie exploration',
                'owner': 'user@company.com',
                'created_at': datetime.utcnow().isoformat() + 'Z',
                'version': '1.0.0',
                'tags': {
                    'source': 'genie_handoff',
                    'genie_session_id': context.genie_session_id or ''
                }
            },
            'pattern': {
                'type': suggested_pattern
            },
            'source': {
                'catalog': source_parts[0],
                'schema': source_parts[1],
                'table': source_parts[2],
                'columns': context.validated_columns.get(source_table, [])
            },
            'target': {
                'catalog': target_parts[0],
                'schema': target_parts[1],
                'table': target_parts[2],
                'write_mode': self._get_write_mode(suggested_pattern)
            },
            'pattern_config': self._generate_pattern_config(
                suggested_pattern,
                source_table,
                context.validated_columns.get(source_table, [])
            ),
            'execution_config': {
                'warehouse_id': 'YOUR_WAREHOUSE_ID',
                'timeout_seconds': 3600,
                'max_retries': 3
            },
            'schedule': {
                'type': 'manual'
            },
            '_genie_context': context.to_dict()
        }
        
        return plan
    
    def document_handoff_boundary(self) -> Dict[str, Any]:
        """
        Document the Genie ↔ SQLPilot boundary
        
        Returns:
            Documentation dictionary
        """
        return {
            'genie_responsibilities': [
                'Data exploration and understanding',
                'Ad-hoc queries and analysis',
                'Schema validation',
                'Sample data preview',
                'Business logic validation'
            ],
            'sqlpilot_responsibilities': [
                'Production SQL execution',
                'Versioned plan management',
                'Governance and audit',
                'Scheduled execution',
                'Idempotency guarantees'
            ],
            'handoff_artifacts': [
                'Table references (fully qualified names)',
                'Column names (validated)',
                'Business intent (natural language)',
                'Sample queries (for understanding only)',
                'Metadata (from Unity Catalog)'
            ],
            'strict_boundaries': {
                'genie_cannot': [
                    'Create SQLPilot plans',
                    'Execute production SQL',
                    'Schedule recurring jobs',
                    'Provide governance guarantees'
                ],
                'sqlpilot_cannot': [
                    'Answer exploratory questions',
                    'Generate ad-hoc queries',
                    'Provide BI visualizations',
                    'Perform free-form analysis'
                ]
            }
        }
    
    def _verify_table_exists(self, table_fqn: str) -> bool:
        """Verify table exists in Unity Catalog"""
        try:
            parts = table_fqn.split('.')
            if len(parts) != 3:
                return False
            
            catalog, schema, table = parts
            self.workspace_client.tables.get(f"{catalog}.{schema}.{table}")
            return True
        except Exception:
            return False
    
    def _infer_pattern_from_intent(self, intent: str) -> str:
        """Infer pattern type from user intent"""
        intent_lower = intent.lower()
        
        if any(word in intent_lower for word in ['incremental', 'new', 'append']):
            return 'INCREMENTAL_APPEND'
        elif any(word in intent_lower for word in ['replace', 'refresh']):
            return 'FULL_REPLACE'
        elif any(word in intent_lower for word in ['merge', 'upsert']):
            return 'MERGE_UPSERT'
        elif any(word in intent_lower for word in ['history', 'scd']):
            return 'SCD2'
        else:
            return 'INCREMENTAL_APPEND'
    
    def _generate_plan_name(self, intent: str) -> str:
        """Generate plan name from intent"""
        words = intent.lower().split()[:4]
        name = '_'.join(w for w in words if w.isalnum())
        return name[:64]
    
    def _get_write_mode(self, pattern: str) -> str:
        """Get write mode for pattern"""
        mode_map = {
            'INCREMENTAL_APPEND': 'append',
            'FULL_REPLACE': 'overwrite',
            'MERGE_UPSERT': 'merge',
            'SCD2': 'merge',
        }
        return mode_map.get(pattern, 'append')
    
    def _generate_pattern_config(self, pattern: str, table_fqn: Optional[str], columns: List[str]) -> Dict[str, Any]:
        """Generate pattern-specific configuration"""
        config = {}
        
        if pattern == 'INCREMENTAL_APPEND':
            timestamp_cols = [c for c in columns if 'timestamp' in c.lower() or 'date' in c.lower()]
            config = {
                'watermark_column': timestamp_cols[0] if timestamp_cols else 'created_at',
                'watermark_type': 'timestamp'
            }
        elif pattern == 'MERGE_UPSERT':
            id_cols = [c for c in columns if 'id' in c.lower()]
            config = {
                'merge_keys': id_cols[:1] if id_cols else ['id']
            }
        
        return config


