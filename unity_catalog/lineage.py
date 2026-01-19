"""
Unity Catalog Lineage - Tracks data lineage for executed SQL
"""

from typing import Dict, List, Optional
from datetime import datetime
from databricks.sdk import WorkspaceClient


class LineageTracker:
    """Tracks data lineage in Unity Catalog"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        """
        Initialize lineage tracker
        
        Args:
            workspace_client: Databricks workspace client
        """
        self.workspace_client = workspace_client
        self.lineage_api = workspace_client.lineage
    
    def register_lineage(self,
                        plan_id: str,
                        execution_id: str,
                        source_tables: List[str],
                        target_table: str,
                        sql_text: str,
                        pattern_type: str) -> None:
        """
        Register lineage for SQL execution
        
        Args:
            plan_id: Plan ID
            execution_id: Execution ID
            source_tables: List of source table FQNs
            target_table: Target table FQN
            sql_text: Executed SQL
            pattern_type: Pattern type used
        """
        # Unity Catalog automatically tracks lineage for executed SQL
        # This method adds additional metadata and tags
        
        # Add custom tags to target table for SQLPilot tracking
        self._add_sqlpilot_tags(
            table_fqn=target_table,
            plan_id=plan_id,
            execution_id=execution_id,
            pattern_type=pattern_type
        )
    
    def _add_sqlpilot_tags(self,
                          table_fqn: str,
                          plan_id: str,
                          execution_id: str,
                          pattern_type: str) -> None:
        """Add SQLPilot metadata tags to table"""
        try:
            parts = table_fqn.split('.')
            if len(parts) != 3:
                return
            
            catalog, schema, table = parts
            
            # Get current table info
            table_info = self.workspace_client.tables.get(f"{catalog}.{schema}.{table}")
            
            # Add/update properties
            properties = table_info.properties or {}
            properties.update({
                'sqlpilot.plan_id': plan_id,
                'sqlpilot.last_execution_id': execution_id,
                'sqlpilot.pattern_type': pattern_type,
                'sqlpilot.last_updated': datetime.utcnow().isoformat()
            })
            
            # Update table properties
            self.workspace_client.tables.update(
                full_name=table_fqn,
                properties=properties
            )
            
        except Exception as e:
            # Log error but don't fail execution
            pass
    
    def get_table_lineage(self, table_fqn: str) -> Dict:
        """
        Get lineage information for a table
        
        Args:
            table_fqn: Fully qualified table name
            
        Returns:
            Lineage information dictionary
        """
        try:
            # Get upstream tables
            upstream = self.lineage_api.get_lineage_by_table(
                table_name=table_fqn,
                depth=1
            )
            
            return {
                'table': table_fqn,
                'upstream_tables': [t.name for t in upstream.upstream_tables] if upstream.upstream_tables else [],
                'downstream_tables': [t.name for t in upstream.downstream_tables] if upstream.downstream_tables else []
            }
            
        except Exception as e:
            return {
                'table': table_fqn,
                'upstream_tables': [],
                'downstream_tables': [],
                'error': str(e)
            }
    
    def get_sqlpilot_metadata(self, table_fqn: str) -> Optional[Dict]:
        """
        Get SQLPilot metadata from table properties
        
        Args:
            table_fqn: Fully qualified table name
            
        Returns:
            SQLPilot metadata dictionary or None
        """
        try:
            parts = table_fqn.split('.')
            if len(parts) != 3:
                return None
            
            catalog, schema, table = parts
            
            table_info = self.workspace_client.tables.get(f"{catalog}.{schema}.{table}")
            
            properties = table_info.properties or {}
            
            # Extract SQLPilot properties
            sqlpilot_props = {
                k.replace('sqlpilot.', ''): v 
                for k, v in properties.items() 
                if k.startswith('sqlpilot.')
            }
            
            return sqlpilot_props if sqlpilot_props else None
            
        except Exception:
            return None
    
    def track_column_lineage(self,
                            execution_id: str,
                            column_mappings: List[Dict[str, str]]) -> None:
        """
        Track column-level lineage
        
        Args:
            execution_id: Execution ID
            column_mappings: List of column mappings
                             [{'source': 'source.col', 'target': 'target.col'}]
        """
        # Unity Catalog tracks column lineage automatically from SQL
        # This is a placeholder for future custom column lineage tracking
        pass


