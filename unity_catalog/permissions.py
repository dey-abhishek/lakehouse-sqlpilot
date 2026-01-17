"""
Unity Catalog Permissions - Validates permissions before SQL execution
"""

from typing import Dict, List, Optional, Set
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, Privilege, SecurableType


class PermissionError(Exception):
    """Exception raised for permission violations"""
    pass


class PermissionValidator:
    """Validates Unity Catalog permissions for SQL operations"""
    
    def __init__(self, workspace_client: WorkspaceClient):
        """
        Initialize permission validator
        
        Args:
            workspace_client: Databricks workspace client
        """
        self.workspace_client = workspace_client
        self.catalog_api = workspace_client.catalogs
        self.schema_api = workspace_client.schemas
        self.table_api = workspace_client.tables
        self.grants_api = workspace_client.grants
    
    def validate_plan_permissions(self, 
                                  plan: Dict,
                                  user: str) -> tuple[bool, List[str]]:
        """
        Validate all permissions required for plan execution
        
        Args:
            plan: Plan dictionary
            user: User executing the plan
            
        Returns:
            Tuple of (is_valid, list_of_violations)
        """
        violations = []
        
        # Extract table references
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Validate source permissions (SELECT)
        source_fqn = self._get_fqn(source)
        if not self._check_table_permission(source_fqn, user, 'SELECT'):
            violations.append(f"User {user} lacks SELECT permission on {source_fqn}")
        
        # Validate target permissions based on pattern
        target_fqn = self._get_fqn(target)
        write_mode = target.get('write_mode')
        
        required_perms = self._get_required_permissions(pattern_type, write_mode)
        
        for perm in required_perms:
            if not self._check_table_permission(target_fqn, user, perm):
                violations.append(f"User {user} lacks {perm} permission on {target_fqn}")
        
        # Validate catalog and schema permissions
        target_catalog = target.get('catalog')
        target_schema = target.get('schema')
        
        if not self._check_catalog_permission(target_catalog, user, 'USE_CATALOG'):
            violations.append(f"User {user} lacks USE_CATALOG permission on {target_catalog}")
        
        if not self._check_schema_permission(target_catalog, target_schema, user, 'USE_SCHEMA'):
            violations.append(f"User {user} lacks USE_SCHEMA permission on {target_catalog}.{target_schema}")
        
        return len(violations) == 0, violations
    
    def validate_and_raise(self, plan: Dict, user: str) -> None:
        """
        Validate permissions and raise exception if invalid
        
        Args:
            plan: Plan dictionary
            user: User executing the plan
            
        Raises:
            PermissionError: If permissions are insufficient
        """
        is_valid, violations = self.validate_plan_permissions(plan, user)
        if not is_valid:
            raise PermissionError(f"Permission violations: {', '.join(violations)}")
    
    def _get_required_permissions(self, pattern_type: str, write_mode: str) -> List[str]:
        """Get required permissions based on pattern and write mode"""
        base_perms = []
        
        if write_mode == 'append':
            base_perms = ['SELECT', 'MODIFY']
        elif write_mode == 'overwrite':
            base_perms = ['SELECT', 'MODIFY']  # CREATE OR REPLACE needs MODIFY
        elif write_mode == 'merge':
            base_perms = ['SELECT', 'MODIFY']
        
        return base_perms
    
    def _check_table_permission(self, table_fqn: str, user: str, permission: str) -> bool:
        """
        Check if user has permission on table
        
        Args:
            table_fqn: Fully qualified table name (catalog.schema.table)
            user: User principal
            permission: Permission to check (SELECT, MODIFY, etc.)
            
        Returns:
            True if user has permission
        """
        try:
            parts = table_fqn.split('.')
            if len(parts) != 3:
                return False
            
            catalog, schema, table = parts
            
            # Get effective permissions for user
            grants = self.grants_api.get(
                securable_type=SecurableType.TABLE,
                full_name=table_fqn
            )
            
            # Check if user has the required privilege
            for grant in grants.privilege_assignments:
                if grant.principal == user:
                    if permission.upper() in [p.value for p in grant.privileges]:
                        return True
            
            return False
            
        except Exception:
            # If we can't check, assume no permission
            return False
    
    def _check_catalog_permission(self, catalog: str, user: str, permission: str) -> bool:
        """Check if user has permission on catalog"""
        try:
            grants = self.grants_api.get(
                securable_type=SecurableType.CATALOG,
                full_name=catalog
            )
            
            for grant in grants.privilege_assignments:
                if grant.principal == user:
                    if permission.upper() in [p.value for p in grant.privileges]:
                        return True
            
            return False
            
        except Exception:
            return False
    
    def _check_schema_permission(self, catalog: str, schema: str, user: str, permission: str) -> bool:
        """Check if user has permission on schema"""
        try:
            schema_fqn = f"{catalog}.{schema}"
            
            grants = self.grants_api.get(
                securable_type=SecurableType.SCHEMA,
                full_name=schema_fqn
            )
            
            for grant in grants.privilege_assignments:
                if grant.principal == user:
                    if permission.upper() in [p.value for p in grant.privileges]:
                        return True
            
            return False
            
        except Exception:
            return False
    
    def _get_fqn(self, table_ref: Dict) -> str:
        """Get fully qualified name from table reference"""
        return f"{table_ref['catalog']}.{table_ref['schema']}.{table_ref['table']}"
    
    def get_user_effective_permissions(self, table_fqn: str, user: str) -> List[str]:
        """
        Get all effective permissions for user on table
        
        Args:
            table_fqn: Fully qualified table name
            user: User principal
            
        Returns:
            List of permissions
        """
        try:
            grants = self.grants_api.get(
                securable_type=SecurableType.TABLE,
                full_name=table_fqn
            )
            
            permissions = []
            for grant in grants.privilege_assignments:
                if grant.principal == user:
                    permissions.extend([p.value for p in grant.privileges])
            
            return permissions
            
        except Exception:
            return []

