"""
Plan Validator - Validates plans against JSON schema and semantic rules
"""

import json
import yaml
from typing import Dict, Any, Tuple, List
from pathlib import Path


class PlanValidationError(Exception):
    """Raised when plan validation fails"""
    pass


class PlanValidator:
    """Validates SQLPilot plans against schema and semantic rules"""
    
    def __init__(self, schema_path: str):
        """
        Initialize validator with schema
        
        Args:
            schema_path: Path to JSON schema file
        """
        self.schema_path = schema_path
        self.schema = self._load_schema()
    
    def _load_schema(self) -> Dict[str, Any]:
        """Load JSON schema from file"""
        try:
            with open(self.schema_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            raise PlanValidationError(f"Failed to load schema: {e}")
    
    def validate_plan(self, plan: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate plan against schema and semantic rules
        
        Args:
            plan: Plan dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Schema validation
        schema_errors = self._validate_schema(plan)
        errors.extend(schema_errors)
        
        # Semantic validation
        if not schema_errors:  # Only if schema is valid
            semantic_errors = self._validate_semantic(plan)
            errors.extend(semantic_errors)
        
        return (len(errors) == 0, errors)
    
    def _validate_schema(self, plan: Dict[str, Any]) -> List[str]:
        """Validate against JSON schema"""
        errors = []
        
        try:
            import jsonschema
            from jsonschema import Draft7Validator, FormatChecker
            
            # Use format checker for email, date-time etc.
            validator = Draft7Validator(self.schema, format_checker=FormatChecker())
            
            for error in validator.iter_errors(plan):
                # Include the path to the error
                path = '.'.join(str(p) for p in error.absolute_path) if error.absolute_path else 'root'
                errors.append(f"Schema validation error at {path}: {error.message}")
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        return errors
    
    def _validate_semantic(self, plan: Dict[str, Any]) -> List[str]:
        """Validate semantic rules"""
        errors = []
        
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Pattern-specific validation
        if pattern_type == 'SCD2':
            errors.extend(self._validate_scd2(plan))
        elif pattern_type == 'INCREMENTAL_APPEND':
            errors.extend(self._validate_incremental(plan))
        elif pattern_type == 'MERGE_UPSERT':
            errors.extend(self._validate_merge(plan))
        
        # General semantic rules
        errors.extend(self._validate_general_rules(plan))
        
        return errors
    
    def _validate_scd2(self, plan: Dict[str, Any]) -> List[str]:
        """Validate SCD2-specific rules"""
        errors = []
        
        pattern_config = plan.get('pattern_config', {})
        source = plan.get('source', {})
        target = plan.get('target', {})
        
        # Must have business keys
        business_keys = pattern_config.get('business_keys', [])
        if not business_keys:
            errors.append("SCD2 pattern requires business_keys in pattern_config")
        
        # Must have explicit columns
        source_columns = source.get('columns', [])
        if not source_columns:
            errors.append("SCD2 pattern requires explicit columns in source")
        
        # Business keys must be in source columns
        for key in business_keys:
            if key not in source_columns:
                errors.append(f"Business key '{key}' not found in source columns")
        
        # SCD metadata columns must NOT be in source
        scd_columns = [
            pattern_config.get('effective_date_column', 'valid_from'),
            pattern_config.get('end_date_column', 'valid_to'),
            pattern_config.get('current_flag_column', 'is_current')
        ]
        for col in scd_columns:
            if col in source_columns:
                errors.append(f"SCD metadata column '{col}' should not be in source columns")
        
        # Must use merge write mode
        if target.get('write_mode') != 'merge':
            errors.append("SCD2 pattern requires write_mode='merge'")
        
        return errors
    
    def _validate_incremental(self, plan: Dict[str, Any]) -> List[str]:
        """Validate incremental append rules"""
        errors = []
        
        pattern_config = plan.get('pattern_config', {})
        
        # Must have watermark column
        if not pattern_config.get('watermark_column'):
            errors.append("Incremental append requires watermark_column in pattern_config")
        
        return errors
    
    def _validate_merge(self, plan: Dict[str, Any]) -> List[str]:
        """Validate merge/upsert rules"""
        errors = []
        
        pattern_config = plan.get('pattern_config', {})
        
        # Must have merge keys
        if not pattern_config.get('merge_keys'):
            errors.append("Merge/upsert requires merge_keys in pattern_config")
        
        return errors
    
    def _validate_general_rules(self, plan: Dict[str, Any]) -> List[str]:
        """Validate general semantic rules"""
        errors = []
        
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Source and target cannot be identical (unless merge pattern)
        if pattern_type not in ['MERGE_UPSERT', 'SCD2']:
            source_fqn = f"{source.get('catalog')}.{source.get('schema')}.{source.get('table')}"
            target_fqn = f"{target.get('catalog')}.{target.get('schema')}.{target.get('table')}"
            if source_fqn == target_fqn:
                errors.append("Source and target cannot be identical for this pattern")
        
        # Check for duplicate partition columns
        partition_by = target.get('partition_by', [])
        if len(partition_by) != len(set(partition_by)):
            errors.append("Duplicate columns in partition_by")
        
        # Cron schedule must have expression
        schedule = plan.get('schedule', {})
        if schedule.get('type') == 'cron' and not schedule.get('cron_expression'):
            errors.append("Cron schedule requires cron_expression")
        
        return errors
    
    def validate_with_runtime_checks(self, plan: Dict[str, Any], workspace_client=None) -> Tuple[bool, List[str]]:
        """
        Validate plan with optional runtime checks (table existence)
        
        This performs semantic validation that requires connectivity to Databricks.
        Used at execution time to guarantee source table existence.
        
        Args:
            plan: Plan dictionary to validate
            workspace_client: Optional Databricks WorkspaceClient for runtime checks
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # First run standard validation
        is_valid, validation_errors = self.validate_plan(plan)
        errors.extend(validation_errors)
        
        # If workspace client provided, run runtime checks
        if workspace_client:
            runtime_errors = self._validate_runtime_checks(plan, workspace_client)
            errors.extend(runtime_errors)
        
        return (len(errors) == 0, errors)
    
    def _validate_runtime_checks(self, plan: Dict[str, Any], workspace_client) -> List[str]:
        """
        Validate runtime requirements (requires Databricks connection)
        
        Args:
            plan: Plan dictionary
            workspace_client: Databricks WorkspaceClient
            
        Returns:
            List of runtime validation errors
        """
        errors = []
        
        source = plan.get('source', {})
        catalog = source.get('catalog')
        schema = source.get('schema')
        table = source.get('table')
        
        if not all([catalog, schema, table]):
            errors.append("Source must specify catalog, schema, and table")
            return errors
        
        # Check source table existence
        try:
            # Use Unity Catalog API to check table existence
            tables = list(workspace_client.tables.list(
                catalog_name=catalog,
                schema_name=schema,
                max_results=1000
            ))
            
            table_names = [t.name for t in tables]
            
            if table not in table_names:
                errors.append(
                    f"Source table does not exist: `{catalog}`.`{schema}`.`{table}`. "
                    f"SQLPilot requires source tables to exist before execution."
                )
        except Exception as e:
            errors.append(f"Failed to verify source table existence: {str(e)}")
        
        return errors


def load_and_validate_plan(plan_path: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load plan from YAML/JSON file and validate
    
    Args:
        plan_path: Path to plan file
        schema: JSON schema
        
    Returns:
        Validated plan dictionary
        
    Raises:
        PlanValidationError: If validation fails
    """
    # Load plan
    with open(plan_path, 'r') as f:
        if plan_path.endswith('.yaml') or plan_path.endswith('.yml'):
            plan = yaml.safe_load(f)
        else:
            plan = json.load(f)
    
    # Validate
    validator = PlanValidator(schema_path='plan-schema/v1/plan.schema.json')
    is_valid, errors = validator.validate_plan(plan)
    
    if not is_valid:
        raise PlanValidationError(f"Plan validation failed: {'; '.join(errors)}")
    
    return plan

