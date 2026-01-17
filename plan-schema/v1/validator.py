"""
Plan Validator - Validates plans against JSON schema and semantic rules
"""

import json
import re
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from uuid import UUID
import jsonschema
from jsonschema import validate, ValidationError


class PlanValidationError(Exception):
    """Exception raised for plan validation failures"""
    
    def __init__(self, errors: List[str]):
        self.errors = errors
        super().__init__(f"Plan validation failed with {len(errors)} error(s)")


class PlanValidator:
    """Validates SQLPilot plans against schema and semantic rules"""
    
    def __init__(self, schema_path: str):
        """
        Initialize validator with JSON schema
        
        Args:
            schema_path: Path to plan JSON schema file
        """
        with open(schema_path, 'r') as f:
            self.schema = json.load(f)
        
        self.validator = jsonschema.Draft7Validator(self.schema)
    
    def validate_plan(self, plan: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a plan against all rules
        
        Args:
            plan: Plan dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # 1. Schema validation
        schema_errors = self._validate_schema(plan)
        errors.extend(schema_errors)
        
        # 2. Semantic validation
        semantic_errors = self._validate_semantics(plan)
        errors.extend(semantic_errors)
        
        # 3. Pattern-specific validation
        pattern_errors = self._validate_pattern_config(plan)
        errors.extend(pattern_errors)
        
        return len(errors) == 0, errors
    
    def _validate_schema(self, plan: Dict[str, Any]) -> List[str]:
        """Validate plan against JSON schema"""
        errors = []
        
        try:
            validate(instance=plan, schema=self.schema)
        except ValidationError as e:
            errors.append(f"Schema validation error: {e.message} at {'.'.join(str(p) for p in e.path)}")
        except Exception as e:
            errors.append(f"Schema validation error: {str(e)}")
        
        return errors
    
    def _validate_semantics(self, plan: Dict[str, Any]) -> List[str]:
        """Validate semantic rules"""
        errors = []
        
        # Validate plan_id is valid UUID
        try:
            plan_id = plan.get('plan_metadata', {}).get('plan_id')
            if plan_id:
                UUID(plan_id)
        except (ValueError, AttributeError):
            errors.append(f"Invalid plan_id: must be valid UUID v4")
        
        # Validate version is semantic version
        version = plan.get('plan_metadata', {}).get('version', '')
        if not re.match(r'^\d+\.\d+\.\d+$', version):
            errors.append(f"Invalid version '{version}': must follow semantic versioning (major.minor.patch)")
        
        # Validate created_at is valid ISO 8601 timestamp
        try:
            created_at = plan.get('plan_metadata', {}).get('created_at')
            if created_at:
                datetime.fromisoformat(created_at.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            errors.append(f"Invalid created_at: must be valid ISO 8601 timestamp")
        
        # Validate source and target are not identical for non-merge patterns
        source = plan.get('source', {})
        target = plan.get('target', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        if pattern_type not in ['MERGE_UPSERT', 'SCD2']:
            if (source.get('catalog') == target.get('catalog') and
                source.get('schema') == target.get('schema') and
                source.get('table') == target.get('table')):
                errors.append("Source and target cannot be identical for this pattern type")
        
        # Validate partition columns don't contain duplicates
        partition_by = target.get('partition_by', [])
        if len(partition_by) != len(set(partition_by)):
            errors.append("Duplicate columns in partition_by")
        
        # Validate cluster columns don't contain duplicates
        cluster_by = target.get('cluster_by', [])
        if len(cluster_by) != len(set(cluster_by)):
            errors.append("Duplicate columns in cluster_by")
        
        # Validate schedule configuration
        schedule = plan.get('schedule', {})
        if schedule.get('type') == 'cron' and not schedule.get('cron_expression'):
            errors.append("cron_expression required when schedule type is 'cron'")
        
        if schedule.get('type') == 'event' and not schedule.get('event_trigger'):
            errors.append("event_trigger required when schedule type is 'event'")
        
        return errors
    
    def _validate_pattern_config(self, plan: Dict[str, Any]) -> List[str]:
        """Validate pattern-specific configuration"""
        errors = []
        
        pattern_type = plan.get('pattern', {}).get('type')
        pattern_config = plan.get('pattern_config', {})
        source = plan.get('source', {})
        target = plan.get('target', {})
        
        if pattern_type == 'INCREMENTAL_APPEND':
            # Watermark column is required
            if not pattern_config.get('watermark_column'):
                errors.append("INCREMENTAL_APPEND requires watermark_column in pattern_config")
            
            if not pattern_config.get('watermark_type'):
                errors.append("INCREMENTAL_APPEND requires watermark_type in pattern_config")
            
            # Write mode must be append
            if target.get('write_mode') != 'append':
                errors.append("INCREMENTAL_APPEND requires write_mode='append'")
        
        elif pattern_type == 'FULL_REPLACE':
            # Write mode must be overwrite
            if target.get('write_mode') != 'overwrite':
                errors.append("FULL_REPLACE requires write_mode='overwrite'")
        
        elif pattern_type == 'MERGE_UPSERT':
            # Merge keys are required
            if not pattern_config.get('merge_keys'):
                errors.append("MERGE_UPSERT requires merge_keys in pattern_config")
            
            # Write mode must be merge
            if target.get('write_mode') != 'merge':
                errors.append("MERGE_UPSERT requires write_mode='merge'")
        
        elif pattern_type == 'SCD2':
            # All SCD2 fields are required
            required_fields = ['business_keys', 'effective_date_column', 'end_date_column', 'current_flag_column']
            for field in required_fields:
                if not pattern_config.get(field):
                    errors.append(f"SCD2 requires {field} in pattern_config")
            
            # Write mode must be merge
            if target.get('write_mode') != 'merge':
                errors.append("SCD2 requires write_mode='merge'")
        
        elif pattern_type == 'SNAPSHOT':
            if not pattern_config.get('snapshot_column'):
                errors.append("SNAPSHOT requires snapshot_column in pattern_config")
            
            # Target should be partitioned by snapshot column
            partition_by = target.get('partition_by', [])
            snapshot_col = pattern_config.get('snapshot_column')
            if snapshot_col and snapshot_col not in partition_by:
                errors.append(f"SNAPSHOT requires snapshot_column '{snapshot_col}' in target partition_by")
        
        elif pattern_type == 'AGGREGATE_REFRESH':
            if not pattern_config.get('group_by_columns'):
                errors.append("AGGREGATE_REFRESH requires group_by_columns in pattern_config")
            
            if not pattern_config.get('aggregations'):
                errors.append("AGGREGATE_REFRESH requires aggregations in pattern_config")
        
        elif pattern_type == 'SURROGATE_KEY':
            if not pattern_config.get('key_column'):
                errors.append("SURROGATE_KEY requires key_column in pattern_config")
            
            if not pattern_config.get('key_generation_method'):
                errors.append("SURROGATE_KEY requires key_generation_method in pattern_config")
            
            # If HASH method, hash_columns required
            if pattern_config.get('key_generation_method') == 'HASH' and not pattern_config.get('hash_columns'):
                errors.append("SURROGATE_KEY with HASH method requires hash_columns in pattern_config")
        
        elif pattern_type == 'DEDUPLICATE':
            if not pattern_config.get('dedupe_keys'):
                errors.append("DEDUPLICATE requires dedupe_keys in pattern_config")
            
            if not pattern_config.get('order_by_column'):
                errors.append("DEDUPLICATE requires order_by_column in pattern_config")
        
        return errors
    
    def validate_and_raise(self, plan: Dict[str, Any]) -> None:
        """
        Validate plan and raise exception if invalid
        
        Args:
            plan: Plan dictionary to validate
            
        Raises:
            PlanValidationError: If plan is invalid
        """
        is_valid, errors = self.validate_plan(plan)
        if not is_valid:
            raise PlanValidationError(errors)


def load_and_validate_plan(plan_path: str, schema_path: str) -> Dict[str, Any]:
    """
    Load a plan from file and validate it
    
    Args:
        plan_path: Path to plan YAML/JSON file
        schema_path: Path to schema JSON file
        
    Returns:
        Validated plan dictionary
        
    Raises:
        PlanValidationError: If plan is invalid
    """
    import yaml
    
    # Load plan
    with open(plan_path, 'r') as f:
        if plan_path.endswith('.json'):
            plan = json.load(f)
        else:
            plan = yaml.safe_load(f)
    
    # Validate
    validator = PlanValidator(schema_path)
    validator.validate_and_raise(plan)
    
    return plan

