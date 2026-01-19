"""
Validation Agent - Validates plan correctness
"""

from typing import Dict, Any, List
from .base_agent import BaseAgent


class ValidationAgent(BaseAgent):
    """Validates plan correctness and suggests fixes"""
    
    def __init__(self):
        super().__init__(
            name="ValidationAgent",
            description="Validates plan correctness"
        )
    
    def process(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process validation request
        
        Input format:
        {
            'request_type': 'validate_plan',
            'plan': {...},
            'source_schema': {...},
            'target_schema': {...}
        }
        
        Output format:
        {
            'success': True/False,
            'is_valid': True/False,
            'errors': [...],
            'warnings': [...],
            'suggestions': [...]  # YAML suggestions, NOT SQL
        }
        """
        # Validate input
        is_valid, errors = self.validate_input(input_data)
        if not is_valid:
            return {'success': False, 'errors': errors}
        
        # Enforce boundary: cannot auto-fix errors
        self.enforce_boundaries('auto_fix_errors')
        
        plan = input_data.get('plan', {})
        source_schema = input_data.get('source_schema', {})
        target_schema = input_data.get('target_schema', {})
        
        # Validate plan structure
        validation_errors = []
        warnings = []
        suggestions = []
        
        # Validate pattern configuration
        pattern_errors = self._validate_pattern_config(plan)
        validation_errors.extend(pattern_errors)
        
        # Validate schema compatibility
        schema_errors, schema_warnings = self._validate_schema_compatibility(
            plan, source_schema, target_schema
        )
        validation_errors.extend(schema_errors)
        warnings.extend(schema_warnings)
        
        # Generate suggestions (YAML only, NOT SQL)
        if validation_errors:
            suggestions = self._generate_fix_suggestions(validation_errors, plan)
        
        result = {
            'success': True,
            'is_valid': len(validation_errors) == 0,
            'errors': validation_errors,
            'warnings': warnings,
            'suggestions': suggestions
        }
        
        self.log_interaction(input_data, result)
        
        return result
    
    def get_allowed_inputs(self) -> List[str]:
        return ['validate_plan', 'check_compatibility']
    
    def get_allowed_outputs(self) -> List[str]:
        return ['validation_results', 'suggestions']  # YAML suggestions only
    
    def _validate_pattern_config(self, plan: Dict) -> List[str]:
        """Validate pattern-specific configuration"""
        errors = []
        pattern_type = plan.get('pattern', {}).get('type')
        pattern_config = plan.get('pattern_config', {})
        
        if pattern_type == 'INCREMENTAL_APPEND':
            if not pattern_config.get('watermark_column'):
                errors.append("INCREMENTAL_APPEND requires watermark_column in pattern_config")
        
        elif pattern_type == 'MERGE_UPSERT':
            if not pattern_config.get('merge_keys'):
                errors.append("MERGE_UPSERT requires merge_keys in pattern_config")
        
        elif pattern_type == 'SCD2':
            required = ['business_keys', 'effective_date_column', 'end_date_column', 'current_flag_column']
            for field in required:
                if not pattern_config.get(field):
                    errors.append(f"SCD2 requires {field} in pattern_config")
        
        return errors
    
    def _validate_schema_compatibility(self,
                                      plan: Dict,
                                      source_schema: Dict,
                                      target_schema: Dict) -> tuple[List[str], List[str]]:
        """Validate schema compatibility between source and target"""
        errors = []
        warnings = []
        
        pattern_config = plan.get('pattern_config', {})
        pattern_type = plan.get('pattern', {}).get('type')
        
        # Check if watermark column exists in source
        if pattern_type == 'INCREMENTAL_APPEND':
            watermark_col = pattern_config.get('watermark_column')
            if watermark_col and source_schema:
                source_cols = source_schema.get('columns', [])
                if watermark_col not in [c['name'] for c in source_cols]:
                    errors.append(f"Watermark column '{watermark_col}' not found in source schema")
        
        # Check merge keys exist
        if pattern_type == 'MERGE_UPSERT':
            merge_keys = pattern_config.get('merge_keys', [])
            if source_schema and target_schema:
                source_cols = [c['name'] for c in source_schema.get('columns', [])]
                target_cols = [c['name'] for c in target_schema.get('columns', [])]
                
                for key in merge_keys:
                    if key not in source_cols:
                        errors.append(f"Merge key '{key}' not found in source schema")
                    if key not in target_cols:
                        errors.append(f"Merge key '{key}' not found in target schema")
        
        # Check data type compatibility
        if source_schema and target_schema:
            type_warnings = self._check_type_compatibility(source_schema, target_schema)
            warnings.extend(type_warnings)
        
        return errors, warnings
    
    def _check_type_compatibility(self, source_schema: Dict, target_schema: Dict) -> List[str]:
        """Check data type compatibility"""
        warnings = []
        
        source_cols = {c['name']: c['type'] for c in source_schema.get('columns', [])}
        target_cols = {c['name']: c['type'] for c in target_schema.get('columns', [])}
        
        for col_name, source_type in source_cols.items():
            if col_name in target_cols:
                target_type = target_cols[col_name]
                if source_type != target_type:
                    warnings.append(
                        f"Column '{col_name}' type mismatch: source is {source_type}, target is {target_type}"
                    )
        
        return warnings
    
    def _generate_fix_suggestions(self, errors: List[str], plan: Dict) -> List[Dict[str, str]]:
        """Generate fix suggestions (YAML only, NOT SQL)"""
        suggestions = []
        
        for error in errors:
            if 'watermark_column' in error:
                suggestions.append({
                    'error': error,
                    'suggestion': 'Add watermark_column to pattern_config',
                    'example_yaml': """
pattern_config:
  watermark_column: "created_at"
  watermark_type: "timestamp"
"""
                })
            
            elif 'merge_keys' in error:
                suggestions.append({
                    'error': error,
                    'suggestion': 'Add merge_keys to pattern_config',
                    'example_yaml': """
pattern_config:
  merge_keys: ["id"]
"""
                })
            
            elif 'SCD2' in error:
                suggestions.append({
                    'error': error,
                    'suggestion': 'Add all required SCD2 fields to pattern_config',
                    'example_yaml': """
pattern_config:
  business_keys: ["customer_id"]
  effective_date_column: "valid_from"
  end_date_column: "valid_to"
  current_flag_column: "is_current"
"""
                })
        
        return suggestions


