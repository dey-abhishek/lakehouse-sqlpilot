"""
SQL Compiler - Compiles plans into SQL using patterns and templates
"""

from typing import Dict, Any, Tuple, List
from datetime import datetime, timezone
import uuid

from plan_schema.v1.validator import PlanValidator
from compiler.patterns import PatternFactory
from compiler.guardrails import SQLGuardrails, SQLGuardrailError


class CompilationError(Exception):
    """Raised when compilation fails"""
    pass


class SQLCompiler:
    """Compiles validated plans into executable SQL"""
    
    def __init__(self, schema_path: str, strict_guardrails: bool = True):
        """
        Initialize compiler
        
        Args:
            schema_path: Path to plan JSON schema
            strict_guardrails: Whether to enforce guardrails strictly
        """
        self.validator = PlanValidator(schema_path)
        self.guardrails = SQLGuardrails(strict_mode=strict_guardrails)
        self.strict_guardrails = strict_guardrails
    
    def compile(self, plan: Dict[str, Any], context: Dict[str, Any] = None) -> str:
        """
        Compile plan to SQL
        
        Args:
            plan: Plan dictionary
            context: Execution context (optional)
            
        Returns:
            Generated SQL string
            
        Raises:
            CompilationError: If compilation fails
        """
        # Validate plan
        is_valid, errors = self.validator.validate_plan(plan)
        if not is_valid:
            raise CompilationError(f"Plan validation failed: {'; '.join(errors)}")
        
        # Create execution context
        if context is None:
            context = self._create_default_context()
        
        # Create pattern instance
        try:
            pattern = PatternFactory.create_pattern(plan)
        except ValueError as e:
            raise CompilationError(str(e))
        
        # Validate pattern config
        config_errors = pattern.validate_config()
        if config_errors:
            raise CompilationError(f"Pattern validation failed: {'; '.join(config_errors)}")
        
        # Generate SQL
        try:
            sql = pattern.generate_sql(context)
        except Exception as e:
            raise CompilationError(f"SQL generation failed: {e}")
        
        # Validate guardrails
        if self.strict_guardrails:
            self.guardrails.validate_and_raise(sql)
        
        return sql
    
    def compile_with_runtime_validation(self, plan: Dict[str, Any], workspace_client, context: Dict[str, Any] = None) -> str:
        """
        Compile plan with runtime validation (checks table existence)
        
        This method performs pre-flight checks to ensure source tables exist
        before generating SQL. Use this when you have Databricks connectivity.
        
        Args:
            plan: Plan dictionary
            workspace_client: Databricks WorkspaceClient for runtime checks
            context: Execution context (optional)
            
        Returns:
            Generated SQL string
            
        Raises:
            CompilationError: If compilation or runtime validation fails
        """
        # Validate plan with runtime checks
        is_valid, errors = self.validator.validate_with_runtime_checks(plan, workspace_client)
        if not is_valid:
            raise CompilationError(f"Plan validation failed: {'; '.join(errors)}")
        
        # Create execution context
        if context is None:
            context = self._create_default_context()
        
        # Create pattern instance
        try:
            pattern = PatternFactory.create_pattern(plan)
        except ValueError as e:
            raise CompilationError(str(e))
        
        # Validate pattern config
        config_errors = pattern.validate_config()
        if config_errors:
            raise CompilationError(f"Pattern validation failed: {'; '.join(config_errors)}")
        
        # Generate SQL
        try:
            sql = pattern.generate_sql(context)
        except Exception as e:
            raise CompilationError(f"SQL generation failed: {e}")
        
        # Validate guardrails
        if self.strict_guardrails:
            self.guardrails.validate_and_raise(sql)
        
        return sql
    
    def compile_safe(self, plan: Dict[str, Any], context: Dict[str, Any] = None) -> Tuple[bool, str, List[str]]:
        """
        Safely compile plan (doesn't raise exceptions)
        
        Args:
            plan: Plan dictionary
            context: Execution context (optional)
            
        Returns:
            Tuple of (success, result_or_sql, errors)
        """
        try:
            sql = self.compile(plan, context)
            return (True, sql, [])
        except Exception as e:
            return (False, "", [str(e)])
    
    def validate_plan(self, plan: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate plan without compiling
        
        Args:
            plan: Plan dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        return self.validator.validate_plan(plan)
    
    def preview(self, plan: Dict[str, Any], context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Generate preview of plan compilation
        
        Args:
            plan: Plan dictionary
            context: Execution context (optional)
            
        Returns:
            Preview dictionary with validation, SQL, and metadata
        """
        if context is None:
            context = self._create_default_context()
        
        preview = {
            'plan_id': plan.get('plan_metadata', {}).get('plan_id'),
            'plan_name': plan.get('plan_metadata', {}).get('plan_name'),
            'pattern_type': plan.get('pattern', {}).get('type'),
            'generated_at': context['generated_at'],
        }
        
        # Validate
        is_valid, errors = self.validate_plan(plan)
        preview['is_valid'] = is_valid
        preview['validation_errors'] = errors
        
        if not is_valid:
            preview['sql'] = None
            preview['metadata'] = None
            return preview
        
        # Compile
        try:
            sql = self.compile(plan, context)
            preview['sql'] = sql
            
            # Get pattern for metadata
            pattern = PatternFactory.create_pattern(plan)
            preview['metadata'] = {
                'source': pattern.get_source_fqn(),
                'target': pattern.get_target_fqn(),
                'pattern_type': pattern.pattern_type,
            }
        except Exception as e:
            preview['sql'] = None
            preview['compilation_error'] = str(e)
        
        return preview
    
    def get_supported_patterns(self) -> List[str]:
        """Get list of supported patterns"""
        return PatternFactory.get_supported_patterns()
    
    def _create_default_context(self) -> Dict[str, Any]:
        """Create default execution context"""
        now = datetime.now(timezone.utc)
        return {
            'execution_id': str(uuid.uuid4()),
            'generated_at': now.isoformat(),
            'execution_date': now.strftime('%Y-%m-%d'),
            'execution_timestamp': now.isoformat(),
            'variables': {}
        }


# Export
__all__ = ['SQLCompiler', 'CompilationError']
