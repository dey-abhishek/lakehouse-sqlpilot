"""
SQL Guardrails - Enforce safety rules on generated SQL
"""

import re
from typing import List, Tuple


class SQLGuardrailError(Exception):
    """Raised when SQL violates guardrails"""
    pass


class SQLGuardrails:
    """Enforces safety guardrails on generated SQL"""
    
    # Blocked SQL operations
    BLOCKED_OPERATIONS = [
        r'\bDROP\s+TABLE\b',
        r'\bDROP\s+DATABASE\b',
        r'\bDROP\s+SCHEMA\b',
        r'\bTRUNCATE\s+TABLE\b',
        r'\bALTER\s+TABLE\b.*\bDROP\b',
    ]
    
    # Operations requiring WHERE clause
    REQUIRE_WHERE = [
        r'\bDELETE\s+FROM\b',
    ]
    
    def __init__(self, strict_mode: bool = True):
        """
        Initialize guardrails
        
        Args:
            strict_mode: If True, raise on violations. If False, just return violations.
        """
        self.strict_mode = strict_mode
    
    def validate_sql(self, sql: str) -> Tuple[bool, List[str]]:
        """
        Validate SQL against guardrails
        
        Args:
            sql: SQL to validate
            
        Returns:
            Tuple of (is_valid, list_of_violations)
        """
        violations = []
        
        # Check for SQLPilot header
        if '-- LAKEHOUSE SQLPILOT GENERATED SQL' not in sql:
            violations.append("SQL must have SQLPilot header (generated SQL only)")
        
        # Remove comments for pattern matching
        # This allows us to have commented-out operations without triggering guardrails
        sql_without_comments = self._remove_comments(sql)
        
        # Check blocked operations
        for pattern in self.BLOCKED_OPERATIONS:
            # Special case: Allow "DROP TABLE IF EXISTS" for format conversion
            if pattern == r'\bDROP\s+TABLE\b':
                if re.search(r'\bDROP\s+TABLE\s+IF\s+EXISTS\b', sql_without_comments, re.IGNORECASE):
                    # This is allowed for format conversion - skip this check
                    continue
                elif re.search(pattern, sql_without_comments, re.IGNORECASE):
                    # This is a bare DROP TABLE without IF EXISTS - block it
                    violations.append(f"Blocked operation detected: {pattern} (use DROP TABLE IF EXISTS for format conversion)")
            else:
                # Other blocked operations - check normally
                if re.search(pattern, sql_without_comments, re.IGNORECASE):
                    violations.append(f"Blocked operation detected: {pattern}")
        
        # Check operations requiring WHERE
        for pattern in self.REQUIRE_WHERE:
            if re.search(pattern, sql_without_comments, re.IGNORECASE):
                # Check if WHERE clause exists after the operation
                match = re.search(pattern + r'.*?(?=;|$)', sql_without_comments, re.IGNORECASE | re.DOTALL)
                if match and r'\bWHERE\b' not in match.group(0).upper():
                    violations.append(f"Operation requires WHERE clause: {pattern}")
        
        is_valid = len(violations) == 0
        return (is_valid, violations)
    
    def _remove_comments(self, sql: str) -> str:
        """Remove SQL comments (both -- and /* */)"""
        # Remove single-line comments (-- ...)
        sql = re.sub(r'--[^\n]*', '', sql)
        # Remove multi-line comments (/* ... */)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql
    
    def validate_and_raise(self, sql: str):
        """
        Validate SQL and raise if violations found
        
        Args:
            sql: SQL to validate
            
        Raises:
            SQLGuardrailError: If violations found
        """
        is_valid, violations = self.validate_sql(sql)
        
        if not is_valid:
            raise SQLGuardrailError(
                f"SQL guardrail violations:\n" + "\n".join(f"  - {v}" for v in violations)
            )
