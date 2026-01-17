"""
Test Suite for SQL Compiler

Tests the complete compilation pipeline from plan to SQL.
"""

import pytest
import uuid
from datetime import datetime, timezone

from compiler import SQLCompiler, CompilationError
from compiler.guardrails import SQLGuardrails, SQLGuardrailError


@pytest.fixture
def schema_path():
    return "plan-schema/v1/plan.schema.json"


@pytest.fixture
def compiler(schema_path):
    """SQL compiler instance"""
    return SQLCompiler(schema_path, strict_guardrails=True)


@pytest.fixture
def valid_plan():
    """Valid test plan"""
    return {
        'schema_version': '1.0',
        'plan_metadata': {
            'plan_id': str(uuid.uuid4()),
            'plan_name': 'test_compile',
            'description': 'Test compilation',
            'owner': 'test@databricks.com',
            'created_at': datetime.now(timezone.utc).isoformat(),
            'version': '1.0.0'
        },
        'pattern': {'type': 'INCREMENTAL_APPEND'},
        'source': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'test_source'
        },
        'target': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'test_target',
            'write_mode': 'append'
        },
        'pattern_config': {
            'watermark_column': 'created_at',
            'watermark_type': 'timestamp'
        },
        'execution_config': {
            'warehouse_id': 'test_warehouse',
            'timeout_seconds': 3600,
            'max_retries': 3
        }
    }


class TestSQLCompiler:
    """Test SQL compiler"""
    
    def test_compile_valid_plan(self, compiler, valid_plan):
        """Test compiling valid plan produces SQL"""
        sql = compiler.compile(valid_plan)
        
        assert sql is not None
        assert isinstance(sql, str)
        assert len(sql) > 0
        assert '-- LAKEHOUSE SQLPILOT GENERATED SQL' in sql
        assert 'INSERT INTO' in sql
    
    def test_compile_invalid_plan(self, compiler):
        """Test compiling invalid plan raises error"""
        invalid_plan = {'invalid': 'plan'}
        
        with pytest.raises(Exception):  # Could be PlanValidationError or CompilationError
            compiler.compile(invalid_plan)
    
    def test_compile_safe_valid_plan(self, compiler, valid_plan):
        """Test safe compilation with valid plan"""
        success, result, errors = compiler.compile_safe(valid_plan)
        
        assert success is True
        assert 'INSERT INTO' in result
        assert len(errors) == 0
    
    def test_compile_safe_invalid_plan(self, compiler):
        """Test safe compilation with invalid plan"""
        invalid_plan = {'pattern': {'type': 'INVALID'}}
        
        success, result, errors = compiler.compile_safe(invalid_plan)
        
        assert success is False
        assert len(errors) > 0
    
    def test_preview_plan(self, compiler, valid_plan):
        """Test preview generation"""
        preview = compiler.preview(valid_plan)
        
        assert preview['plan_id'] == valid_plan['plan_metadata']['plan_id']
        assert preview['plan_name'] == valid_plan['plan_metadata']['plan_name']
        assert preview['pattern_type'] == 'INCREMENTAL_APPEND'
        assert 'is_valid' in preview
        assert 'sql' in preview
        assert 'metadata' in preview
    
    def test_validate_plan(self, compiler, valid_plan):
        """Test plan validation"""
        is_valid, errors = compiler.validate_plan(valid_plan)
        
        assert is_valid is True
        assert len(errors) == 0
    
    def test_get_supported_patterns(self, compiler):
        """Test getting supported patterns"""
        patterns = compiler.get_supported_patterns()
        
        assert isinstance(patterns, list)
        assert 'INCREMENTAL_APPEND' in patterns
        assert 'SCD2' in patterns


class TestSQLGuardrails:
    """Test SQL guardrails"""
    
    @pytest.fixture
    def guardrails(self):
        return SQLGuardrails(strict_mode=True)
    
    def test_valid_sql_passes(self, guardrails):
        """Test valid SQL passes guardrails"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        INSERT INTO lakehouse-sqlpilot.test.table
        SELECT * FROM lakehouse-sqlpilot.test.source;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is True
        assert len(violations) == 0
    
    def test_drop_table_blocked(self, guardrails):
        """Test DROP TABLE is blocked"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        DROP TABLE lakehouse-sqlpilot.test.table;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is False
        assert any('drop' in v.lower() for v in violations)
    
    def test_truncate_blocked(self, guardrails):
        """Test TRUNCATE is blocked"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        TRUNCATE TABLE lakehouse-sqlpilot.test.table;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is False
        assert any('truncate' in v.lower() for v in violations)
    
    def test_delete_without_where_blocked(self, guardrails):
        """Test DELETE without WHERE is blocked"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        DELETE FROM lakehouse-sqlpilot.test.table;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is False
        assert any('delete' in v.lower() and 'where' in v.lower() for v in violations)
    
    def test_missing_header_blocked(self, guardrails):
        """Test SQL without SQLPilot header is blocked"""
        sql = "SELECT * FROM table;"
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is False
        assert any('header' in v.lower() for v in violations)
    
    def test_merge_allowed(self, guardrails):
        """Test MERGE is allowed"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        MERGE INTO lakehouse-sqlpilot.test.target AS t
        USING lakehouse-sqlpilot.test.source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is True
    
    def test_create_or_replace_allowed(self, guardrails):
        """Test CREATE OR REPLACE TABLE is allowed"""
        sql = """
        -- LAKEHOUSE SQLPILOT GENERATED SQL
        CREATE OR REPLACE TABLE lakehouse-sqlpilot.test.table
        AS SELECT * FROM lakehouse-sqlpilot.test.source;
        """
        
        is_valid, violations = guardrails.validate_sql(sql)
        assert is_valid is True
    
    def test_validate_and_raise(self, guardrails):
        """Test validate_and_raise raises on violations"""
        sql = "DROP TABLE test;"
        
        with pytest.raises(SQLGuardrailError):
            guardrails.validate_and_raise(sql)


class TestDeterministicCompilation:
    """Test deterministic SQL generation"""
    
    def test_same_plan_same_sql(self, compiler, valid_plan):
        """Test compiling same plan twice produces same SQL"""
        context = {
            'execution_id': 'test-123',
            'generated_at': '2026-01-16T10:00:00Z',
            'execution_date': '2026-01-16',
            'execution_timestamp': '2026-01-16T10:00:00Z',
            'variables': {}
        }
        
        sql1 = compiler.compile(valid_plan, context)
        sql2 = compiler.compile(valid_plan, context)
        
        assert sql1 == sql2
    
    def test_plan_version_in_sql(self, compiler, valid_plan):
        """Test plan version appears in generated SQL"""
        sql = compiler.compile(valid_plan)
        
        assert f"-- plan_version: {valid_plan['plan_metadata']['version']}" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

