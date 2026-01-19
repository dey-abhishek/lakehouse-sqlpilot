"""
Test Suite for SQL Pattern Generation

Tests SQL generation for all patterns, especially SCD2.
"""

import pytest
from datetime import datetime
import uuid

from compiler.patterns import PatternFactory
from compiler.patterns.incremental_append import IncrementalAppendPattern
from compiler.patterns.full_replace import FullReplacePattern
from compiler.patterns.merge_upsert import MergeUpsertPattern
from compiler.patterns.scd2 import SCD2Pattern
from compiler.patterns.snapshot import SnapshotPattern


@pytest.fixture
def execution_context():
    """Standard execution context for testing"""
    return {
        'execution_id': str(uuid.uuid4()),
        'generated_at': '2026-01-16T10:00:00Z',
        'execution_date': '2026-01-16',
        'execution_timestamp': '2026-01-16T10:00:00Z',
        'variables': {}
    }


@pytest.fixture
def incremental_plan():
    """Sample incremental append plan"""
    return {
        'plan_metadata': {
            'plan_id': str(uuid.uuid4()),
            'plan_name': 'test_incremental',
            'version': '1.0.0'
        },
        'pattern': {'type': 'INCREMENTAL_APPEND'},
        'source': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'events_raw'
        },
        'target': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'events_processed',
            'write_mode': 'append'
        },
        'pattern_config': {
            'watermark_column': 'event_timestamp',
            'watermark_type': 'timestamp'
        }
    }


@pytest.fixture
def scd2_plan():
    """Sample SCD2 plan"""
    return {
        'plan_metadata': {
            'plan_id': str(uuid.uuid4()),
            'plan_name': 'test_scd2',
            'version': '1.0.0'
        },
        'pattern': {'type': 'SCD2'},
        'source': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'customer_current',
            'columns': ['customer_id', 'name', 'email', 'status']
        },
        'target': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'customer_dim',
            'write_mode': 'merge'
        },
        'pattern_config': {
            'business_keys': ['customer_id'],
            'effective_date_column': 'valid_from',
            'end_date_column': 'valid_to',
            'current_flag_column': 'is_current',
            'end_date_default': '9999-12-31 23:59:59',
            'compare_columns': ['name', 'email', 'status']
        }
    }


class TestIncrementalAppendPattern:
    """Test incremental append SQL generation"""
    
    def test_generate_incremental_sql(self, incremental_plan, execution_context):
        """Test basic incremental append SQL generation"""
        pattern = IncrementalAppendPattern(incremental_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Check SQL structure (with backticks around identifiers)
        assert 'INSERT INTO' in sql
        assert '`lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`events_processed`' in sql
        assert 'SELECT' in sql
        assert 'FROM' in sql
        assert '`lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`events_raw`' in sql
        assert 'WHERE `event_timestamp` >' in sql
        assert 'MAX(`event_timestamp`)' in sql
        assert 'COALESCE' in sql
    
    def test_incremental_sql_has_header(self, incremental_plan, execution_context):
        """Test SQL includes SQLPilot header"""
        pattern = IncrementalAppendPattern(incremental_plan)
        sql = pattern.generate_sql(execution_context)
        
        assert '-- LAKEHOUSE SQLPILOT GENERATED SQL' in sql
        assert f"-- plan_id: {incremental_plan['plan_metadata']['plan_id']}" in sql
        assert '-- pattern: INCREMENTAL_APPEND' in sql
    
    def test_incremental_validation(self, incremental_plan):
        """Test incremental pattern validation"""
        pattern = IncrementalAppendPattern(incremental_plan)
        errors = pattern.validate_config()
        assert len(errors) == 0
    
    def test_incremental_merge_mode(self, execution_context):
        """Test incremental append with MERGE write mode"""
        merge_plan = {
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'test_incremental_merge',
                'version': '1.0.0'
            },
            'pattern': {'type': 'INCREMENTAL_APPEND'},
            'source': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'orders_raw'
            },
            'target': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'orders_processed',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'watermark_column': 'order_date',
                'watermark_type': 'timestamp',
                'match_columns': ['order_id']
            }
        }
        
        pattern = IncrementalAppendPattern(merge_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Check MERGE syntax
        assert 'MERGE INTO' in sql
        assert 'USING' in sql
        assert 'ON target.`order_id` = source.`order_id`' in sql
        assert 'WHEN MATCHED THEN' in sql
        assert 'UPDATE SET' in sql
        assert 'WHEN NOT MATCHED THEN' in sql
        assert 'INSERT *' in sql  # Delta Lake shorthand for INSERT
        
        # Verify SELECT * (no need for explicit columns!)
        assert 'SELECT *' in sql
        assert 'FROM `lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`orders_raw`' in sql
        
        # Verify UPDATE SET * EXCEPT syntax for excluding match columns
        assert 'UPDATE SET * EXCEPT' in sql or 'UPDATE SET *' in sql
    
    def test_incremental_overwrite_mode(self, execution_context):
        """Test incremental append with OVERWRITE write mode"""
        overwrite_plan = {
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'test_incremental_overwrite',
                'version': '1.0.0'
            },
            'pattern': {'type': 'INCREMENTAL_APPEND'},
            'source': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'events_raw'
            },
            'target': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'events_processed',
                'write_mode': 'overwrite'
            },
            'pattern_config': {
                'watermark_column': 'event_timestamp',
                'watermark_type': 'timestamp'
            }
        }
        
        pattern = IncrementalAppendPattern(overwrite_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Check CREATE OR REPLACE syntax
        assert 'CREATE OR REPLACE TABLE' in sql
        assert '`lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`events_processed`' in sql
        assert 'SELECT' in sql
        assert 'WHERE `event_timestamp` >' in sql
    
    def test_incremental_merge_validation_requires_match_columns(self):
        """Test that MERGE mode requires match_columns"""
        invalid_plan = {
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'test_invalid',
                'version': '1.0.0'
            },
            'pattern': {'type': 'INCREMENTAL_APPEND'},
            'source': {
                'catalog': 'test',
                'schema': 'test',
                'table': 'test'
            },
            'target': {
                'catalog': 'test',
                'schema': 'test',
                'table': 'test',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'watermark_column': 'updated_at',
                'watermark_type': 'timestamp'
                # Missing match_columns!
            }
        }
        
        pattern = IncrementalAppendPattern(invalid_plan)
        errors = pattern.validate_config()
        assert len(errors) > 0
        assert any('match_columns' in err for err in errors)


class TestSCD2Pattern:
    """Test SCD2 SQL generation"""
    
    def test_generate_scd2_sql(self, scd2_plan, execution_context):
        """Test SCD2 SQL generation produces two steps"""
        pattern = SCD2Pattern(scd2_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Check three steps are present (new Databricks standard)
        assert '-- STEP 1: Update late-arriving records' in sql
        assert '-- STEP 2: Expire changed records' in sql
        assert '-- STEP 3: Insert new versions and new records' in sql
        
        # Check MERGE statement (Step 2)
        assert 'MERGE INTO' in sql
        assert '`lakehouse-sqlpilot`.`lakehouse-sqlpilot-schema`.`customer_dim`' in sql
        assert 'WHEN MATCHED AND' in sql
        assert 'UPDATE SET' in sql
        assert 'target.`valid_to` = CURRENT_TIMESTAMP()' in sql
        assert 'target.`is_current` = FALSE' in sql
        
        # Check INSERT statement (Step 3)
        assert 'INSERT INTO' in sql
        assert 'LEFT JOIN' in sql
        assert 'WHERE target.customer_id IS NULL' in sql
    
    def test_scd2_change_detection(self, scd2_plan, execution_context):
        """Test SCD2 generates change detection conditions using equal_null()"""
        pattern = SCD2Pattern(scd2_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Should use equal_null() for NULL-safe comparisons (new Databricks standard)
        assert 'NOT equal_null(target.`name`, source.`name`)' in sql
        assert 'NOT equal_null(target.`email`, source.`email`)' in sql
        assert 'NOT equal_null(target.`status`, source.`status`)' in sql
        assert 'OR' in sql  # Multiple conditions ORed together
    
    def test_scd2_business_key_matching(self, scd2_plan, execution_context):
        """Test SCD2 generates business key matching"""
        pattern = SCD2Pattern(scd2_plan)
        sql = pattern.generate_sql(execution_context)
        
        assert 'target.`customer_id` = source.`customer_id`' in sql
        assert 'target.`is_current` = TRUE' in sql
    
    def test_scd2_validation_success(self, scd2_plan):
        """Test valid SCD2 plan passes validation"""
        pattern = SCD2Pattern(scd2_plan)
        errors = pattern.validate_config()
        assert len(errors) == 0
    
    def test_scd2_validation_missing_business_keys(self, scd2_plan):
        """Test SCD2 validation fails without business keys"""
        del scd2_plan['pattern_config']['business_keys']
        pattern = SCD2Pattern(scd2_plan)
        errors = pattern.validate_config()
        assert len(errors) > 0
        assert any('business_keys' in err.lower() for err in errors)
    
    def test_scd2_validation_wrong_write_mode(self, scd2_plan):
        """Test SCD2 validation fails with wrong write mode"""
        scd2_plan['target']['write_mode'] = 'append'
        pattern = SCD2Pattern(scd2_plan)
        errors = pattern.validate_config()
        assert len(errors) > 0
        assert any('merge' in err.lower() for err in errors)


class TestMergeUpsertPattern:
    """Test merge/upsert SQL generation"""
    
    @pytest.fixture
    def merge_plan(self):
        """Sample merge upsert plan"""
        return {
            'plan_metadata': {
                'plan_id': str(uuid.uuid4()),
                'plan_name': 'test_merge',
                'version': '1.0.0'
            },
            'pattern': {'type': 'MERGE_UPSERT'},
            'source': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'customer_updates',
                'columns': ['customer_id', 'name', 'email']
            },
            'target': {
                'catalog': 'lakehouse-sqlpilot',
                'schema': 'lakehouse-sqlpilot-schema',
                'table': 'customers',
                'write_mode': 'merge'
            },
            'pattern_config': {
                'merge_keys': ['customer_id'],
                'update_columns': ['name', 'email']
            }
        }
    
    def test_generate_merge_sql(self, merge_plan, execution_context):
        """Test merge/upsert SQL generation"""
        pattern = MergeUpsertPattern(merge_plan)
        sql = pattern.generate_sql(execution_context)
        
        assert 'MERGE INTO' in sql
        assert 'USING' in sql
        assert 'ON target.`customer_id` = source.`customer_id`' in sql
        assert 'WHEN MATCHED THEN' in sql
        assert 'UPDATE SET' in sql
        assert 'WHEN NOT MATCHED THEN' in sql
        assert 'INSERT' in sql


@pytest.fixture
def snapshot_plan():
    """Sample Snapshot plan"""
    return {
        'plan_metadata': {
            'plan_id': str(uuid.uuid4()),
            'plan_name': 'test_snapshot',
            'version': '1.0.0'
        },
        'pattern': {'type': 'SNAPSHOT'},
        'source': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'inventory_current'
        },
        'target': {
            'catalog': 'lakehouse-sqlpilot',
            'schema': 'lakehouse-sqlpilot-schema',
            'table': 'inventory_snapshots',
            'write_mode': 'append'
        },
        'pattern_config': {
            'snapshot_date_column': 'snapshot_date',
            'partition_columns': ['snapshot_date']
        }
    }


class TestSnapshotPattern:
    """Test Snapshot pattern SQL generation"""
    
    def test_generate_snapshot_sql(self, snapshot_plan, execution_context):
        """Test basic snapshot SQL generation"""
        pattern = SnapshotPattern(snapshot_plan)
        sql = pattern.generate_sql(execution_context)
        
        # Check SQL structure
        assert 'INSERT INTO' in sql
        assert 'snapshot_date' in sql
        assert 'inventory_current' in sql
        assert 'inventory_snapshots' in sql
        assert 'SELECT' in sql
        print(f"Generated SQL:\n{sql}")
    
    def test_snapshot_with_custom_date(self, snapshot_plan):
        """Test snapshot with custom snapshot date"""
        pattern = SnapshotPattern(snapshot_plan)
        context = {
            'execution_id': str(uuid.uuid4()),
            'snapshot_date': "'2026-01-15'"  # Custom date
        }
        sql = pattern.generate_sql(context)
        
        assert "'2026-01-15'" in sql
        assert 'snapshot_date' in sql
    
    def test_snapshot_validation(self):
        """Test snapshot pattern validation"""
        invalid_plan = {
            'plan_metadata': {'plan_id': str(uuid.uuid4())},
            'pattern': {'type': 'SNAPSHOT'},
            'source': {'catalog': 'c', 'schema': 's', 'table': 't'},
            'target': {'catalog': 'c', 'schema': 's', 'table': 't'},
            'pattern_config': {}  # Missing snapshot_date_column
        }
        pattern = SnapshotPattern(invalid_plan)
        errors = pattern.validate_config()
        
        assert len(errors) > 0
        assert 'snapshot_date_column' in errors[0]


class TestPatternFactory:
    """Test pattern factory"""
    
    def test_create_incremental_pattern(self, incremental_plan):
        """Test factory creates incremental pattern"""
        pattern = PatternFactory.create_pattern(incremental_plan)
        assert isinstance(pattern, IncrementalAppendPattern)
    
    def test_create_scd2_pattern(self, scd2_plan):
        """Test factory creates SCD2 pattern"""
        pattern = PatternFactory.create_pattern(scd2_plan)
        assert isinstance(pattern, SCD2Pattern)
    
    def test_create_invalid_pattern(self):
        """Test factory raises error for invalid pattern"""
        invalid_plan = {
            'pattern': {'type': 'INVALID_PATTERN'},
            'plan_metadata': {}
        }
        with pytest.raises(ValueError):
            PatternFactory.create_pattern(invalid_plan)
    
    def test_get_supported_patterns(self):
        """Test factory returns supported patterns"""
        patterns = PatternFactory.get_supported_patterns()
        assert 'INCREMENTAL_APPEND' in patterns
        assert 'SCD2' in patterns
        assert 'MERGE_UPSERT' in patterns
        assert 'FULL_REPLACE' in patterns
        assert 'SNAPSHOT' in patterns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

