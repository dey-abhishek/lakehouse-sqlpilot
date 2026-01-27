"""
Test all 12 Full Replace scenarios
"""
import pytest
from compiler.patterns.full_replace import FullReplacePattern


@pytest.fixture
def execution_context():
    """Fixture providing execution context"""
    return {
        'warehouse_id': 'test-warehouse',
        'executor_user': 'test@example.com'
    }


def create_pattern_with_tables(pattern_config, source_table, target_table):
    """Helper to create pattern with source/target tables"""
    # Parse table FQNs
    source_parts = source_table.split('.')
    target_parts = target_table.split('.')
    
    # Create full plan structure
    plan = {
        'plan_metadata': {},
        'pattern': {'type': 'FULL_REPLACE'},
        'source': {
            'catalog': source_parts[0],
            'schema': source_parts[1],
            'table': source_parts[2]
        },
        'target': {
            'catalog': target_parts[0],
            'schema': target_parts[1],
            'table': target_parts[2]
        },
        'pattern_config': pattern_config
    }
    
    pattern = FullReplacePattern(plan)
    
    return pattern


class TestFullReplaceAllScenarios:
    """Test all 12 scenarios from the complete matrix"""
    
    # ============================================================================
    # DIRECT TABLE REPLACEMENT (ALTER TABLE) - Scenarios 1-4
    # ============================================================================
    
    def test_scenario_1_delta_to_uniform_v2(self, execution_context):
        """Scenario 1: Delta → Delta + UniForm v2 (ALTER TABLE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': True,
                'table_format': 'delta',
                'enable_uniform': True,  # Enable UniForm
                'iceberg_version': '2',  # UniForm v2
            },
            'catalog.schema.my_table',
            'catalog.schema.my_table'  # Same table
        )
        
        sql = pattern.generate_sql(execution_context)
        
        # Should use ALTER TABLE for UniForm v2
        assert 'ALTER TABLE' in sql
        assert 'delta.universalFormat.enabledFormats' in sql
        assert 'delta.enableIcebergCompatV2' in sql
        assert 'delta.columnMapping.mode' in sql
        # Should NOT have v3 properties
        assert 'delta.enableIcebergCompatV3' not in sql
        assert 'delta.enableRowTracking' not in sql
    
    def test_scenario_2_delta_to_uniform_v3(self, execution_context):
        """Scenario 2: Delta → Delta + UniForm v3 (ALTER TABLE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': True,
                'table_format': 'delta',
                'enable_uniform': True,  # Enable UniForm
                'iceberg_version': '3',  # UniForm v3
            },
            'catalog.schema.my_table',
            'catalog.schema.my_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        # Should use ALTER TABLE for UniForm v3
        assert 'ALTER TABLE' in sql
        assert 'delta.universalFormat.enabledFormats' in sql
        assert 'delta.enableIcebergCompatV3' in sql
        assert 'delta.enableIcebergCompatV2' in sql  # Should be 'false'
        assert 'delta.columnMapping.mode' in sql
        assert 'delta.enableRowTracking' in sql
        assert 'delta.enableDeletionVectors' in sql
    
    def test_scenario_3_uniform_v2_to_v3(self, execution_context):
        """Scenario 3: Delta + UniForm v2 → v3 (ALTER TABLE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': True,
                'table_format': 'delta',
                'enable_uniform': True,  # Enable UniForm
                'iceberg_version': '3',  # Upgrade to v3
            },
            'catalog.schema.uniform_table',
            'catalog.schema.uniform_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        # Should use ALTER TABLE to upgrade
        assert 'ALTER TABLE' in sql
        assert 'delta.enableIcebergCompatV3' in sql
        assert "'false'" in sql.lower()  # V2 should be disabled
    
    def test_scenario_4_iceberg_v2_to_v3(self, execution_context):
        """Scenario 4: Managed Iceberg v2 → v3 (ALTER TABLE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': True,
                'table_format': 'iceberg',
                'iceberg_version': '3',  # Upgrade to v3
            },
            'catalog.schema.iceberg_table',
            'catalog.schema.iceberg_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        # Should use ALTER TABLE
        assert 'ALTER TABLE' in sql
        assert 'format-version' in sql
        assert "'3'" in sql
    
    # ============================================================================
    # STAGING TABLE (DROP + CREATE) - Scenarios 5-12
    # ============================================================================
    
    def test_scenario_5_delta_to_iceberg_v2(self, execution_context):
        """Scenario 5: Delta → Managed Iceberg v2 (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'iceberg',
                'iceberg_version': '2',
            },
            'catalog.schema.delta_table',
            'catalog.schema.iceberg_table'  # Different table
        )
        
        sql = pattern.generate_sql(execution_context)
        
        # Should use DROP + CREATE for format conversion
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'CREATE TABLE' in sql
        assert 'USING ICEBERG' in sql
        assert 'format-version' in sql
        assert "'2'" in sql
        assert 'SELECT *' in sql
        assert 'FROM' in sql
    
    def test_scenario_6_delta_to_iceberg_v3(self, execution_context):
        """Scenario 6: Delta → Managed Iceberg v3 (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'iceberg',
                'iceberg_version': '3',
            },
            'catalog.schema.delta_table',
            'catalog.schema.iceberg_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING ICEBERG' in sql
        assert "'3'" in sql
    
    def test_scenario_7_iceberg_v2_to_delta(self, execution_context):
        """Scenario 7: Managed Iceberg v2 → Delta (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'delta',
                'enable_uniform': False,  # Standard Delta (no UniForm)
                'iceberg_version': '2',
            },
            'catalog.schema.iceberg_table',
            'catalog.schema.delta_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING DELTA' in sql
        # Should NOT have UniForm properties (standard Delta)
        assert 'universalFormat' not in sql
    
    def test_scenario_8_iceberg_v3_to_delta(self, execution_context):
        """Scenario 8: Managed Iceberg v3 → Delta (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'delta',
                'enable_uniform': False,  # Standard Delta (no UniForm)
                'iceberg_version': '2',
            },
            'catalog.schema.iceberg_v3_table',
            'catalog.schema.delta_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING DELTA' in sql
    
    def test_scenario_9_uniform_to_pure_delta(self, execution_context):
        """Scenario 9: Delta + UniForm → Pure Delta (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'delta',
                'enable_uniform': False,  # Standard Delta (no UniForm)
                'iceberg_version': '2',
            },
            'catalog.schema.uniform_table',
            'catalog.schema.pure_delta_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING DELTA' in sql
        # Should NOT have UniForm properties
        assert 'universalFormat' not in sql
    
    def test_scenario_10_uniform_v2_to_iceberg_v2(self, execution_context):
        """Scenario 10: Delta + UniForm v2 → Managed Iceberg v2 (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'iceberg',
                'iceberg_version': '2',
            },
            'catalog.schema.uniform_v2_table',
            'catalog.schema.iceberg_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING ICEBERG' in sql
        assert "'2'" in sql
    
    def test_scenario_11_uniform_v3_to_iceberg_v3(self, execution_context):
        """Scenario 11: Delta + UniForm v3 → Managed Iceberg v3 (DROP + CREATE)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'iceberg',
                'iceberg_version': '3',
            },
            'catalog.schema.uniform_v3_table',
            'catalog.schema.iceberg_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING ICEBERG' in sql
        assert "'3'" in sql
    
    def test_scenario_12_uniform_v2_to_iceberg_v3(self, execution_context):
        """Scenario 12: Delta + UniForm v2 → Managed Iceberg v3 (DROP + CREATE with upgrade)"""
        pattern = create_pattern_with_tables(
            {
                'refresh_mode': 'direct',
                'refresh_inplace': False,
                'table_format': 'iceberg',
                'iceberg_version': '3',  # Upgrade to v3 during conversion
            },
            'catalog.schema.uniform_v2_table',
            'catalog.schema.iceberg_table'
        )
        
        sql = pattern.generate_sql(execution_context)
        
        assert 'DROP TABLE IF EXISTS' in sql
        assert 'USING ICEBERG' in sql
        assert "'3'" in sql  # Should create as v3, not v2
