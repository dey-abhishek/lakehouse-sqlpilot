"""
Test Suite for Plan Schema Validation

Tests the JSON schema validation for all pattern types.
"""

import pytest
import json
import yaml
from datetime import datetime, timezone
import uuid

from plan_schema.v1.validator import PlanValidator, PlanValidationError


@pytest.fixture
def schema_path():
    """Path to plan schema"""
    return "plan-schema/v1/plan.schema.json"


@pytest.fixture
def validator(schema_path):
    """Plan validator instance"""
    return PlanValidator(schema_path)


@pytest.fixture
def base_plan():
    """Base valid plan for testing"""
    return {
        "schema_version": "1.0",
        "plan_metadata": {
            "plan_id": str(uuid.uuid4()),
            "plan_name": "test_plan",
            "description": "Test plan",
            "owner": "test@databricks.com",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0"
        },
        "pattern": {
            "type": "INCREMENTAL_APPEND"
        },
        "source": {
            "catalog": "lakehouse-sqlpilot",
            "schema": "lakehouse-sqlpilot-schema",
            "table": "test_source"
        },
        "target": {
            "catalog": "lakehouse-sqlpilot",
            "schema": "lakehouse-sqlpilot-schema",
            "table": "test_target",
            "write_mode": "append"
        },
        "pattern_config": {
            "watermark_column": "created_at",
            "watermark_type": "timestamp"
        },
        "execution_config": {
            "warehouse_id": "test_warehouse"
        }
    }


class TestSchemaValidation:
    """Test JSON schema validation"""
    
    def test_valid_incremental_append_plan(self, validator, base_plan):
        """Test valid incremental append plan passes validation"""
        is_valid, errors = validator.validate_plan(base_plan)
        assert is_valid, f"Validation failed: {errors}"
        assert len(errors) == 0
    
    def test_missing_required_field(self, validator, base_plan):
        """Test missing required field fails validation"""
        del base_plan["pattern"]
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert len(errors) > 0
        assert any("pattern" in err.lower() for err in errors)
    
    def test_invalid_plan_name_format(self, validator, base_plan):
        """Test invalid plan name format fails validation"""
        base_plan["plan_metadata"]["plan_name"] = "Invalid Name With Spaces"
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("plan_name" in err.lower() for err in errors)
    
    def test_invalid_email_format(self, validator, base_plan):
        """Test invalid email format fails validation"""
        base_plan["plan_metadata"]["owner"] = "not-an-email"
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("owner" in err.lower() or "email" in err.lower() for err in errors)
    
    def test_invalid_version_format(self, validator, base_plan):
        """Test invalid semantic version fails validation"""
        base_plan["plan_metadata"]["version"] = "v1.0"  # Should be "1.0.0"
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("version" in err.lower() for err in errors)


class TestSCD2Validation:
    """Test SCD2-specific validation"""
    
    @pytest.fixture
    def scd2_plan(self, base_plan):
        """Valid SCD2 plan"""
        plan = base_plan.copy()
        plan["pattern"]["type"] = "SCD2"
        plan["source"]["columns"] = ["customer_id", "name", "email"]
        plan["target"]["write_mode"] = "merge"
        plan["pattern_config"] = {
            "business_keys": ["customer_id"],
            "effective_date_column": "valid_from",
            "end_date_column": "valid_to",
            "current_flag_column": "is_current"
        }
        return plan
    
    def test_valid_scd2_plan(self, validator, scd2_plan):
        """Test valid SCD2 plan passes validation"""
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert is_valid, f"Validation failed: {errors}"
    
    def test_scd2_missing_business_keys(self, validator, scd2_plan):
        """Test SCD2 without business keys fails"""
        del scd2_plan["pattern_config"]["business_keys"]
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert not is_valid
        assert any("business_keys" in err.lower() for err in errors)
    
    def test_scd2_missing_columns(self, validator, scd2_plan):
        """Test SCD2 without explicit columns fails"""
        del scd2_plan["source"]["columns"]
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert not is_valid
        assert any("column" in err.lower() for err in errors)
    
    def test_scd2_wrong_write_mode(self, validator, scd2_plan):
        """Test SCD2 with wrong write mode fails"""
        scd2_plan["target"]["write_mode"] = "append"
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert not is_valid
        assert any("write_mode" in err.lower() or "merge" in err.lower() for err in errors)
    
    def test_scd2_business_key_not_in_columns(self, validator, scd2_plan):
        """Test SCD2 with business key not in columns fails"""
        scd2_plan["pattern_config"]["business_keys"] = ["nonexistent_column"]
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert not is_valid
        assert any("business_key" in err.lower() or "column" in err.lower() for err in errors)
    
    def test_scd2_scd_column_in_source(self, validator, scd2_plan):
        """Test SCD2 with SCD metadata column in source fails"""
        scd2_plan["source"]["columns"].append("valid_from")
        is_valid, errors = validator.validate_plan(scd2_plan)
        assert not is_valid
        assert any("valid_from" in err.lower() or "scd" in err.lower() for err in errors)


class TestSemanticValidation:
    """Test semantic validation rules"""
    
    def test_source_target_same_for_non_merge(self, validator, base_plan):
        """Test source and target cannot be identical for non-merge patterns"""
        base_plan["target"]["catalog"] = base_plan["source"]["catalog"]
        base_plan["target"]["schema"] = base_plan["source"]["schema"]
        base_plan["target"]["table"] = base_plan["source"]["table"]
        
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("identical" in err.lower() or "source" in err.lower() for err in errors)
    
    def test_duplicate_partition_columns(self, validator, base_plan):
        """Test duplicate partition columns fails validation"""
        base_plan["target"]["partition_by"] = ["date", "date"]
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("duplicate" in err.lower() or "partition" in err.lower() for err in errors)
    
    def test_cron_without_expression(self, validator, base_plan):
        """Test cron schedule without expression fails"""
        base_plan["schedule"] = {"type": "cron"}
        is_valid, errors = validator.validate_plan(base_plan)
        assert not is_valid
        assert any("cron" in err.lower() or "expression" in err.lower() for err in errors)


class TestYAMLPlanLoading:
    """Test loading plans from YAML files"""
    
    def test_load_valid_yaml_plan(self, validator, base_plan, tmp_path):
        """Test loading valid YAML plan"""
        plan_file = tmp_path / "test_plan.yaml"
        with open(plan_file, 'w') as f:
            yaml.dump(base_plan, f)
        
        from plan_schema.v1.validator import load_and_validate_plan
        loaded_plan = load_and_validate_plan(str(plan_file), validator.schema)
        
        assert loaded_plan["plan_metadata"]["plan_name"] == "test_plan"
    
    def test_load_invalid_yaml_plan(self, validator, base_plan, tmp_path):
        """Test loading invalid YAML plan raises error"""
        del base_plan["pattern"]
        plan_file = tmp_path / "invalid_plan.yaml"
        with open(plan_file, 'w') as f:
            yaml.dump(base_plan, f)
        
        from plan_schema.v1.validator import load_and_validate_plan
        
        with pytest.raises(PlanValidationError):
            load_and_validate_plan(str(plan_file), validator.schema)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

