# Tests Directory

Comprehensive test suite for Lakehouse SQLPilot.

## 📁 Directory Structure

```
tests/
├── scripts/              # Test and setup scripts
│   ├── run_all_tests.sh
│   ├── run_integration_tests.sh
│   ├── setup_test_env.sh
│   ├── verify_lakebase.sh
│   ├── check_env.py
│   └── validate_flagship.py
├── fixtures/             # Test data and setup
│   ├── setup_uat_test_data.py
│   ├── setup_uat_test_data.sql
│   └── setup_uat_test_data_CTAS.sql
├── integration/          # Integration tests
│   ├── test_scd2_integration.py
│   └── setup_scd2_tables.sql
├── test_*.py            # Main test files
├── conftest.py          # Pytest configuration
└── databricks_rest_api.py  # Test utilities

## 🧪 Test Categories

### Unit Tests (No Infrastructure Required)
- `test_compiler.py` - SQL compiler logic
- `test_pattern_generation.py` - Pattern SQL generation
- `test_plan_validation.py` - JSON schema validation
- `test_security.py` - Security functions
- `test_circuit_breaker.py` - Circuit breaker (32 tests) ✅ 100%
- `test_lakebase_backend.py` - Lakebase backend (37 tests) ✅ 100%

### API Tests (Mock Authentication)
- `test_api_integration.py` - API endpoints
- `test_api_integration_simple.py` - Basic API tests
- `test_api_security_integration.py` - Security integration
- `test_api_execution.py` - Execution endpoints

### Integration Tests (Require Databricks)
- `test_integration_e2e.py` - End-to-end workflows
- `test_source_table_validation.py` - Table validation
- `integration/test_scd2_integration.py` - SCD2 on real warehouse

### UAT Tests (Require Databricks SQL Warehouse)
- `test_uat_end_to_end.py` - User acceptance testing
- Requires: DATABRICKS_HOST, DATABRICKS_TOKEN, WAREHOUSE_ID

### Scalability Tests
- `test_scalability_integration.py` - Lakebase + Circuit Breaker (24 tests)

## 🚀 Running Tests

### Quick Start (All Tests)
```bash
# From project root
cd tests/scripts
./run_all_tests.sh
```

### Unit Tests Only (Fast)
```bash
pytest tests/test_compiler.py tests/test_pattern_generation.py -v
```

### Lakebase & Circuit Breaker Tests
```bash
pytest tests/test_lakebase_backend.py tests/test_circuit_breaker.py -v
# 69 tests, 100% passing
```

### Integration Tests (Requires Credentials)
```bash
# Setup credentials first
cp env.example .env
# Edit .env with your credentials

# Run integration tests
cd tests/scripts
./run_integration_tests.sh
```

### UAT Tests (Requires Databricks Warehouse)
```bash
# Requires .env with DATABRICKS_* credentials
pytest tests/test_uat_end_to_end.py -v -m requires_databricks
```

## 📋 Test Scripts

### `scripts/run_all_tests.sh`
Runs complete test suite (188+ tests)
```bash
cd tests/scripts
./run_all_tests.sh
```

### `scripts/run_integration_tests.sh`
Runs only integration tests requiring Databricks
```bash
cd tests/scripts
./run_integration_tests.sh
```

### `scripts/setup_test_env.sh`
Sets up test environment and verifies configuration
```bash
cd tests/scripts
./setup_test_env.sh
```

### `scripts/verify_lakebase.sh`
Verifies Lakebase PostgreSQL connection
```bash
cd tests/scripts
./verify_lakebase.sh
```

### `scripts/check_env.py`
Validates environment variables and credentials
```bash
cd tests/scripts
python check_env.py
```

### `scripts/validate_flagship.py`
Validates flagship plan examples
```bash
cd tests/scripts
python validate_flagship.py
```

## 🔧 Test Fixtures

### `fixtures/setup_uat_test_data.py`
Python script to setup UAT test data
```bash
python tests/fixtures/setup_uat_test_data.py
```

### `fixtures/setup_uat_test_data.sql`
SQL script for UAT test tables and data
```bash
# Run via Databricks SQL Editor or dbsqlcli
```

### `fixtures/setup_uat_test_data_CTAS.sql`
CTAS (Create Table As Select) version for UAT setup
```bash
# Preferred for clean test environments
```

## 📊 Test Coverage

| Category | Tests | Pass Rate | Requires Credentials |
|----------|-------|-----------|---------------------|
| **Unit Tests** | 50+ | 100% | ❌ No |
| **API Tests** | 40+ | 95%+ | ❌ No |
| **Security** | 20+ | 95%+ | ❌ No |
| **Lakebase** | 37 | 100% | ✅ Yes (mocked) |
| **Circuit Breaker** | 32 | 100% | ❌ No |
| **Scalability** | 24 | 85%+ | ✅ Yes (mocked) |
| **Integration** | 10+ | 90%+ | ✅ Yes (Databricks) |
| **UAT** | 10+ | 80%+ | ✅ Yes (Databricks) |
| **TOTAL** | **188+** | **~92%** | Mixed |

## 🔒 Testing with Credentials

### Setup `.env` File
```bash
# Copy template
cp env.example .env

# Edit with your credentials
DATABRICKS_HOST=your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
DATABRICKS_SQL_WAREHOUSE_ID=your-warehouse-id
LAKEBASE_HOST=your-lakebase-host
LAKEBASE_USER=your-user
LAKEBASE_PASSWORD=your-password
```

### Run Tests Requiring Credentials
```bash
# With .env configured
pytest tests/test_uat_end_to_end.py -v
pytest tests/integration/ -v
```

## 🎯 CI/CD

### GitHub Actions
GitHub Actions runs **only tests without credentials**:
- Python syntax check
- Unit tests (compiler, patterns)
- Frontend build check

See: `.github/workflows/test.yml`

### Local Full Testing
For complete testing before deployment:
```bash
# 1. Setup environment
cd tests/scripts
./setup_test_env.sh

# 2. Run all tests
./run_all_tests.sh

# 3. Check results
pytest --cov=. --cov-report=term-missing
```

## 📚 Test Utilities

### `conftest.py`
Pytest configuration and shared fixtures
- Mock workspace client
- Mock authentication
- Common test data

### `databricks_rest_api.py`
Databricks REST API client for tests
- Statement execution API
- Async statement handling
- Used in UAT tests

## 🐛 Debugging Tests

### Run Specific Test
```bash
pytest tests/test_compiler.py::TestCompiler::test_scd2_compilation -v
```

### Run with Debug Output
```bash
pytest tests/test_uat_end_to_end.py -v -s --tb=long
```

### Run Failed Tests Only
```bash
pytest --lf -v  # Last failed
pytest --ff -v  # Failed first
```

### Skip Slow Tests
```bash
pytest -m "not slow" -v
```

## ✅ Test Best Practices

1. **Unit tests** don't need credentials
2. **Integration tests** use `@pytest.mark.requires_databricks`
3. **Flaky tests** are marked with `@pytest.mark.flaky`
4. **Mock extensively** to avoid external dependencies
5. **Clean up** test data after runs

## 📖 More Information

- **Lakebase Testing**: See `LAKEBASE_VERIFICATION_REPORT.md`
- **Scalability Testing**: See `LAKEBASE_SCALABILITY_COMPLETE.md`
- **Security Testing**: See `SECURITY_IMPLEMENTATION_COMPLETE.md`
- **API Testing**: See individual test files for examples

---

**Test Coverage**: 188+ tests with ~92% pass rate 🎉
