# Test Suite for Lakehouse SQLPilot

Comprehensive testing for backend (Python) and frontend (TypeScript/React).

## Test Structure

```
tests/
├── test_plan_validation.py      # Plan schema validation tests
├── test_pattern_generation.py   # SQL pattern generation tests  
├── test_compiler.py              # SQL compiler and guardrails tests
├── test_integration_e2e.py       # End-to-end backend integration tests
└── conftest.py                   # Pytest configuration

ui/plan-editor/src/tests/
├── components.test.tsx           # React component unit tests
├── e2e.spec.ts                   # Playwright end-to-end UI tests
└── setup.ts                      # Test setup
```

## Backend Tests (Python)

### Setup

```bash
# Activate virtual environment
source sqlpilot/bin/activate

# Install test dependencies
pip install -r requirements-test.txt

# Or install specific packages
pip install pytest pytest-cov pytest-asyncio pytest-mock responses
```

### Running Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_plan_validation.py

# Run specific test class
pytest tests/test_plan_validation.py::TestSCD2Validation

# Run specific test
pytest tests/test_plan_validation.py::TestSCD2Validation::test_valid_scd2_plan

# Run with coverage
pytest --cov=. --cov-report=html

# Run in parallel (faster)
pytest -n auto
```

### Test Categories

#### 1. Plan Validation Tests (`test_plan_validation.py`)
- JSON schema validation
- Semantic validation rules
- Pattern-specific validation
- SCD2 validation (flagship pattern)
- YAML plan loading

**Key Tests:**
- `test_valid_incremental_append_plan` - Valid plan passes
- `test_scd2_business_key_not_in_columns` - SCD2 validation
- `test_source_target_same_for_non_merge` - Semantic rules

#### 2. Pattern Generation Tests (`test_pattern_generation.py`)
- SQL generation for all patterns
- SCD2 two-step SQL generation
- Change detection logic
- Business key matching
- Pattern validation

**Key Tests:**
- `test_generate_scd2_sql` - SCD2 generates both steps
- `test_scd2_change_detection` - Compares all columns
- `test_incremental_sql_has_header` - SQLPilot header present

#### 3. Compiler Tests (`test_compiler.py`)
- End-to-end compilation
- SQL guardrails enforcement
- Deterministic compilation
- Preview generation

**Key Tests:**
- `test_compile_valid_plan` - Successful compilation
- `test_drop_table_blocked` - Guardrails block DROP
- `test_same_plan_same_sql` - Determinism

#### 4. Integration Tests (`test_integration_e2e.py`)
- Complete workflows
- Agent integration
- Permission validation
- Execution tracking
- SCD2 end-to-end

**Key Tests:**
- `test_plan_to_sql_to_preview_flow` - Complete flow
- `test_plan_execution_with_tracking` - With state tracking
- `test_scd2_compilation_and_validation` - SCD2 E2E

### Test Data

Tests use mocked data and don't require actual Databricks connections. Key fixtures:

- `mock_workspace_client` - Mocked Databricks SDK client
- `base_plan` - Valid plan template
- `scd2_plan` - SCD2 plan template
- `execution_context` - Execution context template

### Coverage Goals

- **Plan Validation**: >95% coverage
- **Pattern Generation**: >90% coverage
- **Compiler**: >85% coverage
- **Integration**: >75% coverage

## Frontend Tests (React/TypeScript)

### Setup

```bash
cd ui/plan-editor

# Install dependencies
npm install

# Install test dependencies
npm install --save-dev @testing-library/react @testing-library/jest-dom \
  @testing-library/user-event @playwright/test
```

### Running Tests

```bash
# Run unit tests (Jest + React Testing Library)
npm test

# Run with coverage
npm test -- --coverage

# Run in watch mode
npm test -- --watch

# Run E2E tests (Playwright)
npx playwright test

# Run E2E in headed mode (see browser)
npx playwright test --headed

# Run E2E in debug mode
npx playwright test --debug

# Run specific E2E test
npx playwright test tests/e2e.spec.ts
```

### Test Categories

#### 1. Component Unit Tests (`components.test.tsx`)
- React component rendering
- Form validation
- User interactions
- Accessibility

**Key Tests:**
- `test('renders plan list page')` - Plan list renders
- `test('shows governance message')` - Security message shown
- `test('validates plan name format')` - Form validation
- `test('form labels are properly associated')` - Accessibility

#### 2. End-to-End UI Tests (`e2e.spec.ts`)
- Complete user workflows
- Plan creation flow
- Preview workflow
- Execution dashboard
- Navigation
- Security checks

**Key Tests:**
- `test('complete plan creation flow')` - Full plan creation
- `test('SCD2 plan creation with all fields')` - SCD2 workflow
- `test('preview shows generated SQL')` - Preview display
- `test('no SQL editor is present')` - Security check

### E2E Test Coverage

- ✅ Plan creation (all patterns)
- ✅ SCD2 plan creation with all fields
- ✅ Preview generation and display
- ✅ SQL is read-only (no user editing)
- ✅ Validation errors shown correctly
- ✅ Navigation between pages
- ✅ Execution dashboard
- ✅ Responsive design (mobile/tablet)

## Running All Tests

### Backend Only
```bash
pytest -v --cov=. --cov-report=term-missing
```

### Frontend Only
```bash
cd ui/plan-editor
npm test && npx playwright test
```

### Full Suite
```bash
# Backend
pytest -v

# Frontend
cd ui/plan-editor && npm test && npx playwright test
```

## Continuous Integration

### GitHub Actions Workflow

Tests run automatically on:
- Every pull request
- Every push to main
- Scheduled daily runs

### CI Configuration

```yaml
# .github/workflows/test.yml
name: Test Suite

on: [push, pull_request]

jobs:
  backend-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - run: |
          python -m venv sqlpilot
          source sqlpilot/bin/activate
          pip install -r requirements.txt
          pip install -r requirements-test.txt
          pytest --cov=. --cov-report=xml
      - uses: codecov/codecov-action@v3
  
  frontend-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-node@v3
        with:
          node-version: '18'
      - run: |
          cd ui/plan-editor
          npm ci
          npm test -- --coverage
          npx playwright install --with-deps
          npx playwright test
```

## Test Best Practices

### Backend (Python)

1. **Use fixtures** for common test data
2. **Mock external dependencies** (Databricks SDK, SQL connections)
3. **Test both success and failure paths**
4. **Validate error messages** are helpful
5. **Test idempotency** of SQL operations
6. **Check SQL output** for security issues

### Frontend (TypeScript)

1. **Test user workflows**, not implementation
2. **Use semantic queries** (`getByRole`, `getByLabelText`)
3. **Test accessibility** (ARIA, keyboard navigation)
4. **Mock API calls** with MSW or similar
5. **Test responsive behavior** at different viewports
6. **Verify security** (no SQL editors, read-only display)

## Security Testing

### What We Test

- ✅ No free-form SQL input
- ✅ SQL guardrails (block DROP, TRUNCATE, etc.)
- ✅ Permission validation before execution
- ✅ SQL is read-only in preview
- ✅ Credential handling (no hardcoded secrets)
- ✅ Input validation (email, plan names, etc.)

### Security Test Examples

```python
# Guardrails test
def test_drop_table_blocked(guardrails):
    sql = "DROP TABLE test;"
    is_valid, violations = guardrails.validate_sql(sql)
    assert not is_valid
    assert any('drop' in v.lower() for v in violations)
```

```typescript
// No SQL editor test
test('no SQL editor is present', async ({ page }) => {
  await page.goto('/plans/new');
  await expect(page.locator('textarea[name="sql"]')).not.toBeVisible();
});
```

## Debugging Tests

### Backend

```bash
# Run with debugger
pytest --pdb

# Run with print statements visible
pytest -s

# Run last failed tests only
pytest --lf

# Run most recently changed tests
pytest --ff
```

### Frontend

```bash
# Debug E2E tests
npx playwright test --debug

# Show browser during test
npx playwright test --headed

# Slow down execution
npx playwright test --headed --slow-mo=1000
```

## Test Reports

### Coverage Reports

```bash
# Backend
pytest --cov=. --cov-report=html
open htmlcov/index.html

# Frontend
npm test -- --coverage
open ui/plan-editor/coverage/lcov-report/index.html
```

### E2E Test Reports

```bash
# Generate HTML report
npx playwright test --reporter=html

# Open report
npx playwright show-report
```

## Known Issues & Limitations

1. **Mock Data**: Tests use mocked Databricks connections
2. **No Real Execution**: SQL is not actually executed against databases
3. **Timing**: E2E tests may be flaky due to timing issues
4. **Browser Compatibility**: E2E tests run on Chromium by default

## Contributing

When adding new features:

1. Write tests FIRST (TDD)
2. Ensure all tests pass
3. Maintain >80% coverage
4. Add E2E tests for new user workflows
5. Update this README with new test categories

## Getting Help

- Run `pytest --help` for pytest options
- Run `npx playwright test --help` for Playwright options
- See [pytest documentation](https://docs.pytest.org/)
- See [Playwright documentation](https://playwright.dev/)
- See [React Testing Library docs](https://testing-library.com/react)

