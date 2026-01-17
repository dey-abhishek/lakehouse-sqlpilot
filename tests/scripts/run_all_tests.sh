#!/bin/bash
# Run all tests with correct environment configuration

echo "========================================="
echo "Running All Tests with Security Enabled"
echo "========================================="

# Set environment variables for testing
export SQLPILOT_REQUIRE_AUTH=false
export SQLPILOT_RATE_LIMIT_ENABLED=true
export SQLPILOT_AUDIT_LOG_ENABLED=true

# Run backend tests
echo ""
echo "1. Backend Core Tests..."
python -m pytest tests/test_validation.py tests/test_pattern.py tests/test_compiler.py tests/test_integration.py -v --tb=short

# Run security tests
echo ""
echo "2. Security Tests..."
python -m pytest tests/test_security.py -v --tb=short

# Run API integration tests
echo ""
echo "3. API Integration Tests..."
python -m pytest tests/test_api_integration_simple.py tests/test_api_execution.py tests/test_api_security_integration.py -v --tb=short

# Run frontend tests
echo ""
echo "4. Frontend Tests..."
cd ui/plan-editor && npm test -- --run

echo ""
echo "========================================="
echo "All Tests Complete!"
echo "========================================="
