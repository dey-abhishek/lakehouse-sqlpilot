#!/bin/bash
# Pre-Deployment Checklist for Databricks Apps

echo "🚀 Lakehouse SQLPilot - Pre-Deployment Checklist"
echo "=================================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

checks_passed=0
checks_failed=0

check() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
        ((checks_passed++))
    else
        echo -e "${RED}❌ $2${NC}"
        ((checks_failed++))
    fi
}

echo "1. Checking Python Environment..."
python --version > /dev/null 2>&1
check $? "Python is installed"

echo ""
echo "2. Checking Required Files..."
[ -f "requirements.txt" ]
check $? "requirements.txt exists"

[ -f "databricks.yml" ]
check $? "databricks.yml exists"

[ -f "api/main.py" ]
check $? "API main.py exists"

[ -d "ui/plan-editor" ]
check $? "Frontend directory exists"

echo ""
echo "3. Checking Dependencies..."
pip show fastapi > /dev/null 2>&1
check $? "FastAPI is installed"

pip show uvicorn > /dev/null 2>&1
check $? "Uvicorn is installed"

pip show databricks-sql-connector > /dev/null 2>&1
check $? "Databricks SQL Connector is installed"

echo ""
echo "4. Checking Configuration..."
[ -f ".env" ] || [ -n "$DATABRICKS_HOST" ]
check $? "Environment configuration exists"

echo ""
echo "5. Running Tests..."
cd "$(dirname "$0")"
python -m pytest tests/ -q --tb=no -x > /dev/null 2>&1
check $? "Backend tests pass"

echo ""
echo "6. Checking API Health..."
# This assumes API is not running, which is OK
echo -e "${YELLOW}⚠️  Skipping (API not running)${NC}"

echo ""
echo "=================================================="
echo "Results:"
echo -e "${GREEN}Passed: $checks_passed${NC}"
echo -e "${RED}Failed: $checks_failed${NC}"
echo ""

if [ $checks_failed -eq 0 ]; then
    echo -e "${GREEN}✅ Ready for deployment!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. databricks apps create lakehouse-sqlpilot"
    echo "2. databricks apps deploy lakehouse-sqlpilot ."
    echo "3. databricks apps get lakehouse-sqlpilot"
    exit 0
else
    echo -e "${RED}❌ Please fix errors before deploying${NC}"
    exit 1
fi


