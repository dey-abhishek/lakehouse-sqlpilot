#!/bin/bash
# Deploy Lakehouse SQLPilot to Databricks Apps

set -e

echo "🚀 Deploying Lakehouse SQLPilot to Databricks Apps"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    echo -e "${RED}❌ Databricks CLI not found${NC}"
    echo "Install it with: pip install databricks-cli"
    exit 1
fi

echo -e "${GREEN}✅ Databricks CLI found${NC}"

# Check if app.yaml exists
if [ ! -f "app.yaml" ]; then
    echo -e "${RED}❌ app.yaml not found${NC}"
    exit 1
fi

echo -e "${GREEN}✅ app.yaml found${NC}"

# Build frontend (if needed)
if [ -d "ui/plan-editor" ]; then
    echo -e "${YELLOW}📦 Building frontend...${NC}"
    cd ui/plan-editor
    if [ -f "package.json" ]; then
        npm install
        npm run build
    fi
    cd ../..
    echo -e "${GREEN}✅ Frontend built${NC}"
fi

# Create requirements.txt if not exists
if [ ! -f "requirements.txt" ]; then
    echo -e "${YELLOW}⚠️  requirements.txt not found, creating...${NC}"
    cat > requirements.txt << EOF
fastapi>=0.109.0
uvicorn[standard]>=0.27.0
pydantic>=2.5.0
jsonschema>=4.17.0
pyyaml>=6.0
jinja2>=3.1.0
databricks-sdk>=0.18.0
sqlparse>=0.4.4
python-dotenv>=1.0.0
EOF
    echo -e "${GREEN}✅ requirements.txt created${NC}"
fi

# Deploy to Databricks Apps
echo -e "${YELLOW}🚀 Deploying to Databricks Apps...${NC}"

# Option 1: Using Databricks CLI (if supported)
# databricks apps deploy --app-config app.yaml

# Option 2: Using Databricks SDK
python << PYTHON
from databricks.sdk import WorkspaceClient
import yaml

w = WorkspaceClient()

# Read app.yaml
with open('app.yaml', 'r') as f:
    config = yaml.safe_load(f)

print(f"📝 App Name: {config['name']}")
print(f"📝 Version: {config['version']}")
print(f"📝 Description: {config['description']}")

# Deploy app (pseudo-code - adjust based on actual Databricks Apps API)
# app = w.apps.create_or_update(
#     name=config['name'],
#     config=config
# )

print("✅ Deployment configuration validated")
print("")
print("⚠️  Manual deployment steps:")
print("1. Build Docker image:")
print("   docker build -t lakehouse-sqlpilot:latest .")
print("")
print("2. Push to Databricks container registry")
print("")
print("3. Deploy via Databricks Apps UI or CLI")
print("   https://<your-workspace>.databricks.com/apps")

PYTHON

echo ""
echo -e "${GREEN}✅ Deployment preparation complete!${NC}"
echo ""
echo "Next steps:"
echo "1. Review app.yaml configuration"
echo "2. Build and push Docker image"
echo "3. Deploy via Databricks Apps"
echo ""
echo "Documentation: https://docs.databricks.com/apps/"

