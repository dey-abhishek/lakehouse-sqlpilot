#!/bin/bash
# Create Service Principal for Lakehouse SQLPilot
# This script guides you through creating and configuring a service principal

echo "🔐 Lakehouse SQLPilot - Service Principal Setup"
echo "================================================"
echo ""

echo "📋 You need to create a service principal for this application."
echo "   This will be used for automated, machine-to-machine authentication."
echo ""

echo "🎯 Recommended Service Principal Name:"
echo "   lakehouse-sqlpilot-prod    (for production)"
echo "   lakehouse-sqlpilot-dev     (for development)"
echo "   lakehouse-sqlpilot-staging (for staging)"
echo ""

echo "📝 Steps to Create Service Principal:"
echo "======================================"
echo ""

echo "1️⃣  Go to your Databricks workspace:"
echo "   https://e2-demo-field-eng.cloud.databricks.com"
echo ""

echo "2️⃣  Navigate to:"
echo "   Settings → Identity and access → Service principals"
echo ""

echo "3️⃣  Click 'Add service principal'"
echo ""

echo "4️⃣  Enter a name:"
echo "   Suggested: lakehouse-sqlpilot-dev"
echo "   (Use a descriptive name that indicates this is for the SQLPilot app)"
echo ""

echo "5️⃣  Click 'Add'"
echo "   You'll get an Application (client) ID"
echo "   Example: abc12345-def6-7890-ghij-klmnopqrstuv"
echo ""

echo "6️⃣  Click on the service principal you just created"
echo ""

echo "7️⃣  Go to 'OAuth secrets' tab"
echo ""

echo "8️⃣  Click 'Generate secret'"
echo "   ⚠️  IMPORTANT: Copy the secret immediately!"
echo "   It will only be shown once."
echo "   The secret starts with 'dapi' or similar"
echo ""

echo "9️⃣  Grant Access to Lakebase:"
echo "   a. Go to: Apps → Lakebase Postgres → Provisioned"
echo "   b. Click on your instance: instance-a73678e3-666c-4dea-b950-cdac83ca2004"
echo "   c. Click 'Permissions' tab"
echo "   d. Click 'Grant'"
echo "   e. Add your service principal: lakehouse-sqlpilot-dev"
echo "   f. Select role: 'Can Use' or 'Can Manage'"
echo "   g. Click 'Save'"
echo ""

echo "🔟  Update .env.dev with the credentials:"
echo ""
echo "    DATABRICKS_CLIENT_ID=\"<application_id_from_step_5>\""
echo "    DATABRICKS_CLIENT_SECRET=\"<secret_from_step_8>\""
echo ""

echo "1️⃣1️⃣  Test the configuration:"
echo "    python3 scripts/regenerate_lakebase_password.py"
echo ""

echo "================================================"
echo ""

read -p "Would you like me to help you note down the credentials? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo "📝 Please enter the credentials:"
    echo ""
    
    read -p "Service Principal Name: " SP_NAME
    read -p "Application (Client) ID: " CLIENT_ID
    read -s -p "Client Secret: " CLIENT_SECRET
    echo ""
    echo ""
    
    echo "✅ Credentials noted. Updating .env.dev..."
    
    # Update .env.dev
    if [ -f ".env.dev" ]; then
        # Backup
        cp .env.dev .env.dev.backup.$(date +%Y%m%d_%H%M%S)
        echo "✅ Backed up .env.dev"
        
        # Check if CLIENT_ID already exists
        if grep -q "DATABRICKS_CLIENT_ID=" .env.dev; then
            # Update existing
            sed -i.bak "s|^DATABRICKS_CLIENT_ID=.*|DATABRICKS_CLIENT_ID=\"$CLIENT_ID\"|" .env.dev
            sed -i.bak "s|^DATABRICKS_CLIENT_SECRET=.*|DATABRICKS_CLIENT_SECRET=\"$CLIENT_SECRET\"|" .env.dev
            echo "✅ Updated existing credentials in .env.dev"
        else
            # Add new
            cat >> .env.dev << EOF

# Service Principal (added by setup script)
DATABRICKS_CLIENT_ID="$CLIENT_ID"
DATABRICKS_CLIENT_SECRET="$CLIENT_SECRET"
EOF
            echo "✅ Added credentials to .env.dev"
        fi
        
        # Comment out DATABRICKS_TOKEN if present
        if grep -q "^DATABRICKS_TOKEN=" .env.dev; then
            sed -i.bak "s|^DATABRICKS_TOKEN=|# DATABRICKS_TOKEN=|" .env.dev
            echo "✅ Commented out DATABRICKS_TOKEN (using service principal instead)"
        fi
        
        echo ""
        echo "✅ Configuration complete!"
        echo ""
        echo "🧪 Next step: Generate initial token"
        echo "    python3 scripts/regenerate_lakebase_password.py"
        
    else
        echo "❌ .env.dev not found"
        echo ""
        echo "Please create .env.dev with:"
        echo ""
        echo "DATABRICKS_SERVER_HOSTNAME=\"e2-demo-field-eng.cloud.databricks.com\""
        echo "DATABRICKS_CLIENT_ID=\"$CLIENT_ID\""
        echo "DATABRICKS_CLIENT_SECRET=\"$CLIENT_SECRET\""
        echo "LAKEBASE_HOST=\"instance-a73678e3-666c-4dea-b950-cdac83ca2004.database.cloud.databricks.com\""
        echo "LAKEBASE_USER=\"$SP_NAME\""
    fi
else
    echo ""
    echo "📝 Manual Configuration:"
    echo ""
    echo "Add these to your .env.dev:"
    echo ""
    echo "# Service Principal Authentication"
    echo "DATABRICKS_CLIENT_ID=\"<your_application_id>\""
    echo "DATABRICKS_CLIENT_SECRET=\"<your_client_secret>\""
    echo ""
    echo "# Comment out PAT if present:"
    echo "# DATABRICKS_TOKEN=\"dapi...\""
fi

echo ""
echo "================================================"
echo ""
echo "📚 Documentation:"
echo "   docs/SERVICE_PRINCIPAL_AUTH_SETUP.md"
echo ""
echo "🔗 Databricks Service Principal Docs:"
echo "   https://docs.databricks.com/en/dev-tools/service-principals.html"
echo ""


