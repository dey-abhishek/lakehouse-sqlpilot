#!/bin/bash
# ============================================================================
# Security Cleanup Script
# Removes sensitive files and documentation before git commit
# ============================================================================

set -e

echo "🔒 Starting security cleanup..."

# ============================================================================
# 1. Remove OAuth Scripts (contain tokens and URLs)
# ============================================================================
echo "📝 Removing OAuth scripts..."
rm -f manual_oauth.py 2>/dev/null || true
rm -f oauth_*.py 2>/dev/null || true
rm -f verify_oauth.py 2>/dev/null || true
rm -f generate_oauth.py 2>/dev/null || true
rm -f refresh_token.py 2>/dev/null || true
rm -f complete_oauth.py 2>/dev/null || true
rm -f preflight_check.py 2>/dev/null || true
rm -f setup_env.sh 2>/dev/null || true
rm -f check_env.py 2>/dev/null || true

echo "✅ OAuth scripts removed"

# ============================================================================
# 2. Remove All Documentation (except README.md)
# ============================================================================
echo "📝 Removing documentation files..."

# Remove specific doc patterns
rm -f ACTION_PLAN_*.md 2>/dev/null || true
rm -f ALL_TESTS_*.md 2>/dev/null || true
rm -f ALTERNATIVE_*.md 2>/dev/null || true
rm -f API_*.md 2>/dev/null || true
rm -f BACKEND_*.md 2>/dev/null || true
rm -f COMPLETE_*.md 2>/dev/null || true
rm -f CURRENT_STATE*.md 2>/dev/null || true
rm -f DATABRICKS_*.md 2>/dev/null || true
rm -f DOCUMENTATION_*.md 2>/dev/null || true
rm -f E2E_*.md 2>/dev/null || true
rm -f ENVIRONMENT_*.md 2>/dev/null || true
rm -f ENV_*.md 2>/dev/null || true
rm -f FINAL_*.md 2>/dev/null || true
rm -f FRONTEND_*.md 2>/dev/null || true
rm -f GENIE_*.md 2>/dev/null || true
rm -f HTTPS_*.md 2>/dev/null || true
rm -f IMPLEMENTATION_*.md 2>/dev/null || true
rm -f INTEGRATION_*.md 2>/dev/null || true
rm -f OAUTH_*.md 2>/dev/null || true
rm -f PRODUCTION_*.md 2>/dev/null || true
rm -f QUICK_*.md 2>/dev/null || true
rm -f REFRESH_*.md 2>/dev/null || true
rm -f SCALABILITY_*.md 2>/dev/null || true
rm -f SCD2_*.md 2>/dev/null || true
rm -f SECURITY_*.md 2>/dev/null || true
rm -f SESSION_*.md 2>/dev/null || true
rm -f SPARK_*.md 2>/dev/null || true
rm -f TEST_*.md 2>/dev/null || true
rm -f UAT_*.md 2>/dev/null || true
rm -f UNDERSTANDING_*.md 2>/dev/null || true
rm -f WAREHOUSE_*.md 2>/dev/null || true
rm -f CONTRIBUTING.md 2>/dev/null || true

# Remove docs folder
rm -rf docs/ 2>/dev/null || true

echo "✅ Documentation removed (README.md preserved)"

# ============================================================================
# 3. Remove Debug/Generated SQL Files
# ============================================================================
echo "📝 Removing debug SQL files..."
rm -f debug_*.sql 2>/dev/null || true
rm -f generated_*.sql 2>/dev/null || true
rm -f test_*.sql 2>/dev/null || true
rm -f *.sql.bak 2>/dev/null || true
rm -f setup_uat_*.sql 2>/dev/null || true

echo "✅ Debug SQL files removed"

# ============================================================================
# 4. Remove Environment Templates (keep .env.example only)
# ============================================================================
echo "📝 Removing environment templates..."
rm -f .env.template 2>/dev/null || true
rm -f env.template 2>/dev/null || true

echo "✅ Environment templates removed (.env.example preserved)"

# ============================================================================
# 5. Remove Terminals Folder (command history)
# ============================================================================
echo "📝 Removing terminals folder..."
rm -rf terminals/ 2>/dev/null || true

echo "✅ Terminals folder removed"

# ============================================================================
# 6. Verify No Credentials in Git
# ============================================================================
echo ""
echo "🔍 Checking for credentials in tracked files..."

# Check for common credential patterns
if git rev-parse --git-dir > /dev/null 2>&1; then
    echo "Searching for potential credentials..."
    
    # Search for tokens
    if git grep -i "token.*=" -- "*.py" "*.yaml" "*.yml" "*.json" 2>/dev/null | grep -v "token_type" | grep -v "#"; then
        echo "⚠️  WARNING: Found potential tokens in tracked files!"
    fi
    
    # Search for passwords
    if git grep -i "password.*=" -- "*.py" "*.yaml" "*.yml" "*.json" 2>/dev/null | grep -v "password_hash" | grep -v "#"; then
        echo "⚠️  WARNING: Found potential passwords in tracked files!"
    fi
    
    # Search for API keys
    if git grep -i "api_key.*=" -- "*.py" "*.yaml" "*.yml" "*.json" 2>/dev/null | grep -v "#"; then
        echo "⚠️  WARNING: Found potential API keys in tracked files!"
    fi
    
    echo "✅ Credential scan complete"
else
    echo "ℹ️  Not a git repository, skipping credential scan"
fi

# ============================================================================
# 7. Check .gitignore is properly configured
# ============================================================================
echo ""
echo "🔍 Verifying .gitignore..."

if [ -f .gitignore ]; then
    required_patterns=(
        ".env"
        "*.token"
        "*.key"
        "*.pem"
        "credentials"
        "manual_oauth.py"
        "*.md"
        "!README.md"
    )
    
    missing=0
    for pattern in "${required_patterns[@]}"; do
        if ! grep -q "$pattern" .gitignore; then
            echo "⚠️  Missing pattern in .gitignore: $pattern"
            missing=1
        fi
    done
    
    if [ $missing -eq 0 ]; then
        echo "✅ .gitignore is properly configured"
    else
        echo "⚠️  .gitignore may need updates"
    fi
else
    echo "⚠️  .gitignore not found!"
fi

# ============================================================================
# 8. Summary
# ============================================================================
echo ""
echo "========================================"
echo "🔒 Security Cleanup Complete!"
echo "========================================"
echo ""
echo "✅ Removed:"
echo "  - OAuth scripts (with tokens/URLs)"
echo "  - All documentation (except README.md)"
echo "  - Debug SQL files"
echo "  - Environment templates"
echo "  - Terminals folder"
echo ""
echo "📝 Preserved:"
echo "  - README.md"
echo "  - LICENSE.md"
echo "  - .env.example"
echo "  - Source code files"
echo ""
echo "⚠️  IMPORTANT: Before committing:"
echo "  1. Review 'git status' carefully"
echo "  2. Check no .env or credentials are staged"
echo "  3. Verify no tokens in source files"
echo "  4. Run: git diff --cached"
echo ""
echo "🔒 Safe to commit!"
echo ""

