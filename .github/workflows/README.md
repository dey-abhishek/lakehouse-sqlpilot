# GitHub Actions CI/CD

## Overview

This directory contains GitHub Actions workflows for automated testing and validation.

## Current Workflow: `test.yml`

### What It Tests (Without Credentials)

✅ **Syntax Check**: Validates Python syntax across all files  
✅ **Unit Tests**: Runs compiler, pattern, and validation tests  
✅ **Frontend Build**: Builds the React UI with Vite  
✅ **Security Scan**: Checks for leaked secrets with gitleaks  

### What It SKIPS (Requires Credentials)

⚠️ **Integration Tests**: Require Databricks credentials  
⚠️ **UAT Tests**: Require Databricks SQL Warehouse  
⚠️ **Lakebase Tests**: Require PostgreSQL connection  
⚠️ **E2E Tests**: Require running backend server  

## Why This Approach?

The workflow is designed to run **without exposing credentials** in GitHub Actions. Tests requiring Databricks/Lakebase infrastructure must be run locally with proper `.env` configuration.

## Running Full Test Suite Locally

```bash
# 1. Configure credentials
cp env.example .env
# Edit .env with your Databricks/Lakebase credentials

# 2. Run all tests
source sqlpilot/bin/activate
pytest -v

# Results: 188+ tests
```

## Local Test Categories

| Category | Count | Requires Credentials |
|----------|-------|---------------------|
| Unit Tests | 50+ | ❌ No |
| API Tests | 40+ | ❌ No |
| Security Tests | 20+ | ❌ No |
| Lakebase Tests | 37 | ✅ Yes (Lakebase) |
| Circuit Breaker | 32 | ❌ No |
| Integration Tests | 24 | ✅ Yes (Databricks) |
| UAT Tests | 10+ | ✅ Yes (Databricks) |

## CI Philosophy

**CI = Continuous Integration (what can be tested without infrastructure)**  
**CD = Continuous Deployment (full testing before production)**

The GitHub Actions workflow validates:
- Code syntax
- Unit logic
- Frontend builds
- Security (no leaked secrets)

Full integration testing happens:
- Locally during development
- In staging environment with credentials
- Before production deployment

## Disabling CI (Optional)

If you want to disable GitHub Actions entirely:

```bash
# Rename the workflow directory
mv .github/workflows .github/workflows.disabled
git add .github
git commit -m "Disable CI workflow"
git push
```

## Alternative: Add Secrets to GitHub

If you want full CI testing:

1. Go to GitHub repository → Settings → Secrets
2. Add secrets:
   - `DATABRICKS_HOST`
   - `DATABRICKS_TOKEN`
   - `LAKEBASE_HOST`
   - `LAKEBASE_USER`
   - `LAKEBASE_PASSWORD`
3. Update `test.yml` to use these secrets
4. **⚠️ Security Risk**: Credentials in GitHub Actions

**We recommend the current approach (no credentials in CI).**

## Summary

✅ **Current workflow**: Safe, no credentials, validates syntax and builds  
✅ **Full testing**: Run locally with proper `.env` setup  
✅ **Security**: No credentials exposed in GitHub Actions  


