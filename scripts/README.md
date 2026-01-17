# Scripts Directory

Utility scripts for setup, deployment, security, and operations.

## 📁 Directory Structure

```
scripts/
├── deployment/           # Deployment and setup
│   ├── deploy.sh
│   ├── quickstart.sh
│   └── setup_env.sh (not tracked)
├── security/            # Security utilities
│   ├── secrets_manager.py
│   └── security_cleanup.sh (not tracked)
├── oauth/              # OAuth setup (NOT tracked - in .gitignore)
│   ├── setup_oauth.py
│   ├── manual_oauth.py
│   ├── complete_oauth.py
│   └── verify_oauth.py
├── preflight_check.py (not tracked)
└── show_v2_roadmap.sh

```

## 🚀 Deployment Scripts

### `deployment/quickstart.sh`
**Quick start for new users**
```bash
./scripts/deployment/quickstart.sh
```

What it does:
- Creates virtual environment
- Installs dependencies
- Sets up .env configuration
- Runs preflight checks
- Starts backend API

### `deployment/deploy.sh`
**Production deployment**
```bash
./scripts/deployment/deploy.sh
```

What it does:
- Validates environment
- Runs security checks
- Builds frontend
- Deploys to production
- Runs smoke tests

### `deployment/setup_env.sh` (not tracked)
**Environment setup**
```bash
./scripts/deployment/setup_env.sh
```

Creates `.env` from template and validates configuration.

## 🔒 Security Scripts

### `security/secrets_manager.py`
**Secrets management utility**
```bash
# Get secret
python scripts/security/secrets_manager.py get SECRET_NAME

# Set secret
python scripts/security/secrets_manager.py set SECRET_NAME value

# List secrets
python scripts/security/secrets_manager.py list
```

Supports multiple backends:
- Databricks Secrets
- AWS Secrets Manager
- Azure Key Vault
- File-based (encrypted)
- Environment variables

### `security/security_cleanup.sh` (not tracked)
**Remove sensitive files before commit**
```bash
./scripts/security/security_cleanup.sh
```

What it removes:
- OAuth scripts with tokens
- All .md files (except README.md)
- Debug SQL files
- Terminal logs
- Scans for credentials

**Note**: Also enforced by `.git/hooks/pre-commit`

## 🔐 OAuth Scripts (NOT Tracked)

**⚠️ CRITICAL**: OAuth scripts are in `.gitignore` and should NEVER be committed.

### `oauth/setup_oauth.py`
**Interactive OAuth setup**
```bash
python scripts/oauth/setup_oauth.py
```

Full automated flow:
1. Generates PKCE challenge
2. Opens browser for authorization
3. Exchanges code for tokens
4. Tests token validity
5. Updates .env file

### `oauth/manual_oauth.py`
**Manual OAuth flow**
```bash
python scripts/oauth/manual_oauth.py
```

For when automatic flow doesn't work:
1. Manually copy authorization URL
2. Paste authorization code
3. Exchanges for tokens
4. Updates .env file

### `oauth/complete_oauth.py`
**Complete partial OAuth flow**
```bash
python scripts/oauth/complete_oauth.py
```

If setup_oauth.py was interrupted, this completes it.

### `oauth/verify_oauth.py`
**Quick OAuth verification**
```bash
python scripts/oauth/verify_oauth.py
```

Checks:
- Environment variables
- Token validity
- API connectivity

## 🔍 Utility Scripts

### `preflight_check.py` (not tracked)
**Pre-deployment checks**
```bash
python scripts/preflight_check.py
```

Validates:
- Databricks connectivity
- Unity Catalog access
- SQL Warehouse availability
- Credentials validity

### `show_v2_roadmap.sh`
**Display product roadmap**
```bash
./scripts/show_v2_roadmap.sh
```

Shows planned features and timeline.

## 🎯 Quick Reference

### First-Time Setup
```bash
# 1. Quick start
./scripts/deployment/quickstart.sh

# 2. Setup OAuth (if needed)
python scripts/oauth/setup_oauth.py

# 3. Verify configuration
python scripts/preflight_check.py
```

### Before Committing
```bash
# Clean sensitive files
./scripts/security/security_cleanup.sh

# Check git status
git status

# Pre-commit hook will also scan for credentials
git commit -m "your message"
```

### Deployment
```bash
# Production deployment
./scripts/deployment/deploy.sh

# Verify deployment
python scripts/oauth/verify_oauth.py
```

## 📋 Script Categories

| Category | Purpose | Tracked | Credentials |
|----------|---------|---------|-------------|
| **deployment/** | Setup & deploy | ✅ Yes | ❌ No |
| **security/** | Security utils | ✅ Yes | ❌ No |
| **oauth/** | OAuth setup | ❌ NO | ⚠️ YES (writes to .env) |
| **Utilities** | Helpers | Mixed | ❌ No |

## ⚠️ Important Notes

### OAuth Scripts
- **NEVER commit** - They're in `.gitignore`
- May contain tokens/URLs temporarily
- Write credentials to `.env` (also gitignored)

### Security Cleanup
- Run `security_cleanup.sh` before commits
- Pre-commit hook also scans
- Removes all potential credential files

### Environment Files
These are NOT tracked (in `.gitignore`):
- `.env` - Main configuration
- `.env.local` - Local overrides
- `.env.*.local` - Environment-specific
- `oauth_*.py` - Generated OAuth scripts

## 🔐 Credentials Safety

### ✅ Safe to Commit
- `deployment/` scripts (no credentials)
- `security/secrets_manager.py` (utility only)
- `show_v2_roadmap.sh`

### ❌ NEVER Commit
- `oauth/` scripts (may contain tokens)
- `preflight_check.py` (may have hardcoded values)
- `setup_env.sh` (may contain credentials)
- `security_cleanup.sh` (contains patterns)

### 🛡️ Protection Layers
1. **`.gitignore`** - Blocks files
2. **`pre-commit` hook** - Scans for secrets
3. **`security_cleanup.sh`** - Manual cleanup
4. **Gitleaks** - CI/CD scanning

## 📚 More Information

- **OAuth Setup**: See `OAUTH_AUTHENTICATION.md` (if exists)
- **Security**: See `SECURITY_IMPLEMENTATION_COMPLETE.md`
- **Deployment**: See `DEPLOYMENT_GUIDE.md` (if exists)
- **Test Scripts**: See `tests/scripts/README.md`

---

**Keep credentials out of git!** 🔐

