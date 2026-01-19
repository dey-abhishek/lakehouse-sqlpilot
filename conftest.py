"""
Pytest configuration and fixtures
Loads environment variables and sets up test infrastructure
"""

import pytest
import os
from pathlib import Path


def load_env_file():
    """Load environment variables from .env.dev for testing"""
    # Load .env.dev if it exists
    env_file = Path(__file__).parent / '.env.dev'
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes
                    value = value.strip('"').strip("'")
                    # Only set if not already set (allow override)
                    if key not in os.environ:
                        os.environ[key] = value


def pytest_configure(config):
    """Configure pytest and load environment"""
    # Load environment first
    load_env_file()
    
    # Register custom markers
    config.addinivalue_line(
        "markers", "lakebase: marks tests as requiring Lakebase connection"
    )
    config.addinivalue_line(
        "markers", "databricks: marks tests as requiring Databricks connection"
    )


@pytest.fixture(scope="session")
def lakebase_enabled():
    """Check if Lakebase is enabled for integration tests"""
    return os.getenv("LAKEBASE_ENABLED", "false").lower() == "true"


@pytest.fixture(scope="session")
def skip_if_no_lakebase(lakebase_enabled):
    """Skip test if Lakebase is not enabled"""
    if not lakebase_enabled:
        pytest.skip("Lakebase not enabled (set LAKEBASE_ENABLED=true)")

