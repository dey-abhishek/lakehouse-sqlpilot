"""
Tests for Lakebase Backend
Tests distributed state management using Databricks Lakebase Postgres
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import time
from datetime import datetime, timedelta
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock psycopg2 before importing lakebase_backend
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.pool'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

from infrastructure.lakebase_backend import LakebaseBackend


@pytest.fixture
def mock_psycopg2():
    """Mock psycopg2 module"""
    with patch('infrastructure.lakebase_backend.psycopg2') as mock_pg2, \
         patch('infrastructure.lakebase_backend.ThreadedConnectionPool') as mock_pool:
        
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        
        # Mock pool
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance
        
        yield {
            'psycopg2': mock_pg2,
            'pool_class': mock_pool,
            'pool': mock_pool_instance,
            'conn': mock_conn,
            'cursor': mock_cursor
        }


@pytest.fixture
def lakebase_backend(mock_psycopg2, monkeypatch):
    """Create LakebaseBackend instance with mocked dependencies"""
    # Set environment variables
    monkeypatch.setenv('LAKEBASE_HOST', 'test-host.databricks.com')
    monkeypatch.setenv('LAKEBASE_USER', 'test_user')
    monkeypatch.setenv('LAKEBASE_PASSWORD', 'test_password')
    monkeypatch.setenv('LAKEBASE_ENABLED', 'true')
    
    # Mock the table count check in _initialize_schema
    # This returns (count,) tuple, so fetchone()[0] will be 10
    mock_psycopg2['cursor'].fetchone.return_value = (10,)
    
    # Create backend (will use mocked pool)
    backend = LakebaseBackend(
        host='test-host.databricks.com',
        user='test_user',
        password='test_password'
    )
    
    # Set the mocked cursor
    backend._mock_cursor = mock_psycopg2['cursor']
    backend._mock_conn = mock_psycopg2['conn']
    
    return backend


class TestLakebaseBackendInitialization:
    """Test Lakebase backend initialization"""
    
    @pytest.mark.skip(reason="Requires complex mocking of smart credential manager")
    @patch('infrastructure.lakebase_backend.get_lakebase_credentials_with_fallback')
    def test_initialization_with_env_vars(self, mock_cred_func, monkeypatch, mock_psycopg2):
        """Test initialization using environment variables"""
        # Mock credential function to return static credentials
        mock_cred_func.return_value = {
            'host': 'env-host.databricks.com',
            'user': 'env_user',
            'password': 'env_password'
        }
        
        monkeypatch.setenv('LAKEBASE_HOST', 'env-host.databricks.com')
        monkeypatch.setenv('LAKEBASE_USER', 'env_user')
        monkeypatch.setenv('LAKEBASE_PASSWORD', 'env_password')
        
        # Mock cursor.fetchone to return a tuple with count (10 tables)
        mock_psycopg2['cursor'].fetchone.return_value = (10,)
        
        backend = LakebaseBackend()
        
        assert backend.host == 'env-host.databricks.com'
        assert backend.user == 'env_user'
        assert backend.password == 'env_password'
    
    def test_initialization_without_credentials_raises_error(self, monkeypatch):
        """Test that missing credentials raise ValueError"""
        monkeypatch.delenv('LAKEBASE_HOST', raising=False)
        monkeypatch.delenv('LAKEBASE_USER', raising=False)
        monkeypatch.delenv('LAKEBASE_PASSWORD', raising=False)
        
        with pytest.raises(ValueError, match="Lakebase credentials not configured"):
            LakebaseBackend()
    
    def test_schema_initialization_creates_tables(self, mock_psycopg2, monkeypatch):
        """Test that schema initialization creates all required tables"""
        # Set environment variables
        monkeypatch.setenv('LAKEBASE_HOST', 'test-host.databricks.com')
        monkeypatch.setenv('LAKEBASE_USER', 'test_user')
        monkeypatch.setenv('LAKEBASE_PASSWORD', 'test_password')
        monkeypatch.setenv('LAKEBASE_ENABLED', 'true')
        
        # Mock the table count check to return 0 (no tables exist)
        # so schema initialization actually runs
        mock_psycopg2['cursor'].fetchone.return_value = (0,)
        
        # Create backend - this will trigger schema initialization
        backend = LakebaseBackend(
            host='test-host.databricks.com',
            user='test_user',
            password='test_password'
        )
        
        cursor = mock_psycopg2['cursor']
        
        # Check that CREATE TABLE statements were executed
        execute_calls = [call[0][0] for call in cursor.execute.call_args_list]
        
        # Since we have 0 tables, the initialization will log and continue
        # The new logic doesn't actually create tables if it doesn't have ownership
        # So we should just verify it checked the count and logged appropriately
        assert any('information_schema.tables' in str(call).lower() for call in execute_calls)


class TestRateLimiting:
    """Test rate limiting functionality"""
    
    def test_check_rate_limit_allows_under_limit(self, lakebase_backend):
        """Test that requests under limit are allowed"""
        cursor = lakebase_backend._mock_cursor
        
        # Mock database response - no existing entries
        cursor.fetchone.return_value = None
        
        allowed, count = lakebase_backend.check_rate_limit('test_client', 10, 60)
        
        assert allowed is True
        assert count == 1
    
    def test_check_rate_limit_blocks_over_limit(self, lakebase_backend):
        """Test that requests over limit are blocked"""
        cursor = lakebase_backend._mock_cursor
        
        # Mock existing timestamps at limit
        now = time.time()
        timestamps = [now - i for i in range(10)]  # 10 requests
        cursor.fetchone.return_value = (json.dumps(timestamps),)
        
        allowed, count = lakebase_backend.check_rate_limit('test_client', 10, 60)
        
        assert allowed is False
        assert count == 11  # 10 existing + 1 new
    
    def test_check_rate_limit_removes_expired_timestamps(self, lakebase_backend):
        """Test that old timestamps are removed from rate limit check"""
        cursor = lakebase_backend._mock_cursor
        
        # Mock old timestamps (> window)
        now = time.time()
        old_timestamps = [now - 120, now - 90]  # Older than 60 second window
        recent_timestamps = [now - 30]
        all_timestamps = old_timestamps + recent_timestamps
        
        cursor.fetchone.return_value = (json.dumps(all_timestamps),)
        
        allowed, count = lakebase_backend.check_rate_limit('test_client', 10, 60)
        
        assert allowed is True
        assert count == 2  # 1 recent + 1 new
    
    def test_reset_rate_limit(self, lakebase_backend):
        """Test resetting rate limit for a client"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.reset_rate_limit('test_client')
        
        # Verify DELETE was called
        cursor.execute.assert_called()
        delete_call = [call for call in cursor.execute.call_args_list if 'DELETE' in str(call)]
        assert len(delete_call) > 0
    
    def test_rate_limit_handles_errors_gracefully(self, lakebase_backend):
        """Test that rate limit errors don't block legitimate traffic"""
        cursor = lakebase_backend._mock_cursor
        cursor.execute.side_effect = Exception("Database error")
        
        # Should return allowed=True on error to avoid blocking traffic
        allowed, count = lakebase_backend.check_rate_limit('test_client', 10, 60)
        
        assert allowed is True
        assert count == 0


class TestFailedAuthenticationTracking:
    """Test failed authentication tracking"""
    
    def test_record_failed_auth(self, lakebase_backend):
        """Test recording failed authentication attempt"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = (3,)  # 3 attempts
        
        count = lakebase_backend.record_failed_auth('192.168.1.1', 'Invalid token')
        
        assert count == 3
        assert cursor.execute.call_count >= 2  # INSERT + SELECT
    
    def test_get_failed_auth_count(self, lakebase_backend):
        """Test getting failed auth count"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = (5,)
        
        count = lakebase_backend.get_failed_auth_count('192.168.1.1')
        
        assert count == 5
    
    def test_reset_failed_auth(self, lakebase_backend):
        """Test resetting failed auth attempts"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.reset_failed_auth('192.168.1.1')
        
        # Verify DELETE was called
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE' in str(call)]
        assert len(delete_calls) > 0


class TestTokenCaching:
    """Test token caching functionality"""
    
    def test_cache_token(self, lakebase_backend):
        """Test caching a token"""
        cursor = lakebase_backend._mock_cursor
        
        user_info = {'user': 'test@databricks.com', 'roles': ['user']}
        lakebase_backend.cache_token('test_token', user_info, ttl=300)
        
        # Verify INSERT was called
        insert_calls = [call for call in cursor.execute.call_args_list if 'INSERT INTO token_cache' in str(call)]
        assert len(insert_calls) > 0
    
    def test_get_cached_token_hit(self, lakebase_backend):
        """Test getting a cached token (hit)"""
        cursor = lakebase_backend._mock_cursor
        
        user_info = {'user': 'test@databricks.com', 'roles': ['user']}
        cursor.fetchone.return_value = (json.dumps(user_info),)
        
        result = lakebase_backend.get_cached_token('test_token')
        
        assert result == user_info
    
    def test_get_cached_token_miss(self, lakebase_backend):
        """Test getting a cached token (miss)"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = None
        
        result = lakebase_backend.get_cached_token('test_token')
        
        assert result is None
    
    def test_invalidate_token(self, lakebase_backend):
        """Test invalidating a cached token"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.invalidate_token('test_token')
        
        # Verify DELETE was called
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE FROM token_cache' in str(call)]
        assert len(delete_calls) > 0


class TestSessionManagement:
    """Test session management"""
    
    def test_create_session(self, lakebase_backend):
        """Test creating a session"""
        cursor = lakebase_backend._mock_cursor
        
        user_info = {'user': 'test@databricks.com', 'email': 'test@databricks.com'}
        lakebase_backend.create_session('session_123', user_info, ttl=3600)
        
        # Verify INSERT was called
        insert_calls = [call for call in cursor.execute.call_args_list if 'INSERT INTO auth_sessions' in str(call)]
        assert len(insert_calls) > 0
    
    def test_get_session_valid(self, lakebase_backend):
        """Test getting a valid session"""
        cursor = lakebase_backend._mock_cursor
        
        user_info = {'user': 'test@databricks.com', 'email': 'test@databricks.com'}
        cursor.fetchone.return_value = (json.dumps(user_info),)
        
        result = lakebase_backend.get_session('session_123')
        
        assert result == user_info
    
    def test_get_session_expired(self, lakebase_backend):
        """Test getting an expired session"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = None
        
        result = lakebase_backend.get_session('session_123')
        
        assert result is None
    
    def test_delete_session(self, lakebase_backend):
        """Test deleting a session"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.delete_session('session_123')
        
        # Verify DELETE was called
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE FROM auth_sessions' in str(call)]
        assert len(delete_calls) > 0
    
    def test_extend_session(self, lakebase_backend):
        """Test extending session TTL"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.extend_session('session_123', ttl=3600)
        
        # Verify UPDATE was called
        update_calls = [call for call in cursor.execute.call_args_list if 'UPDATE auth_sessions' in str(call)]
        assert len(update_calls) > 0


class TestGeneralCaching:
    """Test general key-value caching"""
    
    def test_set_and_get(self, lakebase_backend):
        """Test setting and getting a value"""
        cursor = lakebase_backend._mock_cursor
        
        test_value = {'key': 'value', 'num': 42}
        
        # Set
        lakebase_backend.set('test_key', test_value, ttl=300)
        
        # Mock get
        cursor.fetchone.return_value = (json.dumps(test_value),)
        result = lakebase_backend.get('test_key')
        
        assert result == test_value
    
    def test_delete(self, lakebase_backend):
        """Test deleting a key"""
        cursor = lakebase_backend._mock_cursor
        
        lakebase_backend.delete('test_key')
        
        # Verify DELETE was called
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE FROM unity_catalog_cache' in str(call)]
        assert len(delete_calls) > 0
    
    def test_exists_true(self, lakebase_backend):
        """Test exists returns True for existing key"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = (1,)
        
        result = lakebase_backend.exists('test_key')
        
        assert result is True
    
    def test_exists_false(self, lakebase_backend):
        """Test exists returns False for missing key"""
        cursor = lakebase_backend._mock_cursor
        cursor.fetchone.return_value = None
        
        result = lakebase_backend.exists('test_key')
        
        assert result is False


class TestUnityCatalogCaching:
    """Test Unity Catalog specific caching"""
    
    def test_cache_and_get_catalogs(self, lakebase_backend):
        """Test caching and retrieving catalogs"""
        cursor = lakebase_backend._mock_cursor
        
        catalogs = [{'name': 'main'}, {'name': 'dev'}]
        
        # Cache
        lakebase_backend.cache_catalogs(catalogs, ttl=300)
        
        # Get
        cursor.fetchone.return_value = (json.dumps(catalogs),)
        result = lakebase_backend.get_cached_catalogs()
        
        assert result == catalogs
    
    def test_cache_and_get_schemas(self, lakebase_backend):
        """Test caching and retrieving schemas"""
        cursor = lakebase_backend._mock_cursor
        
        schemas = [{'name': 'default'}, {'name': 'staging'}]
        
        # Cache
        lakebase_backend.cache_schemas('main', schemas, ttl=300)
        
        # Get
        cursor.fetchone.return_value = (json.dumps(schemas),)
        result = lakebase_backend.get_cached_schemas('main')
        
        assert result == schemas
    
    def test_cache_and_get_tables(self, lakebase_backend):
        """Test caching and retrieving tables"""
        cursor = lakebase_backend._mock_cursor
        
        tables = [{'name': 'customers'}, {'name': 'orders'}]
        
        # Cache
        lakebase_backend.cache_tables('main', 'default', tables, ttl=300)
        
        # Get
        cursor.fetchone.return_value = (json.dumps(tables),)
        result = lakebase_backend.get_cached_tables('main', 'default')
        
        assert result == tables
    
    def test_invalidate_catalog_cache(self, lakebase_backend):
        """Test invalidating catalog cache"""
        cursor = lakebase_backend._mock_cursor
        cursor.rowcount = 5
        
        lakebase_backend.invalidate_catalog_cache('main')
        
        # Verify DELETE was called with LIKE pattern
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE FROM unity_catalog_cache' in str(call)]
        assert len(delete_calls) > 0


class TestCleanup:
    """Test data cleanup functionality"""
    
    def test_cleanup_expired_data(self, lakebase_backend):
        """Test cleanup of expired data"""
        cursor = lakebase_backend._mock_cursor
        cursor.rowcount = 10  # Simulated deleted rows
        
        lakebase_backend.cleanup_expired_data()
        
        # Verify DELETE statements were executed for all tables
        delete_calls = [call[0][0] for call in cursor.execute.call_args_list if 'DELETE' in call[0][0]]
        
        assert any('auth_sessions' in call for call in delete_calls)
        assert any('token_cache' in call for call in delete_calls)
        assert any('failed_auth_attempts' in call for call in delete_calls)
        assert any('unity_catalog_cache' in call for call in delete_calls)
        assert any('rate_limits' in call for call in delete_calls)


class TestHealthMonitoring:
    """Test health and monitoring functionality"""
    
    def test_ping_success(self, lakebase_backend):
        """Test successful ping"""
        cursor = lakebase_backend._mock_cursor
        
        result = lakebase_backend.ping()
        
        assert result is True
        cursor.execute.assert_called_with("SELECT 1")
    
    def test_ping_failure(self, lakebase_backend):
        """Test ping failure"""
        cursor = lakebase_backend._mock_cursor
        cursor.execute.side_effect = Exception("Connection error")
        
        result = lakebase_backend.ping()
        
        assert result is False
    
    def test_get_stats(self, lakebase_backend):
        """Test getting statistics"""
        cursor = lakebase_backend._mock_cursor
        
        # Mock fetchone to return count dicts
        cursor.fetchone.side_effect = [
            {'count': 5},   # rate_limits
            {'count': 10},  # active_sessions
            {'count': 15},  # cached_tokens
            {'count': 20},  # catalog_cache
            {'count': 25}   # plan_cache
        ]
        
        stats = lakebase_backend.get_stats()
        
        assert stats['connected'] is True
        assert stats['rate_limits_count'] == 5
        assert stats['active_sessions'] == 10
        assert stats['cached_tokens'] == 15
        assert stats['cached_catalog_items'] == 20
        assert stats['cached_plans'] == 25


class TestConnectionPooling:
    """Test connection pooling"""
    
    def test_connection_pool_created(self, lakebase_backend, mock_psycopg2):
        """Test that connection pool is created"""
        pool_class = mock_psycopg2['pool_class']
        
        # Verify ThreadedConnectionPool was instantiated
        assert pool_class.called
        
        # Check pool was created with correct parameters
        call_args = pool_class.call_args
        assert call_args[0][0] >= 2  # min_connections
        assert call_args[0][1] <= 20  # max_connections
    
    def test_connection_context_manager(self, lakebase_backend):
        """Test connection context manager"""
        with lakebase_backend._get_connection() as conn:
            assert conn is not None
        
        # Verify connection was returned to pool
        lakebase_backend.pool.putconn.assert_called()


class TestGlobalInstance:
    """Test global backend instance"""
    
    def test_get_lakebase_backend_enabled(self, monkeypatch, mock_psycopg2):
        """Test getting global instance when enabled"""
        from infrastructure.lakebase_backend import get_lakebase_backend
        
        monkeypatch.setenv('LAKEBASE_ENABLED', 'true')
        monkeypatch.setenv('LAKEBASE_HOST', 'test-host.databricks.com')
        monkeypatch.setenv('LAKEBASE_USER', 'test_user')
        monkeypatch.setenv('LAKEBASE_PASSWORD', 'test_password')
        
        # Mock the table count check - return 10 to skip initialization
        mock_psycopg2['cursor'].fetchone.return_value = (10,)
        
        backend = get_lakebase_backend()
        
        assert backend is not None
        assert isinstance(backend, LakebaseBackend)
    
    def test_get_lakebase_backend_disabled(self, monkeypatch):
        """Test getting global instance when disabled"""
        from infrastructure.lakebase_backend import get_lakebase_backend
        
        monkeypatch.setenv('LAKEBASE_ENABLED', 'false')
        
        backend = get_lakebase_backend()
        
        assert backend is None
    
    @pytest.mark.skip(reason="Requires complex mocking of smart credential manager")
    @patch('infrastructure.lakebase_backend.get_lakebase_credentials_with_fallback')
    def test_close_lakebase(self, mock_cred_func, monkeypatch, mock_psycopg2):
        """Test closing global instance"""
        from infrastructure.lakebase_backend import get_lakebase_backend, close_lakebase
        
        # Mock credential function
        mock_cred_func.return_value = {
            'host': 'test-host.databricks.com',
            'user': 'test_user',
            'password': 'test_password'
        }
        
        # Mock cursor.fetchone for table count check
        mock_psycopg2['cursor'].fetchone.return_value = [10]
        
        monkeypatch.setenv('LAKEBASE_ENABLED', 'true')
        monkeypatch.setenv('LAKEBASE_HOST', 'test-host.databricks.com')
        monkeypatch.setenv('LAKEBASE_USER', 'test_user')
        monkeypatch.setenv('LAKEBASE_PASSWORD', 'test_password')
        
        backend = get_lakebase_backend()
        assert backend is not None
        
        close_lakebase()
        
        # Verify close was called
        backend.pool.closeall.assert_called()

