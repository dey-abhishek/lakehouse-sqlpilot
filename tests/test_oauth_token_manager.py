"""
Tests for OAuth Token Manager

Tests automatic OAuth token generation and rotation for Databricks services.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta
import time
import threading

from infrastructure.oauth_token_manager import (
    OAuthTokenManager,
    OAuthToken,
    get_oauth_token_manager,
    stop_oauth_token_manager,
    get_oauth_token,
    get_oauth_headers
)


class TestOAuthToken:
    """Test OAuthToken dataclass"""
    
    def test_token_not_expired(self):
        """Test token that hasn't expired"""
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        token = OAuthToken(
            access_token="test_token",
            expires_at=expires_at
        )
        
        assert not token.is_expired()
        assert token.seconds_until_expiry() > 3500  # ~1 hour
        assert token.minutes_until_expiry() >= 59
    
    def test_token_expired(self):
        """Test expired token"""
        expires_at = datetime.now(timezone.utc) - timedelta(minutes=5)
        token = OAuthToken(
            access_token="test_token",
            expires_at=expires_at
        )
        
        assert token.is_expired()
        assert token.seconds_until_expiry() == 0
        assert token.minutes_until_expiry() == 0
    
    def test_token_expiring_soon(self):
        """Test token expiring in 2 minutes"""
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=2)
        token = OAuthToken(
            access_token="test_token",
            expires_at=expires_at
        )
        
        assert not token.is_expired()
        assert 100 < token.seconds_until_expiry() < 130
        assert token.minutes_until_expiry() in [1, 2]


class TestOAuthTokenManager:
    """Test OAuthTokenManager"""
    
    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        """Setup and teardown for each test"""
        # Stop any existing manager
        stop_oauth_token_manager()
        yield
        # Clean up after test
        stop_oauth_token_manager()
    
    @patch('requests.post')
    def test_initialization_generates_token(self, mock_post):
        """Test manager initializes and generates token"""
        # Mock OAuth response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token_123',
            'token_type': 'Bearer',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False  # CRITICAL: Don't start background thread in tests
        )
        
        assert manager._token is not None
        assert manager._token.access_token == 'test_token_123'
        assert manager._token.token_type == 'Bearer'
        
        # Verify API call
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert 'https://test.databricks.com/oidc/v1/token' in call_args[0]
    
    @patch('requests.post')
    def test_get_token_returns_valid_token(self, mock_post):
        """Test get_token returns current token"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        token = manager.get_token()
        assert token == 'test_token'
    
    @patch('requests.post')
    def test_get_token_refreshes_if_expired(self, mock_post):
        """Test get_token refreshes expired token"""
        from datetime import datetime, timezone, timedelta
        
        # First call - initial token with normal expiry
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            'access_token': 'old_token',
            'expires_in': 3600  # Normal expiry
        }
        
        # Second call - refreshed token
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            'access_token': 'new_token',
            'expires_in': 3600
        }
        
        mock_post.side_effect = [mock_response1, mock_response2]
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False,
            refresh_buffer_minutes=0  # No buffer for testing
        )
        
        # First token should be old_token
        assert manager.get_token() == 'old_token'
        
        # Manually set token to expired
        manager._token.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        
        # Get token - should trigger refresh
        token = manager.get_token()
        assert token == 'new_token'
        assert mock_post.call_count == 2
    
    @patch('requests.post')
    def test_force_refresh(self, mock_post):
        """Test forcing token refresh"""
        mock_response1 = Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {
            'access_token': 'token1',
            'expires_in': 3600
        }
        
        mock_response2 = Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {
            'access_token': 'token2',
            'expires_in': 3600
        }
        
        mock_post.side_effect = [mock_response1, mock_response2]
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        token1 = manager.get_token()
        assert token1 == 'token1'
        
        # Force refresh even though token is valid
        token2 = manager.get_token(force_refresh=True)
        assert token2 == 'token2'
        assert mock_post.call_count == 2
    
    @patch('requests.post')
    def test_get_authorization_header(self, mock_post):
        """Test get_authorization_header returns correct format"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        header = manager.get_authorization_header()
        assert header == 'Bearer test_token'
    
    @patch('requests.post')
    def test_get_headers(self, mock_post):
        """Test get_headers returns correct dictionary"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        headers = manager.get_headers()
        assert headers == {
            'Authorization': 'Bearer test_token',
            'Content-Type': 'application/json'
        }
    
    @patch('requests.post')
    def test_needs_refresh_with_buffer(self, mock_post):
        """Test token needs refresh within buffer time"""
        from datetime import datetime, timezone, timedelta
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600  # 60 minutes
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False,
            refresh_buffer_minutes=5  # Refresh 5 min before expiry
        )
        
        # Manually set token to expire in 4 minutes
        manager._token.expires_at = datetime.now(timezone.utc) + timedelta(minutes=4)
        
        # Token expires in 4 min, buffer is 5 min → needs refresh
        assert manager._needs_refresh()
    
    @patch('requests.post')
    def test_token_refresh_failure_raises_error(self, mock_post):
        """Test token refresh failure raises exception"""
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_post.return_value = mock_response
        
        with pytest.raises(Exception, match="Token request failed"):
            OAuthTokenManager(
                databricks_host="test.databricks.com",
                client_id="bad_client",
                client_secret="bad_secret",
                auto_refresh=False
            )
    
    @patch('requests.post')
    def test_get_token_info(self, mock_post):
        """Test get_token_info returns correct info"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'token_type': 'Bearer',
            'expires_in': 3600,
            'scope': 'all-apis'
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        info = manager.get_token_info()
        assert info['status'] == 'active'
        assert info['token_type'] == 'Bearer'
        assert info['scope'] == 'all-apis'
        assert info['is_expired'] is False
        assert 'expires_at' in info
        assert 'seconds_until_expiry' in info
        assert 'minutes_until_expiry' in info
    
    def test_missing_credentials_raises_error(self, monkeypatch):
        """Test initialization without credentials raises error"""
        # Clear environment variables to ensure they don't provide fallback values
        monkeypatch.delenv("DATABRICKS_SERVER_HOSTNAME", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        
        with pytest.raises(ValueError, match="OAuth token manager requires"):
            OAuthTokenManager(
                databricks_host=None,
                client_id=None,
                client_secret=None
            )
    
    @patch('requests.post')
    def test_stop_manager(self, mock_post):
        """Test stopping the manager"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=True  # Start background thread
        )
        
        try:
            # Give thread a moment to start
            time.sleep(0.1)
            
            # Thread should be running
            assert manager._refresh_thread is not None
            assert manager._refresh_thread.is_alive()
            
            # Stop manager - this should signal the thread to stop
            manager.stop()
            
            # Give thread time to stop (up to 2 seconds)
            manager._refresh_thread.join(timeout=2)
            
            # Thread should be stopped
            assert not manager._refresh_thread.is_alive()
        except Exception:
            # Ensure cleanup
            manager.stop()
            if manager._refresh_thread:
                manager._refresh_thread.join(timeout=2)
            raise


class TestSingletonFunctions:
    """Test singleton access functions"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up singleton after each test"""
        yield
        stop_oauth_token_manager()
    
    @patch('requests.post')
    def test_get_oauth_token_manager_singleton(self, mock_post):
        """Test get_oauth_token_manager returns singleton"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager1 = get_oauth_token_manager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        manager2 = get_oauth_token_manager()
        
        # Should be same instance
        assert manager1 is manager2
    
    @patch('requests.post')
    def test_get_oauth_token_convenience_function(self, mock_post):
        """Test get_oauth_token convenience function"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        get_oauth_token_manager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        token = get_oauth_token()
        assert token == 'test_token'
    
    @patch('requests.post')
    def test_get_oauth_headers_convenience_function(self, mock_post):
        """Test get_oauth_headers convenience function"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        get_oauth_token_manager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        headers = get_oauth_headers()
        assert headers['Authorization'] == 'Bearer test_token'
        assert headers['Content-Type'] == 'application/json'
    
    @patch('requests.post')
    def test_stop_oauth_token_manager(self, mock_post):
        """Test stop_oauth_token_manager stops singleton"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = get_oauth_token_manager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        assert manager is not None
        
        # Stop manager
        stop_oauth_token_manager()
        
        # Next call should create new instance
        manager2 = get_oauth_token_manager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        assert manager2 is not manager


class TestBackgroundRefresh:
    """Test background refresh thread"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Clean up after each test"""
        yield
        stop_oauth_token_manager()
    
    @patch('requests.post')
    def test_background_thread_starts(self, mock_post):
        """Test background refresh thread starts"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=True
        )
        
        try:
            # Background thread should be running
            assert manager._refresh_thread is not None
            assert manager._refresh_thread.is_alive()
            assert manager._refresh_thread.daemon is True
            
            # Stop immediately to prevent hanging
            manager.stop()
            if manager._refresh_thread:
                manager._refresh_thread.join(timeout=2)
        except Exception:
            # Ensure cleanup even on failure
            manager.stop()
            if manager._refresh_thread:
                manager._refresh_thread.join(timeout=2)
            raise
    
    @patch('requests.post')
    def test_background_thread_not_started_when_disabled(self, mock_post):
        """Test background thread doesn't start when auto_refresh=False"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False
        )
        
        try:
            # Background thread should not be running
            assert manager._refresh_thread is None
        finally:
            manager.stop()

