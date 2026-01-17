"""
Tests for OAuth Token Manager

Tests automatic token refresh, thread safety, and error handling.
"""

import pytest
from unittest.mock import patch, MagicMock, Mock
import time
from datetime import datetime, timedelta, timezone
import threading

# Mock modules before importing
with patch('scripts.security.secrets_manager.get_secret') as mock_get_secret:
    mock_get_secret.return_value = None
    from security.oauth_manager import (
        OAuthTokenManager,
        get_token_manager,
        get_current_token,
        stop_token_manager
    )


class TestOAuthTokenManager:
    """Test OAuth Token Manager functionality"""
    
    @pytest.fixture
    def mock_secrets(self):
        """Mock secrets manager"""
        with patch('security.oauth_manager.get_secret') as mock:
            mock.side_effect = lambda key: {
                'oauth-client-id': 'test-client-id',
                'oauth-client-secret': 'test-client-secret',
                'oauth-refresh-token': 'test-refresh-token',
                'databricks-host': 'test.cloud.databricks.com'
            }.get(key)
            yield mock
    
    @pytest.fixture
    def mock_token_response(self):
        """Mock successful token response"""
        return {
            'access_token': 'new-access-token-' + str(time.time()),
            'expires_in': 3600,
            'refresh_token': 'new-refresh-token'
        }
    
    def test_token_manager_initialization(self, mock_secrets, mock_token_response):
        """Test token manager initializes correctly"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            assert manager._access_token is not None
            assert manager._token_expiry is not None
            assert mock_post.called
    
    def test_get_access_token(self, mock_secrets, mock_token_response):
        """Test getting access token"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            token = manager.get_access_token()
            
            assert token == mock_token_response['access_token']
    
    def test_token_refresh_when_expired(self, mock_secrets, mock_token_response):
        """Test token refreshes when expired"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            # Force expiry
            manager._token_expiry = datetime.now(timezone.utc) - timedelta(minutes=1)
            old_token = manager._access_token
            
            # Get token should trigger refresh
            new_token = manager.get_access_token()
            
            # Should have made additional API call
            assert mock_post.call_count >= 2
    
    def test_token_refresh_retry(self, mock_secrets, mock_token_response):
        """Test token refresh retries on failure"""
        with patch('requests.post') as mock_post:
            # Fail twice, then succeed
            mock_post.side_effect = [
                Mock(status_code=500, text='Server error'),
                Mock(status_code=500, text='Server error'),
                Mock(status_code=200, json=lambda: mock_token_response)
            ]
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            assert manager._access_token == mock_token_response['access_token']
            assert mock_post.call_count == 3
    
    def test_token_refresh_failure(self, mock_secrets):
        """Test token refresh warns on failure during initialization"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 500
            mock_post.return_value.text = 'Server error'
            
            # Should not raise, but warn
            manager = OAuthTokenManager(auto_refresh=False)
            
            # Token should be None
            assert manager._access_token is None
            assert mock_post.call_count == 3  # max_retries
            
            # But calling get_access_token should raise
            with pytest.raises(Exception) as exc_info:
                manager.get_access_token()
            
            assert 'Failed to refresh OAuth token' in str(exc_info.value)
    
    def test_should_refresh(self, mock_secrets, mock_token_response):
        """Test should_refresh logic"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            # Fresh token should not need refresh
            assert not manager._should_refresh()
            
            # Token expiring in 10 minutes should need refresh
            manager._token_expiry = datetime.now(timezone.utc) + timedelta(minutes=10)
            assert manager._should_refresh()
            
            # Token expiring in 15 minutes should not need refresh
            manager._token_expiry = datetime.now(timezone.utc) + timedelta(minutes=15)
            assert not manager._should_refresh()
    
    def test_force_refresh(self, mock_secrets, mock_token_response):
        """Test force refresh"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            old_token = manager._access_token
            
            # Force refresh
            success = manager.force_refresh()
            
            assert success
            assert mock_post.call_count >= 2
    
    def test_get_token_info(self, mock_secrets, mock_token_response):
        """Test get_token_info"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            info = manager.get_token_info()
            
            assert info['has_token']
            assert info['configured']
            assert not info['needs_refresh']
            assert 'expiry' in info
    
    def test_thread_safety(self, mock_secrets, mock_token_response):
        """Test concurrent token access is thread-safe"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            tokens = []
            errors = []
            
            def get_token_thread():
                try:
                    token = manager.get_access_token()
                    tokens.append(token)
                except Exception as e:
                    errors.append(str(e))
            
            # Start 10 concurrent threads
            threads = [threading.Thread(target=get_token_thread) for _ in range(10)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            
            # All threads should succeed
            assert len(errors) == 0
            assert len(tokens) == 10
            
            # All tokens should be the same
            assert len(set(tokens)) == 1
    
    def test_auto_refresh_disabled(self, mock_secrets, mock_token_response):
        """Test auto-refresh can be disabled"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=False)
            
            assert not manager.auto_refresh
            assert manager._refresh_thread is None or not manager._refresh_thread.is_alive()
    
    def test_auto_refresh_enabled(self, mock_secrets, mock_token_response):
        """Test auto-refresh starts background thread"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=True)
            
            assert manager.auto_refresh
            assert manager._refresh_thread is not None
            assert manager._refresh_thread.is_alive()
            
            # Cleanup
            manager.stop_auto_refresh()
    
    def test_stop_auto_refresh(self, mock_secrets, mock_token_response):
        """Test stopping auto-refresh"""
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            mock_post.return_value.json.return_value = mock_token_response
            
            manager = OAuthTokenManager(auto_refresh=True)
            assert manager._refresh_thread.is_alive()
            
            manager.stop_auto_refresh()
            time.sleep(0.1)
            
            assert not manager._refresh_thread.is_alive()


class TestGlobalTokenManager:
    """Test global token manager functions"""
    
    @pytest.fixture(autouse=True)
    def cleanup(self):
        """Cleanup global manager between tests"""
        yield
        stop_token_manager()
    
    def test_get_token_manager_singleton(self):
        """Test get_token_manager returns singleton"""
        with patch('security.oauth_manager.OAuthTokenManager') as mock_class:
            mock_instance = MagicMock()
            mock_class.return_value = mock_instance
            
            manager1 = get_token_manager()
            manager2 = get_token_manager()
            
            assert manager1 is manager2
            assert mock_class.call_count == 1
    
    def test_get_current_token(self):
        """Test get_current_token convenience function"""
        with patch('security.oauth_manager.get_token_manager') as mock_get_manager:
            mock_manager = MagicMock()
            mock_manager.get_access_token.return_value = 'test-token'
            mock_get_manager.return_value = mock_manager
            
            token = get_current_token()
            
            assert token == 'test-token'
            assert mock_manager.get_access_token.called


@pytest.mark.integration
class TestOAuthIntegration:
    """Integration tests for OAuth (requires real credentials)"""
    
    @pytest.mark.skip(reason="Requires real OAuth credentials")
    def test_real_token_refresh(self):
        """Test with real OAuth credentials"""
        # This test requires real credentials to be set in environment
        manager = OAuthTokenManager(auto_refresh=False)
        token = manager.get_access_token()
        
        assert token is not None
        assert len(token) > 0
        
        info = manager.get_token_info()
        assert info['has_token']
        assert info['configured']

