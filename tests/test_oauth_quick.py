"""
Quick tests for OAuth Token Manager and Databricks Client

Fast unit tests without background threads or timing dependencies.
"""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timezone, timedelta

from infrastructure.oauth_token_manager import OAuthToken
from infrastructure.databricks_client import DatabricksClient


class TestOAuthToken:
    """Test OAuthToken dataclass"""
    
    def test_token_not_expired(self):
        """Test token that hasn't expired"""
        expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        token = OAuthToken(access_token="test", expires_at=expires_at)
        
        assert not token.is_expired()
        assert token.seconds_until_expiry() > 3500
        assert token.minutes_until_expiry() >= 59
    
    def test_token_expired(self):
        """Test expired token"""
        expires_at = datetime.now(timezone.utc) - timedelta(minutes=5)
        token = OAuthToken(access_token="test", expires_at=expires_at)
        
        assert token.is_expired()
        assert token.seconds_until_expiry() == 0


class TestOAuthTokenManagerBasics:
    """Test basic OAuth manager functionality (no background threads)"""
    
    @patch('requests.post')
    def test_initialization(self, mock_post):
        """Test manager initializes"""
        from infrastructure.oauth_token_manager import OAuthTokenManager, stop_oauth_token_manager
        
        stop_oauth_token_manager()  # Clean up
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'test_token',
            'token_type': 'Bearer',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="test_client",
            client_secret="test_secret",
            auto_refresh=False  # Disable background thread
        )
        
        assert manager._token is not None
        assert manager._token.access_token == 'test_token'
        
        manager.stop()
    
    @patch('requests.post')
    def test_get_token(self, mock_post):
        """Test get_token returns valid token"""
        from infrastructure.oauth_token_manager import OAuthTokenManager, stop_oauth_token_manager
        
        stop_oauth_token_manager()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'my_token',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="client",
            client_secret="secret",
            auto_refresh=False
        )
        
        token = manager.get_token()
        assert token == 'my_token'
        
        manager.stop()
    
    @patch('requests.post')
    def test_get_headers(self, mock_post):
        """Test get_headers returns correct format"""
        from infrastructure.oauth_token_manager import OAuthTokenManager, stop_oauth_token_manager
        
        stop_oauth_token_manager()
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'token123',
            'expires_in': 3600
        }
        mock_post.return_value = mock_response
        
        manager = OAuthTokenManager(
            databricks_host="test.databricks.com",
            client_id="client",
            client_secret="secret",
            auto_refresh=False
        )
        
        headers = manager.get_headers()
        assert headers['Authorization'] == 'Bearer token123'
        assert headers['Content-Type'] == 'application/json'
        
        manager.stop()


class TestDatabricksClientBasics:
    """Test basic Databricks client functionality"""
    
    @patch('infrastructure.databricks_client.get_oauth_token_manager')
    def test_client_initialization(self, mock_manager):
        """Test client initializes correctly"""
        manager = Mock()
        manager.databricks_host = "test.databricks.com"
        manager.get_headers.return_value = {'Authorization': 'Bearer test'}
        mock_manager.return_value = manager
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        
        assert client.databricks_host == "test.databricks.com"
        assert client.base_url == "https://test.databricks.com/api/2.0"
    
    @patch('requests.request')
    @patch('infrastructure.databricks_client.get_oauth_token_manager')
    def test_list_catalogs(self, mock_manager, mock_request):
        """Test listing catalogs"""
        # Mock OAuth manager
        manager = Mock()
        manager.databricks_host = "test.databricks.com"
        manager.get_headers.return_value = {'Authorization': 'Bearer test'}
        mock_manager.return_value = manager
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"catalogs": [{"name": "main"}]}'
        mock_response.json.return_value = {'catalogs': [{'name': 'main'}]}
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        catalogs = client.list_catalogs()
        
        assert len(catalogs) == 1
        assert catalogs[0]['name'] == 'main'
    
    @patch('requests.request')
    @patch('infrastructure.databricks_client.get_oauth_token_manager')
    def test_list_warehouses(self, mock_manager, mock_request):
        """Test listing warehouses"""
        # Mock OAuth manager
        manager = Mock()
        manager.databricks_host = "test.databricks.com"
        manager.get_headers.return_value = {'Authorization': 'Bearer test'}
        mock_manager.return_value = manager
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"warehouses": [{"id": "wh1", "name": "Main"}]}'
        mock_response.json.return_value = {'warehouses': [{'id': 'wh1', 'name': 'Main'}]}
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        warehouses = client.list_warehouses()
        
        assert len(warehouses) == 1
        assert warehouses[0]['id'] == 'wh1'
    
    @patch('requests.request')
    @patch('infrastructure.databricks_client.get_oauth_token_manager')
    def test_execute_statement(self, mock_manager, mock_request):
        """Test executing SQL statement"""
        # Mock OAuth manager
        manager = Mock()
        manager.databricks_host = "test.databricks.com"
        manager.get_headers.return_value = {'Authorization': 'Bearer test'}
        mock_manager.return_value = manager
        
        # Mock API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.content = b'{"statement_id": "stmt123", "status": {"state": "SUCCEEDED"}}'
        mock_response.json.return_value = {
            'statement_id': 'stmt123',
            'status': {'state': 'SUCCEEDED'}
        }
        mock_request.return_value = mock_response
        
        client = DatabricksClient(databricks_host="test.databricks.com")
        result = client.execute_statement(
            warehouse_id='wh1',
            statement='SELECT 1',
            catalog='main',
            schema='default'
        )
        
        assert result['statement_id'] == 'stmt123'
        assert result['status']['state'] == 'SUCCEEDED'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])


