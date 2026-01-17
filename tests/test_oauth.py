"""
OAuth Authentication Tests
Tests OAuth 2.0 token validation and authentication flow
"""

import pytest
import jwt
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock
import os

# Test imports
from security.oauth import (
    validate_oauth_token,
    validate_token_type,
    exchange_authorization_code,
    refresh_access_token,
    get_authorization_url,
    OAuthError,
    _validate_jwt_token,
    _validate_via_userinfo
)


# Test data
MOCK_JWT_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6InRlc3Qta2V5In0.eyJzdWIiOiJ1c2VyQGV4YW1wbGUuY29tIiwiZW1haWwiOiJ1c2VyQGV4YW1wbGUuY29tIiwicHJlZmVycmVkX3VzZXJuYW1lIjoidXNlckBleGFtcGxlLmNvbSIsInJvbGVzIjpbInVzZXIiXSwiZ3JvdXBzIjpbImRhdGEtZW5naW5lZXJzIl0sImV4cCI6OTk5OTk5OTk5OX0.signature"
MOCK_OPAQUE_TOKEN = "dapi1234567890abcdef"
MOCK_OAUTH_TOKEN = "oauth_token_abc123"


class TestTokenTypeValidation:
    """Test token type detection"""
    
    @patch('jwt.get_unverified_header')
    def test_validate_jwt_token_type(self, mock_get_header):
        """Test JWT token detection"""
        # Mock JWT header
        mock_get_header.return_value = {'alg': 'RS256', 'typ': 'JWT'}
        
        token_type = validate_token_type(MOCK_JWT_TOKEN)
        assert token_type == 'jwt'
    
    def test_validate_api_key_token_type(self):
        """Test API key detection"""
        token_type = validate_token_type("dapi1234567890")
        assert token_type == 'api_key'
    
    def test_validate_oauth_token_type(self):
        """Test OAuth token detection"""
        token_type = validate_token_type(MOCK_OAUTH_TOKEN)
        assert token_type == 'oauth'


class TestJWTValidation:
    """Test JWT token validation"""
    
    @patch('security.oauth.get_jwks_client')
    def test_validate_jwt_success(self, mock_jwks_client):
        """Test successful JWT validation"""
        # Mock JWKS client
        mock_signing_key = MagicMock()
        mock_signing_key.key = "test-key"
        mock_jwks_client.return_value.get_signing_key_from_jwt.return_value = mock_signing_key
        
        # Mock JWT decode
        with patch('jwt.decode') as mock_decode:
            mock_decode.return_value = {
                'sub': 'user@example.com',
                'email': 'user@example.com',
                'preferred_username': 'user@example.com',
                'roles': ['user', 'admin'],
                'groups': ['data-engineers'],
                'exp': 9999999999
            }
            
            user_info = _validate_jwt_token(MOCK_JWT_TOKEN)
            
            assert user_info is not None
            assert user_info['user'] == 'user@example.com'
            assert user_info['email'] == 'user@example.com'
            assert 'admin' in user_info['roles']
            assert 'data-engineers' in user_info['groups']
            assert user_info['token_type'] == 'oauth_jwt'
    
    @patch('security.oauth.get_jwks_client')
    def test_validate_expired_jwt(self, mock_jwks_client):
        """Test expired JWT token"""
        mock_signing_key = MagicMock()
        mock_signing_key.key = "test-key"
        mock_jwks_client.return_value.get_signing_key_from_jwt.return_value = mock_signing_key
        
        with patch('jwt.decode') as mock_decode:
            mock_decode.side_effect = jwt.ExpiredSignatureError("Token expired")
            
            with pytest.raises(OAuthError, match="Token has expired"):
                _validate_jwt_token(MOCK_JWT_TOKEN)
    
    @patch('security.oauth.get_jwks_client')
    def test_validate_invalid_jwt(self, mock_jwks_client):
        """Test invalid JWT token"""
        mock_signing_key = MagicMock()
        mock_signing_key.key = "test-key"
        mock_jwks_client.return_value.get_signing_key_from_jwt.return_value = mock_signing_key
        
        with patch('jwt.decode') as mock_decode:
            mock_decode.side_effect = jwt.InvalidTokenError("Invalid token")
            
            with pytest.raises(OAuthError, match="Invalid JWT token"):
                _validate_jwt_token(MOCK_JWT_TOKEN)


class TestUserinfoValidation:
    """Test userinfo endpoint validation"""
    
    @patch('requests.get')
    def test_validate_via_userinfo_success(self, mock_get):
        """Test successful userinfo validation"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'sub': 'user@example.com',
            'email': 'user@example.com',
            'preferred_username': 'user@example.com',
            'roles': ['user'],
            'groups': ['data-team']
        }
        mock_get.return_value = mock_response
        
        user_info = _validate_via_userinfo(MOCK_OPAQUE_TOKEN)
        
        assert user_info is not None
        assert user_info['user'] == 'user@example.com'
        assert user_info['token_type'] == 'oauth_opaque'
    
    @patch('requests.get')
    def test_validate_via_userinfo_unauthorized(self, mock_get):
        """Test unauthorized userinfo request"""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        with pytest.raises(OAuthError, match="Userinfo endpoint returned 401"):
            _validate_via_userinfo(MOCK_OPAQUE_TOKEN)
    
    @patch('requests.get')
    def test_validate_via_userinfo_network_error(self, mock_get):
        """Test network error during userinfo validation"""
        import requests
        mock_get.side_effect = requests.RequestException("Network error")
        
        with pytest.raises(OAuthError, match="Failed to validate token via userinfo"):
            _validate_via_userinfo(MOCK_OPAQUE_TOKEN)


class TestTokenCaching:
    """Test token caching mechanism"""
    
    @patch('security.oauth._validate_jwt_token')
    @patch('security.oauth._token_cache', {})  # Clear cache
    def test_token_cache_hit(self, mock_validate):
        """Test token cache hit"""
        # First call - should validate
        mock_validate.return_value = {
            'user': 'user@example.com',
            'email': 'user@example.com',
            'roles': ['user'],
            'exp': int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
        }
        
        with patch.dict(os.environ, {'DATABRICKS_SERVER_HOSTNAME': 'test.databricks.com'}):
            user_info1 = validate_oauth_token("test_token")
            
            # Second call - should use cache
            user_info2 = validate_oauth_token("test_token")
            
            assert user_info1 == user_info2
            # Should only call validation once
            assert mock_validate.call_count == 1
    
    @patch('security.oauth._validate_jwt_token')
    @patch('security.oauth._token_cache', {})  # Clear cache
    def test_token_cache_expired(self, mock_validate):
        """Test expired token cache"""
        # Mock successful validation
        mock_validate.return_value = {
            'user': 'user@example.com',
            'email': 'user@example.com',
            'roles': ['user'],
            'exp': int((datetime.now(timezone.utc) - timedelta(hours=1)).timestamp())
        }
        
        with patch.dict(os.environ, {'DATABRICKS_SERVER_HOSTNAME': 'test.databricks.com'}):
            # First call caches the token
            user_info = validate_oauth_token("expired_token")
            
            # Manually expire the cache entry
            from security import oauth
            if "expired_token" in oauth._token_cache:
                oauth._token_cache["expired_token"]['expires_at'] = datetime.now(timezone.utc) - timedelta(hours=1)
            
            # Second call should re-validate
            user_info2 = validate_oauth_token("expired_token")
            
            # Should have called validation twice
            assert mock_validate.call_count == 2


class TestCodeExchange:
    """Test authorization code exchange"""
    
    @patch('requests.post')
    def test_exchange_code_success(self, mock_post):
        """Test successful code exchange"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'access_token_123',
            'refresh_token': 'refresh_token_456',
            'expires_in': 3600,
            'token_type': 'bearer'
        }
        mock_post.return_value = mock_response
        
        # Patch the environment check in the function
        with patch('security.oauth.OAUTH_CLIENT_ID', 'test-client-id'):
            with patch('security.oauth.OAUTH_CLIENT_SECRET', 'test-secret'):
                with patch('security.oauth.DATABRICKS_HOST', 'test.databricks.com'):
                    result = exchange_authorization_code('auth_code_123', 'https://app.com/callback')
                    
                    assert result['access_token'] == 'access_token_123'
                    assert result['refresh_token'] == 'refresh_token_456'
    
    @patch('requests.post')
    def test_exchange_code_failure(self, mock_post):
        """Test failed code exchange"""
        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = 'Invalid code'
        mock_post.return_value = mock_response
        
        with patch('security.oauth.OAUTH_CLIENT_ID', 'test-client-id'):
            with patch('security.oauth.OAUTH_CLIENT_SECRET', 'test-secret'):
                with patch('security.oauth.DATABRICKS_HOST', 'test.databricks.com'):
                    with pytest.raises(OAuthError, match="Token exchange failed"):
                        exchange_authorization_code('invalid_code', 'https://app.com/callback')
    
    def test_exchange_code_missing_credentials(self):
        """Test code exchange without credentials"""
        with patch('security.oauth.OAUTH_CLIENT_ID', ''):
            with patch('security.oauth.OAUTH_CLIENT_SECRET', ''):
                with pytest.raises(OAuthError, match="OAuth client credentials not configured"):
                    exchange_authorization_code('code', 'https://app.com/callback')


class TestTokenRefresh:
    """Test token refresh"""
    
    @patch('requests.post')
    def test_refresh_token_success(self, mock_post):
        """Test successful token refresh"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'access_token': 'new_access_token',
            'expires_in': 3600,
            'token_type': 'bearer'
        }
        mock_post.return_value = mock_response
        
        with patch('security.oauth.OAUTH_CLIENT_ID', 'test-client-id'):
            with patch('security.oauth.OAUTH_CLIENT_SECRET', 'test-secret'):
                with patch('security.oauth.DATABRICKS_HOST', 'test.databricks.com'):
                    result = refresh_access_token('refresh_token_123')
                    
                    assert result['access_token'] == 'new_access_token'
    
    @patch('requests.post')
    def test_refresh_token_failure(self, mock_post):
        """Test failed token refresh"""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_post.return_value = mock_response
        
        with patch('security.oauth.OAUTH_CLIENT_ID', 'test-client-id'):
            with patch('security.oauth.OAUTH_CLIENT_SECRET', 'test-secret'):
                with patch('security.oauth.DATABRICKS_HOST', 'test.databricks.com'):
                    with pytest.raises(OAuthError, match="Token refresh failed"):
                        refresh_access_token('invalid_refresh_token')


class TestAuthorizationURL:
    """Test authorization URL generation"""
    
    def test_get_authorization_url(self):
        """Test authorization URL generation"""
        with patch('security.oauth.OAUTH_CLIENT_ID', 'test-client-id'):
            with patch('security.oauth.DATABRICKS_HOST', 'test.databricks.com'):
                url = get_authorization_url(
                    redirect_uri='https://app.com/callback',
                    state='state123',
                    scopes=['openid', 'profile', 'email']
                )
                
                assert 'test.databricks.com/oidc/v1/authorize' in url
                assert 'client_id=test-client-id' in url
                assert 'response_type=code' in url
                assert 'state=state123' in url
                assert 'scope=openid' in url
    
    def test_get_authorization_url_missing_client_id(self):
        """Test authorization URL without client ID"""
        with patch('security.oauth.OAUTH_CLIENT_ID', ''):
            with pytest.raises(OAuthError, match="OAuth client ID not configured"):
                get_authorization_url('https://app.com/callback', 'state123')


class TestOAuthIntegration:
    """Integration tests for OAuth flow"""
    
    @patch('security.oauth._validate_jwt_token')
    @patch.dict(os.environ, {'DATABRICKS_SERVER_HOSTNAME': 'test.databricks.com'})
    def test_validate_oauth_token_jwt_success(self, mock_validate_jwt):
        """Test full OAuth token validation with JWT"""
        mock_validate_jwt.return_value = {
            'user': 'user@example.com',
            'email': 'user@example.com',
            'roles': ['user'],
            'token_type': 'oauth_jwt'
        }
        
        user_info = validate_oauth_token(MOCK_JWT_TOKEN)
        
        assert user_info['user'] == 'user@example.com'
        assert user_info['token_type'] == 'oauth_jwt'
    
    @patch('security.oauth._validate_via_userinfo')
    @patch('security.oauth._validate_jwt_token')
    @patch.dict(os.environ, {'DATABRICKS_SERVER_HOSTNAME': 'test.databricks.com'})
    def test_validate_oauth_token_fallback_to_userinfo(self, mock_jwt, mock_userinfo):
        """Test fallback to userinfo endpoint"""
        # JWT validation fails
        mock_jwt.side_effect = Exception("Not a JWT")
        
        # Userinfo succeeds
        mock_userinfo.return_value = {
            'user': 'user@example.com',
            'email': 'user@example.com',
            'roles': ['user'],
            'token_type': 'oauth_opaque'
        }
        
        user_info = validate_oauth_token("opaque_token")
        
        assert user_info['user'] == 'user@example.com'
        assert user_info['token_type'] == 'oauth_opaque'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])

