"""
OAuth Authentication for Databricks
Implements OAuth 2.0 token validation for secure user authentication
"""

import os
import requests
import structlog
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from functools import lru_cache
import jwt
from jwt import PyJWKClient

logger = structlog.get_logger()

# OAuth Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
OAUTH_TOKEN_ENDPOINT = f"https://{DATABRICKS_HOST}/oidc/v1/token"
OAUTH_JWKS_ENDPOINT = f"https://{DATABRICKS_HOST}/oidc/v1/keys"
OAUTH_USERINFO_ENDPOINT = f"https://{DATABRICKS_HOST}/oidc/v1/userinfo"

# OAuth Settings
OAUTH_ENABLED = os.getenv("SQLPILOT_OAUTH_ENABLED", "true").lower() == "true"
OAUTH_CLIENT_ID = os.getenv("SQLPILOT_OAUTH_CLIENT_ID", "")
OAUTH_CLIENT_SECRET = os.getenv("SQLPILOT_OAUTH_CLIENT_SECRET", "")
OAUTH_AUDIENCE = os.getenv("SQLPILOT_OAUTH_AUDIENCE", "")

# Token cache (in production, use Redis)
_token_cache: Dict[str, Dict[str, Any]] = {}
_token_cache_ttl = 300  # 5 minutes


class OAuthError(Exception):
    """OAuth authentication error"""
    pass


@lru_cache(maxsize=1)
def get_jwks_client():
    """Get JWKS client for token validation (cached)"""
    if not DATABRICKS_HOST:
        raise OAuthError("DATABRICKS_SERVER_HOSTNAME not configured")
    return PyJWKClient(OAUTH_JWKS_ENDPOINT)


def validate_oauth_token(token: str) -> Dict[str, Any]:
    """
    Validate Databricks OAuth token
    
    Args:
        token: OAuth access token
        
    Returns:
        User information dictionary
        
    Raises:
        OAuthError: If token is invalid
    """
    # Check cache first
    if token in _token_cache:
        cached = _token_cache[token]
        if datetime.now(timezone.utc) < cached['expires_at']:
            logger.debug("oauth_token_cache_hit", user=cached['user_info'].get('user'))
            return cached['user_info']
        else:
            # Remove expired token from cache
            del _token_cache[token]
    
    try:
        # Method 1: Validate JWT token (if it's a JWT)
        user_info = _validate_jwt_token(token)
        if user_info:
            _cache_token(token, user_info)
            return user_info
    except Exception as e:
        logger.debug("jwt_validation_failed", error=str(e))
    
    try:
        # Method 2: Call userinfo endpoint (for opaque tokens)
        user_info = _validate_via_userinfo(token)
        if user_info:
            _cache_token(token, user_info)
            return user_info
    except Exception as e:
        logger.warning("oauth_validation_failed", error=str(e))
        raise OAuthError(f"Invalid OAuth token: {str(e)}")


def _validate_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    """Validate JWT token using JWKS"""
    try:
        jwks_client = get_jwks_client()
        
        # Get signing key
        signing_key = jwks_client.get_signing_key_from_jwt(token)
        
        # Decode and validate token
        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=OAUTH_AUDIENCE if OAUTH_AUDIENCE else None,
            options={"verify_exp": True}
        )
        
        # Extract user information
        user_info = {
            'user': decoded.get('preferred_username') or decoded.get('email') or decoded.get('sub'),
            'email': decoded.get('email'),
            'sub': decoded.get('sub'),
            'roles': decoded.get('roles', ['user']),
            'groups': decoded.get('groups', []),
            'exp': decoded.get('exp'),
            'token_type': 'oauth_jwt'
        }
        
        logger.info("oauth_jwt_validated", user=user_info['user'])
        return user_info
        
    except jwt.ExpiredSignatureError:
        raise OAuthError("Token has expired")
    except jwt.InvalidTokenError as e:
        raise OAuthError(f"Invalid JWT token: {str(e)}")
    except Exception as e:
        logger.debug("jwt_decode_error", error=str(e))
        return None


def _validate_via_userinfo(token: str) -> Optional[Dict[str, Any]]:
    """Validate token by calling userinfo endpoint"""
    try:
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.get(
            OAUTH_USERINFO_ENDPOINT,
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            
            user_info = {
                'user': data.get('preferred_username') or data.get('email') or data.get('sub'),
                'email': data.get('email'),
                'sub': data.get('sub'),
                'roles': data.get('roles', ['user']),
                'groups': data.get('groups', []),
                'token_type': 'oauth_opaque'
            }
            
            logger.info("oauth_userinfo_validated", user=user_info['user'])
            return user_info
        else:
            raise OAuthError(f"Userinfo endpoint returned {response.status_code}")
            
    except requests.RequestException as e:
        raise OAuthError(f"Failed to validate token via userinfo: {str(e)}")


def _cache_token(token: str, user_info: Dict[str, Any]):
    """Cache validated token"""
    # Determine expiry
    if 'exp' in user_info:
        expires_at = datetime.fromtimestamp(user_info['exp'], tz=timezone.utc)
    else:
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=_token_cache_ttl)
    
    _token_cache[token] = {
        'user_info': user_info,
        'expires_at': expires_at
    }
    
    # Clean up old cache entries
    _cleanup_token_cache()


def _cleanup_token_cache():
    """Remove expired tokens from cache"""
    now = datetime.now(timezone.utc)
    expired_tokens = [
        token for token, data in _token_cache.items()
        if now >= data['expires_at']
    ]
    for token in expired_tokens:
        del _token_cache[token]


def exchange_authorization_code(code: str, redirect_uri: str) -> Dict[str, Any]:
    """
    Exchange authorization code for access token
    
    Args:
        code: Authorization code from OAuth callback
        redirect_uri: Redirect URI used in authorization request
        
    Returns:
        Token response with access_token, refresh_token, etc.
    """
    if not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
        raise OAuthError("OAuth client credentials not configured")
    
    data = {
        'grant_type': 'authorization_code',
        'code': code,
        'redirect_uri': redirect_uri,
        'client_id': OAUTH_CLIENT_ID,
        'client_secret': OAUTH_CLIENT_SECRET
    }
    
    try:
        response = requests.post(
            OAUTH_TOKEN_ENDPOINT,
            data=data,
            timeout=10
        )
        
        if response.status_code == 200:
            token_response = response.json()
            logger.info("oauth_code_exchanged", 
                       has_access_token=bool(token_response.get('access_token')),
                       has_refresh_token=bool(token_response.get('refresh_token')))
            return token_response
        else:
            raise OAuthError(f"Token exchange failed: {response.status_code} - {response.text}")
            
    except requests.RequestException as e:
        raise OAuthError(f"Token exchange request failed: {str(e)}")


def refresh_access_token(refresh_token: str) -> Dict[str, Any]:
    """
    Refresh access token using refresh token
    
    Args:
        refresh_token: Refresh token
        
    Returns:
        New token response
    """
    if not OAUTH_CLIENT_ID or not OAUTH_CLIENT_SECRET:
        raise OAuthError("OAuth client credentials not configured")
    
    data = {
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
        'client_id': OAUTH_CLIENT_ID,
        'client_secret': OAUTH_CLIENT_SECRET
    }
    
    try:
        response = requests.post(
            OAUTH_TOKEN_ENDPOINT,
            data=data,
            timeout=10
        )
        
        if response.status_code == 200:
            token_response = response.json()
            logger.info("oauth_token_refreshed")
            return token_response
        else:
            raise OAuthError(f"Token refresh failed: {response.status_code}")
            
    except requests.RequestException as e:
        raise OAuthError(f"Token refresh request failed: {str(e)}")


def get_authorization_url(redirect_uri: str, state: str, scopes: list = None) -> str:
    """
    Get OAuth authorization URL
    
    Args:
        redirect_uri: Redirect URI for callback
        state: State parameter for CSRF protection
        scopes: List of OAuth scopes
        
    Returns:
        Authorization URL
    """
    if not OAUTH_CLIENT_ID:
        raise OAuthError("OAuth client ID not configured")
    
    if scopes is None:
        scopes = ['openid', 'profile', 'email', 'offline_access']
    
    scope_str = ' '.join(scopes)
    
    params = {
        'client_id': OAUTH_CLIENT_ID,
        'response_type': 'code',
        'redirect_uri': redirect_uri,
        'scope': scope_str,
        'state': state
    }
    
    param_str = '&'.join([f'{k}={requests.utils.quote(v)}' for k, v in params.items()])
    auth_url = f"https://{DATABRICKS_HOST}/oidc/v1/authorize?{param_str}"
    
    return auth_url


def validate_token_type(token: str) -> str:
    """
    Determine token type (OAuth, JWT, or API key)
    
    Returns:
        'oauth', 'jwt', or 'api_key'
    """
    # Check if it's a JWT (has 3 parts separated by dots)
    if token.count('.') == 2:
        try:
            # Try to decode header
            header = jwt.get_unverified_header(token)
            # Check if it's signed with RS256 (OAuth JWT)
            if header.get('alg') == 'RS256':
                return 'jwt'
            # Otherwise it's an internal JWT
            return 'jwt'
        except:
            pass
    
    # Check if it looks like a Databricks PAT (starts with 'dapi')
    if token.startswith('dapi'):
        return 'api_key'
    
    # Assume OAuth token
    return 'oauth'

