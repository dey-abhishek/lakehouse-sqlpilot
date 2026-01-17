"""
Security module initialization
"""

from .middleware import (
    authenticate_request,
    check_rate_limit,
    require_roles,
    sanitize_input,
    audit_log,
    mask_sensitive_data,
    create_access_token,
    verify_token,
    verify_api_key,
    load_security_config,
    SecurityConfig,
    AuthenticationError,
    AuthorizationError,
    RateLimitError,
    SecurityMiddleware,
    get_current_user,
    _rate_limit_store,
    _failed_auth_attempts
)

# OAuth imports (optional, only if configured)
try:
    from .oauth import (
        validate_oauth_token,
        exchange_authorization_code,
        refresh_access_token,
        get_authorization_url,
        OAuthError
    )
    oauth_available = True
except ImportError:
    oauth_available = False

__all__ = [
    'authenticate_request',
    'check_rate_limit',
    'require_roles',
    'sanitize_input',
    'audit_log',
    'mask_sensitive_data',
    'create_access_token',
    'verify_token',
    'verify_api_key',
    'load_security_config',
    'SecurityConfig',
    'AuthenticationError',
    'AuthorizationError',
    'RateLimitError',
    'SecurityMiddleware',
    'get_current_user',
    '_rate_limit_store',
    '_failed_auth_attempts'
]

# Add OAuth exports if available
if oauth_available:
    __all__.extend([
        'validate_oauth_token',
        'exchange_authorization_code',
        'refresh_access_token',
        'get_authorization_url',
        'OAuthError'
    ])

