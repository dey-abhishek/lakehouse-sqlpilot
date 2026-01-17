"""
Security Middleware for Lakehouse SQLPilot API
Implements authentication, authorization, rate limiting, and audit logging
Supports OAuth 2.0, JWT, and API keys
"""

from fastapi import HTTPException, Security, Depends, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List, Callable
import time
import hashlib
import os
import structlog
from functools import wraps
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import jwt

logger = structlog.get_logger()

# Security bearer scheme
security = HTTPBearer()

# Configuration
SECRET_KEY = os.getenv("SQLPILOT_SECRET_KEY", "change-me-in-production")
TOKEN_ALGORITHM = "HS256"
TOKEN_EXPIRY_HOURS = 24

# OAuth Configuration
OAUTH_ENABLED = os.getenv("SQLPILOT_OAUTH_ENABLED", "true").lower() == "true"

# Rate limiting store (in production, use Redis)
_rate_limit_store = defaultdict(list)
_failed_auth_attempts = defaultdict(list)

# API Keys cache
_API_KEYS_HASHES = set()


def load_security_config():
    """Load/reload security configuration from environment"""
    global _API_KEYS_HASHES
    api_keys_str = os.getenv("SQLPILOT_API_KEYS", "")
    if api_keys_str:
        _API_KEYS_HASHES = {
            hashlib.sha256(key.strip().encode()).hexdigest()
            for key in api_keys_str.split(",") if key.strip()
        }
    else:
        _API_KEYS_HASHES = set()


# Load config on import
load_security_config()


class SecurityConfig:
    """Security configuration"""
    
    # Authentication
    REQUIRE_AUTH = os.getenv("SQLPILOT_REQUIRE_AUTH", "true").lower() == "true"
    API_KEYS = os.getenv("SQLPILOT_API_KEYS", "").split(",")
    
    # Rate limiting
    RATE_LIMIT_ENABLED = os.getenv("SQLPILOT_RATE_LIMIT_ENABLED", "true").lower() == "true"
    RATE_LIMIT_REQUESTS = int(os.getenv("SQLPILOT_RATE_LIMIT_REQUESTS", "100"))
    RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("SQLPILOT_RATE_LIMIT_WINDOW", "60"))
    
    # Failed auth tracking
    MAX_FAILED_AUTH_ATTEMPTS = int(os.getenv("SQLPILOT_MAX_FAILED_AUTH", "5"))
    FAILED_AUTH_WINDOW_SECONDS = int(os.getenv("SQLPILOT_FAILED_AUTH_WINDOW", "300"))
    
    # CORS
    ALLOWED_ORIGINS = os.getenv("SQLPILOT_ALLOWED_ORIGINS", "*").split(",")
    
    # Request limits
    MAX_REQUEST_SIZE = int(os.getenv("SQLPILOT_MAX_REQUEST_SIZE", "10485760"))  # 10MB
    
    # Audit logging
    AUDIT_LOG_ENABLED = os.getenv("SQLPILOT_AUDIT_LOG_ENABLED", "true").lower() == "true"


class SecurityException(HTTPException):
    """Base security exception"""
    pass


class AuthenticationError(SecurityException):
    """Authentication failed"""
    def __init__(self, detail: str = "Authentication failed"):
        super().__init__(status_code=401, detail=detail)


class AuthorizationError(SecurityException):
    """Authorization failed"""
    def __init__(self, detail: str = "Insufficient permissions"):
        super().__init__(status_code=403, detail=detail)


class RateLimitError(SecurityException):
    """Rate limit exceeded"""
    def __init__(self, detail: str = "Rate limit exceeded"):
        super().__init__(status_code=429, detail=detail)


def create_access_token(user: str, email: str, roles: List[str] = None) -> str:
    """Create JWT access token"""
    payload = {
        "user": user,
        "email": email,
        "roles": roles or ["user"],
        "exp": datetime.now(timezone.utc) + timedelta(hours=TOKEN_EXPIRY_HOURS),
        "iat": datetime.now(timezone.utc)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=TOKEN_ALGORITHM)


def verify_token(token: str) -> dict:
    """
    Verify authentication token (OAuth, JWT, or API key)
    Supports multiple authentication methods
    """
    # Try OAuth token validation first if enabled
    if OAUTH_ENABLED:
        try:
            from security.oauth import validate_oauth_token, validate_token_type
            
            token_type = validate_token_type(token)
            
            if token_type == 'oauth' or token_type == 'jwt':
                user_info = validate_oauth_token(token)
                logger.info("oauth_authentication_success", 
                           user=user_info.get('user'),
                           token_type=token_type)
                return user_info
        except Exception as e:
            logger.debug("oauth_validation_failed", error=str(e))
            # Fall through to try other methods
    
    # Try JWT token validation (internal tokens)
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[TOKEN_ALGORITHM])
        logger.info("jwt_authentication_success", user=payload.get('user'))
        return payload
    except jwt.ExpiredSignatureError:
        raise AuthenticationError("Token has expired")
    except jwt.InvalidTokenError:
        # Not a valid JWT, try API key
        pass
    
    # Try API key validation
    if verify_api_key(token):
        logger.info("api_key_authentication_success")
        return {
            'user': 'api_key_user',
            'email': 'api_key@system',
            'roles': ['user'],
            'auth_method': 'api_key'
        }
    
    raise AuthenticationError("Invalid token")


def verify_api_key(api_key: str) -> bool:
    """Verify API key against configured keys"""
    # If no API keys configured, deny access (secure default)
    if not _API_KEYS_HASHES:
        return False
    
    hashed_api_key = hashlib.sha256(api_key.encode()).hexdigest()
    return hashed_api_key in _API_KEYS_HASHES


async def authenticate_request(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(security)
) -> dict:
    """
    Authenticate request using Bearer token or API key
    Returns user info if authenticated
    """
    if not SecurityConfig.REQUIRE_AUTH:
        # Auth disabled for development
        return {"user": "dev_user", "email": "dev@databricks.com", "roles": ["admin"]}
    
    client_ip = request.client.host
    
    # Check for account lockout due to failed attempts
    check_failed_auth_attempts(client_ip)
    
    token = credentials.credentials
    
    try:
        # Try JWT token first
        payload = verify_token(token)
        
        # Log successful authentication
        if SecurityConfig.AUDIT_LOG_ENABLED:
            logger.info(
                "authentication_success",
                user=payload.get("user"),
                email=payload.get("email"),
                client_ip=client_ip,
                method=request.method,
                path=request.url.path
            )
        
        # Reset failed attempts on success
        if client_ip in _failed_auth_attempts:
            del _failed_auth_attempts[client_ip]
        
        return payload
        
    except AuthenticationError:
        # Try API key as fallback
        if verify_api_key(token):
            if SecurityConfig.AUDIT_LOG_ENABLED:
                logger.info(
                    "authentication_success_api_key",
                    client_ip=client_ip,
                    method=request.method,
                    path=request.url.path
                )
            return {"user": "api_key_user", "email": "api@databricks.com", "roles": ["user"]}
        else:
            # Record failed attempt
            record_failed_auth_attempt(client_ip)
            
            if SecurityConfig.AUDIT_LOG_ENABLED:
                logger.warning(
                    "authentication_failed",
                    client_ip=client_ip,
                    method=request.method,
                    path=request.url.path
                )
            
            raise AuthenticationError("Invalid credentials")


def check_failed_auth_attempts(client_ip: str):
    """Check if IP is locked out due to failed auth attempts"""
    if client_ip not in _failed_auth_attempts:
        return
    
    # Clean old attempts
    cutoff = time.time() - SecurityConfig.FAILED_AUTH_WINDOW_SECONDS
    _failed_auth_attempts[client_ip] = [
        ts for ts in _failed_auth_attempts[client_ip] if ts > cutoff
    ]
    
    # Check if locked out
    if len(_failed_auth_attempts[client_ip]) >= SecurityConfig.MAX_FAILED_AUTH_ATTEMPTS:
        logger.warning(
            "account_lockout",
            client_ip=client_ip,
            failed_attempts=len(_failed_auth_attempts[client_ip])
        )
        raise AuthenticationError(
            f"Too many failed authentication attempts. Try again in {SecurityConfig.FAILED_AUTH_WINDOW_SECONDS // 60} minutes."
        )


def record_failed_auth_attempt(client_ip: str):
    """Record a failed authentication attempt"""
    _failed_auth_attempts[client_ip].append(time.time())


def check_rate_limit(client_id: str, limit: int = None, window: int = None):
    """
    Check rate limit for a client
    Raises RateLimitError if limit exceeded
    """
    if not SecurityConfig.RATE_LIMIT_ENABLED:
        return
    
    limit = limit or SecurityConfig.RATE_LIMIT_REQUESTS
    window = window or SecurityConfig.RATE_LIMIT_WINDOW_SECONDS
    
    now = time.time()
    cutoff = now - window
    
    # Clean old requests
    _rate_limit_store[client_id] = [
        ts for ts in _rate_limit_store[client_id] if ts > cutoff
    ]
    
    # Check limit
    if len(_rate_limit_store[client_id]) >= limit:
        logger.warning(
            "rate_limit_exceeded",
            client_id=client_id,
            requests=len(_rate_limit_store[client_id]),
            limit=limit,
            window=window
        )
        raise RateLimitError(
            f"Rate limit exceeded: {limit} requests per {window} seconds"
        )
    
    # Record this request
    _rate_limit_store[client_id].append(now)


async def rate_limit_middleware(request: Request, call_next):
    """Middleware to apply rate limiting"""
    if SecurityConfig.RATE_LIMIT_ENABLED:
        client_id = request.client.host
        try:
            check_rate_limit(client_id)
        except RateLimitError as e:
            return JSONResponse(
                status_code=e.status_code,
                content={"detail": e.detail}
            )
    
    response = await call_next(request)
    return response


def require_roles(required_roles: List[str]):
    """
    Decorator to require specific roles for an endpoint
    Usage: @require_roles(["admin", "data_engineer"])
    """
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # Get user info from kwargs (injected by authenticate_request)
            user_info = kwargs.get("user_info")
            if not user_info:
                raise AuthorizationError("User information not found")
            
            user_roles = user_info.get("roles", [])
            
            # Check if user has any of the required roles
            if not any(role in user_roles for role in required_roles):
                logger.warning(
                    "authorization_failed",
                    user=user_info.get("user"),
                    required_roles=required_roles,
                    user_roles=user_roles
                )
                raise AuthorizationError(
                    f"Requires one of: {', '.join(required_roles)}"
                )
            
            return await func(*args, **kwargs)
        return wrapper
    return decorator


def sanitize_input(text: str, max_length: int = 10000) -> str:
    """
    Sanitize user input to prevent injection attacks
    """
    if not text:
        return ""
    
    # Limit length
    text = text[:max_length]
    
    # Remove null bytes
    text = text.replace('\x00', '')
    
    # Remove control characters except newlines and tabs
    text = ''.join(char for char in text if char in '\n\t' or not char.isspace() or char == ' ')
    
    return text


def audit_log(event_type: str, user: str = "unknown", **details):
    """
    Log security-relevant events for audit trail
    Simplified signature: audit_log("event", user="email", key1=val1, key2=val2)
    """
    if not SecurityConfig.AUDIT_LOG_ENABLED:
        return
    
    logger.info(
        "security_audit",
        event_type=event_type,
        user=user,
        timestamp=datetime.now(timezone.utc).isoformat(),
        **details
    )


def mask_sensitive_data(data: dict, sensitive_fields: List[str] = None) -> dict:
    """
    Mask sensitive data in logs
    """
    if sensitive_fields is None:
        sensitive_fields = ["password", "token", "api_key", "secret", "credentials"]
    
    masked = data.copy()
    for key, value in masked.items():
        if any(sensitive in key.lower() for sensitive in sensitive_fields):
            if isinstance(value, str):
                masked[key] = "***REDACTED***"
            else:
                masked[key] = "***"
    
    return masked


# Security headers middleware
async def security_headers_middleware(request: Request, call_next):
    """Add security headers to all responses"""
    response = await call_next(request)
    
    # Security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self'"
    
    return response


# Request size limiter
async def request_size_limiter(request: Request, call_next):
    """Limit request body size"""
    content_length = request.headers.get("content-length")
    
    if content_length and int(content_length) > SecurityConfig.MAX_REQUEST_SIZE:
        return JSONResponse(
            status_code=413,
            content={"detail": f"Request body too large. Maximum: {SecurityConfig.MAX_REQUEST_SIZE} bytes"}
        )
    
    response = await call_next(request)
    return response


from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware


class SecurityMiddleware(BaseHTTPMiddleware):
    """
    Combined security middleware that applies:
    - Request size limiting
    - Rate limiting
    - Security headers
    - Audit logging
    """
    
    async def dispatch(self, request: Request, call_next):
        """Process request through security checks"""
        
        # 1. Check request size
        content_length = request.headers.get("content-length")
        if content_length and int(content_length) > SecurityConfig.MAX_REQUEST_SIZE:
            return JSONResponse(
                status_code=413,
                content={"detail": f"Request body too large. Maximum: {SecurityConfig.MAX_REQUEST_SIZE} bytes"}
            )
        
        # 2. Apply rate limiting (skip for health check)
        if SecurityConfig.RATE_LIMIT_ENABLED and request.url.path != "/health":
            client_id = request.client.host if request.client else "unknown"
            try:
                check_rate_limit(client_id)
            except RateLimitError as e:
                return JSONResponse(
                    status_code=e.status_code,
                    content={"detail": e.detail}
                )
        
        # 3. Process request
        try:
            response = await call_next(request)
        except Exception as e:
            logger.error("request_processing_error", error=str(e), path=request.url.path)
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error"}
            )
        
        # 4. Add security headers
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        
        return response


async def get_current_user(request: Request):
    """
    FastAPI dependency to get current authenticated user
    Returns user info dict or raises HTTPException
    If auth is disabled, returns a default user
    """
    # If authentication is disabled, return default user
    if not SecurityConfig.REQUIRE_AUTH:
        return {
            "user": "dev_user",
            "email": "dev@example.com",
            "roles": ["admin", "user"]
        }
    
    # Extract authorization header
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        raise HTTPException(status_code=401, detail="Missing authentication token")
    
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authentication scheme")
    
    token = auth_header[7:]  # Remove "Bearer " prefix
    
    try:
        payload = verify_token(token)
        return {
            "user": payload.get("user"),
            "email": payload.get("email"),
            "roles": payload.get("roles", ["user"])
        }
    except AuthenticationError as e:
        raise HTTPException(status_code=e.status_code, detail=e.detail)


