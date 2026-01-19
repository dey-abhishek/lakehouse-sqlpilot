"""
OAuth Authentication Endpoints
Provides OAuth 2.0 login and token management
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional
import secrets
import structlog

router = APIRouter(prefix="/auth", tags=["authentication"])
logger = structlog.get_logger()

# OAuth state store (in production, use Redis)
_oauth_states = {}


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: Optional[int] = None
    refresh_token: Optional[str] = None
    scope: Optional[str] = None


class OAuthCallbackRequest(BaseModel):
    code: str
    state: str


@router.get("/login")
async def oauth_login(
    request: Request,
    redirect_uri: str = Query(..., description="Redirect URI after authentication")
):
    """
    Initiate OAuth login flow
    
    Redirects user to Databricks OAuth authorization page
    """
    try:
        from security.oauth import get_authorization_url, OAUTH_ENABLED
        
        if not OAUTH_ENABLED:
            raise HTTPException(
                status_code=501,
                detail="OAuth authentication is not enabled"
            )
        
        # Generate state for CSRF protection
        state = secrets.token_urlsafe(32)
        
        # Store state with redirect URI
        _oauth_states[state] = {
            'redirect_uri': redirect_uri,
            'created_at': __import__('datetime').datetime.now(__import__('datetime').timezone.utc)
        }
        
        # Clean up old states (older than 10 minutes)
        _cleanup_oauth_states()
        
        # Get authorization URL
        callback_uri = f"{request.base_url}auth/callback"
        auth_url = get_authorization_url(
            redirect_uri=callback_uri,
            state=state,
            scopes=['openid', 'profile', 'email', 'offline_access', 'sql']
        )
        
        logger.info("oauth_login_initiated", state=state)
        
        return RedirectResponse(url=auth_url)
        
    except Exception as e:
        logger.error("oauth_login_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"OAuth login failed: {str(e)}")


@router.get("/callback")
async def oauth_callback(
    code: str = Query(..., description="Authorization code"),
    state: str = Query(..., description="State parameter"),
    error: Optional[str] = Query(None, description="Error from OAuth provider")
):
    """
    OAuth callback endpoint
    
    Handles the OAuth callback from Databricks
    """
    if error:
        logger.warning("oauth_callback_error", error=error)
        raise HTTPException(status_code=400, detail=f"OAuth error: {error}")
    
    # Validate state
    if state not in _oauth_states:
        raise HTTPException(status_code=400, detail="Invalid or expired state parameter")
    
    state_data = _oauth_states.pop(state)
    redirect_uri = state_data['redirect_uri']
    
    try:
        from security.oauth import exchange_authorization_code
        
        # Exchange code for tokens
        token_response = exchange_authorization_code(
            code=code,
            redirect_uri=redirect_uri
        )
        
        logger.info("oauth_callback_success")
        
        # In production, you might want to:
        # 1. Store refresh token securely
        # 2. Set access token in secure HTTP-only cookie
        # 3. Redirect to frontend with session
        
        # For now, return tokens (in production, use secure cookies)
        return TokenResponse(**token_response)
        
    except Exception as e:
        logger.error("oauth_callback_error", error=str(e))
        raise HTTPException(status_code=500, detail=f"Token exchange failed: {str(e)}")


@router.post("/refresh")
async def refresh_token(refresh_token: str):
    """
    Refresh access token
    
    Use refresh token to get a new access token
    """
    try:
        from security.oauth import refresh_access_token
        
        token_response = refresh_access_token(refresh_token)
        
        logger.info("oauth_token_refreshed")
        
        return TokenResponse(**token_response)
        
    except Exception as e:
        logger.error("oauth_refresh_error", error=str(e))
        raise HTTPException(status_code=401, detail=f"Token refresh failed: {str(e)}")


@router.post("/logout")
async def oauth_logout():
    """
    Logout user
    
    Invalidate current session (in production, revoke tokens)
    """
    # In production:
    # 1. Revoke access token
    # 2. Revoke refresh token
    # 3. Clear session
    # 4. Clear cookies
    
    logger.info("oauth_logout")
    
    return {"message": "Logged out successfully"}


def _cleanup_oauth_states():
    """Remove expired OAuth states"""
    import datetime
    
    now = datetime.datetime.now(datetime.timezone.utc)
    expired_states = [
        state for state, data in _oauth_states.items()
        if (now - data['created_at']).total_seconds() > 600  # 10 minutes
    ]
    
    for state in expired_states:
        del _oauth_states[state]


