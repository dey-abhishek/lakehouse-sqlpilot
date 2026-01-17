"""
OAuth Token Manager with automatic refresh for Databricks Apps

This module provides automatic OAuth token refresh to avoid 1-hour token expiry issues.
Tokens are refreshed proactively before expiration to ensure uninterrupted service.

Features:
- Automatic token refresh before expiration
- Thread-safe token storage and access
- Retry logic with exponential backoff
- Token caching and reuse
- Health monitoring and logging

Usage:
    from security.oauth_manager import get_current_token
    
    # Get always-fresh token
    token = get_current_token()
    
    # Use with Databricks connector
    conn = sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=get_current_token()
    )
"""

import time
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import requests
import structlog
from scripts.security.secrets_manager import get_secret

logger = structlog.get_logger(__name__)


class OAuthTokenManager:
    """
    Manages OAuth access tokens with automatic refresh
    
    This class handles:
    1. Initial token acquisition using refresh token
    2. Proactive token refresh (at 80% of lifetime)
    3. Thread-safe token access
    4. Automatic retry with exponential backoff
    5. Background refresh thread
    """
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        refresh_token: Optional[str] = None,
        token_endpoint: Optional[str] = None,
        auto_refresh: bool = True
    ):
        """
        Initialize OAuth Token Manager
        
        Args:
            client_id: OAuth client ID (from secrets if None)
            client_secret: OAuth client secret (from secrets if None)
            refresh_token: OAuth refresh token (from secrets if None)
            token_endpoint: Token endpoint URL (from secrets if None)
            auto_refresh: Enable automatic background refresh
        """
        # Load from secrets if not provided
        self.client_id = client_id or get_secret("oauth-client-id")
        self.client_secret = client_secret or get_secret("oauth-client-secret")
        self.refresh_token = refresh_token or get_secret("oauth-refresh-token")
        
        # Build token endpoint
        if token_endpoint:
            self.token_endpoint = token_endpoint
        else:
            host = get_secret("databricks-host")
            if host:
                # Remove https:// if present
                host = host.replace("https://", "").replace("http://", "")
                self.token_endpoint = f"https://{host}/oidc/v1/token"
            else:
                self.token_endpoint = None
        
        # Token state
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[datetime] = None
        self._lock = threading.RLock()
        
        # Auto-refresh configuration
        self.auto_refresh = auto_refresh
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_refresh = threading.Event()
        
        # Initial token fetch (if credentials available)
        if self.refresh_token and self.token_endpoint:
            try:
                self._refresh_access_token()
            except Exception as e:
                logger.warning("initial_token_fetch_failed", error=str(e))
        
        # Start auto-refresh thread
        if self.auto_refresh and self._access_token:
            self._start_auto_refresh()
    
    def get_access_token(self) -> str:
        """
        Get current access token (refreshes if expired)
        
        Returns:
            Valid access token
            
        Raises:
            Exception if token refresh fails
        """
        with self._lock:
            # Check if token needs refresh
            if self._should_refresh():
                logger.info("access_token_needs_refresh", 
                           expiry=self._token_expiry.isoformat() if self._token_expiry else None)
                self._refresh_access_token()
            
            if not self._access_token:
                raise Exception("Failed to obtain access token. Check OAuth configuration.")
            
            return self._access_token
    
    def _should_refresh(self) -> bool:
        """
        Check if token should be refreshed
        
        Returns:
            True if token is missing, expired, or close to expiry
        """
        if not self._access_token or not self._token_expiry:
            return True
        
        # Refresh proactively at 80% of token lifetime
        # For 1hr token (3600s), refresh after 48 minutes (2880s)
        now = datetime.now(timezone.utc)
        refresh_threshold = self._token_expiry - timedelta(minutes=12)
        
        return now >= refresh_threshold
    
    def _refresh_access_token(self, max_retries: int = 3) -> bool:
        """
        Refresh the access token using refresh token
        
        Args:
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if successful, False otherwise
            
        Raises:
            Exception if all retries fail
        """
        if not self.refresh_token or not self.token_endpoint:
            raise Exception("OAuth not configured. Missing refresh_token or token_endpoint.")
        
        for attempt in range(max_retries):
            try:
                logger.info("refreshing_oauth_token", 
                           attempt=attempt + 1, 
                           endpoint=self.token_endpoint)
                
                response = requests.post(
                    self.token_endpoint,
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": self.refresh_token,
                        "client_id": self.client_id,
                        "client_secret": self.client_secret,
                    },
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=30
                )
                
                if response.status_code == 200:
                    token_data = response.json()
                    
                    with self._lock:
                        self._access_token = token_data["access_token"]
                        
                        # Calculate expiry time
                        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
                        self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
                        
                        # Update refresh token if provided (rolling refresh token)
                        if "refresh_token" in token_data:
                            self.refresh_token = token_data["refresh_token"]
                            logger.info("refresh_token_updated")
                        
                        logger.info(
                            "oauth_token_refreshed",
                            expires_in=expires_in,
                            expiry=self._token_expiry.isoformat(),
                            token_prefix=self._access_token[:20] + "..." if self._access_token else None
                        )
                    
                    return True
                else:
                    logger.error(
                        "oauth_refresh_failed",
                        status_code=response.status_code,
                        error=response.text[:200]  # Truncate error
                    )
                    
            except Exception as e:
                logger.error("oauth_refresh_exception", 
                            error=str(e), 
                            attempt=attempt + 1)
            
            # Exponential backoff
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 1s, 2s, 4s
                logger.info("retrying_oauth_refresh", wait_seconds=wait_time)
                time.sleep(wait_time)
        
        raise Exception(f"Failed to refresh OAuth token after {max_retries} attempts")
    
    def _start_auto_refresh(self):
        """Start background thread for automatic token refresh"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            logger.info("auto_refresh_already_running")
            return
        
        self._stop_refresh.clear()
        self._refresh_thread = threading.Thread(
            target=self._auto_refresh_loop,
            daemon=True,
            name="OAuth-Token-Refresh"
        )
        self._refresh_thread.start()
        logger.info("oauth_auto_refresh_started")
    
    def _auto_refresh_loop(self):
        """Background loop for automatic token refresh"""
        while not self._stop_refresh.is_set():
            try:
                # Check every 5 minutes
                self._stop_refresh.wait(300)
                
                if self._stop_refresh.is_set():
                    break
                
                if self._should_refresh():
                    logger.info("auto_refresh_triggered")
                    try:
                        self._refresh_access_token()
                    except Exception as e:
                        logger.error("auto_refresh_failed", error=str(e))
                    
            except Exception as e:
                logger.error("auto_refresh_loop_error", error=str(e))
        
        logger.info("auto_refresh_loop_stopped")
    
    def stop_auto_refresh(self):
        """Stop the auto-refresh thread"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            logger.info("stopping_auto_refresh")
            self._stop_refresh.set()
            self._refresh_thread.join(timeout=5)
            logger.info("oauth_auto_refresh_stopped")
    
    def force_refresh(self) -> bool:
        """
        Force an immediate token refresh
        
        Returns:
            True if successful, False otherwise
        """
        try:
            with self._lock:
                return self._refresh_access_token()
        except Exception as e:
            logger.error("force_refresh_failed", error=str(e))
            return False
    
    def get_token_info(self) -> Dict[str, Any]:
        """
        Get current token state for debugging/monitoring
        
        Returns:
            Dictionary with token status information
        """
        with self._lock:
            return {
                "has_token": self._access_token is not None,
                "token_length": len(self._access_token) if self._access_token else 0,
                "token_prefix": self._access_token[:20] + "..." if self._access_token else None,
                "expiry": self._token_expiry.isoformat() if self._token_expiry else None,
                "needs_refresh": self._should_refresh(),
                "auto_refresh_enabled": self.auto_refresh,
                "refresh_thread_alive": self._refresh_thread.is_alive() if self._refresh_thread else False,
                "configured": bool(self.refresh_token and self.token_endpoint)
            }


# Global token manager instance
_token_manager: Optional[OAuthTokenManager] = None
_manager_lock = threading.Lock()


def get_token_manager() -> OAuthTokenManager:
    """
    Get or create global token manager instance (singleton)
    
    Returns:
        Global OAuthTokenManager instance
    """
    global _token_manager
    if _token_manager is None:
        with _manager_lock:
            # Double-check locking
            if _token_manager is None:
                logger.info("initializing_oauth_token_manager")
                _token_manager = OAuthTokenManager(auto_refresh=True)
    return _token_manager


def get_current_token() -> str:
    """
    Get current valid access token
    
    This is the main function to use throughout the app.
    It automatically handles token refresh.
    
    Returns:
        Valid OAuth access token
        
    Raises:
        Exception if token cannot be obtained
        
    Example:
        >>> from security.oauth_manager import get_current_token
        >>> token = get_current_token()
        >>> conn = sql.connect(..., access_token=token)
    """
    return get_token_manager().get_access_token()


def stop_token_manager():
    """
    Stop the token manager and cleanup resources
    
    Call this on application shutdown.
    """
    global _token_manager
    if _token_manager:
        logger.info("stopping_token_manager")
        _token_manager.stop_auto_refresh()
        _token_manager = None

