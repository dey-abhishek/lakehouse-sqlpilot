"""
Centralized OAuth Token Manager for Service Principal

Manages OAuth tokens for Databricks service principal authentication.
Auto-refreshes tokens before expiry for all Databricks services:
- Unity Catalog API
- SQL Warehouse API
- Jobs API
- Clusters API
- Any other Databricks REST API

Usage:
    from infrastructure.oauth_token_manager import get_oauth_token_manager
    
    manager = get_oauth_token_manager()
    token = manager.get_token()  # Returns valid OAuth token (auto-refreshes)
"""

import os
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any
from dataclasses import dataclass

import structlog

logger = structlog.get_logger(__name__)

# Singleton instance
_token_manager_instance: Optional['OAuthTokenManager'] = None
_token_manager_lock = threading.Lock()


@dataclass
class OAuthToken:
    """OAuth token with metadata"""
    access_token: str
    expires_at: datetime
    token_type: str = "Bearer"
    scope: Optional[str] = None
    
    def is_expired(self) -> bool:
        """Check if token is expired"""
        return datetime.now(timezone.utc) >= self.expires_at
    
    def seconds_until_expiry(self) -> int:
        """Get seconds until token expires"""
        return max(0, int((self.expires_at - datetime.now(timezone.utc)).total_seconds()))
    
    def minutes_until_expiry(self) -> int:
        """Get minutes until token expires"""
        return self.seconds_until_expiry() // 60


class OAuthTokenManager:
    """
    Manages OAuth tokens for Databricks service principal.
    
    Features:
    - Automatic token refresh before expiry
    - Thread-safe token access
    - Background refresh thread
    - Configurable refresh buffer (default: 5 minutes before expiry)
    """
    
    def __init__(self,
                 databricks_host: Optional[str] = None,
                 client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 auto_refresh: bool = True,
                 refresh_buffer_minutes: int = 5):
        """
        Initialize OAuth Token Manager
        
        Args:
            databricks_host: Databricks workspace hostname
            client_id: Service principal client ID
            client_secret: Service principal client secret
            auto_refresh: Enable automatic token refresh (default: True)
            refresh_buffer_minutes: Refresh token N minutes before expiry (default: 5)
        """
        self.databricks_host = databricks_host or os.getenv("DATABRICKS_SERVER_HOSTNAME")
        self.client_id = client_id or os.getenv("DATABRICKS_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("DATABRICKS_CLIENT_SECRET")
        self.refresh_buffer_minutes = refresh_buffer_minutes
        
        if not all([self.databricks_host, self.client_id, self.client_secret]):
            raise ValueError(
                "OAuth token manager requires: "
                "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET"
            )
        
        self._token: Optional[OAuthToken] = None
        self._lock = threading.Lock()
        self._refresh_thread: Optional[threading.Thread] = None
        self._stop_refresh = threading.Event()
        
        # Generate initial token (no lock needed yet - not started)
        self._generate_new_token()
        
        # Start auto-refresh thread
        if auto_refresh:
            self._start_refresh_thread()
            
        logger.info(
            "oauth_token_manager_initialized",
            host=self.databricks_host,
            client_id=self.client_id,
            auto_refresh=auto_refresh,
            refresh_buffer_minutes=refresh_buffer_minutes
        )
    
    def _generate_new_token(self) -> None:
        """
        Generate a new OAuth token (internal implementation)
        
        This method does the actual HTTP request and creates the OAuthToken.
        It should NOT be called directly - use _refresh_token() instead.
        """
        try:
            import requests
            
            token_url = f"https://{self.databricks_host}/oidc/v1/token"
            
            response = requests.post(
                token_url,
                data={
                    'grant_type': 'client_credentials',
                    'scope': 'all-apis'
                },
                auth=(self.client_id, self.client_secret),
                timeout=30
            )
            
            if response.status_code != 200:
                raise Exception(f"Token request failed: {response.status_code} - {response.text}")
            
            token_data = response.json()
            
            # Parse token response
            access_token = token_data['access_token']
            expires_in = token_data.get('expires_in', 3600)  # Default 1 hour
            token_type = token_data.get('token_type', 'Bearer')
            scope = token_data.get('scope')
            
            # Calculate expiration time
            expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
            
            # Create new token (caller handles locking)
            self._token = OAuthToken(
                access_token=access_token,
                expires_at=expires_at,
                token_type=token_type,
                scope=scope
            )
            
            logger.info(
                "oauth_token_refreshed",
                expires_at=expires_at.isoformat(),
                expires_in_seconds=expires_in,
                token_type=token_type
            )
            
        except Exception as e:
            logger.error("oauth_token_refresh_failed", error=str(e))
            raise
    
    def _refresh_token(self) -> None:
        """
        Refresh the OAuth token (thread-safe)
        
        This method acquires the lock and calls _generate_new_token().
        """
        with self._lock:
            self._generate_new_token()
    
    def get_token(self, force_refresh: bool = False) -> str:
        """
        Get current OAuth token (auto-refreshes if needed)
        
        Args:
            force_refresh: Force token refresh even if not expired
            
        Returns:
            Valid OAuth access token
        """
        with self._lock:
            # Check if token needs refresh
            if force_refresh or self._token is None or self._needs_refresh():
                logger.info("oauth_token_needs_refresh", force=force_refresh)
                # Call _generate_new_token directly since we already hold the lock
                self._generate_new_token()
            
            return self._token.access_token
    
    def get_authorization_header(self) -> str:
        """Get Authorization header value"""
        return f"Bearer {self.get_token()}"
    
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with Authorization"""
        return {
            'Authorization': self.get_authorization_header(),
            'Content-Type': 'application/json'
        }
    
    def _needs_refresh(self) -> bool:
        """Check if token needs refresh"""
        if self._token is None:
            return True
        
        # Refresh if expired or within buffer time
        buffer = timedelta(minutes=self.refresh_buffer_minutes)
        return datetime.now(timezone.utc) >= (self._token.expires_at - buffer)
    
    def _start_refresh_thread(self) -> None:
        """Start background token refresh thread"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            logger.warning("oauth_refresh_thread_already_running")
            return
        
        self._stop_refresh.clear()
        self._refresh_thread = threading.Thread(
            target=self._refresh_loop,
            name="OAuthTokenRefresh",
            daemon=True
        )
        self._refresh_thread.start()
        
        logger.info("oauth_token_refresh_thread_started")
    
    def _refresh_loop(self) -> None:
        """Background loop to refresh token periodically"""
        while not self._stop_refresh.is_set():
            try:
                # Sleep for 1 minute between checks
                if self._stop_refresh.wait(timeout=60):
                    break
                
                # Check if refresh needed
                if self._needs_refresh():
                    logger.info("oauth_token_auto_refresh_triggered")
                    self._refresh_token()
                
            except Exception as e:
                logger.error("oauth_token_refresh_loop_error", error=str(e))
                # Continue running even if refresh fails
    
    def stop(self) -> None:
        """Stop the token manager and refresh thread"""
        self._stop_refresh.set()
        
        if self._refresh_thread and self._refresh_thread.is_alive():
            self._refresh_thread.join(timeout=5)
        
        logger.info("oauth_token_manager_stopped")
    
    def get_token_info(self) -> Dict[str, Any]:
        """Get current token information"""
        with self._lock:
            if self._token is None:
                return {"status": "no_token"}
            
            return {
                "status": "active",
                "expires_at": self._token.expires_at.isoformat(),
                "seconds_until_expiry": self._token.seconds_until_expiry(),
                "minutes_until_expiry": self._token.minutes_until_expiry(),
                "is_expired": self._token.is_expired(),
                "needs_refresh": self._needs_refresh(),
                "token_type": self._token.token_type,
                "scope": self._token.scope
            }


# Singleton access functions

def get_oauth_token_manager(
    databricks_host: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    auto_refresh: bool = True,
    refresh_buffer_minutes: int = 5
) -> OAuthTokenManager:
    """
    Get global OAuth token manager instance (singleton)
    
    Args:
        databricks_host: Databricks workspace hostname
        client_id: Service principal client ID
        client_secret: Service principal client secret
        auto_refresh: Enable automatic token refresh
        refresh_buffer_minutes: Refresh buffer in minutes
        
    Returns:
        Global OAuthTokenManager instance
    """
    global _token_manager_instance
    
    with _token_manager_lock:
        if _token_manager_instance is None:
            _token_manager_instance = OAuthTokenManager(
                databricks_host=databricks_host,
                client_id=client_id,
                client_secret=client_secret,
                auto_refresh=auto_refresh,
                refresh_buffer_minutes=refresh_buffer_minutes
            )
        
        return _token_manager_instance


def stop_oauth_token_manager() -> None:
    """Stop the global OAuth token manager"""
    global _token_manager_instance
    
    with _token_manager_lock:
        if _token_manager_instance:
            _token_manager_instance.stop()
            _token_manager_instance = None


def get_oauth_token() -> str:
    """
    Convenience function to get current OAuth token
    
    Returns:
        Valid OAuth access token
    """
    manager = get_oauth_token_manager()
    return manager.get_token()


def get_oauth_headers() -> Dict[str, str]:
    """
    Convenience function to get HTTP headers with OAuth token
    
    Returns:
        Dictionary with Authorization header
    """
    manager = get_oauth_token_manager()
    return manager.get_headers()

