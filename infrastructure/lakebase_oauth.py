"""
Lakebase OAuth Token Manager

Manages OAuth tokens for Lakebase Postgres connections with automatic refresh.
Uses the same refresh token mechanism as the main Databricks OAuth.

Features:
- Automatic token refresh before expiration
- Thread-safe token access
- Integration with LakebaseBackend
- Uses same OAuth credentials as main app
"""

import threading
import time
from typing import Optional
from datetime import datetime, timedelta, timezone
import requests
import structlog
from scripts.security.secrets_manager import get_secret

logger = structlog.get_logger(__name__)


class LakebaseOAuthManager:
    """
    Manages OAuth tokens specifically for Lakebase connections
    
    Lakebase uses OAuth tokens as passwords for PostgreSQL connections.
    These tokens also expire every hour and need refresh.
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
        Initialize Lakebase OAuth Manager
        
        Args:
            client_id: OAuth client ID (shared with main app)
            client_secret: OAuth client secret (shared with main app)
            refresh_token: OAuth refresh token (shared with main app)
            token_endpoint: Token endpoint URL
            auto_refresh: Enable automatic background refresh
        """
        # Use same OAuth credentials as main app
        self.client_id = client_id or get_secret("oauth-client-id")
        self.client_secret = client_secret or get_secret("oauth-client-secret")
        self.refresh_token = refresh_token or get_secret("oauth-refresh-token")
        
        # Build token endpoint
        if token_endpoint:
            self.token_endpoint = token_endpoint
        else:
            host = get_secret("databricks-host")
            if host:
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
        
        # Initial token fetch
        if self.refresh_token and self.token_endpoint:
            try:
                self._refresh_access_token()
            except Exception as e:
                logger.warning("lakebase_initial_token_fetch_failed", error=str(e))
        
        # Start auto-refresh thread
        if self.auto_refresh and self._access_token:
            self._start_auto_refresh()
    
    def get_password(self) -> str:
        """
        Get current OAuth token to use as Lakebase password
        
        Returns:
            Valid OAuth access token for use as PostgreSQL password
            
        Raises:
            Exception if token cannot be obtained
        """
        with self._lock:
            if self._should_refresh():
                logger.info("lakebase_token_needs_refresh", 
                           expiry=self._token_expiry.isoformat() if self._token_expiry else None)
                self._refresh_access_token()
            
            if not self._access_token:
                raise Exception("Failed to obtain Lakebase OAuth token")
            
            return self._access_token
    
    def _should_refresh(self) -> bool:
        """Check if token should be refreshed"""
        if not self._access_token or not self._token_expiry:
            return True
        
        # Refresh proactively at 80% of token lifetime
        now = datetime.now(timezone.utc)
        refresh_threshold = self._token_expiry - timedelta(minutes=12)
        
        return now >= refresh_threshold
    
    def _refresh_access_token(self, max_retries: int = 3) -> bool:
        """
        Refresh the access token using refresh token
        
        Args:
            max_retries: Maximum number of retry attempts
            
        Returns:
            True if successful
            
        Raises:
            Exception if all retries fail
        """
        if not self.refresh_token or not self.token_endpoint:
            raise Exception("Lakebase OAuth not configured")
        
        for attempt in range(max_retries):
            try:
                logger.info("refreshing_lakebase_oauth_token", 
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
                        expires_in = token_data.get("expires_in", 3600)
                        self._token_expiry = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
                        
                        if "refresh_token" in token_data:
                            self.refresh_token = token_data["refresh_token"]
                            logger.info("lakebase_refresh_token_updated")
                        
                        logger.info(
                            "lakebase_oauth_token_refreshed",
                            expires_in=expires_in,
                            expiry=self._token_expiry.isoformat()
                        )
                    
                    return True
                else:
                    logger.error(
                        "lakebase_oauth_refresh_failed",
                        status_code=response.status_code,
                        error=response.text[:200]
                    )
                    
            except Exception as e:
                logger.error("lakebase_oauth_refresh_exception", 
                            error=str(e), 
                            attempt=attempt + 1)
            
            # Exponential backoff
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info("retrying_lakebase_oauth_refresh", wait_seconds=wait_time)
                time.sleep(wait_time)
        
        raise Exception(f"Failed to refresh Lakebase OAuth token after {max_retries} attempts")
    
    def _start_auto_refresh(self):
        """Start background thread for automatic token refresh"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            logger.info("lakebase_auto_refresh_already_running")
            return
        
        self._stop_refresh.clear()
        self._refresh_thread = threading.Thread(
            target=self._auto_refresh_loop,
            daemon=True,
            name="Lakebase-OAuth-Refresh"
        )
        self._refresh_thread.start()
        logger.info("lakebase_oauth_auto_refresh_started")
    
    def _auto_refresh_loop(self):
        """Background loop for automatic token refresh"""
        while not self._stop_refresh.is_set():
            try:
                # Check every 5 minutes
                self._stop_refresh.wait(300)
                
                if self._stop_refresh.is_set():
                    break
                
                if self._should_refresh():
                    logger.info("lakebase_auto_refresh_triggered")
                    try:
                        self._refresh_access_token()
                    except Exception as e:
                        logger.error("lakebase_auto_refresh_failed", error=str(e))
                    
            except Exception as e:
                logger.error("lakebase_auto_refresh_loop_error", error=str(e))
        
        logger.info("lakebase_auto_refresh_loop_stopped")
    
    def stop_auto_refresh(self):
        """Stop the auto-refresh thread"""
        if self._refresh_thread and self._refresh_thread.is_alive():
            logger.info("stopping_lakebase_auto_refresh")
            self._stop_refresh.set()
            self._refresh_thread.join(timeout=5)
            logger.info("lakebase_oauth_auto_refresh_stopped")
    
    def get_connection_params(self) -> dict:
        """
        Get connection parameters for psycopg2 with fresh OAuth token
        
        Returns:
            Dictionary of connection parameters
        """
        return {
            'host': get_secret('lakebase-host'),
            'user': get_secret('lakebase-user'),
            'password': self.get_password(),  # Fresh OAuth token
            'dbname': get_secret('lakebase-database') or 'databricks_postgres',
            'port': int(get_secret('lakebase-port') or 5432),
            'sslmode': 'require'
        }


# Global instance
_lakebase_oauth_manager: Optional[LakebaseOAuthManager] = None
_manager_lock = threading.Lock()


def get_lakebase_oauth_manager() -> LakebaseOAuthManager:
    """
    Get or create global Lakebase OAuth manager instance (singleton)
    
    Returns:
        Global LakebaseOAuthManager instance
    """
    global _lakebase_oauth_manager
    if _lakebase_oauth_manager is None:
        with _manager_lock:
            if _lakebase_oauth_manager is None:
                logger.info("initializing_lakebase_oauth_manager")
                _lakebase_oauth_manager = LakebaseOAuthManager(auto_refresh=True)
    return _lakebase_oauth_manager


def get_lakebase_password() -> str:
    """
    Get current valid OAuth token for Lakebase password
    
    This is the main function to use for Lakebase connections.
    
    Returns:
        Valid OAuth access token
        
    Example:
        >>> from infrastructure.lakebase_oauth import get_lakebase_password
        >>> password = get_lakebase_password()
        >>> conn = psycopg2.connect(
        ...     host=host,
        ...     user=user,
        ...     password=password,
        ...     dbname=dbname
        ... )
    """
    return get_lakebase_oauth_manager().get_password()


def stop_lakebase_oauth_manager():
    """Stop the Lakebase OAuth manager and cleanup resources"""
    global _lakebase_oauth_manager
    if _lakebase_oauth_manager:
        logger.info("stopping_lakebase_oauth_manager")
        _lakebase_oauth_manager.stop_auto_refresh()
        _lakebase_oauth_manager = None

