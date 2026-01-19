"""
Lakebase Backend for Distributed State Management
Uses Databricks Lakebase Postgres for OLTP operations
https://docs.databricks.com/aws/en/oltp/
"""

import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from typing import Optional, Dict, Any, List
import json
import time
import os
import structlog
import hashlib
from datetime import datetime, timedelta
from contextlib import contextmanager

logger = structlog.get_logger()

# Import Smart Credential Manager for multi-method fallback
try:
    from infrastructure.smart_credentials import (
        get_smart_credential_manager,
        get_lakebase_credentials_with_fallback
    )
    SMART_CREDENTIALS_AVAILABLE = True
    logger.info("lakebase_smart_credentials_available",
               message="Using smart credential manager with automatic fallback")
except ImportError:
    SMART_CREDENTIALS_AVAILABLE = False
    logger.warning("lakebase_smart_credentials_not_available")

# Also try to import individual methods for backward compatibility
try:
    from infrastructure.lakebase_credentials import get_lakebase_password, get_lakebase_username
    LAKEBASE_AUTO_REFRESH_AVAILABLE = True
    CREDENTIAL_METHOD = "database_api"
    logger.info("lakebase_credential_api_available", 
               message="Using Databricks Database Credential API for auto-refresh")
except ImportError:
    try:
        from infrastructure.lakebase_oauth import get_lakebase_password
        get_lakebase_username = None
        LAKEBASE_AUTO_REFRESH_AVAILABLE = True
        CREDENTIAL_METHOD = "oauth"
        logger.info("lakebase_oauth_available", 
                   message="Using OAuth for Lakebase authentication")
    except ImportError:
        LAKEBASE_AUTO_REFRESH_AVAILABLE = False
        CREDENTIAL_METHOD = None
        logger.warning("lakebase_auto_refresh_not_available", 
                       message="Neither Database Credential API nor OAuth available - using static credentials")


class LakebaseBackend:
    """
    Lakebase PostgreSQL backend for distributed state management
    
    Uses Databricks-managed PostgreSQL (Lakebase) for OLTP workloads:
    - Rate limiting
    - Authentication sessions
    - Token caching
    - Unity Catalog caching
    - Circuit breaker state
    - Execution metrics
    
    Benefits:
    - Autoscaling compute
    - Scale-to-zero for cost savings
    - Point-in-time restore
    - Native Databricks integration
    - SQL standard (PostgreSQL)
    """
    
    def __init__(self, 
                 host: Optional[str] = None,
                 port: int = 5432,
                 database: Optional[str] = None,
                 user: Optional[str] = None,
                 password: Optional[str] = None,
                 use_oauth: bool = True,
                 min_connections: int = 2,
                 max_connections: int = 20):
        """
        Initialize Lakebase backend with connection pooling
        
        Args:
            host: Lakebase Postgres host
            port: Database port (default 5432)
            database: Database name (defaults to LAKEBASE_DATABASE env var or "databricks_postgres")
            user: Database user
            password: Database password (or will use OAuth if use_oauth=True)
            use_oauth: Use OAuth auto-refresh for password (recommended)
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
        """
        # Get credentials from environment
        self.host = host or os.getenv("LAKEBASE_HOST")
        self.port = port
        self.database = database or os.getenv("LAKEBASE_DATABASE") or "databricks_postgres"
        self.user = user or os.getenv("LAKEBASE_USER")
        self.use_auto_refresh = use_oauth and (LAKEBASE_AUTO_REFRESH_AVAILABLE or SMART_CREDENTIALS_AVAILABLE)
        self.credential_method = CREDENTIAL_METHOD if self.use_auto_refresh else None
        self.use_smart_fallback = SMART_CREDENTIALS_AVAILABLE
        
        # Get password: Use smart credential manager with automatic fallback
        if self.use_auto_refresh:
            try:
                if self.use_smart_fallback:
                    # Use smart credential manager with automatic fallback
                    logger.info("lakebase_using_smart_credentials",
                               message="Using smart credential manager with multi-method fallback")
                    self.user, self.password = get_lakebase_credentials_with_fallback()
                    manager = get_smart_credential_manager()
                    self.credential_method = manager.get_last_successful_method()
                    logger.info("lakebase_smart_credentials_success",
                               method=self.credential_method,
                               message=f"Credentials obtained via {self.credential_method}")
                else:
                    # Use individual method (backward compatibility)
                    self.password = get_lakebase_password()
                    # Try to get dynamic username if available (Database API provides it)
                    if get_lakebase_username:
                        self.user = get_lakebase_username()
                    logger.info("lakebase_using_auto_refresh", 
                               method=self.credential_method,
                               message=f"Auto-refresh enabled via {self.credential_method}")
            except Exception as e:
                logger.error("lakebase_auto_refresh_failed",
                            method=self.credential_method,
                            error=str(e))
                # If explicit password provided, use it as last resort
                if password:
                    self.password = password
                    self.use_auto_refresh = False
                    self.credential_method = "explicit"
                    logger.warning("lakebase_using_explicit_password",
                                  message="Using explicitly provided password")
                else:
                    raise ValueError(
                        "Failed to obtain Lakebase credentials via auto-refresh. "
                        f"Error: {str(e)}. "
                        "Ensure DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are configured."
                    )
        else:
            # use_oauth=False - explicit password required
            self.password = password
            if not self.password:
                raise ValueError(
                    "Lakebase password required when use_oauth=False. "
                    "Either provide password parameter or enable auto-refresh with use_oauth=True"
                )
            self.credential_method = "explicit"
        
        if not all([self.host, self.user, self.password]):
            missing = []
            if not self.host:
                missing.append("LAKEBASE_HOST")
            if not self.user:
                missing.append("LAKEBASE_USER")
            if not self.password:
                missing.append("password (via Database Credential API or explicit parameter)")
            
            raise ValueError(
                f"Lakebase credentials not configured. Missing: {', '.join(missing)}. "
                "For auto-refresh (recommended), ensure: "
                "DATABRICKS_SERVER_HOSTNAME, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET, "
                "LAKEBASE_INSTANCE_NAME, LAKEBASE_HOST, and LAKEBASE_USER are set."
            )
        
        # Create connection pool
        try:
            self.pool = ThreadedConnectionPool(
                min_connections,
                max_connections,
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                options='-c statement_timeout=30000'  # 30 second timeout
            )
            
            logger.info(
                "lakebase_connected",
                host=self.host,
                database=self.database,
                pool_size=f"{min_connections}-{max_connections}"
            )
            
            # Initialize schema
            self._initialize_schema()
            
            # Start background cleanup job
            self._schedule_cleanup()
            
        except Exception as e:
            logger.error("lakebase_connection_failed", error=str(e))
            raise
    
    @contextmanager
    def _get_connection(self):
        """
        Context manager for getting and returning connections
        
        Includes automatic retry with credential refresh on auth failures
        """
        max_retries = 2
        last_error = None
        
        for attempt in range(max_retries):
            try:
                conn = self.pool.getconn()
                try:
                    yield conn
                    return  # Success!
                finally:
                    self.pool.putconn(conn)
                    
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                
                # Check if it's an authentication error
                is_auth_error = any(indicator in error_str for indicator in [
                    'invalid authorization',
                    'authentication failed',
                    'password authentication failed',
                    'fatal: invalid',
                    'could not connect to server'
                ])
                
                if is_auth_error and attempt < max_retries - 1:
                    logger.warning(
                        "lakebase_connection_auth_error_retrying",
                        attempt=attempt + 1,
                        error=str(e)[:200],
                        message="Authentication error detected, refreshing credentials"
                    )
                    
                    # Try to refresh credentials with fallback
                    try:
                        self._refresh_credentials_with_fallback()
                        logger.info("lakebase_credentials_refreshed_after_auth_error",
                                   method=self.credential_method)
                        # Continue to next retry attempt
                        continue
                    except Exception as refresh_error:
                        logger.error("lakebase_credential_refresh_failed",
                                    error=str(refresh_error))
                        # Fall through to raise original error
                
                # If not an auth error, or refresh failed, or last attempt, raise
                raise
        
        # Should not reach here, but just in case
        if last_error:
            raise last_error
    
    def _refresh_credentials_with_fallback(self):
        """
        Refresh credentials using smart fallback mechanism
        
        This method tries to get fresh credentials and reconnect the pool.
        It uses the smart credential manager to try all available methods.
        """
        try:
            if self.use_smart_fallback and SMART_CREDENTIALS_AVAILABLE:
                # Use smart credential manager with automatic fallback
                logger.info("refreshing_credentials_with_smart_fallback")
                manager = get_smart_credential_manager()
                new_username, new_password = manager.get_credentials()
                self.credential_method = manager.get_last_successful_method()
                
            elif self.use_auto_refresh and LAKEBASE_AUTO_REFRESH_AVAILABLE:
                # Use individual method
                logger.info("refreshing_credentials_with_auto_refresh",
                           method=self.credential_method)
                new_password = get_lakebase_password()
                new_username = get_lakebase_username() if get_lakebase_username else self.user
                
            else:
                # Try static credentials as last resort
                logger.info("refreshing_credentials_from_environment")
                new_username = os.getenv("LAKEBASE_USER")
                new_password = os.getenv("LAKEBASE_PASSWORD")
                self.credential_method = "static"
            
            if not new_username or not new_password:
                raise ValueError("No valid credentials available")
            
            # Store pool config
            min_conn = self.pool.minconn
            max_conn = self.pool.maxconn
            
            # Close existing pool
            if hasattr(self, 'pool') and self.pool:
                logger.info("closing_existing_connection_pool")
                self.pool.closeall()
            
            # Create new pool with fresh credentials
            self.user = new_username
            self.password = new_password
            self.pool = ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                sslmode='require'
            )
            
            logger.info("lakebase_credentials_refreshed_successfully",
                       method=self.credential_method)
            
        except Exception as e:
            logger.error("credential_refresh_failed", error=str(e))
            raise
    
    def _initialize_schema(self):
        """Create tables if they don't exist - gracefully handle existing tables"""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                # Check if all tables exist first
                cur.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('rate_limits', 'auth_sessions', 'token_cache', 
                                      'failed_auth_attempts', 'unity_catalog_cache', 
                                      'circuit_breaker_state', 'execution_metrics', 
                                      'plan_cache', 'plans', 'plan_executions')
                """)
                existing_tables = cur.fetchone()[0]
                
                if existing_tables >= 10:
                    # All core tables exist, skip initialization entirely
                    logger.info("lakebase_schema_already_initialized", 
                               existing_tables=existing_tables,
                               message="All tables exist, skipping schema initialization")
                    return
                
                # Some tables missing - just log and continue
                # Don't try to create them since we don't have ownership
                logger.info("lakebase_schema_incomplete", 
                           existing_tables=existing_tables,
                           message=f"Only {existing_tables}/10 tables exist - assuming tables managed externally")

    def _schedule_cleanup(self):
        """Schedule periodic cleanup of expired data"""
        # Note: In production, this should be a scheduled job or cron
        # For now, we'll do manual cleanup on each check
        pass
    
    def cleanup_expired_data(self):
        """Clean up expired data from all tables"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Clean expired sessions
                    cur.execute("""
                        DELETE FROM auth_sessions 
                        WHERE expires_at < CURRENT_TIMESTAMP
                    """)
                    sessions_deleted = cur.rowcount
                    
                    # Clean expired tokens
                    cur.execute("""
                        DELETE FROM token_cache 
                        WHERE expires_at < CURRENT_TIMESTAMP
                    """)
                    tokens_deleted = cur.rowcount
                    
                    # Clean old failed auth attempts (> 1 hour)
                    cur.execute("""
                        DELETE FROM failed_auth_attempts 
                        WHERE attempt_time < CURRENT_TIMESTAMP - INTERVAL '1 hour'
                    """)
                    failed_auth_deleted = cur.rowcount
                    
                    # Clean expired catalog cache
                    cur.execute("""
                        DELETE FROM unity_catalog_cache 
                        WHERE expires_at < CURRENT_TIMESTAMP
                    """)
                    cache_deleted = cur.rowcount
                    
                    # Clean old rate limit entries (> 1 hour)
                    cur.execute("""
                        DELETE FROM rate_limits 
                        WHERE last_updated < CURRENT_TIMESTAMP - INTERVAL '1 hour'
                    """)
                    rate_limits_deleted = cur.rowcount
                    
                    conn.commit()
                    
                    logger.info(
                        "lakebase_cleanup_completed",
                        sessions=sessions_deleted,
                        tokens=tokens_deleted,
                        failed_auth=failed_auth_deleted,
                        cache=cache_deleted,
                        rate_limits=rate_limits_deleted
                    )
                    
            except Exception as e:
                conn.rollback()
                logger.error("lakebase_cleanup_failed", error=str(e))
    
    # ========================================================================
    # Rate Limiting
    # ========================================================================
    
    def check_rate_limit(self, client_id: str, limit: int, window: int) -> tuple[bool, int]:
        """
        Check rate limit using Lakebase
        
        Uses JSONB to store timestamps for efficient querying
        
        Args:
            client_id: Client identifier (usually IP address)
            limit: Maximum requests allowed
            window: Time window in seconds
            
        Returns:
            (allowed, current_count)
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    now = time.time()
                    cutoff = now - window
                    
                    # Get current timestamps
                    cur.execute("""
                        SELECT request_timestamps 
                        FROM rate_limits 
                        WHERE client_id = %s
                    """, (client_id,))
                    
                    row = cur.fetchone()
                    
                    if row and row[0]:
                        timestamps = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        # Filter out old timestamps
                        timestamps = [ts for ts in timestamps if ts > cutoff]
                    else:
                        timestamps = []
                    
                    # Add current timestamp
                    timestamps.append(now)
                    current_count = len(timestamps)
                    
                    # Update or insert
                    cur.execute("""
                        INSERT INTO rate_limits (client_id, request_timestamps, window_seconds, limit_requests, last_updated)
                        VALUES (%s, %s::jsonb, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (client_id) 
                        DO UPDATE SET 
                            request_timestamps = EXCLUDED.request_timestamps,
                            last_updated = CURRENT_TIMESTAMP
                    """, (client_id, json.dumps(timestamps), window, limit))
                    
                    conn.commit()
                    
                    allowed = current_count <= limit
                    
                    if not allowed:
                        logger.warning(
                            "rate_limit_exceeded",
                            client_id=client_id,
                            current=current_count,
                            limit=limit
                        )
                    
                    return allowed, current_count
                    
            except Exception as e:
                conn.rollback()
                logger.error("rate_limit_check_failed", error=str(e), client_id=client_id)
                # Return allowed=True on error to avoid blocking legitimate traffic
                return True, 0
    
    def reset_rate_limit(self, client_id: str):
        """Reset rate limit for a client"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM rate_limits WHERE client_id = %s", (client_id,))
                    conn.commit()
                    logger.info("rate_limit_reset", client_id=client_id)
            except Exception as e:
                conn.rollback()
                logger.error("rate_limit_reset_failed", error=str(e))
    
    # ========================================================================
    # Failed Authentication Tracking
    # ========================================================================
    
    def record_failed_auth(self, client_ip: str, reason: Optional[str] = None) -> int:
        """
        Record a failed authentication attempt
        
        Args:
            client_ip: Client IP address
            reason: Optional reason for failure
            
        Returns:
            Number of failed attempts in last 5 minutes
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Insert failed attempt
                    cur.execute("""
                        INSERT INTO failed_auth_attempts (client_ip, attempt_time, reason)
                        VALUES (%s, CURRENT_TIMESTAMP, %s)
                    """, (client_ip, reason))
                    
                    # Count recent attempts (last 5 minutes)
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM failed_auth_attempts 
                        WHERE client_ip = %s 
                        AND attempt_time > CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                    """, (client_ip,))
                    
                    attempt_count = cur.fetchone()[0]
                    
                    conn.commit()
                    
                    logger.warning(
                        "failed_auth_recorded",
                        client_ip=client_ip,
                        attempts=attempt_count,
                        reason=reason
                    )
                    
                    return attempt_count
                    
            except Exception as e:
                conn.rollback()
                logger.error("failed_auth_record_failed", error=str(e))
                return 0
    
    def get_failed_auth_count(self, client_ip: str, window_minutes: int = 5) -> int:
        """Get number of failed auth attempts for a client"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM failed_auth_attempts 
                        WHERE client_ip = %s 
                        AND attempt_time > CURRENT_TIMESTAMP - INTERVAL '%s minutes'
                    """, (client_ip, window_minutes))
                    
                    return cur.fetchone()[0]
                    
            except Exception as e:
                logger.error("failed_auth_count_failed", error=str(e))
                return 0
    
    def reset_failed_auth(self, client_ip: str):
        """Reset failed auth attempts for a client"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM failed_auth_attempts WHERE client_ip = %s", (client_ip,))
                    conn.commit()
                    logger.info("failed_auth_reset", client_ip=client_ip)
            except Exception as e:
                conn.rollback()
                logger.error("failed_auth_reset_failed", error=str(e))
    
    # ========================================================================
    # Token Caching
    # ========================================================================
    
    def cache_token(self, token: str, user_info: Dict[str, Any], ttl: int = 300):
        """
        Cache validated token
        
        Args:
            token: Access token
            user_info: User information dictionary
            ttl: Time to live in seconds (default 5 minutes)
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    token_hash = hashlib.sha256(token.encode()).hexdigest()
                    expires_at = datetime.now() + timedelta(seconds=ttl)
                    
                    cur.execute("""
                        INSERT INTO token_cache (token_hash, user_info, token_type, expires_at)
                        VALUES (%s, %s::jsonb, %s, %s)
                        ON CONFLICT (token_hash)
                        DO UPDATE SET 
                            user_info = EXCLUDED.user_info,
                            token_type = EXCLUDED.token_type,
                            expires_at = EXCLUDED.expires_at,
                            created_at = CURRENT_TIMESTAMP
                    """, (token_hash, json.dumps(user_info), user_info.get('token_type', 'unknown'), expires_at))
                    
                    conn.commit()
                    logger.debug("token_cached", user=user_info.get('user'), ttl=ttl)
                    
            except Exception as e:
                conn.rollback()
                logger.error("token_cache_failed", error=str(e))
    
    def get_cached_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get cached token info
        
        Args:
            token: Access token
            
        Returns:
            User info dictionary or None if not found/expired
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    token_hash = hashlib.sha256(token.encode()).hexdigest()
                    
                    cur.execute("""
                        SELECT user_info 
                        FROM token_cache 
                        WHERE token_hash = %s AND expires_at > CURRENT_TIMESTAMP
                    """, (token_hash,))
                    
                    row = cur.fetchone()
                    
                    if row and row[0]:
                        user_info = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        logger.debug("token_cache_hit", user=user_info.get('user'))
                        return user_info
                    
                    logger.debug("token_cache_miss")
                    return None
                    
            except Exception as e:
                logger.error("token_cache_get_failed", error=str(e))
                return None
    
    def invalidate_token(self, token: str):
        """Invalidate a cached token"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    token_hash = hashlib.sha256(token.encode()).hexdigest()
                    cur.execute("DELETE FROM token_cache WHERE token_hash = %s", (token_hash,))
                    conn.commit()
                    logger.info("token_invalidated")
            except Exception as e:
                conn.rollback()
                logger.error("token_invalidate_failed", error=str(e))
    
    # ========================================================================
    # General Caching
    # ========================================================================
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """
        Set a key-value pair
        
        Args:
            key: Cache key
            value: Value to cache (will be JSON serialized)
            ttl: Time to live in seconds (None for no expiry)
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    value_json = json.dumps(value) if not isinstance(value, str) else value
                    expires_at = datetime.now() + timedelta(seconds=ttl) if ttl else datetime.max
                    
                    cur.execute("""
                        INSERT INTO unity_catalog_cache (cache_key, cache_type, cache_value, expires_at)
                        VALUES (%s, 'general', %s::jsonb, %s)
                        ON CONFLICT (cache_key)
                        DO UPDATE SET 
                            cache_value = EXCLUDED.cache_value,
                            expires_at = EXCLUDED.expires_at,
                            created_at = CURRENT_TIMESTAMP
                    """, (key, value_json, expires_at))
                    
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error("cache_set_failed", error=str(e), key=key)
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT cache_value 
                        FROM unity_catalog_cache 
                        WHERE cache_key = %s AND expires_at > CURRENT_TIMESTAMP
                    """, (key,))
                    
                    row = cur.fetchone()
                    
                    if row and row[0]:
                        value = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        return value
                    
                    return None
                    
            except Exception as e:
                logger.error("cache_get_failed", error=str(e), key=key)
                return None
    
    def delete(self, key: str):
        """Delete a key"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM unity_catalog_cache WHERE cache_key = %s", (key,))
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error("cache_delete_failed", error=str(e), key=key)
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 1 
                        FROM unity_catalog_cache 
                        WHERE cache_key = %s AND expires_at > CURRENT_TIMESTAMP
                    """, (key,))
                    
                    return cur.fetchone() is not None
                    
            except Exception as e:
                logger.error("cache_exists_failed", error=str(e), key=key)
                return False
    
    # ========================================================================
    # Unity Catalog Caching
    # ========================================================================
    
    def cache_catalogs(self, catalogs: List[Dict[str, Any]], ttl: int = 300):
        """Cache Unity Catalog catalogs"""
        self.set("unity_catalog:catalogs", catalogs, ttl)
    
    def get_cached_catalogs(self) -> Optional[List[Dict[str, Any]]]:
        """Get cached catalogs"""
        return self.get("unity_catalog:catalogs")
    
    def cache_schemas(self, catalog: str, schemas: List[Dict[str, Any]], ttl: int = 300):
        """Cache schemas for a catalog"""
        self.set(f"unity_catalog:schemas:{catalog}", schemas, ttl)
    
    def get_cached_schemas(self, catalog: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached schemas for a catalog"""
        return self.get(f"unity_catalog:schemas:{catalog}")
    
    def cache_tables(self, catalog: str, schema: str, tables: List[Dict[str, Any]], ttl: int = 300):
        """Cache tables for a catalog.schema"""
        self.set(f"unity_catalog:tables:{catalog}.{schema}", tables, ttl)
    
    def get_cached_tables(self, catalog: str, schema: str) -> Optional[List[Dict[str, Any]]]:
        """Get cached tables for a catalog.schema"""
        return self.get(f"unity_catalog:tables:{catalog}.{schema}")
    
    def invalidate_catalog_cache(self, catalog: Optional[str] = None):
        """Invalidate catalog cache"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if catalog:
                        # Invalidate specific catalog
                        cur.execute("""
                            DELETE FROM unity_catalog_cache 
                            WHERE cache_key LIKE %s
                        """, (f"unity_catalog:%:{catalog}%",))
                    else:
                        # Invalidate all catalog cache
                        cur.execute("""
                            DELETE FROM unity_catalog_cache 
                            WHERE cache_key LIKE 'unity_catalog:%'
                        """)
                    
                    deleted = cur.rowcount
                    conn.commit()
                    logger.info("catalog_cache_invalidated", catalog=catalog, count=deleted)
                    
            except Exception as e:
                conn.rollback()
                logger.error("catalog_cache_invalidate_failed", error=str(e))
    
    # ========================================================================
    # Session Management
    # ========================================================================
    
    def create_session(self, session_id: str, user_info: Dict[str, Any], ttl: int = 3600):
        """Create a user session"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    expires_at = datetime.now() + timedelta(seconds=ttl)
                    
                    cur.execute("""
                        INSERT INTO auth_sessions (session_id, user_email, user_info, expires_at)
                        VALUES (%s, %s, %s::jsonb, %s)
                        ON CONFLICT (session_id)
                        DO UPDATE SET 
                            user_info = EXCLUDED.user_info,
                            expires_at = EXCLUDED.expires_at,
                            last_activity = CURRENT_TIMESTAMP
                    """, (session_id, user_info.get('email'), json.dumps(user_info), expires_at))
                    
                    conn.commit()
                    logger.info("session_created", session_id=session_id, user=user_info.get('user'))
                    
            except Exception as e:
                conn.rollback()
                logger.error("session_create_failed", error=str(e))
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT user_info 
                        FROM auth_sessions 
                        WHERE session_id = %s AND expires_at > CURRENT_TIMESTAMP
                    """, (session_id,))
                    
                    row = cur.fetchone()
                    
                    if row and row[0]:
                        # Update last activity
                        cur.execute("""
                            UPDATE auth_sessions 
                            SET last_activity = CURRENT_TIMESTAMP 
                            WHERE session_id = %s
                        """, (session_id,))
                        conn.commit()
                        
                        user_info = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                        return user_info
                    
                    return None
                    
            except Exception as e:
                logger.error("session_get_failed", error=str(e))
                return None
    
    def delete_session(self, session_id: str):
        """Delete a session"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM auth_sessions WHERE session_id = %s", (session_id,))
                    conn.commit()
                    logger.info("session_deleted", session_id=session_id)
            except Exception as e:
                conn.rollback()
                logger.error("session_delete_failed", error=str(e))
    
    def extend_session(self, session_id: str, ttl: int = 3600):
        """Extend session TTL"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    expires_at = datetime.now() + timedelta(seconds=ttl)
                    
                    cur.execute("""
                        UPDATE auth_sessions 
                        SET expires_at = %s, last_activity = CURRENT_TIMESTAMP 
                        WHERE session_id = %s
                    """, (expires_at, session_id))
                    
                    conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error("session_extend_failed", error=str(e))
    
    # ========================================================================
    # Health & Monitoring
    # ========================================================================
    
    def ping(self) -> bool:
        """Check if Lakebase is available"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return True
            except Exception as e:
                logger.error("lakebase_ping_failed", error=str(e))
                return False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Lakebase statistics"""
        with self._get_connection() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    stats = {"connected": True, "database": self.database, "host": self.host}
                    
                    # Get table counts
                    cur.execute("SELECT COUNT(*) as count FROM rate_limits")
                    stats["rate_limits_count"] = cur.fetchone()["count"]
                    
                    cur.execute("SELECT COUNT(*) as count FROM auth_sessions WHERE expires_at > CURRENT_TIMESTAMP")
                    stats["active_sessions"] = cur.fetchone()["count"]
                    
                    cur.execute("SELECT COUNT(*) as count FROM token_cache WHERE expires_at > CURRENT_TIMESTAMP")
                    stats["cached_tokens"] = cur.fetchone()["count"]
                    
                    cur.execute("SELECT COUNT(*) as count FROM unity_catalog_cache WHERE expires_at > CURRENT_TIMESTAMP")
                    stats["cached_catalog_items"] = cur.fetchone()["count"]
                    
                    cur.execute("SELECT COUNT(*) as count FROM plan_cache")
                    stats["cached_plans"] = cur.fetchone()["count"]
                    
                    # Get connection pool stats
                    stats["pool_size"] = f"{self.pool.minconn}-{self.pool.maxconn}"
                    
                    return stats
                    
            except Exception as e:
                logger.error("lakebase_stats_failed", error=str(e))
                return {"connected": False, "error": str(e)}
    
    def close(self):
        """Close all connections"""
        try:
            self.pool.closeall()
            logger.info("lakebase_connections_closed")
        except Exception as e:
            logger.error("lakebase_close_error", error=str(e))
    
    def refresh_oauth_password(self):
        """
        Refresh credentials and reconnect pool
        
        Works with both Database Credential API and OAuth.
        This method should be called periodically if not using auto-refresh.
        
        Note: This will close existing connections and create new ones.
              Use during maintenance windows or low-traffic periods if possible.
        """
        if not self.use_auto_refresh:
            logger.warning("lakebase_refresh_skipped", 
                          message="Auto-refresh not enabled for this instance")
            return
        
        if not LAKEBASE_AUTO_REFRESH_AVAILABLE:
            logger.error("lakebase_auto_refresh_not_available")
            return
        
        try:
            logger.info("refreshing_lakebase_connection_with_new_credentials",
                       method=self.credential_method)
            
            # Get fresh credentials
            new_password = get_lakebase_password()
            new_username = get_lakebase_username() if get_lakebase_username else self.user
            
            # Store pool config
            min_conn = self.pool.minconn
            max_conn = self.pool.maxconn
            
            # Close existing pool
            if hasattr(self, 'pool') and self.pool:
                self.pool.closeall()
            
            # Create new pool with fresh credentials
            self.password = new_password
            self.user = new_username
            self.pool = ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                sslmode='require'
            )
            
            logger.info("lakebase_connection_refreshed_with_new_token",
                       method=self.credential_method)
            
        except Exception as e:
            logger.error("lakebase_oauth_refresh_failed", error=str(e))
            raise
    
    # Plan Execution Tracking Methods
    def create_execution_record(self, plan_id: str, plan_name: str, plan_version: str,
                                executor_user: str, warehouse_id: str, total_statements: int) -> int:
        """
        Create a new execution record
        
        Returns:
            execution_id (int)
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO plan_executions 
                        (plan_id, plan_name, plan_version, executor_user, warehouse_id, 
                         status, total_statements, execution_details)
                        VALUES (%s, %s, %s, %s, %s, 'SUBMITTED', %s, %s)
                        RETURNING execution_id
                    """, (plan_id, plan_name, plan_version, executor_user, warehouse_id, 
                          total_statements, json.dumps({"statements": []})))
                    
                    execution_id = cur.fetchone()[0]
                    conn.commit()
                    
                    logger.info("execution_record_created", 
                               execution_id=execution_id,
                               plan_id=plan_id)
                    return execution_id
                    
            except Exception as e:
                conn.rollback()
                logger.error("create_execution_record_failed", error=str(e))
                raise
    
    def update_execution_status(self, execution_id: int, status: str, 
                                succeeded_statements: int = 0, failed_statements: int = 0,
                                execution_details: dict = None, error_message: str = None):
        """
        Update execution status
        
        Args:
            execution_id: Execution record ID
            status: Status (SUBMITTED, RUNNING, SUCCEEDED, FAILED, PARTIAL)
            succeeded_statements: Number of successfully executed statements
            failed_statements: Number of failed statements
            execution_details: Detailed execution info (statement IDs, etc.)
            error_message: Error message if failed
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    if status in ['SUCCEEDED', 'FAILED', 'PARTIAL']:
                        # Execution completed
                        cur.execute("""
                            UPDATE plan_executions
                            SET status = %s,
                                succeeded_statements = %s,
                                failed_statements = %s,
                                execution_details = %s,
                                error_message = %s,
                                completed_at = CURRENT_TIMESTAMP
                            WHERE execution_id = %s
                        """, (status, succeeded_statements, failed_statements,
                              json.dumps(execution_details) if execution_details else None,
                              error_message, execution_id))
                    else:
                        # Execution in progress
                        cur.execute("""
                            UPDATE plan_executions
                            SET status = %s,
                                succeeded_statements = %s,
                                failed_statements = %s,
                                execution_details = %s
                            WHERE execution_id = %s
                        """, (status, succeeded_statements, failed_statements,
                              json.dumps(execution_details) if execution_details else None,
                              execution_id))
                    
                    conn.commit()
                    logger.info("execution_status_updated",
                               execution_id=execution_id,
                               status=status)
                    
            except Exception as e:
                conn.rollback()
                logger.error("update_execution_status_failed",
                            execution_id=execution_id,
                            error=str(e))
                raise
    
    def get_execution_record(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a single execution record
        
        Returns:
            Dict with execution details or None
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT execution_id, plan_id, plan_name, plan_version,
                               executor_user, warehouse_id, status, started_at,
                               completed_at, total_statements, succeeded_statements,
                               failed_statements, execution_details, error_message
                        FROM plan_executions
                        WHERE execution_id = %s
                    """, (execution_id,))
                    
                    row = cur.fetchone()
                    if not row:
                        return None
                    
                    return {
                        "execution_id": row[0],
                        "plan_id": row[1],
                        "plan_name": row[2],
                        "plan_version": row[3],
                        "executor_user": row[4],
                        "warehouse_id": row[5],
                        "status": row[6],
                        "started_at": row[7].isoformat() if row[7] else None,
                        "completed_at": row[8].isoformat() if row[8] else None,
                        "total_statements": row[9],
                        "succeeded_statements": row[10],
                        "failed_statements": row[11],
                        "execution_details": row[12],
                        "error_message": row[13]
                    }
                    
            except Exception as e:
                logger.error("get_execution_record_failed",
                            execution_id=execution_id,
                            error=str(e))
                return None
    
    def list_executions(self, limit: int = 50, offset: int = 0,
                       status: str = None, executor_user: str = None) -> List[Dict[str, Any]]:
        """
        List execution records with filtering
        
        Args:
            limit: Maximum number of records
            offset: Offset for pagination
            status: Filter by status
            executor_user: Filter by user
            
        Returns:
            List of execution records
        """
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    query = """
                        SELECT execution_id, plan_id, plan_name, plan_version,
                               executor_user, warehouse_id, status, started_at,
                               completed_at, total_statements, succeeded_statements,
                               failed_statements, error_message
                        FROM plan_executions
                        WHERE 1=1
                    """
                    params = []
                    
                    if status:
                        query += " AND status = %s"
                        params.append(status)
                    
                    if executor_user:
                        query += " AND executor_user = %s"
                        params.append(executor_user)
                    
                    query += " ORDER BY started_at DESC LIMIT %s OFFSET %s"
                    params.extend([limit, offset])
                    
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    
                    return [{
                        "execution_id": row[0],
                        "plan_id": row[1],
                        "plan_name": row[2],
                        "plan_version": row[3],
                        "executor_user": row[4],
                        "warehouse_id": row[5],
                        "status": row[6],
                        "started_at": row[7].isoformat() if row[7] else None,
                        "completed_at": row[8].isoformat() if row[8] else None,
                        "total_statements": row[9],
                        "succeeded_statements": row[10],
                        "failed_statements": row[11],
                        "error_message": row[12]
                    } for row in rows]
                    
            except Exception as e:
                logger.error("list_executions_failed", error=str(e))
                return []


# Global instance (lazy initialization)
_lakebase_backend: Optional[LakebaseBackend] = None


def get_lakebase_backend() -> Optional[LakebaseBackend]:
    """
    Get global Lakebase backend instance
    
    Returns:
        LakebaseBackend instance or None if not configured/available
    """
    global _lakebase_backend
    
    # Check if Lakebase is enabled
    lakebase_enabled = os.getenv("LAKEBASE_ENABLED", "false").lower() == "true"
    
    if not lakebase_enabled:
        return None
    
    # Lazy initialization
    if _lakebase_backend is None:
        try:
            _lakebase_backend = LakebaseBackend()
        except Exception as e:
            logger.warning("lakebase_backend_unavailable", error=str(e))
            return None
    
    return _lakebase_backend


def close_lakebase():
    """Close global Lakebase connection"""
    global _lakebase_backend
    if _lakebase_backend:
        _lakebase_backend.close()
        _lakebase_backend = None

