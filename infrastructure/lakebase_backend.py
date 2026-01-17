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

# Try to import Lakebase OAuth manager (optional)
try:
    from infrastructure.lakebase_oauth import get_lakebase_password
    LAKEBASE_OAUTH_AVAILABLE = True
except ImportError:
    LAKEBASE_OAUTH_AVAILABLE = False
    logger.warning("lakebase_oauth_not_available", 
                   message="OAuth auto-refresh not available for Lakebase")


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
                 database: str = "sqlpilot_state",
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
            database: Database name
            user: Database user
            password: Database password (or will use OAuth if use_oauth=True)
            use_oauth: Use OAuth auto-refresh for password (recommended)
            min_connections: Minimum connections in pool
            max_connections: Maximum connections in pool
        """
        # Get credentials from environment
        self.host = host or os.getenv("LAKEBASE_HOST")
        self.port = port
        self.database = database
        self.user = user or os.getenv("LAKEBASE_USER")
        self.use_oauth = use_oauth and LAKEBASE_OAUTH_AVAILABLE
        
        # Get password: OAuth (preferred) or static
        if self.use_oauth:
            try:
                self.password = get_lakebase_password()
                logger.info("lakebase_using_oauth_password", 
                           message="OAuth auto-refresh enabled for Lakebase")
            except Exception as e:
                logger.warning("lakebase_oauth_failed_fallback_to_static",
                              error=str(e))
                self.password = password or os.getenv("LAKEBASE_PASSWORD")
                self.use_oauth = False
        else:
            self.password = password or os.getenv("LAKEBASE_PASSWORD")
        
        if not all([self.host, self.user, self.password]):
            raise ValueError(
                "Lakebase credentials not configured. "
                "Set LAKEBASE_HOST, LAKEBASE_USER, LAKEBASE_PASSWORD "
                "or configure OAuth (OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET, OAUTH_REFRESH_TOKEN)"
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
        """Context manager for getting and returning connections"""
        conn = self.pool.getconn()
        try:
            yield conn
        finally:
            self.pool.putconn(conn)
    
    def _initialize_schema(self):
        """Create tables if they don't exist"""
        with self._get_connection() as conn:
            try:
                with conn.cursor() as cur:
                    # Enable JSONB extension
                    cur.execute("CREATE EXTENSION IF NOT EXISTS btree_gin")
                    
                    # Create rate_limits table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS rate_limits (
                            client_id VARCHAR(255) PRIMARY KEY,
                            request_timestamps JSONB NOT NULL,
                            window_seconds INT NOT NULL DEFAULT 60,
                            limit_requests INT NOT NULL DEFAULT 100,
                            last_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_rate_limits_last_updated 
                        ON rate_limits(last_updated)
                    """)
                    
                    # Create auth_sessions table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS auth_sessions (
                            session_id VARCHAR(255) PRIMARY KEY,
                            user_email VARCHAR(255) NOT NULL,
                            user_info JSONB NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            expires_at TIMESTAMP NOT NULL,
                            last_activity TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_auth_sessions_user 
                        ON auth_sessions(user_email)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires 
                        ON auth_sessions(expires_at)
                    """)
                    
                    # Create token_cache table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS token_cache (
                            token_hash VARCHAR(64) PRIMARY KEY,
                            user_info JSONB NOT NULL,
                            token_type VARCHAR(50) NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            expires_at TIMESTAMP NOT NULL
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_token_cache_expires 
                        ON token_cache(expires_at)
                    """)
                    
                    # Create failed_auth_attempts table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS failed_auth_attempts (
                            id SERIAL PRIMARY KEY,
                            client_ip VARCHAR(45) NOT NULL,
                            attempt_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            reason VARCHAR(255)
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_failed_auth_client_time 
                        ON failed_auth_attempts(client_ip, attempt_time)
                    """)
                    
                    # Create circuit_breaker_state table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS circuit_breaker_state (
                            service_name VARCHAR(100) PRIMARY KEY,
                            state VARCHAR(20) NOT NULL,
                            failure_count INT NOT NULL DEFAULT 0,
                            last_failure_time TIMESTAMP,
                            last_state_change TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            metadata JSONB
                        )
                    """)
                    
                    # Create unity_catalog_cache table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS unity_catalog_cache (
                            cache_key VARCHAR(500) PRIMARY KEY,
                            cache_type VARCHAR(50) NOT NULL,
                            cache_value JSONB NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            expires_at TIMESTAMP NOT NULL
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_catalog_cache_type_expires 
                        ON unity_catalog_cache(cache_type, expires_at)
                    """)
                    
                    # Create plan_cache table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS plan_cache (
                            plan_id VARCHAR(255) PRIMARY KEY,
                            plan_data JSONB NOT NULL,
                            compiled_sql TEXT,
                            validation_result JSONB,
                            owner_email VARCHAR(255),
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            accessed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            access_count INT NOT NULL DEFAULT 0
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_plan_cache_owner 
                        ON plan_cache(owner_email)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_plan_cache_accessed 
                        ON plan_cache(accessed_at)
                    """)
                    
                    # Create execution_metrics table
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS execution_metrics (
                            metric_id SERIAL PRIMARY KEY,
                            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            endpoint VARCHAR(255) NOT NULL,
                            method VARCHAR(10) NOT NULL,
                            status_code INT NOT NULL,
                            response_time_ms INT NOT NULL,
                            user_email VARCHAR(255),
                            error_message TEXT
                        )
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_metrics_timestamp 
                        ON execution_metrics(timestamp)
                    """)
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_metrics_endpoint 
                        ON execution_metrics(endpoint)
                    """)
                    
                    conn.commit()
                    logger.info("lakebase_schema_initialized")
                    
            except Exception as e:
                conn.rollback()
                logger.error("lakebase_schema_init_failed", error=str(e))
                raise
    
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
        Refresh OAuth password and reconnect pool
        
        This method should be called periodically (every ~45 min) to refresh
        the OAuth token used as the database password.
        
        Note: This will close existing connections and create new ones.
              Use during maintenance windows or low-traffic periods if possible.
        """
        if not self.use_oauth:
            logger.warning("lakebase_refresh_skipped", 
                          message="OAuth not enabled for this instance")
            return
        
        if not LAKEBASE_OAUTH_AVAILABLE:
            logger.error("lakebase_oauth_not_available")
            return
        
        try:
            logger.info("refreshing_lakebase_connection_with_new_oauth_token")
            
            # Get fresh OAuth token
            new_password = get_lakebase_password()
            
            # Store pool config
            min_conn = self.pool.minconn
            max_conn = self.pool.maxconn
            
            # Close existing pool
            if hasattr(self, 'pool') and self.pool:
                self.pool.closeall()
            
            # Create new pool with fresh token
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
            
            logger.info("lakebase_connection_refreshed_with_new_token")
            
        except Exception as e:
            logger.error("lakebase_oauth_refresh_failed", error=str(e))
            raise


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

