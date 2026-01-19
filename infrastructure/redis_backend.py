"""
Redis Backend for Distributed State Management
Enables horizontal scaling by storing rate limits, auth state, and cache in Redis
"""

import redis
from typing import Optional, Dict, Any, List
import json
import time
import os
import structlog
from datetime import datetime, timedelta

logger = structlog.get_logger()


class RedisBackend:
    """Redis backend for distributed state management"""
    
    def __init__(self, redis_url: Optional[str] = None, **kwargs):
        """
        Initialize Redis backend
        
        Args:
            redis_url: Redis connection URL (redis://localhost:6379/0)
            **kwargs: Additional redis connection parameters
        """
        redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        
        try:
            self.client = redis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30,
                **kwargs
            )
            
            # Test connection
            self.client.ping()
            logger.info("redis_connected", url=redis_url)
            
        except redis.ConnectionError as e:
            logger.error("redis_connection_failed", error=str(e))
            raise
    
    def ping(self) -> bool:
        """Check if Redis is available"""
        try:
            return self.client.ping()
        except Exception as e:
            logger.error("redis_ping_failed", error=str(e))
            return False
    
    def close(self):
        """Close Redis connection"""
        try:
            self.client.close()
            logger.info("redis_connection_closed")
        except Exception as e:
            logger.error("redis_close_error", error=str(e))
    
    # ========================================================================
    # Rate Limiting
    # ========================================================================
    
    def check_rate_limit(self, client_id: str, limit: int, window: int) -> tuple[bool, int]:
        """
        Check rate limit for a client using Redis
        
        Args:
            client_id: Client identifier
            limit: Maximum requests allowed
            window: Time window in seconds
            
        Returns:
            (allowed, current_count)
        """
        key = f"rate_limit:{client_id}"
        now = time.time()
        cutoff = now - window
        
        pipe = self.client.pipeline()
        
        # Remove old entries
        pipe.zremrangebyscore(key, 0, cutoff)
        
        # Count current requests
        pipe.zcard(key)
        
        # Add current request
        pipe.zadd(key, {str(now): now})
        
        # Set expiry
        pipe.expire(key, window)
        
        # Execute pipeline
        results = pipe.execute()
        current_count = results[1]
        
        allowed = current_count < limit
        
        if not allowed:
            logger.warning(
                "rate_limit_exceeded",
                client_id=client_id,
                current=current_count,
                limit=limit
            )
        
        return allowed, current_count
    
    def reset_rate_limit(self, client_id: str):
        """Reset rate limit for a client"""
        key = f"rate_limit:{client_id}"
        self.client.delete(key)
        logger.info("rate_limit_reset", client_id=client_id)
    
    # ========================================================================
    # Failed Authentication Tracking
    # ========================================================================
    
    def record_failed_auth(self, client_ip: str) -> int:
        """
        Record a failed authentication attempt
        
        Args:
            client_ip: Client IP address
            
        Returns:
            Number of failed attempts in window
        """
        key = f"failed_auth:{client_ip}"
        now = time.time()
        window = 300  # 5 minutes
        cutoff = now - window
        
        pipe = self.client.pipeline()
        
        # Remove old entries
        pipe.zremrangebyscore(key, 0, cutoff)
        
        # Add current attempt
        pipe.zadd(key, {str(now): now})
        
        # Count attempts
        pipe.zcard(key)
        
        # Set expiry
        pipe.expire(key, window)
        
        results = pipe.execute()
        attempt_count = results[2]
        
        logger.warning(
            "failed_auth_recorded",
            client_ip=client_ip,
            attempts=attempt_count
        )
        
        return attempt_count
    
    def get_failed_auth_count(self, client_ip: str) -> int:
        """Get number of failed auth attempts for a client"""
        key = f"failed_auth:{client_ip}"
        window = 300
        cutoff = time.time() - window
        
        # Remove old entries
        self.client.zremrangebyscore(key, 0, cutoff)
        
        # Count remaining
        count = self.client.zcard(key)
        
        return count
    
    def reset_failed_auth(self, client_ip: str):
        """Reset failed auth attempts for a client"""
        key = f"failed_auth:{client_ip}"
        self.client.delete(key)
        logger.info("failed_auth_reset", client_ip=client_ip)
    
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
        key = f"token_cache:{token}"
        value = json.dumps(user_info)
        
        self.client.setex(key, ttl, value)
        
        logger.debug("token_cached", user=user_info.get('user'), ttl=ttl)
    
    def get_cached_token(self, token: str) -> Optional[Dict[str, Any]]:
        """
        Get cached token info
        
        Args:
            token: Access token
            
        Returns:
            User info dictionary or None if not found/expired
        """
        key = f"token_cache:{token}"
        value = self.client.get(key)
        
        if value:
            user_info = json.loads(value)
            logger.debug("token_cache_hit", user=user_info.get('user'))
            return user_info
        
        logger.debug("token_cache_miss")
        return None
    
    def invalidate_token(self, token: str):
        """Invalidate a cached token"""
        key = f"token_cache:{token}"
        self.client.delete(key)
        logger.info("token_invalidated")
    
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
        if not isinstance(value, str):
            value = json.dumps(value)
        
        if ttl:
            self.client.setex(key, ttl, value)
        else:
            self.client.set(key, value)
    
    def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found
        """
        value = self.client.get(key)
        
        if value:
            try:
                return json.loads(value)
            except json.JSONDecodeError:
                return value
        
        return None
    
    def delete(self, key: str):
        """Delete a key"""
        self.client.delete(key)
    
    def exists(self, key: str) -> bool:
        """Check if key exists"""
        return self.client.exists(key) > 0
    
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
        if catalog:
            # Invalidate specific catalog
            pattern = f"unity_catalog:*:{catalog}*"
        else:
            # Invalidate all catalog cache
            pattern = "unity_catalog:*"
        
        keys = self.client.keys(pattern)
        if keys:
            self.client.delete(*keys)
            logger.info("catalog_cache_invalidated", pattern=pattern, count=len(keys))
    
    # ========================================================================
    # Session Management
    # ========================================================================
    
    def create_session(self, session_id: str, user_info: Dict[str, Any], ttl: int = 3600):
        """Create a user session"""
        key = f"session:{session_id}"
        self.set(key, user_info, ttl)
        logger.info("session_created", session_id=session_id, user=user_info.get('user'))
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        key = f"session:{session_id}"
        return self.get(key)
    
    def delete_session(self, session_id: str):
        """Delete a session"""
        key = f"session:{session_id}"
        self.delete(key)
        logger.info("session_deleted", session_id=session_id)
    
    def extend_session(self, session_id: str, ttl: int = 3600):
        """Extend session TTL"""
        key = f"session:{session_id}"
        self.client.expire(key, ttl)
    
    # ========================================================================
    # Health & Monitoring
    # ========================================================================
    
    def get_stats(self) -> Dict[str, Any]:
        """Get Redis statistics"""
        try:
            info = self.client.info()
            return {
                "connected": True,
                "version": info.get("redis_version"),
                "used_memory": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "uptime_seconds": info.get("uptime_in_seconds"),
                "keyspace": info.get("db0", {})
            }
        except Exception as e:
            logger.error("redis_stats_error", error=str(e))
            return {"connected": False, "error": str(e)}


# Global Redis instance (lazy initialization)
_redis_backend: Optional[RedisBackend] = None


def get_redis_backend() -> Optional[RedisBackend]:
    """
    Get global Redis backend instance
    
    Returns:
        RedisBackend instance or None if Redis is not configured/available
    """
    global _redis_backend
    
    # Check if Redis is enabled
    redis_enabled = os.getenv("REDIS_ENABLED", "false").lower() == "true"
    
    if not redis_enabled:
        return None
    
    # Lazy initialization
    if _redis_backend is None:
        try:
            _redis_backend = RedisBackend()
        except Exception as e:
            logger.warning("redis_backend_unavailable", error=str(e))
            return None
    
    return _redis_backend


def close_redis():
    """Close global Redis connection"""
    global _redis_backend
    if _redis_backend:
        _redis_backend.close()
        _redis_backend = None


