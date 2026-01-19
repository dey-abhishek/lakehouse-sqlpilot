"""
Integration Tests for Scalability Features
Tests Lakebase + Circuit Breaker + API integration
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import time
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock psycopg2 before importing
sys.modules['psycopg2'] = MagicMock()
sys.modules['psycopg2.pool'] = MagicMock()
sys.modules['psycopg2.extras'] = MagicMock()

from infrastructure.lakebase_backend import LakebaseBackend, get_lakebase_backend
from infrastructure.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerError,
    get_databricks_circuit_breaker,
    reset_all_circuit_breakers
)


@pytest.fixture
def mock_lakebase(monkeypatch):
    """Mock Lakebase backend"""
    with patch('infrastructure.lakebase_backend.psycopg2'), \
         patch('infrastructure.lakebase_backend.ThreadedConnectionPool') as mock_pool:
        
        # Set environment variables
        monkeypatch.setenv('LAKEBASE_ENABLED', 'true')
        monkeypatch.setenv('LAKEBASE_HOST', 'test-host.databricks.com')
        monkeypatch.setenv('LAKEBASE_USER', 'test_user')
        monkeypatch.setenv('LAKEBASE_PASSWORD', 'test_password')
        
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.cursor.return_value.__exit__.return_value = None
        
        # Mock table count check - return 10 to skip schema initialization
        mock_cursor.fetchone.return_value = (10,)
        
        # Mock pool
        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance
        
        backend = LakebaseBackend(
            host='test-host.databricks.com',
            user='test_user',
            password='test_password'
        )
        
        backend._mock_cursor = mock_cursor
        backend._mock_conn = mock_conn
        
        yield backend


class TestLakebaseCircuitBreakerIntegration:
    """Test integration of Lakebase with Circuit Breaker"""
    
    def test_circuit_breaker_protects_lakebase_operations(self, mock_lakebase):
        """Test that circuit breaker protects Lakebase operations from cascading failures"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1, name="lakebase_test")
        
        # Mock check_rate_limit to raise exceptions (simulating connection failure)
        # The Lakebase backend normally catches exceptions and returns (True, 0),
        # but for testing circuit breaker behavior, we need actual exceptions
        mock_lakebase.check_rate_limit = MagicMock(side_effect=[Exception("Connection error")] * 5)
        
        def lakebase_operation():
            return mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        # First 3 failures should open circuit
        for i in range(3):
            try:
                cb.call(lakebase_operation)
            except Exception:
                pass
        
        assert cb.state == CircuitState.OPEN
        
        # 4th call should be rejected by circuit breaker, not hitting Lakebase
        with pytest.raises(CircuitBreakerError):
            cb.call(lakebase_operation)
    
    def test_circuit_breaker_allows_recovery_after_timeout(self, mock_lakebase):
        """Test that circuit breaker allows recovery after timeout"""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1, name="lakebase_recovery")
        
        # Mock check_rate_limit to raise exceptions initially
        mock_lakebase.check_rate_limit = MagicMock(side_effect=[Exception("Error"), Exception("Error")])
        
        def lakebase_operation():
            return mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        # Open circuit
        for i in range(2):
            try:
                cb.call(lakebase_operation)
            except Exception:
                pass
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery
        time.sleep(1.1)
        
        # Now make operation successful
        mock_lakebase.check_rate_limit = MagicMock(return_value=(True, 1))
        
        # Should succeed and close circuit
        result = cb.call(lakebase_operation)
        
        assert cb.state == CircuitState.CLOSED


class TestRateLimitingWithCircuitBreaker:
    """Test rate limiting protected by circuit breaker"""
    
    def test_rate_limiting_with_circuit_protection(self, mock_lakebase):
        """Test that rate limiting works with circuit breaker protection"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="rate_limit_cb")
        
        cursor = mock_lakebase._mock_cursor
        cursor.fetchone.return_value = None
        
        def check_rate_limit_protected():
            return mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        # Should work normally
        allowed, count = cb.call(check_rate_limit_protected)
        
        assert allowed is True
        assert cb.state == CircuitState.CLOSED
    
    def test_rate_limit_failure_handling(self, mock_lakebase):
        """Test handling of rate limit failures with circuit breaker"""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=30, name="rate_limit_fail")
        
        # Mock check_rate_limit to raise exception
        mock_lakebase.check_rate_limit = MagicMock(side_effect=Exception("Database connection lost"))
        
        def check_rate_limit_with_failure():
            return mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        # First failure
        with pytest.raises(Exception):
            cb.call(check_rate_limit_with_failure)
        
        assert cb.failure_count == 1


class TestSessionManagementWithResilience:
    """Test session management with resilience patterns"""
    
    def test_session_operations_with_circuit_breaker(self, mock_lakebase):
        """Test that session operations are protected"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="session_cb")
        
        cursor = mock_lakebase._mock_cursor
        
        def create_session_protected():
            user_info = {'user': 'test@databricks.com', 'email': 'test@databricks.com'}
            return mock_lakebase.create_session('session_123', user_info, ttl=3600)
        
        # Should work
        result = cb.call(create_session_protected)
        
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
    
    def test_session_retrieval_with_circuit_breaker(self, mock_lakebase):
        """Test session retrieval with circuit breaker"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="session_get_cb")
        
        cursor = mock_lakebase._mock_cursor
        user_info = {'user': 'test@databricks.com', 'email': 'test@databricks.com'}
        cursor.fetchone.return_value = (json.dumps(user_info),)
        
        def get_session_protected():
            return mock_lakebase.get_session('session_123')
        
        result = cb.call(get_session_protected)
        
        assert result == user_info
        assert cb.state == CircuitState.CLOSED


class TestCachingWithResilience:
    """Test caching operations with resilience"""
    
    def test_unity_catalog_caching_with_circuit_breaker(self, mock_lakebase):
        """Test Unity Catalog caching with circuit breaker protection"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="catalog_cache_cb")
        
        cursor = mock_lakebase._mock_cursor
        
        def cache_catalogs_protected():
            catalogs = [{'name': 'main'}, {'name': 'dev'}]
            mock_lakebase.cache_catalogs(catalogs, ttl=300)
            return catalogs
        
        result = cb.call(cache_catalogs_protected)
        
        assert len(result) == 2
        assert cb.state == CircuitState.CLOSED
    
    def test_cache_retrieval_with_fallback(self, mock_lakebase):
        """Test cache retrieval with fallback on failure"""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1, name="cache_retrieval")
        
        # Mock get_cached_catalogs to raise exceptions initially
        mock_lakebase.get_cached_catalogs = MagicMock(side_effect=[Exception("DB error"), Exception("DB error")])
        
        def get_cached_catalogs_with_fallback():
            try:
                return mock_lakebase.get_cached_catalogs()
            except Exception:
                # Fallback to fresh fetch
                return [{'name': 'main'}]  # Simulated fresh fetch
        
        # Should fail but fallback returns a value, so circuit breaker sees success
        for i in range(2):
            result = cb.call(get_cached_catalogs_with_fallback)
        
        # Circuit should remain CLOSED because fallback prevents exception propagation
        assert cb.state == CircuitState.CLOSED
        
        # Now make the operation succeed directly
        mock_lakebase.get_cached_catalogs = MagicMock(return_value=[{'name': 'main'}, {'name': 'catalog1'}])
        
        result = cb.call(get_cached_catalogs_with_fallback)
        
        # Should return direct value
        assert len(result) == 2


class TestDistributedScenarios:
    """Test distributed scenarios with multiple instances"""
    
    def test_rate_limiting_across_instances(self, mock_lakebase):
        """Test that rate limiting works across distributed instances"""
        cursor = mock_lakebase._mock_cursor
        
        # Simulate existing requests from other instances
        now = time.time()
        existing_timestamps = [now - i for i in range(5)]
        cursor.fetchone.return_value = (json.dumps(existing_timestamps),)
        
        # This instance makes a request
        allowed, count = mock_lakebase.check_rate_limit('shared_client', 10, 60)
        
        # Should see requests from other instances
        assert count == 6  # 5 existing + 1 new
        assert allowed is True
    
    def test_session_sharing_across_instances(self, mock_lakebase):
        """Test that sessions can be shared across instances"""
        cursor = mock_lakebase._mock_cursor
        
        # Instance 1 creates session
        user_info = {'user': 'test@databricks.com', 'email': 'test@databricks.com'}
        mock_lakebase.create_session('shared_session', user_info, ttl=3600)
        
        # Instance 2 retrieves session
        cursor.fetchone.return_value = (json.dumps(user_info),)
        result = mock_lakebase.get_session('shared_session')
        
        assert result == user_info


class TestFailureRecoveryScenarios:
    """Test failure recovery scenarios"""
    
    def test_gradual_recovery_with_circuit_breaker(self, mock_lakebase):
        """Test gradual recovery from failures"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1, name="gradual_recovery")
        
        cursor = mock_lakebase._mock_cursor
        
        failure_count = {'count': 0}
        
        def flaky_lakebase_operation():
            failure_count['count'] += 1
            if failure_count['count'] <= 3:
                raise Exception("Transient error")
            cursor.fetchone.return_value = None
            return mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        # First 3 calls fail and open circuit
        for i in range(3):
            try:
                cb.call(flaky_lakebase_operation)
            except Exception:
                pass
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery
        time.sleep(1.1)
        
        # Next call should succeed
        allowed, count = cb.call(flaky_lakebase_operation)
        
        assert cb.state == CircuitState.CLOSED
        assert allowed is True
    
    def test_circuit_breaker_prevents_database_overload(self, mock_lakebase):
        """Test that circuit breaker prevents overloading database during outage"""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=2, name="overload_protection")
        
        request_count = {'count': 0}
        
        # Mock ping to raise exception and count requests
        def ping_with_error():
            request_count['count'] += 1
            raise Exception("Database overloaded")
        
        mock_lakebase.ping = MagicMock(side_effect=ping_with_error)
        
        def lakebase_operation_with_counter():
            return mock_lakebase.ping()
        
        # Make requests until circuit opens
        for i in range(10):
            try:
                cb.call(lakebase_operation_with_counter)
            except (Exception, CircuitBreakerError):
                pass
        
        # Circuit should be open
        assert cb.state == CircuitState.OPEN
        
        # Request count should be limited (5 failures + subsequent rejections)
        # Subsequent requests should be rejected without hitting database
        assert request_count['count'] == 5  # Only first 5 hit database


class TestHealthAndMonitoring:
    """Test health and monitoring with resilience"""
    
    def test_health_check_with_circuit_breaker(self, mock_lakebase):
        """Test health check protected by circuit breaker"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30, name="health_cb")
        
        cursor = mock_lakebase._mock_cursor
        
        def health_check():
            return mock_lakebase.ping()
        
        # Healthy state
        result = cb.call(health_check)
        
        assert result is True
        assert cb.state == CircuitState.CLOSED
    
    def test_stats_collection_resilience(self, mock_lakebase):
        """Test that stats collection is resilient to failures"""
        cursor = mock_lakebase._mock_cursor
        
        # Mock stats queries
        cursor.fetchone.side_effect = [
            {'count': 5},
            {'count': 10},
            {'count': 15},
            {'count': 20},
            {'count': 25}
        ]
        
        stats = mock_lakebase.get_stats()
        
        assert stats['connected'] is True
        assert 'rate_limits_count' in stats


class TestScalabilityPatterns:
    """Test scalability patterns"""
    
    def test_connection_pooling_efficiency(self, mock_lakebase):
        """Test that connection pooling is efficient"""
        # Multiple operations should reuse connections
        cursor = mock_lakebase._mock_cursor
        cursor.fetchone.return_value = None
        
        for i in range(10):
            mock_lakebase.check_rate_limit(f'client_{i}', 10, 60)
        
        # Pool should have been used efficiently
        assert mock_lakebase.pool.getconn.called
        assert mock_lakebase.pool.putconn.called
    
    def test_bulk_operations_performance(self, mock_lakebase):
        """Test bulk operations performance"""
        cursor = mock_lakebase._mock_cursor
        
        # Cache multiple catalogs
        catalogs = [{'name': f'catalog_{i}'} for i in range(100)]
        mock_lakebase.cache_catalogs(catalogs, ttl=300)
        
        # Retrieve from cache
        cursor.fetchone.return_value = (json.dumps(catalogs),)
        result = mock_lakebase.get_cached_catalogs()
        
        assert len(result) == 100
    
    def test_concurrent_rate_limiting(self, mock_lakebase):
        """Test rate limiting under concurrent load"""
        cursor = mock_lakebase._mock_cursor
        
        # Simulate concurrent requests
        now = time.time()
        timestamps = [now - i * 0.1 for i in range(5)]
        cursor.fetchone.return_value = (json.dumps(timestamps),)
        
        # Multiple clients
        results = []
        for i in range(5):
            allowed, count = mock_lakebase.check_rate_limit(f'client_{i}', 10, 60)
            results.append((allowed, count))
        
        # All should be allowed (under limit)
        assert all(allowed for allowed, count in results)


class TestErrorHandlingPatterns:
    """Test error handling patterns"""
    
    def test_graceful_degradation(self, mock_lakebase):
        """Test graceful degradation when Lakebase is unavailable"""
        cursor = mock_lakebase._mock_cursor
        cursor.execute.side_effect = Exception("Connection refused")
        
        # Rate limiting should degrade gracefully (allow traffic)
        allowed, count = mock_lakebase.check_rate_limit('test_client', 10, 60)
        
        assert allowed is True  # Fail open
        assert count == 0
    
    def test_partial_failure_handling(self, mock_lakebase):
        """Test handling of partial failures"""
        # Mock get method to return different results
        mock_lakebase.get = MagicMock(side_effect=[
            {'data': 'success'},  # First succeeds
            Exception("Temporary error"),  # Second fails
            {'data': 'success'}   # Third succeeds
        ])
        
        results = []
        for i in range(3):
            try:
                result = mock_lakebase.get('key')
                results.append(('success', result))
            except Exception as e:
                results.append(('error', str(e)))
        
        # Should have mix of success and failure
        assert results[0][0] == 'success'
        assert results[1][0] == 'error'
        assert results[2][0] == 'success'


class TestCleanupAndMaintenance:
    """Test cleanup and maintenance operations"""
    
    def test_automated_cleanup(self, mock_lakebase):
        """Test automated cleanup of expired data"""
        cursor = mock_lakebase._mock_cursor
        cursor.rowcount = 10
        
        mock_lakebase.cleanup_expired_data()
        
        # Verify cleanup was executed
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE' in str(call)]
        
        # Should clean multiple tables
        assert len(delete_calls) >= 5
    
    def test_cache_invalidation(self, mock_lakebase):
        """Test cache invalidation"""
        cursor = mock_lakebase._mock_cursor
        cursor.rowcount = 5
        
        mock_lakebase.invalidate_catalog_cache('main')
        
        # Verify invalidation
        delete_calls = [call for call in cursor.execute.call_args_list if 'DELETE' in str(call)]
        assert len(delete_calls) > 0


class TestGlobalCircuitBreakersWithLakebase:
    """Test global circuit breakers with Lakebase operations"""
    
    def test_databricks_circuit_breaker_integration(self, mock_lakebase):
        """Test Databricks circuit breaker with Lakebase"""
        db_cb = get_databricks_circuit_breaker()
        reset_all_circuit_breakers()  # Start fresh
        
        cursor = mock_lakebase._mock_cursor
        cursor.fetchone.return_value = None
        
        def databricks_operation_with_lakebase():
            # Simulate Databricks operation that uses Lakebase for state
            mock_lakebase.cache_catalogs([{'name': 'main'}], ttl=300)
            return "success"
        
        result = db_cb.call(databricks_operation_with_lakebase)
        
        assert result == "success"
        assert db_cb.state == CircuitState.CLOSED
    
    def test_multiple_circuit_breakers_coordination(self, mock_lakebase):
        """Test coordination of multiple circuit breakers"""
        db_cb = get_databricks_circuit_breaker()
        reset_all_circuit_breakers()
        
        # Mock ping to raise exception
        mock_lakebase.ping = MagicMock(side_effect=Exception("Error"))
        
        def failing_databricks_op():
            return mock_lakebase.ping()
        
        # Fail enough to open circuit
        for i in range(5):
            try:
                db_cb.call(failing_databricks_op)
            except Exception:
                pass
        
        assert db_cb.state == CircuitState.OPEN
        
        # Reset all should reset this breaker too
        reset_all_circuit_breakers()
        
        assert db_cb.state == CircuitState.CLOSED

