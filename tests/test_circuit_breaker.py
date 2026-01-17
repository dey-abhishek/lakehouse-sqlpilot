"""
Tests for Circuit Breaker Pattern
Tests fault tolerance and failure handling
"""

import pytest
import time
from unittest.mock import Mock, patch
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from infrastructure.circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerError,
    circuit_breaker,
    get_databricks_circuit_breaker,
    get_unity_catalog_circuit_breaker,
    reset_all_circuit_breakers,
    get_all_circuit_breaker_states
)


class TestCircuitBreakerBasics:
    """Test basic circuit breaker functionality"""
    
    def test_initialization(self):
        """Test circuit breaker initialization"""
        cb = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30,
            name="test_breaker"
        )
        
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.failure_threshold == 3
        assert cb.recovery_timeout == 30
        assert cb.name == "test_breaker"
    
    def test_successful_call_in_closed_state(self):
        """Test successful call when circuit is closed"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def successful_function():
            return "success"
        
        result = cb.call(successful_function)
        
        assert result == "success"
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
    
    def test_failed_call_increments_failure_count(self):
        """Test that failed calls increment failure count"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Test error")
        
        # First failure
        with pytest.raises(Exception, match="Test error"):
            cb.call(failing_function)
        
        assert cb.failure_count == 1
        assert cb.state == CircuitState.CLOSED
    
    def test_circuit_opens_after_threshold(self):
        """Test that circuit opens after reaching failure threshold"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Test error")
        
        # Reach threshold
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 3
    
    def test_open_circuit_rejects_calls(self):
        """Test that open circuit rejects calls immediately"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Test error")
        
        # Open the circuit
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        
        # Next call should be rejected without executing function
        with pytest.raises(CircuitBreakerError, match="Circuit breaker .* is OPEN"):
            cb.call(failing_function)
    
    def test_circuit_transitions_to_half_open(self):
        """Test that circuit transitions to half-open after recovery timeout"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)  # 1 second timeout
        
        def failing_function():
            raise Exception("Test error")
        
        # Open the circuit
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Next call should transition to HALF_OPEN
        def successful_function():
            return "success"
        
        result = cb.call(successful_function)
        
        assert result == "success"
        assert cb.state == CircuitState.CLOSED  # Successful call closes circuit
        assert cb.failure_count == 0
    
    def test_half_open_success_closes_circuit(self):
        """Test that successful call in half-open state closes circuit"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        # Open the circuit
        def failing_function():
            raise Exception("Test error")
        
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Successful call
        def successful_function():
            return "success"
        
        result = cb.call(successful_function)
        
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
    
    def test_half_open_failure_reopens_circuit(self):
        """Test that failed call in half-open state reopens circuit"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        
        # Open the circuit
        def failing_function():
            raise Exception("Test error")
        
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        
        # Wait for recovery timeout
        time.sleep(1.1)
        
        # Failed call in half-open
        with pytest.raises(Exception):
            cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
    
    def test_reset_circuit_breaker(self):
        """Test manually resetting circuit breaker"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        # Open the circuit
        def failing_function():
            raise Exception("Test error")
        
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        assert cb.failure_count == 3
        
        # Reset
        cb.reset()
        
        assert cb.state == CircuitState.CLOSED
        assert cb.failure_count == 0
        assert cb.last_failure_time is None
    
    def test_get_state(self):
        """Test getting circuit breaker state"""
        cb = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            name="test_breaker"
        )
        
        state = cb.get_state()
        
        assert state['name'] == "test_breaker"
        assert state['state'] == CircuitState.CLOSED.value
        assert state['failure_count'] == 0
        assert state['failure_threshold'] == 5
        assert state['recovery_timeout'] == 60


class TestCircuitBreakerDecorator:
    """Test circuit breaker decorator"""
    
    def test_decorator_wraps_function(self):
        """Test that decorator properly wraps function"""
        @circuit_breaker(failure_threshold=3, recovery_timeout=30)
        def test_function():
            return "success"
        
        result = test_function()
        
        assert result == "success"
        assert hasattr(test_function, 'circuit_breaker')
        assert isinstance(test_function.circuit_breaker, CircuitBreaker)
    
    def test_decorator_handles_failures(self):
        """Test that decorator handles failures correctly"""
        @circuit_breaker(failure_threshold=2, recovery_timeout=30)
        def failing_function():
            raise ValueError("Test error")
        
        # First failure
        with pytest.raises(ValueError):
            failing_function()
        
        assert failing_function.circuit_breaker.failure_count == 1
        
        # Second failure - should open circuit
        with pytest.raises(ValueError):
            failing_function()
        
        assert failing_function.circuit_breaker.state == CircuitState.OPEN
        
        # Third call - should be rejected by circuit breaker
        with pytest.raises(CircuitBreakerError):
            failing_function()
    
    def test_decorator_with_custom_exception(self):
        """Test decorator with custom exception type"""
        @circuit_breaker(
            failure_threshold=2,
            recovery_timeout=30,
            expected_exception=ValueError
        )
        def function_with_value_error():
            raise ValueError("Value error")
        
        # ValueError should be caught by circuit breaker
        with pytest.raises(ValueError):
            function_with_value_error()
        
        assert function_with_value_error.circuit_breaker.failure_count == 1
        
        @circuit_breaker(
            failure_threshold=2,
            recovery_timeout=30,
            expected_exception=ValueError
        )
        def function_with_type_error():
            raise TypeError("Type error")
        
        # TypeError should NOT be caught by circuit breaker (not expected exception)
        with pytest.raises(TypeError):
            function_with_type_error()
        
        # Failure count should not increment for unexpected exception
        assert function_with_type_error.circuit_breaker.failure_count == 0
    
    def test_decorator_uses_function_name_as_default(self):
        """Test that decorator uses function name as default breaker name"""
        @circuit_breaker(failure_threshold=3, recovery_timeout=30)
        def my_test_function():
            pass
        
        assert my_test_function.circuit_breaker.name == "my_test_function"
    
    def test_decorator_with_custom_name(self):
        """Test decorator with custom breaker name"""
        @circuit_breaker(
            failure_threshold=3,
            recovery_timeout=30,
            name="custom_breaker"
        )
        def my_function():
            pass
        
        assert my_function.circuit_breaker.name == "custom_breaker"


class TestSuccessResetsFailureCount:
    """Test that successful calls reset failure count"""
    
    def test_success_after_failures_resets_count(self):
        """Test that success after failures resets count in closed state"""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Error")
        
        def successful_function():
            return "success"
        
        # Accumulate some failures (not enough to open circuit)
        for i in range(3):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        assert cb.failure_count == 3
        assert cb.state == CircuitState.CLOSED
        
        # Successful call should reset count
        result = cb.call(successful_function)
        
        assert result == "success"
        assert cb.failure_count == 0
        assert cb.state == CircuitState.CLOSED


class TestGlobalCircuitBreakers:
    """Test global circuit breaker instances"""
    
    def test_get_databricks_circuit_breaker(self):
        """Test getting global Databricks circuit breaker"""
        cb = get_databricks_circuit_breaker()
        
        assert isinstance(cb, CircuitBreaker)
        assert cb.name == "databricks_sql"
        assert cb.failure_threshold == 5
        assert cb.recovery_timeout == 60
        
        # Should return same instance on subsequent calls
        cb2 = get_databricks_circuit_breaker()
        assert cb is cb2
    
    def test_get_unity_catalog_circuit_breaker(self):
        """Test getting global Unity Catalog circuit breaker"""
        cb = get_unity_catalog_circuit_breaker()
        
        assert isinstance(cb, CircuitBreaker)
        assert cb.name == "unity_catalog"
        assert cb.failure_threshold == 3
        assert cb.recovery_timeout == 30
        
        # Should return same instance on subsequent calls
        cb2 = get_unity_catalog_circuit_breaker()
        assert cb is cb2
    
    def test_reset_all_circuit_breakers(self):
        """Test resetting all global circuit breakers"""
        # Get and open both breakers
        db_cb = get_databricks_circuit_breaker()
        uc_cb = get_unity_catalog_circuit_breaker()
        
        # Open Databricks breaker
        def failing_function():
            raise Exception("Error")
        
        for i in range(5):
            with pytest.raises(Exception):
                db_cb.call(failing_function)
        
        # Open Unity Catalog breaker
        for i in range(3):
            with pytest.raises(Exception):
                uc_cb.call(failing_function)
        
        assert db_cb.state == CircuitState.OPEN
        assert uc_cb.state == CircuitState.OPEN
        
        # Reset all
        reset_all_circuit_breakers()
        
        assert db_cb.state == CircuitState.CLOSED
        assert uc_cb.state == CircuitState.CLOSED
        assert db_cb.failure_count == 0
        assert uc_cb.failure_count == 0
    
    def test_get_all_circuit_breaker_states(self):
        """Test getting all circuit breaker states"""
        # Initialize breakers
        db_cb = get_databricks_circuit_breaker()
        uc_cb = get_unity_catalog_circuit_breaker()
        
        states = get_all_circuit_breaker_states()
        
        assert 'databricks_sql' in states
        assert 'unity_catalog' in states
        
        assert states['databricks_sql']['name'] == 'databricks_sql'
        assert states['databricks_sql']['state'] == CircuitState.CLOSED.value
        
        assert states['unity_catalog']['name'] == 'unity_catalog'
        assert states['unity_catalog']['state'] == CircuitState.CLOSED.value


class TestCircuitBreakerWithRealFunctions:
    """Test circuit breaker with realistic scenarios"""
    
    def test_protect_database_call(self):
        """Test protecting a database call"""
        call_count = {'count': 0}
        
        @circuit_breaker(failure_threshold=3, recovery_timeout=1)
        def database_query():
            call_count['count'] += 1
            if call_count['count'] <= 3:  # First 3 calls fail
                raise Exception("Database connection error")
            return "success"
        
        # First 3 calls should fail and open circuit
        for i in range(3):
            with pytest.raises(Exception):
                database_query()
        
        assert database_query.circuit_breaker.state == CircuitState.OPEN
        
        # 4th call should be rejected by circuit breaker (doesn't increment count)
        with pytest.raises(CircuitBreakerError):
            database_query()
        
        # Call count should still be 3 (circuit breaker prevented 4th call)
        assert call_count['count'] == 3
        
        # Wait for recovery
        time.sleep(1.1)
        
        # 5th call should succeed (circuit transitions to half-open then closed)
        # This is call #4 from function perspective (circuit prevented one)
        result = database_query()
        assert result == "success"
        assert database_query.circuit_breaker.state == CircuitState.CLOSED
    
    def test_protect_external_api_call(self):
        """Test protecting an external API call"""
        @circuit_breaker(failure_threshold=2, recovery_timeout=30)
        def call_external_api():
            import requests
            response = requests.get("http://api.example.com/data")
            return response.json()
        
        # Circuit breaker should be initialized
        assert call_external_api.circuit_breaker.state == CircuitState.CLOSED
    
    def test_circuit_breaker_with_retries(self):
        """Test circuit breaker combined with retry logic"""
        call_count = {'count': 0}
        
        @circuit_breaker(failure_threshold=3, recovery_timeout=1)
        def flaky_function():
            call_count['count'] += 1
            if call_count['count'] % 2 == 1:
                raise Exception("Transient error")
            return "success"
        
        # Function fails on odd calls, succeeds on even
        # First call fails
        with pytest.raises(Exception):
            flaky_function()
        
        assert flaky_function.circuit_breaker.failure_count == 1
        
        # Second call succeeds - resets failure count
        result = flaky_function()
        assert result == "success"
        assert flaky_function.circuit_breaker.failure_count == 0


class TestCircuitBreakerEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_zero_failure_threshold(self):
        """Test with zero failure threshold (should open immediately)"""
        cb = CircuitBreaker(failure_threshold=0, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Error")
        
        # First failure should open circuit
        with pytest.raises(Exception):
            cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
    
    def test_very_long_recovery_timeout(self):
        """Test with very long recovery timeout"""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=3600)  # 1 hour
        
        def failing_function():
            raise Exception("Error")
        
        # Open circuit
        with pytest.raises(Exception):
            cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
        
        # Should still be open after 1 second
        time.sleep(1)
        
        with pytest.raises(CircuitBreakerError):
            cb.call(failing_function)
        
        assert cb.state == CircuitState.OPEN
    
    def test_concurrent_calls(self):
        """Test circuit breaker behavior with concurrent-like calls"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Error")
        
        # Rapidly make failing calls
        for i in range(5):
            try:
                cb.call(failing_function)
            except (Exception, CircuitBreakerError):
                pass
        
        # Circuit should be open
        assert cb.state == CircuitState.OPEN
        
        # Subsequent calls should fail with CircuitBreakerError
        with pytest.raises(CircuitBreakerError):
            cb.call(failing_function)


class TestCircuitBreakerIntegration:
    """Integration tests for circuit breaker with other components"""
    
    def test_circuit_breaker_with_lakebase(self):
        """Test circuit breaker protecting Lakebase operations"""
        @circuit_breaker(
            failure_threshold=3,
            recovery_timeout=30,
            name="lakebase_operations"
        )
        def lakebase_query():
            # Simulated Lakebase operation
            return {"result": "success"}
        
        result = lakebase_query()
        
        assert result['result'] == "success"
        assert lakebase_query.circuit_breaker.state == CircuitState.CLOSED
    
    def test_circuit_breaker_with_databricks_sql(self):
        """Test circuit breaker protecting Databricks SQL operations"""
        db_cb = get_databricks_circuit_breaker()
        
        def execute_sql_query():
            # Simulated SQL execution
            return {"rows": 100}
        
        result = db_cb.call(execute_sql_query)
        
        assert result['rows'] == 100
        assert db_cb.state == CircuitState.CLOSED
    
    def test_circuit_breaker_with_unity_catalog(self):
        """Test circuit breaker protecting Unity Catalog operations"""
        uc_cb = get_unity_catalog_circuit_breaker()
        
        def list_catalogs():
            # Simulated Unity Catalog operation
            return ["main", "dev", "prod"]
        
        result = uc_cb.call(list_catalogs)
        
        assert len(result) == 3
        assert uc_cb.state == CircuitState.CLOSED


class TestCircuitBreakerLogging:
    """Test circuit breaker logging and monitoring"""
    
    @patch('infrastructure.circuit_breaker.logger')
    def test_initialization_logging(self, mock_logger):
        """Test that initialization is logged"""
        cb = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            name="test_breaker"
        )
        
        mock_logger.info.assert_called()
        call_args = mock_logger.info.call_args[0]
        assert call_args[0] == "circuit_breaker_initialized"
    
    @patch('infrastructure.circuit_breaker.logger')
    def test_failure_logging(self, mock_logger):
        """Test that failures are logged"""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30)
        
        def failing_function():
            raise Exception("Error")
        
        with pytest.raises(Exception):
            cb.call(failing_function)
        
        # Check warning was logged
        mock_logger.warning.assert_called()
    
    @patch('infrastructure.circuit_breaker.logger')
    def test_state_transition_logging(self, mock_logger):
        """Test that state transitions are logged"""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        
        def failing_function():
            raise Exception("Error")
        
        # Open circuit
        for i in range(2):
            with pytest.raises(Exception):
                cb.call(failing_function)
        
        # Check that circuit open was logged
        error_calls = [call for call in mock_logger.error.call_args_list]
        assert any('circuit_breaker_opened' in str(call) for call in error_calls)

