"""
Circuit Breaker Pattern Implementation
Prevents cascading failures by detecting failures and short-circuiting requests
"""

from enum import Enum
from typing import Callable, Any, Optional
import time
import structlog
from functools import wraps
from datetime import datetime, timedelta

logger = structlog.get_logger()


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"       # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""
    pass


class CircuitBreaker:
    """
    Circuit Breaker implementation
    
    Protects against cascading failures by monitoring failures and
    temporarily blocking requests when failure threshold is exceeded.
    """
    
    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: type = Exception,
                 name: str = "circuit_breaker"):
        """
        Initialize circuit breaker
        
        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
            name: Circuit breaker name for logging
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        # State tracking
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_state_change: float = time.time()
        
        logger.info(
            "circuit_breaker_initialized",
            name=name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout
        )
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection
        
        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: If circuit is open
            Exception: If function raises exception
        """
        # Check if circuit should transition from OPEN to HALF_OPEN
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self._transition_to_half_open()
            else:
                raise CircuitBreakerError(
                    f"Circuit breaker '{self.name}' is OPEN. "
                    f"Retry after {self.recovery_timeout - (time.time() - self.last_failure_time):.0f}s"
                )
        
        try:
            # Execute function
            result = func(*args, **kwargs)
            
            # Success - handle state transitions
            self._on_success()
            
            return result
            
        except self.expected_exception as e:
            # Failure - handle state transitions
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call"""
        if self.state == CircuitState.HALF_OPEN:
            # Successful call in HALF_OPEN state - close circuit
            self._transition_to_closed()
        
        # Reset failure count on success
        if self.failure_count > 0:
            logger.debug(
                "circuit_breaker_success_reset",
                name=self.name,
                previous_failures=self.failure_count
            )
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed call"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        logger.warning(
            "circuit_breaker_failure",
            name=self.name,
            failure_count=self.failure_count,
            threshold=self.failure_threshold,
            state=self.state.value
        )
        
        if self.state == CircuitState.HALF_OPEN:
            # Failed in HALF_OPEN - back to OPEN
            self._transition_to_open()
        elif self.failure_count >= self.failure_threshold:
            # Reached threshold - open circuit
            self._transition_to_open()
    
    def _transition_to_open(self):
        """Transition to OPEN state"""
        self.state = CircuitState.OPEN
        self.last_state_change = time.time()
        
        logger.error(
            "circuit_breaker_opened",
            name=self.name,
            failure_count=self.failure_count,
            recovery_timeout=self.recovery_timeout
        )
    
    def _transition_to_half_open(self):
        """Transition to HALF_OPEN state"""
        self.state = CircuitState.HALF_OPEN
        self.last_state_change = time.time()
        
        logger.info(
            "circuit_breaker_half_open",
            name=self.name,
            testing_recovery=True
        )
    
    def _transition_to_closed(self):
        """Transition to CLOSED state"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_state_change = time.time()
        
        logger.info(
            "circuit_breaker_closed",
            name=self.name,
            recovery_successful=True
        )
    
    def reset(self):
        """Manually reset circuit breaker"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.last_failure_time = None
        self.last_state_change = time.time()
        
        logger.info("circuit_breaker_reset", name=self.name)
    
    def get_state(self) -> dict:
        """Get current circuit breaker state"""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "failure_threshold": self.failure_threshold,
            "last_failure_time": self.last_failure_time,
            "last_state_change": self.last_state_change,
            "recovery_timeout": self.recovery_timeout
        }


def circuit_breaker(failure_threshold: int = 5,
                   recovery_timeout: int = 60,
                   expected_exception: type = Exception,
                   name: Optional[str] = None):
    """
    Circuit breaker decorator
    
    Usage:
        @circuit_breaker(failure_threshold=3, recovery_timeout=30)
        def call_external_api():
            # ... API call ...
            pass
    
    Args:
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Seconds to wait before attempting recovery
        expected_exception: Exception type to catch
        name: Circuit breaker name (defaults to function name)
    """
    def decorator(func: Callable):
        breaker_name = name or func.__name__
        breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            expected_exception=expected_exception,
            name=breaker_name
        )
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        
        # Attach breaker instance for testing/monitoring
        wrapper.circuit_breaker = breaker
        
        return wrapper
    
    return decorator


# Global circuit breakers for common services
_databricks_circuit_breaker: Optional[CircuitBreaker] = None
_unity_catalog_circuit_breaker: Optional[CircuitBreaker] = None


def get_databricks_circuit_breaker() -> CircuitBreaker:
    """Get global circuit breaker for Databricks SQL operations"""
    global _databricks_circuit_breaker
    
    if _databricks_circuit_breaker is None:
        _databricks_circuit_breaker = CircuitBreaker(
            failure_threshold=5,
            recovery_timeout=60,
            name="databricks_sql"
        )
    
    return _databricks_circuit_breaker


def get_unity_catalog_circuit_breaker() -> CircuitBreaker:
    """Get global circuit breaker for Unity Catalog operations"""
    global _unity_catalog_circuit_breaker
    
    if _unity_catalog_circuit_breaker is None:
        _unity_catalog_circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30,
            name="unity_catalog"
        )
    
    return _unity_catalog_circuit_breaker


def reset_all_circuit_breakers():
    """Reset all global circuit breakers"""
    global _databricks_circuit_breaker, _unity_catalog_circuit_breaker
    
    if _databricks_circuit_breaker:
        _databricks_circuit_breaker.reset()
    
    if _unity_catalog_circuit_breaker:
        _unity_catalog_circuit_breaker.reset()
    
    logger.info("all_circuit_breakers_reset")


def get_all_circuit_breaker_states() -> dict:
    """Get state of all circuit breakers"""
    states = {}
    
    if _databricks_circuit_breaker:
        states["databricks_sql"] = _databricks_circuit_breaker.get_state()
    
    if _unity_catalog_circuit_breaker:
        states["unity_catalog"] = _unity_catalog_circuit_breaker.get_state()
    
    return states

