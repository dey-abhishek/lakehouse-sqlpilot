"""
Infrastructure Module
Provides scalability and resilience components:
- Lakebase backend for distributed state
- Circuit breaker for fault tolerance
- Connection pooling for performance
"""

# Import circuit breaker (no external dependencies)
from .circuit_breaker import (
    CircuitBreaker,
    CircuitState,
    CircuitBreakerError,
    circuit_breaker,
    get_databricks_circuit_breaker,
    get_unity_catalog_circuit_breaker,
    reset_all_circuit_breakers,
    get_all_circuit_breaker_states
)

# Conditionally import Lakebase (requires psycopg2)
try:
    from .lakebase_backend import (
        LakebaseBackend,
        get_lakebase_backend,
        close_lakebase
    )
    LAKEBASE_AVAILABLE = True
except ImportError:
    LAKEBASE_AVAILABLE = False
    LakebaseBackend = None
    get_lakebase_backend = None
    close_lakebase = None

__all__ = [
    # Circuit Breaker (always available)
    "CircuitBreaker",
    "CircuitState",
    "CircuitBreakerError",
    "circuit_breaker",
    "get_databricks_circuit_breaker",
    "get_unity_catalog_circuit_breaker",
    "reset_all_circuit_breakers",
    "get_all_circuit_breaker_states",
    
    # Lakebase (conditionally available)
    "LakebaseBackend",
    "get_lakebase_backend",
    "close_lakebase",
    "LAKEBASE_AVAILABLE",
]

