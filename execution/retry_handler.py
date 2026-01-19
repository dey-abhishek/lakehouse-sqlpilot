"""
Retry Handler - Handles retry logic for transient failures
"""

import time
from typing import Callable, Any, Optional
from enum import Enum


class RetryStrategy(Enum):
    """Retry strategies"""
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"


class RetryHandler:
    """Handles retry logic with configurable strategies"""
    
    # Transient error patterns that should trigger retry
    TRANSIENT_ERROR_PATTERNS = [
        'connection timeout',
        'warehouse busy',
        'temporarily unavailable',
        'throttled',
        'rate limit',
        'service unavailable',
        'internal error',
        'network error',
    ]
    
    def __init__(self,
                 max_retries: int = 3,
                 base_delay_seconds: int = 60,
                 strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                 max_delay_seconds: int = 600):
        """
        Initialize retry handler
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay_seconds: Base delay between retries
            strategy: Retry strategy to use
            max_delay_seconds: Maximum delay between retries
        """
        self.max_retries = max_retries
        self.base_delay = base_delay_seconds
        self.strategy = strategy
        self.max_delay = max_delay_seconds
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determine if error should trigger retry
        
        Args:
            error: Exception that occurred
            attempt: Current attempt number (0-indexed)
            
        Returns:
            True if should retry, False otherwise
        """
        # Check if max retries exceeded
        if attempt >= self.max_retries:
            return False
        
        # Check if error is transient
        error_message = str(error).lower()
        
        for pattern in self.TRANSIENT_ERROR_PATTERNS:
            if pattern in error_message:
                return True
        
        return False
    
    def get_delay(self, attempt: int) -> int:
        """
        Calculate delay before next retry
        
        Args:
            attempt: Current attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        if self.strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.base_delay * (2 ** attempt)
        elif self.strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.base_delay * (attempt + 1)
        else:  # FIXED_DELAY
            delay = self.base_delay
        
        # Cap at max delay
        return min(delay, self.max_delay)
    
    def execute_with_retry(self,
                          func: Callable,
                          *args,
                          on_retry: Optional[Callable[[int, Exception], None]] = None,
                          **kwargs) -> Any:
        """
        Execute function with retry logic
        
        Args:
            func: Function to execute
            *args: Positional arguments for function
            on_retry: Optional callback called before each retry
            **kwargs: Keyword arguments for function
            
        Returns:
            Function result
            
        Raises:
            Exception: If all retries exhausted
        """
        attempt = 0
        last_error = None
        
        while attempt <= self.max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                
                if self.should_retry(e, attempt):
                    delay = self.get_delay(attempt)
                    
                    # Call retry callback if provided
                    if on_retry:
                        on_retry(attempt, e)
                    
                    time.sleep(delay)
                    attempt += 1
                else:
                    # Non-transient error, don't retry
                    raise
        
        # All retries exhausted
        raise last_error


