"""
Retry decorator with exponential backoff for handling transient failures.

Usage:
    @retry_with_backoff(attempts=3, backoff_factor=2.0)
    async def my_function():
        # Your code here
        pass
"""

import asyncio
import logging
from typing import Callable, Any, Type, Tuple
from functools import wraps

logger = logging.getLogger(__name__)


class RetryableError(Exception):
    """Base exception for errors that should trigger retry logic."""
    pass


class MaxRetriesExceeded(Exception):
    """Raised when max retry attempts are exhausted."""
    pass


def retry_with_backoff(
    attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Retry decorator with exponential backoff.
    
    Args:
        attempts: Maximum number of attempts (including initial try)
        backoff_factor: Multiplier for delay between retries (e.g., 2.0 = exponential)
        initial_delay: Initial delay in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        exceptions: Tuple of exception types to catch and retry
    
    Returns:
        Decorated function with retry logic
    
    Example:
        @retry_with_backoff(attempts=5, backoff_factor=2.0)
        async def place_order():
            # Will retry up to 5 times with exponential backoff
            # Delays: 1s, 2s, 4s, 8s, 16s
            return await binance.place_order(...)
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(1, attempts + 1):
                try:
                    # Attempt the function call
                    result = await func(*args, **kwargs)
                    
                    # Success!
                    if attempt > 1:
                        logger.info(
                            f"✅ {func.__name__} succeeded on attempt {attempt}/{attempts}"
                        )
                    return result
                    
                except exceptions as e:
                    last_exception = e
                    
                    # If this was the last attempt, don't retry
                    if attempt == attempts:
                        logger.error(
                            f"❌ {func.__name__} failed after {attempts} attempts. "
                            f"Last error: {str(e)}"
                        )
                        raise MaxRetriesExceeded(
                            f"{func.__name__} failed after {attempts} attempts"
                        ) from e
                    
                    # Calculate delay for next retry (exponential backoff)
                    delay = min(
                        initial_delay * (backoff_factor ** (attempt - 1)),
                        max_delay
                    )
                    
                    logger.warning(
                        f"⚠️ {func.__name__} attempt {attempt}/{attempts} failed: {str(e)}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    
                    # Wait before retrying
                    await asyncio.sleep(delay)
            
            # Should never reach here, but just in case
            raise last_exception
        
        return wrapper
    return decorator


def retry_sync(
    attempts: int = 3,
    backoff_factor: float = 2.0,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """
    Retry decorator for synchronous functions (non-async).
    
    Args:
        attempts: Maximum number of attempts
        backoff_factor: Multiplier for delay between retries
        initial_delay: Initial delay in seconds
        max_delay: Maximum delay between retries
        exceptions: Tuple of exception types to catch
    
    Returns:
        Decorated function with retry logic
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import time
            last_exception = None
            
            for attempt in range(1, attempts + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 1:
                        logger.info(
                            f"✅ {func.__name__} succeeded on attempt {attempt}/{attempts}"
                        )
                    return result
                    
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == attempts:
                        logger.error(
                            f"❌ {func.__name__} failed after {attempts} attempts. "
                            f"Last error: {str(e)}"
                        )
                        raise MaxRetriesExceeded(
                            f"{func.__name__} failed after {attempts} attempts"
                        ) from e
                    
                    delay = min(
                        initial_delay * (backoff_factor ** (attempt - 1)),
                        max_delay
                    )
                    
                    logger.warning(
                        f"⚠️ {func.__name__} attempt {attempt}/{attempts} failed: {str(e)}. "
                        f"Retrying in {delay:.1f}s..."
                    )
                    
                    time.sleep(delay)
            
            raise last_exception
        
        return wrapper
    return decorator

