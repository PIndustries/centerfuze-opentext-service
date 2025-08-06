"""
Rate limiter for the CenterFuze OpenText Service.

This module provides rate limiting functionality using a token bucket algorithm.
"""

import asyncio
import logging
import time
from datetime import datetime
from typing import Optional


logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token bucket rate limiter for controlling request rates.
    
    This rate limiter provides:
    - Token bucket algorithm implementation
    - Configurable requests per second
    - Burst capacity handling
    - Thread-safe operations
    """

    def __init__(
        self,
        requests_per_second: float = 10.0,
        burst_capacity: Optional[int] = None
    ):
        """
        Initialize the rate limiter.
        
        Args:
            requests_per_second: Maximum requests per second allowed
            burst_capacity: Maximum burst capacity (defaults to requests_per_second)
        """
        self.requests_per_second = requests_per_second
        self.max_tokens = burst_capacity or int(requests_per_second)
        self.current_tokens = self.max_tokens
        self.last_refill: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens from the bucket.
        
        This method will block until the requested number of tokens are available.
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
        """
        async with self._lock:
            while self.current_tokens < tokens:
                # Calculate how long to wait for tokens to be available
                tokens_needed = tokens - self.current_tokens
                wait_time = tokens_needed / self.requests_per_second
                
                logger.debug(
                    f"Rate limit exceeded, waiting {wait_time:.2f}s for {tokens_needed} tokens"
                )
                
                await asyncio.sleep(wait_time)
                self._refill_tokens()
            
            # Consume tokens
            self.current_tokens -= tokens
            
            logger.debug(
                f"Acquired {tokens} tokens, {self.current_tokens} remaining"
            )

    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without blocking.
        
        Args:
            tokens: Number of tokens to acquire (default: 1)
            
        Returns:
            True if tokens were successfully acquired, False otherwise
        """
        async with self._lock:
            self._refill_tokens()
            
            if self.current_tokens >= tokens:
                self.current_tokens -= tokens
                logger.debug(
                    f"Acquired {tokens} tokens, {self.current_tokens} remaining"
                )
                return True
            else:
                logger.debug(
                    f"Cannot acquire {tokens} tokens, only {self.current_tokens} available"
                )
                return False

    def _refill_tokens(self) -> None:
        """Refill tokens based on elapsed time."""
        now = datetime.now()
        
        if self.last_refill is None:
            self.last_refill = now
            return
        
        # Calculate elapsed time and tokens to add
        elapsed = (now - self.last_refill).total_seconds()
        tokens_to_add = elapsed * self.requests_per_second
        
        if tokens_to_add >= 1:
            self.current_tokens = min(
                self.max_tokens,
                self.current_tokens + int(tokens_to_add)
            )
            self.last_refill = now
            
            logger.debug(
                f"Refilled {int(tokens_to_add)} tokens, current: {self.current_tokens}"
            )

    async def get_available_tokens(self) -> int:
        """
        Get the number of currently available tokens.
        
        Returns:
            Number of available tokens
        """
        async with self._lock:
            self._refill_tokens()
            return self.current_tokens

    async def get_wait_time(self, tokens: int = 1) -> float:
        """
        Get the wait time required to acquire the specified number of tokens.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            Wait time in seconds (0 if tokens are immediately available)
        """
        async with self._lock:
            self._refill_tokens()
            
            if self.current_tokens >= tokens:
                return 0.0
            
            tokens_needed = tokens - self.current_tokens
            return tokens_needed / self.requests_per_second

    async def reset(self) -> None:
        """Reset the rate limiter to full capacity."""
        async with self._lock:
            self.current_tokens = self.max_tokens
            self.last_refill = datetime.now()
            logger.info("Rate limiter reset to full capacity")

    def get_stats(self) -> dict:
        """
        Get rate limiter statistics.
        
        Returns:
            Dictionary with rate limiter statistics
        """
        return {
            'requests_per_second': self.requests_per_second,
            'max_tokens': self.max_tokens,
            'current_tokens': self.current_tokens,
            'last_refill': self.last_refill.isoformat() if self.last_refill else None,
            'utilization_percent': ((self.max_tokens - self.current_tokens) / self.max_tokens) * 100
        }


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts rates based on API responses.
    
    This rate limiter can automatically adjust its rate based on:
    - HTTP 429 (Too Many Requests) responses
    - Response time degradation
    - Error rate increases
    """

    def __init__(
        self,
        initial_requests_per_second: float = 10.0,
        min_requests_per_second: float = 1.0,
        max_requests_per_second: float = 100.0,
        adaptation_factor: float = 0.1
    ):
        """
        Initialize the adaptive rate limiter.
        
        Args:
            initial_requests_per_second: Initial request rate
            min_requests_per_second: Minimum allowed request rate
            max_requests_per_second: Maximum allowed request rate
            adaptation_factor: Factor for rate adjustments (0.0 to 1.0)
        """
        super().__init__(initial_requests_per_second)
        self.min_requests_per_second = min_requests_per_second
        self.max_requests_per_second = max_requests_per_second
        self.adaptation_factor = adaptation_factor
        self._recent_responses = []
        self._max_response_history = 100

    async def record_response(
        self,
        status_code: int,
        response_time: float,
        is_error: bool = False
    ) -> None:
        """
        Record an API response for adaptive rate adjustment.
        
        Args:
            status_code: HTTP status code
            response_time: Response time in seconds
            is_error: Whether this was an error response
        """
        response_data = {
            'timestamp': time.time(),
            'status_code': status_code,
            'response_time': response_time,
            'is_error': is_error
        }
        
        async with self._lock:
            self._recent_responses.append(response_data)
            
            # Keep only recent responses
            if len(self._recent_responses) > self._max_response_history:
                self._recent_responses.pop(0)
            
            # Adjust rate based on response
            await self._adjust_rate(status_code, response_time, is_error)

    async def _adjust_rate(
        self,
        status_code: int,
        response_time: float,
        is_error: bool
    ) -> None:
        """Adjust the request rate based on the response."""
        old_rate = self.requests_per_second
        
        if status_code == 429:  # Too Many Requests
            # Decrease rate significantly
            new_rate = self.requests_per_second * (1 - self.adaptation_factor * 2)
        elif is_error:
            # Decrease rate moderately
            new_rate = self.requests_per_second * (1 - self.adaptation_factor)
        elif response_time > 5.0:  # Slow response
            # Decrease rate slightly
            new_rate = self.requests_per_second * (1 - self.adaptation_factor * 0.5)
        elif status_code == 200 and response_time < 1.0:
            # Good response, try to increase rate slightly
            new_rate = self.requests_per_second * (1 + self.adaptation_factor * 0.2)
        else:
            # No adjustment needed
            return
        
        # Apply bounds
        new_rate = max(self.min_requests_per_second, min(self.max_requests_per_second, new_rate))
        
        if new_rate != old_rate:
            self.requests_per_second = new_rate
            self.max_tokens = int(new_rate)
            
            logger.info(
                f"Adjusted rate limit: {old_rate:.2f} -> {new_rate:.2f} RPS "
                f"(status: {status_code}, time: {response_time:.2f}s)"
            )

    def get_adaptation_stats(self) -> dict:
        """
        Get adaptive rate limiter statistics.
        
        Returns:
            Dictionary with adaptation statistics
        """
        stats = self.get_stats()
        
        if self._recent_responses:
            recent_errors = sum(1 for r in self._recent_responses if r['is_error'])
            avg_response_time = sum(r['response_time'] for r in self._recent_responses) / len(self._recent_responses)
            
            stats.update({
                'recent_responses_count': len(self._recent_responses),
                'recent_error_count': recent_errors,
                'recent_error_rate': recent_errors / len(self._recent_responses),
                'avg_response_time': avg_response_time,
                'min_rate': self.min_requests_per_second,
                'max_rate': self.max_requests_per_second,
                'adaptation_factor': self.adaptation_factor
            })
        
        return stats