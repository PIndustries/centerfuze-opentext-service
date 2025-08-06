"""
Cache manager for the CenterFuze OpenText Service.

This module provides caching functionality with TTL support and pattern-based clearing.
"""

import asyncio
import logging
import time
from typing import Any, Dict, Optional, List
import json
import re
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with value and expiration time."""
    value: Any
    expires_at: float


class CacheManager:
    """
    In-memory cache manager with TTL support.
    
    This cache manager provides:
    - TTL-based expiration
    - Pattern-based key matching
    - Automatic cleanup of expired entries
    - Thread-safe operations
    """

    def __init__(self, cleanup_interval: int = 300):
        """
        Initialize the cache manager.
        
        Args:
            cleanup_interval: Interval in seconds for cleanup task
        """
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self.cleanup_interval = cleanup_interval
        self._cleanup_task: Optional[asyncio.Task] = None
        self._start_cleanup_task()

    def _start_cleanup_task(self) -> None:
        """Start the background cleanup task."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def _cleanup_loop(self) -> None:
        """Background task to clean up expired cache entries."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._cleanup_expired()
            except asyncio.CancelledError:
                logger.info("Cache cleanup task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cache cleanup: {e}")

    async def _cleanup_expired(self) -> None:
        """Remove expired entries from the cache."""
        current_time = time.time()
        expired_keys = []
        
        async with self._lock:
            for key, entry in self._cache.items():
                if entry.expires_at <= current_time:
                    expired_keys.append(key)
            
            for key in expired_keys:
                del self._cache[key]
        
        if expired_keys:
            logger.debug(f"Cleaned up {len(expired_keys)} expired cache entries")

    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value from the cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found or expired
        """
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None:
                return None
            
            # Check if entry has expired
            if entry.expires_at <= time.time():
                del self._cache[key]
                return None
            
            return entry.value

    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        """
        Set a value in the cache with TTL.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        expires_at = time.time() + ttl
        entry = CacheEntry(value=value, expires_at=expires_at)
        
        async with self._lock:
            self._cache[key] = entry

    async def delete(self, key: str) -> bool:
        """
        Delete a specific key from the cache.
        
        Args:
            key: Cache key to delete
            
        Returns:
            True if key was deleted, False if not found
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    async def clear(self, pattern: Optional[str] = None) -> int:
        """
        Clear cache entries, optionally filtered by pattern.
        
        Args:
            pattern: Optional regex pattern to match keys
            
        Returns:
            Number of entries cleared
        """
        cleared_count = 0
        
        async with self._lock:
            if pattern is None:
                cleared_count = len(self._cache)
                self._cache.clear()
            else:
                regex = re.compile(pattern)
                keys_to_delete = [key for key in self._cache.keys() if regex.match(key)]
                
                for key in keys_to_delete:
                    del self._cache[key]
                
                cleared_count = len(keys_to_delete)
        
        logger.info(f"Cleared {cleared_count} cache entries")
        return cleared_count

    async def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache statistics
        """
        current_time = time.time()
        
        async with self._lock:
            total_entries = len(self._cache)
            expired_entries = sum(
                1 for entry in self._cache.values()
                if entry.expires_at <= current_time
            )
            active_entries = total_entries - expired_entries
            
            # Calculate memory usage estimate (rough)
            memory_usage = 0
            for key, entry in self._cache.items():
                memory_usage += len(key.encode('utf-8'))
                try:
                    memory_usage += len(json.dumps(entry.value).encode('utf-8'))
                except (TypeError, ValueError):
                    # Fallback for non-serializable objects
                    memory_usage += 100  # Rough estimate
        
        return {
            'total_entries': total_entries,
            'active_entries': active_entries,
            'expired_entries': expired_entries,
            'estimated_memory_bytes': memory_usage,
            'cleanup_interval': self.cleanup_interval
        }

    async def exists(self, key: str) -> bool:
        """
        Check if a key exists and is not expired.
        
        Args:
            key: Cache key to check
            
        Returns:
            True if key exists and is not expired
        """
        return await self.get(key) is not None

    async def touch(self, key: str, ttl: int) -> bool:
        """
        Update the TTL of an existing cache entry.
        
        Args:
            key: Cache key
            ttl: New time-to-live in seconds
            
        Returns:
            True if key was found and updated, False otherwise
        """
        async with self._lock:
            entry = self._cache.get(key)
            
            if entry is None or entry.expires_at <= time.time():
                return False
            
            entry.expires_at = time.time() + ttl
            return True

    async def get_keys(self, pattern: Optional[str] = None) -> List[str]:
        """
        Get all cache keys, optionally filtered by pattern.
        
        Args:
            pattern: Optional regex pattern to match keys
            
        Returns:
            List of matching cache keys
        """
        async with self._lock:
            if pattern is None:
                return list(self._cache.keys())
            
            regex = re.compile(pattern)
            return [key for key in self._cache.keys() if regex.match(key)]

    async def shutdown(self) -> None:
        """Shutdown the cache manager and cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        async with self._lock:
            self._cache.clear()
        
        logger.info("Cache manager shutdown complete")