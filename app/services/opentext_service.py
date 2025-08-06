"""
OpenText service implementation for CenterFuze OpenText Service.

This module provides the core business logic for interacting with OpenText APIs,
managing accounts, tracking usage, handling number porting, and aggregating data.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import aiohttp
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import time
from dataclasses import asdict

from app.models.opentext import (
    OpenTextAccount, FaxUsage, NumberPorting, UsageData, UsageAggregation,
    AccountStatus, PortingStatus, UsageDataType
)
from app.utils.cache_manager import CacheManager
from app.utils.rate_limiter import RateLimiter


logger = logging.getLogger(__name__)


class OpenTextAPIError(Exception):
    """Custom exception for OpenText API errors."""
    pass


class OpenTextService:
    """
    OpenText service for managing accounts, usage, and number porting.
    
    This service provides comprehensive functionality for:
    - Account management and hierarchy
    - Fax usage tracking and reporting
    - Number porting status and management
    - Usage data aggregation
    - Rate limiting and caching
    """

    def __init__(
        self,
        api_base_url: str,
        api_key: str,
        api_secret: str,
        rate_limiter: Optional[RateLimiter] = None,
        cache_manager: Optional[CacheManager] = None,
        batch_size: int = 100,
        max_concurrent_requests: int = 10
    ):
        """
        Initialize the OpenText service.
        
        Args:
            api_base_url: Base URL for OpenText API
            api_key: API key for authentication
            api_secret: API secret for authentication
            rate_limiter: Rate limiter instance
            cache_manager: Cache manager instance
            batch_size: Default batch size for bulk operations
            max_concurrent_requests: Maximum concurrent HTTP requests
        """
        self.api_base_url = api_base_url.rstrip('/')
        self.api_key = api_key
        self.api_secret = api_secret
        self.batch_size = batch_size
        self.max_concurrent_requests = max_concurrent_requests
        
        # Initialize rate limiter and cache
        self.rate_limiter = rate_limiter or RateLimiter(requests_per_second=10)
        self.cache_manager = cache_manager or CacheManager()
        
        # HTTP session and semaphore for concurrency control
        self._session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(max_concurrent_requests)
        
        # Thread pool for CPU-intensive tasks
        self._thread_pool = ThreadPoolExecutor(max_workers=4)

    async def __aenter__(self):
        """Async context manager entry."""
        await self._init_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()

    async def _init_session(self) -> None:
        """Initialize HTTP session."""
        if not self._session:
            timeout = aiohttp.ClientTimeout(total=30)
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers={
                    'Authorization': f'Bearer {self.api_key}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'CenterFuze-OpenText-Service/1.0'
                }
            )

    async def _close_session(self) -> None:
        """Close HTTP session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        cache_key: Optional[str] = None,
        cache_ttl: int = 300
    ) -> Dict[str, Any]:
        """
        Make an HTTP request to the OpenText API with rate limiting and caching.
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            endpoint: API endpoint
            data: Request data for POST/PUT
            params: Query parameters
            cache_key: Cache key for GET requests
            cache_ttl: Cache TTL in seconds
            
        Returns:
            Response data as dictionary
            
        Raises:
            OpenTextAPIError: If the API request fails
        """
        if not self._session:
            await self._init_session()

        # Check cache for GET requests
        if method == 'GET' and cache_key:
            cached_result = await self.cache_manager.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for key: {cache_key}")
                return cached_result

        # Apply rate limiting
        await self.rate_limiter.acquire()
        
        url = f"{self.api_base_url}{endpoint}"
        
        async with self._semaphore:
            try:
                logger.debug(f"Making {method} request to {url}")
                
                async with self._session.request(
                    method=method,
                    url=url,
                    json=data,
                    params=params
                ) as response:
                    response_data = await response.json()
                    
                    if response.status >= 400:
                        logger.error(f"API error {response.status}: {response_data}")
                        raise OpenTextAPIError(
                            f"API request failed with status {response.status}: {response_data}"
                        )
                    
                    # Cache successful GET requests
                    if method == 'GET' and cache_key:
                        await self.cache_manager.set(cache_key, response_data, ttl=cache_ttl)
                    
                    return response_data
                    
            except aiohttp.ClientError as e:
                logger.error(f"HTTP client error: {e}")
                raise OpenTextAPIError(f"HTTP client error: {e}")
            except Exception as e:
                logger.error(f"Unexpected error during API request: {e}")
                raise OpenTextAPIError(f"Unexpected error: {e}")

    # Account Management Methods

    async def get_account(self, account_id: str) -> Optional[OpenTextAccount]:
        """
        Retrieve a single account by ID.
        
        Args:
            account_id: Account identifier
            
        Returns:
            OpenTextAccount instance or None if not found
        """
        cache_key = f"account:{account_id}"
        
        try:
            response_data = await self._make_request(
                'GET',
                f'/accounts/{account_id}',
                cache_key=cache_key,
                cache_ttl=600
            )
            return OpenTextAccount.from_dict(response_data)
        except OpenTextAPIError as e:
            logger.error(f"Failed to get account {account_id}: {e}")
            return None

    async def get_accounts_batch(self, account_ids: List[str]) -> List[OpenTextAccount]:
        """
        Retrieve multiple accounts in batches.
        
        Args:
            account_ids: List of account identifiers
            
        Returns:
            List of OpenTextAccount instances
        """
        accounts = []
        
        # Process accounts in batches
        for i in range(0, len(account_ids), self.batch_size):
            batch_ids = account_ids[i:i + self.batch_size]
            
            # Create concurrent tasks for this batch
            tasks = [self.get_account(account_id) for account_id in batch_ids]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            for result in batch_results:
                if isinstance(result, OpenTextAccount):
                    accounts.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error in batch account retrieval: {result}")
        
        return accounts

    async def get_child_accounts(self, parent_account_id: str) -> List[OpenTextAccount]:
        """
        Retrieve child accounts for a parent account.
        
        Args:
            parent_account_id: Parent account identifier
            
        Returns:
            List of child OpenTextAccount instances
        """
        cache_key = f"child_accounts:{parent_account_id}"
        
        try:
            response_data = await self._make_request(
                'GET',
                f'/accounts/{parent_account_id}/children',
                cache_key=cache_key,
                cache_ttl=300
            )
            
            child_accounts = []
            for account_data in response_data.get('accounts', []):
                child_accounts.append(OpenTextAccount.from_dict(account_data))
            
            return child_accounts
        except OpenTextAPIError as e:
            logger.error(f"Failed to get child accounts for {parent_account_id}: {e}")
            return []

    async def update_account(self, account: OpenTextAccount) -> bool:
        """
        Update an existing account.
        
        Args:
            account: OpenTextAccount instance to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            account.last_updated = datetime.now()
            await self._make_request(
                'PUT',
                f'/accounts/{account.account_id}',
                data=account.to_dict()
            )
            
            # Invalidate cache
            cache_key = f"account:{account.account_id}"
            await self.cache_manager.delete(cache_key)
            
            return True
        except OpenTextAPIError as e:
            logger.error(f"Failed to update account {account.account_id}: {e}")
            return False

    # Fax Usage Methods

    async def get_fax_usage(
        self,
        account_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[FaxUsage]:
        """
        Retrieve fax usage for an account within a date range.
        
        Args:
            account_id: Account identifier
            start_date: Start of the period
            end_date: End of the period
            
        Returns:
            FaxUsage instance or None if not found
        """
        cache_key = f"fax_usage:{account_id}:{start_date.isoformat()}:{end_date.isoformat()}"
        
        try:
            params = {
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }
            
            response_data = await self._make_request(
                'GET',
                f'/accounts/{account_id}/fax/usage',
                params=params,
                cache_key=cache_key,
                cache_ttl=900
            )
            
            return FaxUsage.from_dict(response_data)
        except OpenTextAPIError as e:
            logger.error(f"Failed to get fax usage for {account_id}: {e}")
            return None

    async def sync_fax_usage(
        self,
        account_ids: List[str],
        start_date: datetime,
        end_date: datetime
    ) -> List[FaxUsage]:
        """
        Synchronize fax usage for multiple accounts.
        
        Args:
            account_ids: List of account identifiers
            start_date: Start of the period
            end_date: End of the period
            
        Returns:
            List of FaxUsage instances
        """
        usage_records = []
        
        # Process accounts in batches
        for i in range(0, len(account_ids), self.batch_size):
            batch_ids = account_ids[i:i + self.batch_size]
            
            # Create concurrent tasks for this batch
            tasks = [
                self.get_fax_usage(account_id, start_date, end_date)
                for account_id in batch_ids
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            for result in batch_results:
                if isinstance(result, FaxUsage):
                    usage_records.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error in batch fax usage sync: {result}")
        
        return usage_records

    # Number Porting Methods

    async def get_porting_status(self, phone_number: str) -> Optional[NumberPorting]:
        """
        Get the porting status for a phone number.
        
        Args:
            phone_number: Phone number to check
            
        Returns:
            NumberPorting instance or None if not found
        """
        cache_key = f"porting:{phone_number}"
        
        try:
            response_data = await self._make_request(
                'GET',
                f'/porting/{phone_number}',
                cache_key=cache_key,
                cache_ttl=300
            )
            return NumberPorting.from_dict(response_data)
        except OpenTextAPIError as e:
            logger.error(f"Failed to get porting status for {phone_number}: {e}")
            return None

    async def update_porting_status(self, porting: NumberPorting) -> bool:
        """
        Update the porting status for a phone number.
        
        Args:
            porting: NumberPorting instance to update
            
        Returns:
            True if successful, False otherwise
        """
        try:
            await self._make_request(
                'PUT',
                f'/porting/{porting.phone_number}',
                data=porting.to_dict()
            )
            
            # Invalidate cache
            cache_key = f"porting:{porting.phone_number}"
            await self.cache_manager.delete(cache_key)
            
            return True
        except OpenTextAPIError as e:
            logger.error(f"Failed to update porting status for {porting.phone_number}: {e}")
            return False

    async def batch_porting_status(self, phone_numbers: List[str]) -> List[NumberPorting]:
        """
        Get porting status for multiple phone numbers.
        
        Args:
            phone_numbers: List of phone numbers
            
        Returns:
            List of NumberPorting instances
        """
        porting_records = []
        
        # Process numbers in batches
        for i in range(0, len(phone_numbers), self.batch_size):
            batch_numbers = phone_numbers[i:i + self.batch_size]
            
            # Create concurrent tasks for this batch
            tasks = [self.get_porting_status(number) for number in batch_numbers]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Filter out None results and exceptions
            for result in batch_results:
                if isinstance(result, NumberPorting):
                    porting_records.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Error in batch porting status check: {result}")
        
        return porting_records

    # Usage Data Methods

    async def get_usage_data(
        self,
        account_id: str,
        usage_type: UsageDataType,
        start_date: datetime,
        end_date: datetime
    ) -> List[UsageData]:
        """
        Retrieve usage data for an account and usage type.
        
        Args:
            account_id: Account identifier
            usage_type: Type of usage data
            start_date: Start of the period
            end_date: End of the period
            
        Returns:
            List of UsageData instances
        """
        cache_key = f"usage:{account_id}:{usage_type.value}:{start_date.isoformat()}:{end_date.isoformat()}"
        
        try:
            params = {
                'usage_type': usage_type.value,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }
            
            response_data = await self._make_request(
                'GET',
                f'/accounts/{account_id}/usage',
                params=params,
                cache_key=cache_key,
                cache_ttl=600
            )
            
            usage_data = []
            for usage_item in response_data.get('usage', []):
                usage_data.append(UsageData.from_dict(usage_item))
            
            return usage_data
        except OpenTextAPIError as e:
            logger.error(f"Failed to get usage data for {account_id}: {e}")
            return []

    async def aggregate_usage(
        self,
        account_ids: List[str],
        usage_type: UsageDataType,
        start_date: datetime,
        end_date: datetime
    ) -> UsageAggregation:
        """
        Aggregate usage data across multiple accounts.
        
        Args:
            account_ids: List of account identifiers
            usage_type: Type of usage to aggregate
            start_date: Start of the period
            end_date: End of the period
            
        Returns:
            UsageAggregation instance
        """
        logger.info(f"Aggregating {usage_type.value} usage for {len(account_ids)} accounts")
        
        # Collect usage data for all accounts
        all_usage_data = []
        for account_id in account_ids:
            usage_data = await self.get_usage_data(account_id, usage_type, start_date, end_date)
            all_usage_data.extend(usage_data)
        
        # Aggregate the data
        total_quantity = sum(usage.quantity for usage in all_usage_data)
        total_cost = sum(usage.cost for usage in all_usage_data)
        
        # Create breakdown by account
        breakdown = {}
        for account_id in account_ids:
            account_usage = [usage for usage in all_usage_data if usage.account_id == account_id]
            breakdown[account_id] = {
                'quantity': sum(usage.quantity for usage in account_usage),
                'cost': sum(usage.cost for usage in account_usage),
                'count': len(account_usage)
            }
        
        return UsageAggregation(
            account_ids=account_ids,
            usage_type=usage_type,
            total_quantity=total_quantity,
            total_cost=total_cost,
            period_start=start_date,
            period_end=end_date,
            breakdown=breakdown
        )

    # Utility Methods

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform a health check of the OpenText API.
        
        Returns:
            Health check status and metrics
        """
        start_time = time.time()
        
        try:
            response_data = await self._make_request('GET', '/health')
            end_time = time.time()
            
            return {
                'status': 'healthy',
                'api_status': response_data.get('status', 'unknown'),
                'response_time_ms': int((end_time - start_time) * 1000),
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            end_time = time.time()
            
            return {
                'status': 'unhealthy',
                'error': str(e),
                'response_time_ms': int((end_time - start_time) * 1000),
                'timestamp': datetime.now().isoformat()
            }

    async def get_rate_limit_status(self) -> Dict[str, Any]:
        """
        Get current rate limit status.
        
        Returns:
            Rate limit status information
        """
        return {
            'requests_per_second': self.rate_limiter.requests_per_second,
            'current_tokens': self.rate_limiter.current_tokens,
            'max_tokens': self.rate_limiter.max_tokens,
            'last_refill': self.rate_limiter.last_refill.isoformat() if self.rate_limiter.last_refill else None
        }

    async def clear_cache(self, pattern: Optional[str] = None) -> int:
        """
        Clear cache entries, optionally filtered by pattern.
        
        Args:
            pattern: Optional pattern to match cache keys
            
        Returns:
            Number of cache entries cleared
        """
        return await self.cache_manager.clear(pattern)