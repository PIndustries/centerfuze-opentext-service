"""
Configuration settings for the CenterFuze OpenText Service.

This module manages application configuration using environment variables
with sensible defaults and validation.
"""

import os
import logging
from typing import List, Optional
from dataclasses import dataclass
from urllib.parse import urlparse


@dataclass
class NATSConfig:
    """NATS configuration settings."""
    servers: List[str]
    user: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    reconnect_time_wait: int = 2
    max_reconnect_attempts: int = 60
    ping_interval: int = 120
    max_outstanding_pings: int = 2


@dataclass
class OpenTextConfig:
    """OpenText API configuration settings."""
    api_base_url: str
    api_key: str
    api_secret: str
    timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    requests_per_second: float = 10.0
    burst_capacity: Optional[int] = None
    adaptive: bool = True
    min_requests_per_second: float = 1.0
    max_requests_per_second: float = 100.0


@dataclass
class CacheConfig:
    """Cache configuration settings."""
    enabled: bool = True
    default_ttl: int = 300
    cleanup_interval: int = 300
    max_size: Optional[int] = None


@dataclass
class ServiceConfig:
    """Service-level configuration."""
    service_name: str = "centerfuze-opentext-service"
    version: str = "1.0.0"
    environment: str = "development"
    log_level: str = "INFO"
    batch_size: int = 100
    max_concurrent_requests: int = 10
    worker_threads: int = 4


class Settings:
    """
    Application settings manager.
    
    This class loads configuration from environment variables and provides
    typed access to all configuration values.
    """

    def __init__(self):
        """Initialize settings from environment variables."""
        self.nats = self._load_nats_config()
        self.opentext = self._load_opentext_config()
        self.rate_limit = self._load_rate_limit_config()
        self.cache = self._load_cache_config()
        self.service = self._load_service_config()
        
        # Validate configuration
        self._validate_config()

    def _load_nats_config(self) -> NATSConfig:
        """Load NATS configuration from environment."""
        # NATS servers can be comma-separated list
        nats_servers = os.getenv("NATS_SERVERS", "nats://localhost:4222")
        servers = [server.strip() for server in nats_servers.split(",")]
        
        return NATSConfig(
            servers=servers,
            user=os.getenv("NATS_USER"),
            password=os.getenv("NATS_PASSWORD"),
            token=os.getenv("NATS_TOKEN"),
            reconnect_time_wait=int(os.getenv("NATS_RECONNECT_TIME_WAIT", "2")),
            max_reconnect_attempts=int(os.getenv("NATS_MAX_RECONNECT_ATTEMPTS", "60")),
            ping_interval=int(os.getenv("NATS_PING_INTERVAL", "120")),
            max_outstanding_pings=int(os.getenv("NATS_MAX_OUTSTANDING_PINGS", "2"))
        )

    def _load_opentext_config(self) -> OpenTextConfig:
        """Load OpenText API configuration from environment."""
        api_base_url = os.getenv("OPENTEXT_API_BASE_URL")
        api_key = os.getenv("OPENTEXT_API_KEY")
        api_secret = os.getenv("OPENTEXT_API_SECRET")
        
        if not api_base_url:
            raise ValueError("OPENTEXT_API_BASE_URL environment variable is required")
        if not api_key:
            raise ValueError("OPENTEXT_API_KEY environment variable is required")
        if not api_secret:
            raise ValueError("OPENTEXT_API_SECRET environment variable is required")
        
        return OpenTextConfig(
            api_base_url=api_base_url,
            api_key=api_key,
            api_secret=api_secret,
            timeout=int(os.getenv("OPENTEXT_API_TIMEOUT", "30")),
            max_retries=int(os.getenv("OPENTEXT_MAX_RETRIES", "3")),
            retry_delay=int(os.getenv("OPENTEXT_RETRY_DELAY", "1"))
        )

    def _load_rate_limit_config(self) -> RateLimitConfig:
        """Load rate limiting configuration from environment."""
        burst_capacity = os.getenv("RATE_LIMIT_BURST_CAPACITY")
        
        return RateLimitConfig(
            requests_per_second=float(os.getenv("RATE_LIMIT_REQUESTS_PER_SECOND", "10.0")),
            burst_capacity=int(burst_capacity) if burst_capacity else None,
            adaptive=os.getenv("RATE_LIMIT_ADAPTIVE", "true").lower() == "true",
            min_requests_per_second=float(os.getenv("RATE_LIMIT_MIN_RPS", "1.0")),
            max_requests_per_second=float(os.getenv("RATE_LIMIT_MAX_RPS", "100.0"))
        )

    def _load_cache_config(self) -> CacheConfig:
        """Load cache configuration from environment."""
        max_size = os.getenv("CACHE_MAX_SIZE")
        
        return CacheConfig(
            enabled=os.getenv("CACHE_ENABLED", "true").lower() == "true",
            default_ttl=int(os.getenv("CACHE_DEFAULT_TTL", "300")),
            cleanup_interval=int(os.getenv("CACHE_CLEANUP_INTERVAL", "300")),
            max_size=int(max_size) if max_size else None
        )

    def _load_service_config(self) -> ServiceConfig:
        """Load service-level configuration from environment."""
        return ServiceConfig(
            service_name=os.getenv("SERVICE_NAME", "centerfuze-opentext-service"),
            version=os.getenv("SERVICE_VERSION", "1.0.0"),
            environment=os.getenv("ENVIRONMENT", "development"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            max_concurrent_requests=int(os.getenv("MAX_CONCURRENT_REQUESTS", "10")),
            worker_threads=int(os.getenv("WORKER_THREADS", "4"))
        )

    def _validate_config(self) -> None:
        """Validate configuration values."""
        # Validate NATS servers
        for server in self.nats.servers:
            parsed = urlparse(server)
            if not parsed.scheme or not parsed.netloc:
                raise ValueError(f"Invalid NATS server URL: {server}")
        
        # Validate OpenText API URL
        parsed_url = urlparse(self.opentext.api_base_url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid OpenText API URL: {self.opentext.api_base_url}")
        
        # Validate rate limiting values
        if self.rate_limit.requests_per_second <= 0:
            raise ValueError("Rate limit requests_per_second must be positive")
        
        if (self.rate_limit.min_requests_per_second >= 
            self.rate_limit.max_requests_per_second):
            raise ValueError("min_requests_per_second must be less than max_requests_per_second")
        
        # Validate batch size
        if self.service.batch_size <= 0:
            raise ValueError("Batch size must be positive")
        
        # Validate log level
        valid_log_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if self.service.log_level not in valid_log_levels:
            raise ValueError(f"Invalid log level: {self.service.log_level}")

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.service.environment.lower() == "production"

    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.service.environment.lower() == "development"

    def get_log_level(self) -> int:
        """Get numeric log level for logging configuration."""
        return getattr(logging, self.service.log_level.upper())

    def to_dict(self) -> dict:
        """Convert settings to dictionary (excluding sensitive data)."""
        return {
            "service": {
                "name": self.service.service_name,
                "version": self.service.version,
                "environment": self.service.environment,
                "log_level": self.service.log_level,
                "batch_size": self.service.batch_size,
                "max_concurrent_requests": self.service.max_concurrent_requests,
                "worker_threads": self.service.worker_threads
            },
            "nats": {
                "servers": self.nats.servers,
                "reconnect_time_wait": self.nats.reconnect_time_wait,
                "max_reconnect_attempts": self.nats.max_reconnect_attempts,
                "ping_interval": self.nats.ping_interval,
                "max_outstanding_pings": self.nats.max_outstanding_pings
            },
            "opentext": {
                "api_base_url": self.opentext.api_base_url,
                "timeout": self.opentext.timeout,
                "max_retries": self.opentext.max_retries,
                "retry_delay": self.opentext.retry_delay
            },
            "rate_limit": {
                "requests_per_second": self.rate_limit.requests_per_second,
                "burst_capacity": self.rate_limit.burst_capacity,
                "adaptive": self.rate_limit.adaptive,
                "min_requests_per_second": self.rate_limit.min_requests_per_second,
                "max_requests_per_second": self.rate_limit.max_requests_per_second
            },
            "cache": {
                "enabled": self.cache.enabled,
                "default_ttl": self.cache.default_ttl,
                "cleanup_interval": self.cache.cleanup_interval,
                "max_size": self.cache.max_size
            }
        }


# Global settings instance
settings = Settings()