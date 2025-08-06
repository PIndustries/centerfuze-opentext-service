#!/usr/bin/env python3
"""
Main entry point for the CenterFuze OpenText Service.

This service provides integration with OpenText APIs for account management,
fax usage tracking, number porting, and usage data aggregation via NATS messaging.
"""

import asyncio
import signal
import sys
from typing import Optional
import logging

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

from app.config.settings import settings
from app.utils.logging_config import setup_logging, create_service_logger
from app.services.opentext_service import OpenTextService
from app.controllers.nats_controller import NATSController
from app.utils.cache_manager import CacheManager
from app.utils.rate_limiter import RateLimiter, AdaptiveRateLimiter


class OpenTextServiceApp:
    """
    Main application class for the CenterFuze OpenText Service.
    
    This class orchestrates the initialization and lifecycle of all service components:
    - NATS connection and message handling
    - OpenText API service
    - Rate limiting and caching
    - Graceful shutdown handling
    """

    def __init__(self):
        """Initialize the service application."""
        self.logger = create_service_logger(
            __name__,
            settings.service.service_name,
            settings.service.version
        )
        
        # Service components
        self.nats_client: Optional[NATS] = None
        self.opentext_service: Optional[OpenTextService] = None
        self.nats_controller: Optional[NATSController] = None
        self.cache_manager: Optional[CacheManager] = None
        self.rate_limiter: Optional[RateLimiter] = None
        
        # Shutdown flag
        self.shutdown_event = asyncio.Event()

    async def initialize(self) -> None:
        """Initialize all service components."""
        self.logger.info("Initializing CenterFuze OpenText Service")
        
        try:
            # Initialize cache manager
            if settings.cache.enabled:
                self.cache_manager = CacheManager(
                    cleanup_interval=settings.cache.cleanup_interval
                )
                self.logger.info("Cache manager initialized")
            
            # Initialize rate limiter
            if settings.rate_limit.adaptive:
                self.rate_limiter = AdaptiveRateLimiter(
                    initial_requests_per_second=settings.rate_limit.requests_per_second,
                    min_requests_per_second=settings.rate_limit.min_requests_per_second,
                    max_requests_per_second=settings.rate_limit.max_requests_per_second
                )
                self.logger.info("Adaptive rate limiter initialized")
            else:
                self.rate_limiter = RateLimiter(
                    requests_per_second=settings.rate_limit.requests_per_second,
                    burst_capacity=settings.rate_limit.burst_capacity
                )
                self.logger.info("Standard rate limiter initialized")
            
            # Initialize OpenText service
            self.opentext_service = OpenTextService(
                api_base_url=settings.opentext.api_base_url,
                api_key=settings.opentext.api_key,
                api_secret=settings.opentext.api_secret,
                rate_limiter=self.rate_limiter,
                cache_manager=self.cache_manager,
                batch_size=settings.service.batch_size,
                max_concurrent_requests=settings.service.max_concurrent_requests
            )
            self.logger.info("OpenText service initialized")
            
            # Initialize NATS client
            self.nats_client = NATS()
            await self._connect_nats()
            
            # Initialize NATS controller
            self.nats_controller = NATSController(
                self.nats_client,
                self.opentext_service
            )
            await self.nats_controller.setup_subscriptions()
            
            self.logger.info(
                "Service initialization completed successfully",
                extra={"config": settings.to_dict()}
            )
            
        except Exception as e:
            self.logger.error(f"Failed to initialize service: {e}", exc_info=True)
            raise

    async def _connect_nats(self) -> None:
        """Connect to NATS with retry logic."""
        connect_options = {
            "servers": settings.nats.servers,
            "reconnect_time_wait": settings.nats.reconnect_time_wait,
            "max_reconnect_attempts": settings.nats.max_reconnect_attempts,
            "ping_interval": settings.nats.ping_interval,
            "max_outstanding_pings": settings.nats.max_outstanding_pings,
            "error_cb": self._nats_error_callback,
            "disconnected_cb": self._nats_disconnected_callback,
            "reconnected_cb": self._nats_reconnected_callback,
            "closed_cb": self._nats_closed_callback
        }
        
        # Add authentication if provided
        if settings.nats.token:
            connect_options["token"] = settings.nats.token
        elif settings.nats.user and settings.nats.password:
            connect_options["user"] = settings.nats.user
            connect_options["password"] = settings.nats.password
        
        try:
            await self.nats_client.connect(**connect_options)
            self.logger.info(
                f"Connected to NATS servers: {settings.nats.servers}",
                extra={"nats_servers": settings.nats.servers}
            )
        except (ErrNoServers, ErrTimeout) as e:
            self.logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def _nats_error_callback(self, e):
        """Handle NATS errors."""
        self.logger.error(f"NATS error: {e}")

    async def _nats_disconnected_callback(self):
        """Handle NATS disconnection."""
        self.logger.warning("Disconnected from NATS")

    async def _nats_reconnected_callback(self):
        """Handle NATS reconnection."""
        self.logger.info("Reconnected to NATS")

    async def _nats_closed_callback(self):
        """Handle NATS connection close."""
        self.logger.info("NATS connection closed")

    async def run(self) -> None:
        """Run the service until shutdown is requested."""
        self.logger.info("CenterFuze OpenText Service is running")
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        try:
            # Wait for shutdown signal
            await self.shutdown_event.wait()
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        
        self.logger.info("Service shutdown initiated")

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler():
            self.logger.info("Received shutdown signal")
            self.shutdown_event.set()
        
        # Handle SIGINT and SIGTERM
        try:
            loop = asyncio.get_running_loop()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Signal handlers not supported on this platform (e.g., Windows)
            pass

    async def shutdown(self) -> None:
        """Gracefully shutdown all service components."""
        self.logger.info("Starting graceful shutdown")
        
        try:
            # Close NATS subscriptions
            if self.nats_controller:
                await self.nats_controller.close_subscriptions()
                self.logger.info("NATS subscriptions closed")
            
            # Close NATS connection
            if self.nats_client and self.nats_client.is_connected:
                await self.nats_client.close()
                self.logger.info("NATS connection closed")
            
            # Close OpenText service
            if self.opentext_service:
                await self.opentext_service.__aexit__(None, None, None)
                self.logger.info("OpenText service closed")
            
            # Shutdown cache manager
            if self.cache_manager:
                await self.cache_manager.shutdown()
                self.logger.info("Cache manager shutdown")
            
            self.logger.info("Graceful shutdown completed")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}", exc_info=True)

    async def health_check(self) -> dict:
        """
        Perform comprehensive health check of all service components.
        
        Returns:
            Dictionary with health status of all components
        """
        health_status = {
            "service": settings.service.service_name,
            "version": settings.service.version,
            "status": "healthy",
            "timestamp": asyncio.get_event_loop().time(),
            "components": {}
        }
        
        try:
            # Check NATS connection
            if self.nats_client and self.nats_client.is_connected:
                health_status["components"]["nats"] = {
                    "status": "healthy",
                    "connected_url": self.nats_client.connected_url.netloc if self.nats_client.connected_url else None
                }
            else:
                health_status["components"]["nats"] = {"status": "unhealthy", "error": "Not connected"}
                health_status["status"] = "degraded"
            
            # Check OpenText service
            if self.opentext_service:
                opentext_health = await self.opentext_service.health_check()
                health_status["components"]["opentext"] = opentext_health
                
                if opentext_health.get("status") != "healthy":
                    health_status["status"] = "degraded"
            else:
                health_status["components"]["opentext"] = {"status": "not_initialized"}
                health_status["status"] = "degraded"
            
            # Check cache manager
            if self.cache_manager:
                cache_stats = await self.cache_manager.get_stats()
                health_status["components"]["cache"] = {
                    "status": "healthy",
                    "stats": cache_stats
                }
            else:
                health_status["components"]["cache"] = {"status": "disabled"}
            
            # Check rate limiter
            if self.rate_limiter:
                rate_limit_stats = self.rate_limiter.get_stats()
                health_status["components"]["rate_limiter"] = {
                    "status": "healthy",
                    "stats": rate_limit_stats
                }
            
        except Exception as e:
            health_status["status"] = "unhealthy"
            health_status["error"] = str(e)
            self.logger.error(f"Health check failed: {e}", exc_info=True)
        
        return health_status


async def main():
    """Main entry point for the service."""
    # Setup logging
    setup_logging(
        service_name=settings.service.service_name,
        log_level=settings.service.log_level,
        environment=settings.service.environment
    )
    
    logger = create_service_logger(__name__)
    logger.info(
        f"Starting {settings.service.service_name} v{settings.service.version}",
        extra={
            "service": settings.service.service_name,
            "version": settings.service.version,
            "environment": settings.service.environment
        }
    )
    
    # Create and run the service
    app = OpenTextServiceApp()
    
    try:
        await app.initialize()
        await app.run()
    except Exception as e:
        logger.error(f"Service failed: {e}", exc_info=True)
        return 1
    finally:
        await app.shutdown()
    
    logger.info("Service stopped")
    return 0


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("Service interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Service crashed: {e}")
        sys.exit(1)