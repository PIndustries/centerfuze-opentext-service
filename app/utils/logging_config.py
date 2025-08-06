"""
Logging configuration for the CenterFuze OpenText Service.

This module sets up structured logging with appropriate formatting
and handlers for different environments.
"""

import logging
import logging.config
import sys
from typing import Dict, Any
import json
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }

        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add extra fields from the log record
        for key, value in record.__dict__.items():
            if key not in [
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message"
            ]:
                log_entry[key] = value

        return json.dumps(log_entry, default=str)


class ColoredFormatter(logging.Formatter):
    """Colored formatter for console output in development."""

    # ANSI color codes
    COLORS = {
        'DEBUG': '\033[36m',     # Cyan
        'INFO': '\033[32m',      # Green
        'WARNING': '\033[33m',   # Yellow
        'ERROR': '\033[31m',     # Red
        'CRITICAL': '\033[35m',  # Magenta
        'RESET': '\033[0m'       # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format log record with colors."""
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        reset = self.COLORS['RESET']
        
        # Color the level name
        record.levelname = f"{color}{record.levelname}{reset}"
        
        return super().format(record)


def setup_logging(
    service_name: str = "centerfuze-opentext-service",
    log_level: str = "INFO",
    environment: str = "development"
) -> None:
    """
    Set up logging configuration for the service.
    
    Args:
        service_name: Name of the service for log identification
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        environment: Environment (development, staging, production)
    """
    
    # Convert string level to logging level
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Determine if we should use JSON formatting (production) or colored (development)
    use_json = environment.lower() in ["production", "staging"]
    
    # Configure logging
    config: Dict[str, Any] = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": JSONFormatter,
            },
            "colored": {
                "()": ColoredFormatter,
                "format": "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            },
            "simple": {
                "format": "%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S"
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "stream": sys.stdout,
                "formatter": "json" if use_json else "colored",
                "level": numeric_level
            },
            "error_console": {
                "class": "logging.StreamHandler",
                "stream": sys.stderr,
                "formatter": "json" if use_json else "simple",
                "level": "ERROR"
            }
        },
        "loggers": {
            # Root logger
            "": {
                "level": numeric_level,
                "handlers": ["console"],
                "propagate": False
            },
            # Service-specific logger
            "app": {
                "level": numeric_level,
                "handlers": ["console"],
                "propagate": False
            },
            # External library loggers
            "nats": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "aiohttp": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            },
            "asyncio": {
                "level": "WARNING",
                "handlers": ["console"],
                "propagate": False
            }
        }
    }
    
    # Apply the configuration
    logging.config.dictConfig(config)
    
    # Log the configuration
    logger = logging.getLogger("app.logging")
    logger.info(
        f"Logging configured for {service_name}",
        extra={
            "service": service_name,
            "log_level": log_level,
            "environment": environment,
            "json_format": use_json
        }
    )


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


class LoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter that adds service context to all log messages.
    """
    
    def __init__(self, logger: logging.Logger, service_name: str, version: str):
        """
        Initialize the logger adapter.
        
        Args:
            logger: Base logger instance
            service_name: Name of the service
            version: Service version
        """
        super().__init__(logger, {})
        self.service_name = service_name
        self.version = version
    
    def process(self, msg: str, kwargs: dict) -> tuple:
        """Add service context to log messages."""
        extra = kwargs.get("extra", {})
        extra.update({
            "service": self.service_name,
            "version": self.version
        })
        kwargs["extra"] = extra
        return msg, kwargs


def create_service_logger(
    name: str,
    service_name: str = "centerfuze-opentext-service",
    version: str = "1.0.0"
) -> LoggerAdapter:
    """
    Create a service logger with context.
    
    Args:
        name: Logger name
        service_name: Service name
        version: Service version
        
    Returns:
        LoggerAdapter with service context
    """
    base_logger = logging.getLogger(name)
    return LoggerAdapter(base_logger, service_name, version)


# Context manager for adding request context to logs
class RequestContextManager:
    """Context manager for adding request-specific context to logs."""
    
    def __init__(self, logger: logging.Logger, request_id: str, operation: str):
        """
        Initialize request context manager.
        
        Args:
            logger: Logger instance
            request_id: Unique request identifier
            operation: Operation being performed
        """
        self.logger = logger
        self.request_id = request_id
        self.operation = operation
        self.start_time = None
    
    def __enter__(self):
        """Enter the request context."""
        self.start_time = datetime.utcnow()
        self.logger.info(
            f"Starting {self.operation}",
            extra={
                "request_id": self.request_id,
                "operation": self.operation,
                "start_time": self.start_time.isoformat()
            }
        )
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the request context."""
        end_time = datetime.utcnow()
        duration = (end_time - self.start_time).total_seconds()
        
        if exc_type:
            self.logger.error(
                f"Failed {self.operation}",
                extra={
                    "request_id": self.request_id,
                    "operation": self.operation,
                    "duration_seconds": duration,
                    "error": str(exc_val)
                },
                exc_info=True
            )
        else:
            self.logger.info(
                f"Completed {self.operation}",
                extra={
                    "request_id": self.request_id,
                    "operation": self.operation,
                    "duration_seconds": duration
                }
            )


def log_performance(logger: logging.Logger):
    """Decorator for logging function performance."""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = datetime.utcnow()
            function_name = f"{func.__module__}.{func.__name__}"
            
            try:
                result = func(*args, **kwargs)
                
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                logger.debug(
                    f"Function executed successfully: {function_name}",
                    extra={
                        "function": function_name,
                        "duration_seconds": duration,
                        "success": True
                    }
                )
                
                return result
                
            except Exception as e:
                end_time = datetime.utcnow()
                duration = (end_time - start_time).total_seconds()
                
                logger.error(
                    f"Function execution failed: {function_name}",
                    extra={
                        "function": function_name,
                        "duration_seconds": duration,
                        "success": False,
                        "error": str(e)
                    },
                    exc_info=True
                )
                
                raise
        
        return wrapper
    return decorator