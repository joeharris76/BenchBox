"""Core BenchBox exceptions.

Defines base exceptions used throughout the core BenchBox modules,
including platform adapters and benchmark implementations.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Any


class BenchBoxError(Exception):
    """Base exception for all BenchBox errors.

    All BenchBox-specific exceptions should inherit from this class
    to allow for broad exception handling when needed.
    """


class ConfigurationError(BenchBoxError):
    """Configuration validation error.

    Raised when configuration validation fails in platform adapters,
    such as cache control settings, tuning parameters, or benchmark
    configuration that cannot be properly applied.

    This exception is used by platform adapters to signal configuration
    issues during benchmark setup and execution.

    Attributes:
        message: Human-readable error message
        details: Additional error context and metadata
    """

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        """Initialize configuration error.

        Args:
            message: Human-readable error message
            details: Optional dictionary with additional error context
        """
        self.message = message
        self.details = details or {}
        super().__init__(message)
