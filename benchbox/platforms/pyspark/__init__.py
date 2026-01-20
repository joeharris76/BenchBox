"""PySpark platform utilities shared across BenchBox adapters.

This module provides shared utilities for PySpark-based adapters:
- SparkSessionManager: Singleton lifecycle manager for SparkSession
- Java version detection and compatibility checking
- Automatic JAVA_HOME switching to compatible versions

PySpark 4.x requires Java 17 or 21. The ensure_compatible_java() function
can automatically detect and switch to a compatible Java installation.

Copyright 2026 Joe Harris / BenchBox Project
"""

from .session import (
    PYSPARK_AVAILABLE,
    PYSPARK_VERSION,
    SUPPORTED_JAVA_VERSIONS,
    JavaVersionError,
    SparkConfigurationError,
    SparkSessionError,
    SparkSessionManager,
    SparkUnavailableError,
    detect_java_version,
    ensure_compatible_java,
    find_supported_java_home,
    get_effective_java_home,
    get_java_skip_reason,
    is_java_compatible,
)
from .sql_adapter import PySparkSQLAdapter

__all__ = [
    # Availability flags
    "PYSPARK_AVAILABLE",
    "PYSPARK_VERSION",
    # Java version detection
    "SUPPORTED_JAVA_VERSIONS",
    "detect_java_version",
    "find_supported_java_home",
    "is_java_compatible",
    "ensure_compatible_java",
    "get_java_skip_reason",
    "get_effective_java_home",
    # Session management
    "SparkSessionManager",
    # Exceptions
    "SparkSessionError",
    "SparkUnavailableError",
    "SparkConfigurationError",
    "JavaVersionError",
    # Adapters
    "PySparkSQLAdapter",
]
