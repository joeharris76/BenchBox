"""Shared fixtures and helpers for PySpark platform tests.

This module provides pytest fixtures and skip conditions for PySpark tests.
It uses the centralized Java version detection from benchbox.platforms.pyspark.

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

import sys
from collections.abc import Generator

import pytest

from benchbox.platforms.pyspark import (
    PYSPARK_AVAILABLE,
    SparkSessionManager,
    ensure_compatible_java,
    get_java_skip_reason,
    is_java_compatible,
)

# Ensure compatible Java is configured at import time
# This allows skipif decorators to evaluate correctly
_java_version, _java_home = ensure_compatible_java()

# Skip conditions for PySpark tests
# Windows is skipped because Hadoop requires winutils.exe setup which is not available in CI
_IS_WINDOWS = sys.platform == "win32"
PYSPARK_SQL_TESTS_SKIPPED = _IS_WINDOWS or not PYSPARK_AVAILABLE or not is_java_compatible(_java_version)
PYSPARK_SQL_SKIP_REASON = (
    "PySpark tests skipped on Windows - Hadoop requires winutils.exe setup" if _IS_WINDOWS else get_java_skip_reason()
)


@pytest.fixture(autouse=True)
def reset_spark_session_manager() -> Generator[None, None, None]:
    """Ensure SparkSessionManager is reset between tests."""
    SparkSessionManager.close()
    yield
    SparkSessionManager.close()
