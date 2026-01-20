"""CLI type re-exports for backward compatibility in tests.

Provides BenchmarkResults and QueryResult for CLI helpers.
"""

from benchbox.core.config import QueryResult  # re-export
from benchbox.core.results.models import BenchmarkResults

__all__ = ["BenchmarkResults", "QueryResult"]
