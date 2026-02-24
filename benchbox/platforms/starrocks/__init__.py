"""StarRocks OLAP platform adapter.

Provides BenchBox integration for StarRocks, an open-source columnar
analytics engine designed for fast OLAP queries on large datasets.
"""

from .adapter import StarRocksAdapter

__all__ = ["StarRocksAdapter"]
