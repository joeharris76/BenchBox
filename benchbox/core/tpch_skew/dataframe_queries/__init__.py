"""TPC-H Skew DataFrame queries for Expression and Pandas families.

Reuses TPC-H DataFrame query implementations since TPC-H Skew uses
identical queries on skewed data distributions.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

# Import queries module to trigger registration
from benchbox.core.tpch_skew.dataframe_queries import queries as _queries  # noqa: F401
from benchbox.core.tpch_skew.dataframe_queries.registry import (
    TPCH_SKEW_DATAFRAME_QUERIES,
    get_tpch_skew_query,
    list_tpch_skew_queries,
)

__all__ = [
    "TPCH_SKEW_DATAFRAME_QUERIES",
    "get_tpch_skew_query",
    "list_tpch_skew_queries",
]
