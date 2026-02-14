"""SSB DataFrame query registry.

This module provides the central registry for SSB DataFrame queries,
following the same pattern as TPC-DS.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

# SSB DataFrame Query Registry
SSB_DATAFRAME_QUERIES = QueryRegistry("ssb")


def get_ssb_query(query_id: str) -> DataFrameQuery | None:
    """Get an SSB DataFrame query by ID.

    Args:
        query_id: Query identifier (e.g., "Q1.1", "Q3.2")

    Returns:
        DataFrameQuery if found, None otherwise
    """
    return SSB_DATAFRAME_QUERIES.get(query_id)


def list_ssb_queries(
    family: str | None = None,
    category: QueryCategory | None = None,
) -> list[DataFrameQuery]:
    """List SSB DataFrame queries with optional filtering.

    Args:
        family: Filter by family ("expression" or "pandas")
        category: Filter by query category

    Returns:
        List of matching DataFrameQuery objects
    """
    queries = SSB_DATAFRAME_QUERIES.get_all_queries()

    if family:
        queries = [q for q in queries if q.get_impl_for_family(family) is not None]

    if category:
        queries = [q for q in queries if category in q.categories]

    return queries


def register_query(query: DataFrameQuery) -> None:
    """Register an SSB DataFrame query.

    Args:
        query: DataFrameQuery to register
    """
    SSB_DATAFRAME_QUERIES.register(query)
