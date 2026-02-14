"""ClickBench DataFrame query registry.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

CLICKBENCH_DATAFRAME_QUERIES = QueryRegistry("clickbench")


def get_clickbench_query(query_id: str) -> DataFrameQuery | None:
    """Get a ClickBench DataFrame query by ID.

    Args:
        query_id: Query identifier (e.g., "Q1", "Q43")

    Returns:
        DataFrameQuery if found, None otherwise
    """
    return CLICKBENCH_DATAFRAME_QUERIES.get(query_id)


def list_clickbench_queries(
    family: str | None = None,
    category: QueryCategory | None = None,
) -> list[DataFrameQuery]:
    """List ClickBench DataFrame queries with optional filtering.

    Args:
        family: Filter by family ("expression" or "pandas")
        category: Filter by query category

    Returns:
        List of matching DataFrameQuery objects
    """
    queries = CLICKBENCH_DATAFRAME_QUERIES.get_all_queries()

    if family:
        queries = [q for q in queries if q.get_impl_for_family(family) is not None]

    if category:
        queries = [q for q in queries if category in q.categories]

    return queries


def register_query(query: DataFrameQuery) -> None:
    """Register a ClickBench DataFrame query."""
    CLICKBENCH_DATAFRAME_QUERIES.register(query)
