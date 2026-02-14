"""AMPLab DataFrame query registry.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

AMPLAB_DATAFRAME_QUERIES = QueryRegistry("amplab")


def get_amplab_query(query_id: str) -> DataFrameQuery | None:
    """Get an AMPLab DataFrame query by ID."""
    return AMPLAB_DATAFRAME_QUERIES.get(query_id)


def list_amplab_queries(
    family: str | None = None,
    category: QueryCategory | None = None,
) -> list[DataFrameQuery]:
    """List AMPLab DataFrame queries with optional filtering."""
    queries = AMPLAB_DATAFRAME_QUERIES.get_all_queries()

    if family:
        queries = [q for q in queries if q.get_impl_for_family(family) is not None]

    if category:
        queries = [q for q in queries if category in q.categories]

    return queries


def register_query(query: DataFrameQuery) -> None:
    """Register an AMPLab DataFrame query."""
    AMPLAB_DATAFRAME_QUERIES.register(query)
