"""H2ODB DataFrame query registry.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

H2ODB_DATAFRAME_QUERIES = QueryRegistry("h2odb")


def get_h2odb_query(query_id: str) -> DataFrameQuery | None:
    """Get an H2ODB DataFrame query by ID."""
    return H2ODB_DATAFRAME_QUERIES.get(query_id)


def list_h2odb_queries(
    family: str | None = None,
    category: QueryCategory | None = None,
) -> list[DataFrameQuery]:
    """List H2ODB DataFrame queries with optional filtering."""
    return H2ODB_DATAFRAME_QUERIES.list_queries(family=family, category=category)


def register_query(query: DataFrameQuery) -> None:
    """Register an H2ODB DataFrame query."""
    H2ODB_DATAFRAME_QUERIES.register(query)
