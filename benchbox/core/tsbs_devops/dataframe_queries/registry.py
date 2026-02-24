"""TSBS DevOps DataFrame query registry.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery, QueryCategory, QueryRegistry

TSBS_DEVOPS_DATAFRAME_QUERIES = QueryRegistry("tsbs_devops")


def get_tsbs_devops_query(query_id: str) -> DataFrameQuery | None:
    """Get a TSBS DevOps DataFrame query by ID."""
    return TSBS_DEVOPS_DATAFRAME_QUERIES.get(query_id)


def list_tsbs_devops_queries(
    family: str | None = None,
    category: QueryCategory | None = None,
) -> list[DataFrameQuery]:
    """List TSBS DevOps DataFrame queries with optional filtering."""
    return TSBS_DEVOPS_DATAFRAME_QUERIES.list_queries(family=family, category=category)


def register_query(query: DataFrameQuery) -> None:
    """Register a TSBS DevOps DataFrame query."""
    TSBS_DEVOPS_DATAFRAME_QUERIES.register(query)
