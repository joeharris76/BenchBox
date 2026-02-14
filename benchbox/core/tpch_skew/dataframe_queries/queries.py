"""TPC-H Skew DataFrame query implementations.

Reuses TPC-H DataFrame query implementations since TPC-H Skew uses the
exact same 22 analytical queries on skewed data distributions. The only
difference is in data generation (Zipfian, attribute, join, temporal skew).

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from benchbox.core.dataframe.query import DataFrameQuery
from benchbox.core.tpch.dataframe_queries import TPCH_DATAFRAME_QUERIES
from benchbox.core.tpch_skew.dataframe_queries.registry import register_query


def _register_all_queries() -> None:
    """Re-register all TPC-H DataFrame queries for TPC-H Skew.

    Creates new DataFrameQuery instances that share the same implementation
    functions but are registered in the TPC-H Skew registry.
    """
    for tpch_query in TPCH_DATAFRAME_QUERIES.get_all_queries():
        skew_query = DataFrameQuery(
            query_id=tpch_query.query_id,
            query_name=tpch_query.query_name,
            description=tpch_query.description,
            categories=tpch_query.categories,
            expression_impl=tpch_query.expression_impl,
            pandas_impl=tpch_query.pandas_impl,
            sql_equivalent=tpch_query.sql_equivalent,
            expected_row_count=tpch_query.expected_row_count,
            scale_factor_dependent=tpch_query.scale_factor_dependent,
            timeout_seconds=tpch_query.timeout_seconds,
            skip_platforms=tpch_query.skip_platforms,
        )
        register_query(skew_query)


_register_all_queries()
