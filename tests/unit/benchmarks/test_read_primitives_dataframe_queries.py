"""Unit tests for Read Primitives DataFrame queries.

Tests the DataFrame implementations of Read Primitives benchmark queries
for both expression-family and pandas-family platforms.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.dataframe.query import QueryCategory, QueryRegistry
from benchbox.core.read_primitives.benchmark import ReadPrimitivesBenchmark
from benchbox.core.read_primitives.dataframe_queries import (
    REGISTRY,
    SKIP_FOR_DATAFRAME,
    get_dataframe_queries,
    get_expression_family_only,
    get_skip_for_dataframe,
)


class TestQueryRegistry:
    """Test the Read Primitives DataFrame query registry."""

    def test_registry_is_query_registry(self):
        """Registry should be a QueryRegistry instance."""
        assert isinstance(REGISTRY, QueryRegistry)
        assert REGISTRY.benchmark == "Read Primitives"

    def test_registry_has_queries(self):
        """Registry should contain queries."""
        assert len(REGISTRY) > 0, "Registry should contain at least one query"

    def test_get_dataframe_queries_returns_registry(self):
        """get_dataframe_queries should return the registry."""
        registry = get_dataframe_queries()
        assert registry is REGISTRY

    def test_all_queries_have_both_implementations(self):
        """All registered queries should have both expression and pandas implementations."""
        for query in REGISTRY.get_all_queries():
            assert query.has_expression_impl(), f"Query {query.query_id} missing expression_impl"
            assert query.has_pandas_impl(), f"Query {query.query_id} missing pandas_impl"

    def test_all_queries_have_valid_categories(self):
        """All registered queries should have at least one valid category."""
        for query in REGISTRY.get_all_queries():
            assert len(query.categories) > 0, f"Query {query.query_id} has no categories"
            for category in query.categories:
                assert isinstance(category, QueryCategory), (
                    f"Query {query.query_id} has invalid category type: {type(category)}"
                )


class TestSkipLists:
    """Test the skip/exclusion lists."""

    def test_skip_for_dataframe_contains_correlated_subqueries(self):
        """SKIP_FOR_DATAFRAME should only contain correlated subquery queries."""
        # Only 3 queries are truly SQL-only: correlated subqueries have no DataFrame equivalent
        assert len(SKIP_FOR_DATAFRAME) == 3, f"Should have 3 correlated-subquery queries, got {len(SKIP_FOR_DATAFRAME)}"

    def test_skip_for_dataframe_has_expected_queries(self):
        """SKIP_FOR_DATAFRAME should contain only the correlated subquery optimizer queries."""
        expected = {
            "optimizer_exists_to_semijoin",  # Correlated EXISTS
            "optimizer_in_to_exists",  # Correlated IN
            "optimizer_scalar_subquery_flattening",  # Correlated scalar subquery
        }
        assert set(SKIP_FOR_DATAFRAME) == expected, (
            f"Skip list should only contain correlated subquery queries. "
            f"Expected: {expected}, got: {set(SKIP_FOR_DATAFRAME)}"
        )

    def test_expression_family_only_is_empty(self):
        """get_expression_family_only should return empty list (deprecated)."""
        # EXPRESSION_FAMILY_ONLY has been removed - all queries support both families
        result = get_expression_family_only()
        assert result == [], (
            "get_expression_family_only should return empty list - all queries now support both families"
        )

    def test_get_skip_for_dataframe_returns_copy(self):
        """get_skip_for_dataframe should return a copy, not the original list."""
        result = get_skip_for_dataframe()
        assert result == SKIP_FOR_DATAFRAME
        assert result is not SKIP_FOR_DATAFRAME

    def test_get_expression_family_only_returns_empty_list(self):
        """get_expression_family_only should return empty list (all queries support both families)."""
        result = get_expression_family_only()
        assert result == []
        # Verify it's a new list instance each call (for safety)
        result2 = get_expression_family_only()
        assert result is not result2 or result == []


class TestBenchmarkIntegration:
    """Test integration with ReadPrimitivesBenchmark class."""

    def test_benchmark_has_get_dataframe_queries_method(self):
        """ReadPrimitivesBenchmark should have get_dataframe_queries method."""
        benchmark = ReadPrimitivesBenchmark(scale_factor=0.01)
        assert hasattr(benchmark, "get_dataframe_queries")

    def test_benchmark_get_dataframe_queries_returns_registry(self):
        """Benchmark.get_dataframe_queries should return the registry."""
        benchmark = ReadPrimitivesBenchmark(scale_factor=0.01)
        registry = benchmark.get_dataframe_queries()
        assert isinstance(registry, QueryRegistry)
        assert registry is REGISTRY

    def test_benchmark_has_get_dataframe_skip_queries_method(self):
        """ReadPrimitivesBenchmark should have get_dataframe_skip_queries method."""
        benchmark = ReadPrimitivesBenchmark(scale_factor=0.01)
        assert hasattr(benchmark, "get_dataframe_skip_queries")

    def test_benchmark_get_dataframe_skip_queries_returns_list(self):
        """Benchmark.get_dataframe_skip_queries should return skip list."""
        benchmark = ReadPrimitivesBenchmark(scale_factor=0.01)
        skip_list = benchmark.get_dataframe_skip_queries()
        assert isinstance(skip_list, list)
        # Only 3 queries skipped: correlated subquery queries with no DataFrame equivalent
        assert len(skip_list) == 3

    def test_benchmark_has_get_expression_family_only_queries_method(self):
        """ReadPrimitivesBenchmark should have get_expression_family_only_queries method."""
        benchmark = ReadPrimitivesBenchmark(scale_factor=0.01)
        assert hasattr(benchmark, "get_expression_family_only_queries")


class TestQueryCategories:
    """Test query categorization."""

    def test_has_aggregation_queries(self):
        """Registry should have aggregation queries."""
        agg_queries = REGISTRY.get_queries_by_category(QueryCategory.AGGREGATE)
        assert len(agg_queries) > 0

    def test_has_filter_queries(self):
        """Registry should have filter queries."""
        filter_queries = REGISTRY.get_queries_by_category(QueryCategory.FILTER)
        assert len(filter_queries) > 0

    def test_has_sort_queries(self):
        """Registry should have sort queries."""
        sort_queries = REGISTRY.get_queries_by_category(QueryCategory.SORT)
        assert len(sort_queries) > 0

    def test_has_window_queries(self):
        """Registry should have window queries."""
        window_queries = REGISTRY.get_queries_by_category(QueryCategory.WINDOW)
        assert len(window_queries) > 0

    def test_has_join_queries(self):
        """Registry should have join queries."""
        join_queries = REGISTRY.get_queries_by_category(QueryCategory.JOIN)
        assert len(join_queries) > 0


class TestPlatformSupport:
    """Test platform support for queries."""

    def test_all_queries_support_polars(self):
        """All queries should support Polars (expression family)."""
        for query in REGISTRY.get_all_queries():
            assert query.supports_platform("polars"), f"Query {query.query_id} should support Polars"

    def test_all_queries_support_pandas(self):
        """All queries should support Pandas (pandas family)."""
        for query in REGISTRY.get_all_queries():
            assert query.supports_platform("pandas"), f"Query {query.query_id} should support Pandas"

    def test_all_queries_support_pyspark(self):
        """All queries should support PySpark (expression family)."""
        for query in REGISTRY.get_all_queries():
            assert query.supports_platform("pyspark"), f"Query {query.query_id} should support PySpark"


class TestQueryMetadata:
    """Test query metadata."""

    def test_all_queries_have_query_id(self):
        """All queries should have a non-empty query_id."""
        for query in REGISTRY.get_all_queries():
            assert query.query_id, "Query should have a query_id"
            assert isinstance(query.query_id, str)

    def test_all_queries_have_query_name(self):
        """All queries should have a non-empty query_name."""
        for query in REGISTRY.get_all_queries():
            assert query.query_name, f"Query {query.query_id} should have a query_name"

    def test_all_queries_have_description(self):
        """All queries should have a non-empty description."""
        for query in REGISTRY.get_all_queries():
            assert query.description, f"Query {query.query_id} should have a description"

    def test_query_ids_are_unique(self):
        """All query IDs should be unique."""
        query_ids = REGISTRY.get_query_ids()
        assert len(query_ids) == len(set(query_ids)), "Query IDs should be unique"


@pytest.mark.fast
class TestQueryImplementationSignatures:
    """Test that query implementations have correct signatures."""

    def test_expression_impls_are_callable(self):
        """All expression implementations should be callable."""
        for query in REGISTRY.get_all_queries():
            impl = query.expression_impl
            assert callable(impl), f"Query {query.query_id} expression_impl should be callable"

    def test_pandas_impls_are_callable(self):
        """All pandas implementations should be callable."""
        for query in REGISTRY.get_all_queries():
            impl = query.pandas_impl
            assert callable(impl), f"Query {query.query_id} pandas_impl should be callable"
