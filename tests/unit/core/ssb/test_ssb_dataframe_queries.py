"""Unit tests for SSB DataFrame query implementations.

Tests for:
- Query registry functionality
- Query parameter system
- Expression family implementations
- Pandas family implementations

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

import pytest

from benchbox.core.dataframe.query import QueryCategory

pytestmark = pytest.mark.fast

ALL_QUERY_IDS = [
    "Q1.1",
    "Q1.2",
    "Q1.3",
    "Q2.1",
    "Q2.2",
    "Q2.3",
    "Q3.1",
    "Q3.2",
    "Q3.3",
    "Q3.4",
    "Q4.1",
    "Q4.2",
    "Q4.3",
]


class TestSSBQueryRegistry:
    """Tests for SSB DataFrame query registry."""

    def test_registry_imports_successfully(self):
        """Test that the registry can be imported."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        assert SSB_DATAFRAME_QUERIES is not None

    def test_registry_has_13_queries(self):
        """Test that all 13 SSB queries are registered."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        queries = SSB_DATAFRAME_QUERIES.get_all_queries()
        assert len(queries) == 13

    def test_registry_benchmark_name(self):
        """Test that registry has correct benchmark name."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        assert SSB_DATAFRAME_QUERIES.benchmark == "ssb"

    def test_get_query_by_id(self):
        """Test getting a query by ID."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        query = get_ssb_query("Q1.1")
        assert query is not None
        assert query.query_id == "Q1.1"

    def test_get_nonexistent_query(self):
        """Test getting a query that doesn't exist."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        query = get_ssb_query("Q99")
        assert query is None

    def test_list_queries(self):
        """Test listing all queries."""
        from benchbox.core.ssb.dataframe_queries import list_ssb_queries

        queries = list_ssb_queries()
        assert len(queries) == 13

    @pytest.mark.parametrize("query_id", ALL_QUERY_IDS)
    def test_all_queries_registered(self, query_id):
        """Test that each SSB query is registered."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        query = get_ssb_query(query_id)
        assert query is not None, f"Query {query_id} should be registered"

    def test_queries_have_both_implementations(self):
        """Test that all queries have both expression and pandas implementations."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        for query in SSB_DATAFRAME_QUERIES.get_all_queries():
            assert query.expression_impl is not None, f"{query.query_id} missing expression impl"
            assert query.pandas_impl is not None, f"{query.query_id} missing pandas impl"

    def test_all_queries_callable(self):
        """Test that all query implementations are callable."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        for query in SSB_DATAFRAME_QUERIES.get_all_queries():
            assert callable(query.expression_impl), f"{query.query_id} expression_impl not callable"
            assert callable(query.pandas_impl), f"{query.query_id} pandas_impl not callable"

    def test_query_descriptions_not_empty(self):
        """Test that all queries have descriptions."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        for query in SSB_DATAFRAME_QUERIES.get_all_queries():
            assert query.description, f"{query.query_id} missing description"
            assert len(query.description) > 10, f"{query.query_id} description too short"

    def test_query_names_not_empty(self):
        """Test that all queries have names."""
        from benchbox.core.ssb.dataframe_queries import SSB_DATAFRAME_QUERIES

        for query in SSB_DATAFRAME_QUERIES.get_all_queries():
            assert query.query_name, f"{query.query_id} missing query_name"


class TestSSBQueryCategories:
    """Tests for SSB query category assignments."""

    def test_flight1_queries_have_filter_aggregate(self):
        """Test that Flight 1 queries have FILTER and AGGREGATE categories."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        for qid in ["Q1.1", "Q1.2", "Q1.3"]:
            query = get_ssb_query(qid)
            assert QueryCategory.FILTER in query.categories, f"{qid} should have FILTER"
            assert QueryCategory.AGGREGATE in query.categories, f"{qid} should have AGGREGATE"

    def test_flight2_queries_have_multi_join(self):
        """Test that Flight 2 queries have MULTI_JOIN category."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        for qid in ["Q2.1", "Q2.2", "Q2.3"]:
            query = get_ssb_query(qid)
            assert QueryCategory.MULTI_JOIN in query.categories, f"{qid} should have MULTI_JOIN"

    def test_flight3_queries_have_multi_join(self):
        """Test that Flight 3 queries have MULTI_JOIN category."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        for qid in ["Q3.1", "Q3.2", "Q3.3", "Q3.4"]:
            query = get_ssb_query(qid)
            assert QueryCategory.MULTI_JOIN in query.categories, f"{qid} should have MULTI_JOIN"

    def test_flight4_queries_have_multi_join(self):
        """Test that Flight 4 queries have MULTI_JOIN category."""
        from benchbox.core.ssb.dataframe_queries import get_ssb_query

        for qid in ["Q4.1", "Q4.2", "Q4.3"]:
            query = get_ssb_query(qid)
            assert QueryCategory.MULTI_JOIN in query.categories, f"{qid} should have MULTI_JOIN"


class TestSSBParameters:
    """Tests for SSB query parameters."""

    def test_get_parameters_q1_1(self):
        """Test getting parameters for Q1.1."""
        from benchbox.core.ssb.dataframe_queries.parameters import get_parameters

        params = get_parameters("Q1.1")
        assert params.query_id == "Q1.1"
        assert params.get("year") == 1993
        assert params.get("discount_min") == 1
        assert params.get("discount_max") == 3
        assert params.get("quantity") == 25

    def test_get_parameters_q3_3(self):
        """Test getting parameters for Q3.3 (city pairs)."""
        from benchbox.core.ssb.dataframe_queries.parameters import get_parameters

        params = get_parameters("Q3.3")
        assert params.get("c_city1") == "UNITED KI1"
        assert params.get("c_city2") == "UNITED KI5"
        assert params.get("s_city1") == "UNITED KI1"
        assert params.get("s_city2") == "UNITED KI5"

    def test_get_parameters_q4_2(self):
        """Test getting parameters for Q4.2."""
        from benchbox.core.ssb.dataframe_queries.parameters import get_parameters

        params = get_parameters("Q4.2")
        assert params.get("region") == "AMERICA"
        assert params.get("year1") == 1997
        assert params.get("year2") == 1998
        assert params.get("mfgr1") == "MFGR#1"
        assert params.get("mfgr2") == "MFGR#2"

    @pytest.mark.parametrize("query_id", ALL_QUERY_IDS)
    def test_all_queries_have_parameters(self, query_id):
        """Test that all queries have parameter definitions."""
        from benchbox.core.ssb.dataframe_queries.parameters import get_parameters

        params = get_parameters(query_id)
        assert params.query_id == query_id
        assert len(params.params) > 0, f"{query_id} should have at least one parameter"

    def test_parameter_get_with_default(self):
        """Test getting parameter with default value."""
        from benchbox.core.ssb.dataframe_queries.parameters import get_parameters

        params = get_parameters("Q1.1")
        assert params.get("nonexistent", "default") == "default"


class TestSSBBenchmarkRegistry:
    """Tests for SSB DataFrame support in benchmark registry."""

    def test_ssb_supports_dataframe(self):
        """Test that SSB is marked as supporting DataFrames."""
        from benchbox.core.benchmark_registry import get_benchmark_metadata

        meta = get_benchmark_metadata("ssb")
        assert meta is not None
        assert meta.get("supports_dataframe") is True
