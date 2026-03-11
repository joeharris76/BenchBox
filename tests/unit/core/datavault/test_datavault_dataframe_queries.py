"""Unit tests for Data Vault DataFrame query implementations.

Copyright 2026 Joe Harris / BenchBox Project
"""

from __future__ import annotations

import pytest

from benchbox.core.dataframe.query import QueryCategory

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


ALL_QUERY_IDS = [f"Q{i}" for i in range(1, 23)]


class TestDataVaultQueryRegistry:
    """Tests for Data Vault DataFrame query registry."""

    def test_registry_imports_successfully(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        assert DATAVAULT_DATAFRAME_QUERIES is not None

    def test_registry_has_22_queries(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        queries = DATAVAULT_DATAFRAME_QUERIES.get_all_queries()
        assert len(queries) == 22

    def test_registry_benchmark_name(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        assert DATAVAULT_DATAFRAME_QUERIES.benchmark == "datavault"

    def test_get_query_by_id(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        query = get_datavault_query("Q1")
        assert query is not None
        assert query.query_id == "Q1"

    def test_get_nonexistent_query(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        assert get_datavault_query("Q99") is None

    def test_list_queries(self):
        from benchbox.core.datavault.dataframe_queries import list_datavault_queries

        assert len(list_datavault_queries()) == 22

    @pytest.mark.parametrize("query_id", ALL_QUERY_IDS)
    def test_all_queries_registered(self, query_id):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        assert get_datavault_query(query_id) is not None, f"Query {query_id} should be registered"

    def test_queries_have_both_implementations(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        for query in DATAVAULT_DATAFRAME_QUERIES.get_all_queries():
            assert query.expression_impl is not None, f"{query.query_id} missing expression impl"
            assert query.pandas_impl is not None, f"{query.query_id} missing pandas impl"

    def test_all_queries_callable(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        for query in DATAVAULT_DATAFRAME_QUERIES.get_all_queries():
            assert callable(query.expression_impl), f"{query.query_id} expression_impl not callable"
            assert callable(query.pandas_impl), f"{query.query_id} pandas_impl not callable"

    def test_query_descriptions_not_empty(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        for query in DATAVAULT_DATAFRAME_QUERIES.get_all_queries():
            assert query.description, f"{query.query_id} missing description"
            assert len(query.description) > 10, f"{query.query_id} description too short"

    def test_query_names_not_empty(self):
        from benchbox.core.datavault.dataframe_queries import DATAVAULT_DATAFRAME_QUERIES

        for query in DATAVAULT_DATAFRAME_QUERIES.get_all_queries():
            assert query.query_name, f"{query.query_id} missing query_name"


class TestDataVaultQueryCategories:
    """Tests for Data Vault query category assignments."""

    def test_simple_queries_have_aggregate(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        for qid in ["Q1", "Q6"]:
            query = get_datavault_query(qid)
            assert QueryCategory.AGGREGATE in query.categories, f"{qid} should have AGGREGATE"

    def test_multi_join_queries(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        for qid in ["Q2", "Q3", "Q5", "Q7", "Q8", "Q9", "Q10", "Q18", "Q20", "Q21"]:
            query = get_datavault_query(qid)
            assert QueryCategory.MULTI_JOIN in query.categories, f"{qid} should have MULTI_JOIN"

    def test_subquery_queries(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        for qid in ["Q2", "Q4", "Q11", "Q13", "Q15", "Q16", "Q17", "Q18", "Q20", "Q21", "Q22"]:
            query = get_datavault_query(qid)
            assert QueryCategory.SUBQUERY in query.categories, f"{qid} should have SUBQUERY"

    def test_analytical_queries(self):
        from benchbox.core.datavault.dataframe_queries import get_datavault_query

        query = get_datavault_query("Q8")
        assert QueryCategory.ANALYTICAL in query.categories, "Q8 should have ANALYTICAL"


class TestDataVaultParameters:
    """Tests for Data Vault query parameters."""

    def test_all_queries_have_parameters(self):
        from benchbox.core.datavault.dataframe_queries.parameters import DATAVAULT_DEFAULT_PARAMS

        assert len(DATAVAULT_DEFAULT_PARAMS) == 22

    @pytest.mark.parametrize("query_id", ALL_QUERY_IDS)
    def test_parameter_entries_exist(self, query_id):
        from benchbox.core.datavault.dataframe_queries.parameters import DATAVAULT_DEFAULT_PARAMS

        assert query_id in DATAVAULT_DEFAULT_PARAMS, f"Missing parameters for {query_id}"

    def test_q1_has_delta(self):
        from benchbox.core.datavault.dataframe_queries.parameters import get_parameters

        result = get_parameters("Q1")
        assert result.get("delta") == 90

    def test_q2_has_region(self):
        from benchbox.core.datavault.dataframe_queries.parameters import get_parameters

        result = get_parameters("Q2")
        assert result.get("region") == "EUROPE"

    def test_q22_has_country_codes(self):
        from benchbox.core.datavault.dataframe_queries.parameters import get_parameters

        result = get_parameters("Q22")
        codes = result.get("country_codes")
        assert len(codes) == 7

    def test_get_parameters_returns_container(self):
        from benchbox.core.datavault.dataframe_queries.parameters import DataVaultParameters, get_parameters

        result = get_parameters("Q1")
        assert isinstance(result, DataVaultParameters)
        assert result.query_id == "Q1"


class TestDataVaultBenchmarkRegistry:
    """Tests for Data Vault DataFrame support in benchmark registry."""

    def test_datavault_supports_dataframe(self):
        from benchbox.core.benchmark_registry import get_benchmark_metadata

        meta = get_benchmark_metadata("datavault")
        assert meta is not None
        assert meta.get("supports_dataframe") is True
