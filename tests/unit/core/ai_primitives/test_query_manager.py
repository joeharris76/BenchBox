"""Tests for AI Primitives query manager.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.ai_primitives.queries import AIQueryManager

pytestmark = pytest.mark.fast


class TestAIQueryManager:
    """Tests for AIQueryManager."""

    @pytest.fixture()
    def manager(self):
        """Create a query manager instance."""
        return AIQueryManager()

    def test_manager_creation(self, manager):
        """Test manager initializes successfully."""
        assert manager is not None
        assert manager.catalog_version >= 1

    def test_get_all_queries(self, manager):
        """Test getting all queries."""
        queries = manager.get_all_queries()

        assert isinstance(queries, dict)
        assert len(queries) > 0
        for query_id, sql in queries.items():
            assert isinstance(query_id, str)
            assert isinstance(sql, str)

    def test_get_query_valid_id(self, manager):
        """Test getting a query by valid ID."""
        queries = manager.get_all_queries()
        first_id = list(queries.keys())[0]

        sql = manager.get_query(first_id)
        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_get_query_invalid_id(self, manager):
        """Test getting a query with invalid ID raises error."""
        with pytest.raises(ValueError, match="Invalid query ID"):
            manager.get_query("nonexistent_query_12345")

    def test_get_query_with_dialect(self, manager):
        """Test getting query with dialect variant."""
        # Get a query that has snowflake variant
        sql = manager.get_query("generative_complete_simple", dialect="snowflake")
        assert "SNOWFLAKE.CORTEX" in sql or "placeholder" not in sql.lower()

    def test_get_query_skipped_dialect(self, manager):
        """Test getting query for skipped dialect raises error."""
        with pytest.raises(ValueError, match="not supported on dialect"):
            manager.get_query("generative_complete_simple", dialect="duckdb")

    def test_get_query_entry(self, manager):
        """Test getting full query entry."""
        entry = manager.get_query_entry("generative_complete_simple")

        assert entry.id == "generative_complete_simple"
        assert entry.category == "generative"
        assert entry.sql is not None

    def test_get_query_entry_invalid(self, manager):
        """Test getting invalid query entry raises error."""
        with pytest.raises(ValueError, match="Invalid query ID"):
            manager.get_query_entry("nonexistent_query")

    def test_get_queries_by_category(self, manager):
        """Test getting queries by category."""
        generative = manager.get_queries_by_category("generative")

        assert isinstance(generative, dict)
        assert len(generative) > 0
        for query_id in generative.keys():
            assert "generative" in query_id

    def test_get_queries_by_category_empty(self, manager):
        """Test getting queries for nonexistent category returns empty dict."""
        result = manager.get_queries_by_category("nonexistent_category")
        assert result == {}

    def test_get_query_categories(self, manager):
        """Test getting list of categories."""
        categories = manager.get_query_categories()

        assert isinstance(categories, list)
        assert len(categories) > 0
        assert "generative" in categories
        assert "nlp" in categories
        assert "transform" in categories
        assert "embedding" in categories

    def test_get_supported_queries_snowflake(self, manager):
        """Test getting queries supported on Snowflake."""
        supported = manager.get_supported_queries("snowflake")

        assert isinstance(supported, dict)
        assert len(supported) > 0

        # Snowflake queries should have Cortex functions
        for query_id, sql in supported.items():
            assert sql is not None

    def test_get_supported_queries_unsupported(self, manager):
        """Test getting queries for unsupported platform returns variants only."""
        supported = manager.get_supported_queries("duckdb")

        # DuckDB should have no supported queries (all skipped)
        assert len(supported) == 0

    def test_get_query_cost_estimate(self, manager):
        """Test cost estimation for a query."""
        cost = manager.get_query_cost_estimate("generative_complete_simple", num_rows=10)

        assert isinstance(cost, float)
        assert cost > 0

    def test_get_query_cost_estimate_scales_with_rows(self, manager):
        """Test cost scales with number of rows."""
        cost_10 = manager.get_query_cost_estimate("nlp_sentiment_batch", num_rows=10)
        cost_100 = manager.get_query_cost_estimate("nlp_sentiment_batch", num_rows=100)

        assert cost_100 > cost_10
        # Should scale roughly linearly
        assert cost_100 / cost_10 == pytest.approx(10.0, rel=0.1)


class TestQueryVariants:
    """Tests for query variant handling."""

    @pytest.fixture()
    def manager(self):
        """Create a query manager instance."""
        return AIQueryManager()

    def test_snowflake_variants_use_cortex(self, manager):
        """Test Snowflake variants use Cortex functions."""
        supported = manager.get_supported_queries("snowflake")

        for query_id, sql in supported.items():
            # Snowflake AI queries should use CORTEX namespace
            assert "SNOWFLAKE.CORTEX" in sql or "placeholder" in sql.lower()

    def test_bigquery_variants_use_ml(self, manager):
        """Test BigQuery variants use ML functions."""
        supported = manager.get_supported_queries("bigquery")

        for query_id, sql in supported.items():
            # BigQuery AI queries should use ML namespace
            assert "ML." in sql or "placeholder" in sql.lower()

    def test_databricks_variants_use_ai(self, manager):
        """Test Databricks variants use ai_ functions."""
        supported = manager.get_supported_queries("databricks")

        for query_id, sql in supported.items():
            # Databricks AI queries should use ai_ prefix
            assert "ai_" in sql.lower() or "placeholder" in sql.lower()

    def test_base_sql_used_when_no_variant(self, manager):
        """Test base SQL is used when no variant exists."""
        # Get a query without specifying dialect
        sql_base = manager.get_query("generative_complete_simple")

        # Base SQL should be the placeholder
        assert "placeholder" in sql_base.lower()
