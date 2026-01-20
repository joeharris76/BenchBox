"""Tests for AI/ML SQL function query management.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.aiml_functions.functions import AIMLFunctionCategory
from benchbox.core.aiml_functions.queries import AIMLQuery, AIMLQueryManager

pytestmark = pytest.mark.fast


class TestAIMLQuery:
    """Tests for AIMLQuery dataclass."""

    def test_basic_creation(self):
        """Should create query."""
        query = AIMLQuery(
            query_id="test_query",
            function_id="sentiment_analysis",
            category=AIMLFunctionCategory.SENTIMENT,
            name="Test Query",
            description="A test query",
        )
        assert query.query_id == "test_query"
        assert query.function_id == "sentiment_analysis"
        assert query.category == AIMLFunctionCategory.SENTIMENT

    def test_with_platform_queries(self):
        """Should create query with platform implementations."""
        query = AIMLQuery(
            query_id="test_query",
            function_id="sentiment_analysis",
            category=AIMLFunctionCategory.SENTIMENT,
            name="Test Query",
            description="A test query",
            platform_queries={
                "snowflake": "SELECT SNOWFLAKE.CORTEX.SENTIMENT(col) FROM tbl",
                "databricks": "SELECT ai_analyze_sentiment(col) FROM tbl",
            },
        )
        assert query.get_query("snowflake") is not None
        assert query.get_query("databricks") is not None
        assert query.get_query("bigquery") is None

    def test_get_query_case_insensitive(self):
        """Should be case insensitive for platform lookup."""
        query = AIMLQuery(
            query_id="test",
            function_id="test",
            category=AIMLFunctionCategory.SENTIMENT,
            name="Test",
            description="Test",
            platform_queries={"snowflake": "SELECT 1"},
        )
        assert query.get_query("Snowflake") is not None
        assert query.get_query("SNOWFLAKE") is not None

    def test_defaults(self):
        """Should have sensible defaults."""
        query = AIMLQuery(
            query_id="test",
            function_id="test",
            category=AIMLFunctionCategory.SENTIMENT,
            name="Test",
            description="Test",
        )
        assert query.requires_sample_data is True
        assert query.batch_size == 10
        assert query.timeout_seconds == 120

    def test_to_dict(self):
        """Should convert to dictionary."""
        query = AIMLQuery(
            query_id="test_query",
            function_id="sentiment",
            category=AIMLFunctionCategory.SENTIMENT,
            name="Test",
            description="A test",
            platform_queries={"snowflake": "SELECT 1"},
            batch_size=50,
            timeout_seconds=60,
        )
        d = query.to_dict()
        assert d["query_id"] == "test_query"
        assert d["function_id"] == "sentiment"
        assert d["category"] == "sentiment"
        assert "snowflake" in d["platforms"]
        assert d["batch_size"] == 50
        assert d["timeout_seconds"] == 60


class TestAIMLQueryManager:
    """Tests for AIMLQueryManager class."""

    @pytest.fixture
    def manager(self):
        """Create query manager instance."""
        return AIMLQueryManager()

    def test_basic_creation(self, manager):
        """Should create manager with queries."""
        queries = manager.get_all_queries()
        assert len(queries) > 0

    def test_get_query(self, manager):
        """Should get query by ID."""
        query = manager.get_query("sentiment_single")
        assert query is not None
        assert query.query_id == "sentiment_single"
        assert query.category == AIMLFunctionCategory.SENTIMENT

    def test_get_query_not_found(self, manager):
        """Should return None for unknown query."""
        assert manager.get_query("unknown_query") is None

    def test_get_queries_by_category(self, manager):
        """Should get queries by category."""
        sentiment_queries = manager.get_queries_by_category(AIMLFunctionCategory.SENTIMENT)
        assert len(sentiment_queries) >= 1
        assert all(q.category == AIMLFunctionCategory.SENTIMENT for q in sentiment_queries)

    def test_get_queries_for_platform(self, manager):
        """Should get queries available for a platform."""
        snowflake_queries = manager.get_queries_for_platform("snowflake")
        assert len(snowflake_queries) > 0
        assert all("snowflake" in q.platform_queries for q in snowflake_queries)

    def test_get_query_ids(self, manager):
        """Should get all query IDs."""
        ids = manager.get_query_ids()
        assert len(ids) > 0
        assert "sentiment_single" in ids
        assert "sentiment_batch" in ids

    def test_get_categories(self, manager):
        """Should get unique categories from queries."""
        categories = manager.get_categories()
        assert len(categories) > 0
        assert AIMLFunctionCategory.SENTIMENT in categories

    def test_export_queries(self, manager):
        """Should export all queries as dictionary."""
        export = manager.export_queries()
        assert len(export) > 0
        assert "sentiment_single" in export
        assert "query_id" in export["sentiment_single"]


class TestAIMLQueryContent:
    """Tests for actual query content."""

    @pytest.fixture
    def manager(self):
        """Create query manager instance."""
        return AIMLQueryManager()

    def test_sentiment_queries_exist(self, manager):
        """Should have sentiment analysis queries."""
        assert manager.get_query("sentiment_single") is not None
        assert manager.get_query("sentiment_batch") is not None
        assert manager.get_query("sentiment_aggregation") is not None

    def test_classification_queries_exist(self, manager):
        """Should have classification queries."""
        assert manager.get_query("classification_single") is not None
        assert manager.get_query("classification_batch") is not None

    def test_summarization_queries_exist(self, manager):
        """Should have summarization queries."""
        assert manager.get_query("summarization_single") is not None
        assert manager.get_query("summarization_batch") is not None

    def test_completion_queries_exist(self, manager):
        """Should have completion queries."""
        assert manager.get_query("completion_simple") is not None
        assert manager.get_query("completion_with_context") is not None

    def test_embedding_queries_exist(self, manager):
        """Should have embedding queries."""
        assert manager.get_query("embedding_single") is not None
        assert manager.get_query("embedding_batch") is not None

    def test_translation_queries_exist(self, manager):
        """Should have translation queries."""
        assert manager.get_query("translation_single") is not None
        assert manager.get_query("translation_batch") is not None

    def test_extraction_queries_exist(self, manager):
        """Should have extraction queries."""
        assert manager.get_query("extraction_single") is not None

    def test_snowflake_query_syntax(self, manager):
        """Should have valid Snowflake syntax."""
        query = manager.get_query("sentiment_single")
        assert query is not None
        sql = query.get_query("snowflake")
        assert sql is not None
        assert "SNOWFLAKE.CORTEX.SENTIMENT" in sql

    def test_databricks_query_syntax(self, manager):
        """Should have valid Databricks syntax."""
        query = manager.get_query("sentiment_single")
        assert query is not None
        sql = query.get_query("databricks")
        assert sql is not None
        assert "ai_analyze_sentiment" in sql

    def test_batch_queries_have_limits(self, manager):
        """Should have LIMIT in batch queries."""
        batch_query = manager.get_query("sentiment_batch")
        assert batch_query is not None
        sql = batch_query.get_query("snowflake")
        assert sql is not None
        assert "LIMIT" in sql.upper()

    def test_queries_reference_sample_tables(self, manager):
        """Should reference expected sample tables."""
        query = manager.get_query("sentiment_single")
        assert query is not None
        sql = query.get_query("snowflake")
        assert sql is not None
        assert "aiml_sample_data" in sql.lower()


class TestQueryPlatformCoverage:
    """Tests for platform coverage of queries."""

    @pytest.fixture
    def manager(self):
        """Create query manager instance."""
        return AIMLQueryManager()

    def test_snowflake_has_most_queries(self, manager):
        """Snowflake should have comprehensive query coverage."""
        queries = manager.get_queries_for_platform("snowflake")
        assert len(queries) >= 10

    def test_databricks_has_queries(self, manager):
        """Databricks should have query coverage."""
        queries = manager.get_queries_for_platform("databricks")
        assert len(queries) >= 5

    def test_bigquery_has_queries(self, manager):
        """BigQuery should have some queries."""
        queries = manager.get_queries_for_platform("bigquery")
        assert len(queries) >= 2

    def test_each_category_has_queries(self, manager):
        """Each major category should have at least one query."""
        categories_with_queries = set()
        for query in manager.get_all_queries().values():
            categories_with_queries.add(query.category)

        assert AIMLFunctionCategory.SENTIMENT in categories_with_queries
        assert AIMLFunctionCategory.CLASSIFICATION in categories_with_queries
        assert AIMLFunctionCategory.SUMMARIZATION in categories_with_queries
        assert AIMLFunctionCategory.COMPLETION in categories_with_queries
        assert AIMLFunctionCategory.EMBEDDING in categories_with_queries
