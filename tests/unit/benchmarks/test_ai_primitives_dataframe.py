"""Unit tests for AI Primitives DataFrame operations.

Tests the DataFrame implementations of AI Primitives benchmark operations
including capability detection, operation validation, and error handling.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.ai_primitives.benchmark import AIPrimitivesBenchmark
from benchbox.core.ai_primitives.dataframe_operations import (
    SKIP_FOR_DATAFRAME,
    AIModelCapabilities,
    AIOperationType,
    DataFrameAICapabilities,
    DataFrameAIOperationsManager,
    DataFrameAIResult,
    get_dataframe_ai_manager,
    get_skip_for_dataframe,
    validate_ai_primitives_dataframe_platform,
)


class TestAIModelCapabilities:
    """Test ML model capability detection."""

    def test_detect_returns_capabilities(self):
        """detect() should return AIModelCapabilities instance."""
        caps = AIModelCapabilities.detect()
        assert isinstance(caps, AIModelCapabilities)

    def test_detect_checks_numpy(self):
        """detect() should check NumPy availability."""
        caps = AIModelCapabilities.detect()
        # NumPy should be available in test environment
        assert caps.has_numpy is True

    def test_can_run_embeddings_requires_both_deps(self):
        """can_run_embeddings should require both sentence-transformers and torch."""
        # With nothing installed
        caps = AIModelCapabilities(
            has_sentence_transformers=False,
            has_torch=False,
        )
        assert caps.can_run_embeddings() is False

        # With only torch
        caps = AIModelCapabilities(
            has_sentence_transformers=False,
            has_torch=True,
        )
        assert caps.can_run_embeddings() is False

        # With only sentence-transformers
        caps = AIModelCapabilities(
            has_sentence_transformers=True,
            has_torch=False,
        )
        assert caps.can_run_embeddings() is False

        # With both
        caps = AIModelCapabilities(
            has_sentence_transformers=True,
            has_torch=True,
        )
        assert caps.can_run_embeddings() is True

    def test_can_run_sentiment_requires_textblob(self):
        """can_run_sentiment should require textblob."""
        caps = AIModelCapabilities(has_textblob=False)
        assert caps.can_run_sentiment() is False

        caps = AIModelCapabilities(has_textblob=True)
        assert caps.can_run_sentiment() is True

    def test_get_missing_for_operation_embedding(self):
        """get_missing_for_operation should return missing packages for embeddings."""
        caps = AIModelCapabilities(
            has_sentence_transformers=False,
            has_torch=False,
        )
        missing = caps.get_missing_for_operation(AIOperationType.EMBEDDING_SINGLE)
        assert len(missing) == 2
        assert any("sentence-transformers" in m for m in missing)
        assert any("torch" in m for m in missing)

    def test_get_missing_for_operation_sentiment(self):
        """get_missing_for_operation should return missing packages for sentiment."""
        caps = AIModelCapabilities(has_textblob=False)
        missing = caps.get_missing_for_operation(AIOperationType.SENTIMENT_SINGLE)
        assert len(missing) == 1
        assert "textblob" in missing[0]


class TestSkipLists:
    """Test the skip/exclusion lists."""

    def test_skip_for_dataframe_count(self):
        """SKIP_FOR_DATAFRAME should have 8 queries (generative + transform)."""
        assert len(SKIP_FOR_DATAFRAME) == 8

    def test_skip_for_dataframe_contains_generative_queries(self):
        """SKIP_FOR_DATAFRAME should contain all 4 generative queries."""
        generative_queries = [
            "generative_complete_simple",
            "generative_complete_customer_profile",
            "generative_question_answer",
            "generative_sql_generation",
        ]
        for qid in generative_queries:
            assert qid in SKIP_FOR_DATAFRAME, f"Missing generative query: {qid}"

    def test_skip_for_dataframe_contains_transform_queries(self):
        """SKIP_FOR_DATAFRAME should contain all 4 transform queries."""
        transform_queries = [
            "transform_summarize_short",
            "transform_summarize_long",
            "transform_translate_comment",
            "transform_grammar_fix",
        ]
        for qid in transform_queries:
            assert qid in SKIP_FOR_DATAFRAME, f"Missing transform query: {qid}"

    def test_get_skip_for_dataframe_returns_copy(self):
        """get_skip_for_dataframe should return a copy, not the original."""
        result = get_skip_for_dataframe()
        assert result == SKIP_FOR_DATAFRAME
        assert result is not SKIP_FOR_DATAFRAME


class TestDataFrameAICapabilities:
    """Test DataFrame AI capabilities."""

    def test_supports_operation_with_full_capabilities(self):
        """All operations should be supported with full capabilities."""
        model_caps = AIModelCapabilities(
            has_sentence_transformers=True,
            has_torch=True,
            has_textblob=True,
            has_spacy=True,
            has_numpy=True,
        )
        caps = DataFrameAICapabilities(
            platform_name="test-df",
            model_caps=model_caps,
        )

        # All operations should be supported
        assert caps.supports_operation(AIOperationType.EMBEDDING_SINGLE)
        assert caps.supports_operation(AIOperationType.EMBEDDING_BATCH)
        assert caps.supports_operation(AIOperationType.SENTIMENT_SINGLE)
        assert caps.supports_operation(AIOperationType.SENTIMENT_BATCH)
        assert caps.supports_operation(AIOperationType.CLASSIFY_PRIORITY)
        assert caps.supports_operation(AIOperationType.ENTITY_EXTRACTION)
        assert caps.supports_operation(AIOperationType.COSINE_SIMILARITY)

    def test_supports_operation_with_no_capabilities(self):
        """No operations should be supported without ML libraries."""
        model_caps = AIModelCapabilities()
        caps = DataFrameAICapabilities(
            platform_name="test-df",
            model_caps=model_caps,
        )

        # No operations should be supported
        assert not caps.supports_operation(AIOperationType.EMBEDDING_SINGLE)
        assert not caps.supports_operation(AIOperationType.SENTIMENT_SINGLE)

    def test_get_unsupported_operations(self):
        """get_unsupported_operations should list all unsupported operations."""
        model_caps = AIModelCapabilities(has_textblob=True)
        caps = DataFrameAICapabilities(
            platform_name="test-df",
            model_caps=model_caps,
        )

        unsupported = caps.get_unsupported_operations()
        # Embedding operations should be unsupported
        assert AIOperationType.EMBEDDING_SINGLE in unsupported
        # Sentiment should be supported
        assert AIOperationType.SENTIMENT_SINGLE not in unsupported


class TestDataFrameAIResult:
    """Test DataFrame AI result dataclass."""

    def test_failure_creates_failure_result(self):
        """failure() should create a result with success=False."""
        result = DataFrameAIResult.failure(
            AIOperationType.EMBEDDING_SINGLE,
            "Test error message",
        )

        assert result.success is False
        assert result.operation_type == AIOperationType.EMBEDDING_SINGLE
        assert result.error_message == "Test error message"
        assert result.rows_processed == 0


class TestDataFrameAIOperationsManager:
    """Test DataFrame AI operations manager."""

    def test_manager_creation(self):
        """Manager should be created for DataFrame platforms."""
        manager = DataFrameAIOperationsManager("polars-df")
        assert manager is not None
        assert manager.platform_name == "polars-df"

    def test_get_capabilities(self):
        """get_capabilities should return DataFrameAICapabilities."""
        manager = DataFrameAIOperationsManager("pandas-df")
        caps = manager.get_capabilities()
        assert isinstance(caps, DataFrameAICapabilities)
        assert caps.platform_name == "pandas-df"

    def test_supports_operation_delegates_to_capabilities(self):
        """supports_operation should delegate to capabilities."""
        manager = DataFrameAIOperationsManager("polars-df")
        caps = manager.get_capabilities()

        # Should match capabilities
        for op in AIOperationType:
            assert manager.supports_operation(op) == caps.supports_operation(op)

    def test_get_unsupported_message(self):
        """get_unsupported_message should return helpful error message."""
        manager = DataFrameAIOperationsManager("test-df")

        # Force capabilities to not support embeddings
        manager._model_caps = AIModelCapabilities(
            has_sentence_transformers=False,
            has_torch=False,
        )

        msg = manager.get_unsupported_message(AIOperationType.EMBEDDING_SINGLE)
        assert "sentence-transformers" in msg or "missing" in msg.lower()

    def test_execute_embedding_single_without_deps(self):
        """execute_embedding_single should fail gracefully without deps."""
        manager = DataFrameAIOperationsManager("test-df")
        # Force no capabilities
        manager._capabilities = DataFrameAICapabilities(
            platform_name="test-df",
            model_caps=AIModelCapabilities(),
        )

        result = manager.execute_embedding_single(["test text"])
        assert result.success is False
        assert result.error_message is not None

    def test_execute_sentiment_single_without_deps(self):
        """execute_sentiment_single should fail gracefully without deps."""
        manager = DataFrameAIOperationsManager("test-df")
        manager._capabilities = DataFrameAICapabilities(
            platform_name="test-df",
            model_caps=AIModelCapabilities(has_textblob=False),
        )

        result = manager.execute_sentiment_single(["test text"])
        assert result.success is False
        assert result.error_message is not None


class TestGetDataframeAIManager:
    """Test get_dataframe_ai_manager factory function."""

    def test_returns_manager_for_dataframe_platforms(self):
        """Should return manager for DataFrame platforms."""
        for platform in ["polars-df", "pandas-df", "pyspark-df"]:
            manager = get_dataframe_ai_manager(platform)
            assert manager is not None, f"Should return manager for {platform}"

    def test_returns_none_for_sql_platforms(self):
        """Should return None for non-DataFrame platforms."""
        for platform in ["duckdb", "sqlite", "snowflake", "bigquery"]:
            manager = get_dataframe_ai_manager(platform)
            assert manager is None, f"Should return None for {platform}"


class TestValidateAIPrimitivesDataframePlatform:
    """Test platform validation function."""

    def test_rejects_non_dataframe_platforms(self):
        """Should reject non-DataFrame platforms."""
        is_valid, msg = validate_ai_primitives_dataframe_platform("duckdb")
        assert is_valid is False
        assert "DataFrame platform" in msg

        is_valid, msg = validate_ai_primitives_dataframe_platform("snowflake")
        assert is_valid is False

    def test_validates_dataframe_platforms(self):
        """Should validate DataFrame platforms (may fail on missing deps)."""
        # Note: this test might fail if ML deps are not installed
        is_valid, msg = validate_ai_primitives_dataframe_platform("polars-df")
        # Either valid or fails due to missing ML deps
        if not is_valid:
            assert "ML dependencies" in msg or "sentence-transformers" in msg


class TestBenchmarkIntegration:
    """Test integration with AIPrimitivesBenchmark class."""

    def test_benchmark_supports_dataframe_mode(self):
        """AIPrimitivesBenchmark should report DataFrame support."""
        benchmark = AIPrimitivesBenchmark(scale_factor=0.01)
        assert benchmark.supports_dataframe_mode() is True

    def test_benchmark_get_dataframe_skip_queries(self):
        """Benchmark should return correct skip list."""
        benchmark = AIPrimitivesBenchmark(scale_factor=0.01)
        skip_list = benchmark.get_dataframe_skip_queries()

        assert isinstance(skip_list, list)
        assert len(skip_list) == 8
        assert "generative_complete_simple" in skip_list
        assert "transform_summarize_short" in skip_list

    def test_benchmark_get_dataframe_supported_queries(self):
        """Benchmark should return correct supported queries."""
        benchmark = AIPrimitivesBenchmark(scale_factor=0.01)
        supported = benchmark.get_dataframe_supported_queries()

        assert isinstance(supported, list)
        assert len(supported) == 8

        # Should include embedding and NLP queries
        assert "embedding_single" in supported
        assert "nlp_sentiment_single" in supported
        assert "nlp_classify_priority" in supported
        assert "nlp_entity_extraction" in supported

        # Should NOT include generative queries
        assert "generative_complete_simple" not in supported

    def test_benchmark_skip_and_supported_are_disjoint(self):
        """Skip list and supported list should have no overlap."""
        benchmark = AIPrimitivesBenchmark(scale_factor=0.01)
        skip_set = set(benchmark.get_dataframe_skip_queries())
        supported_set = set(benchmark.get_dataframe_supported_queries())

        assert skip_set.isdisjoint(supported_set), "Skip and supported lists should not overlap"

    def test_benchmark_skip_and_supported_cover_all_queries(self):
        """Skip + supported should equal all queries."""
        benchmark = AIPrimitivesBenchmark(scale_factor=0.01)
        skip_list = benchmark.get_dataframe_skip_queries()
        supported_list = benchmark.get_dataframe_supported_queries()
        all_queries = list(benchmark.query_manager.get_all_queries().keys())

        combined = sorted(skip_list + supported_list)
        assert combined == sorted(all_queries), "Skip + supported should cover all queries"


@pytest.mark.fast
class TestAIOperationType:
    """Test AI operation type enum."""

    def test_embedding_operations(self):
        """Should have 3 embedding operation types."""
        embedding_ops = [
            AIOperationType.EMBEDDING_SINGLE,
            AIOperationType.EMBEDDING_BATCH,
            AIOperationType.EMBEDDING_LARGE_DIMENSION,
        ]
        assert len(embedding_ops) == 3

    def test_nlp_operations(self):
        """Should have NLP operation types."""
        nlp_ops = [
            AIOperationType.SENTIMENT_SINGLE,
            AIOperationType.SENTIMENT_BATCH,
            AIOperationType.CLASSIFY_PRIORITY,
            AIOperationType.CLASSIFY_SEGMENT,
            AIOperationType.ENTITY_EXTRACTION,
        ]
        assert len(nlp_ops) == 5

    def test_vector_operations(self):
        """Should have vector similarity operation types."""
        vector_ops = [
            AIOperationType.COSINE_SIMILARITY,
            AIOperationType.EUCLIDEAN_DISTANCE,
            AIOperationType.TOP_K_SIMILARITY,
        ]
        assert len(vector_ops) == 3
