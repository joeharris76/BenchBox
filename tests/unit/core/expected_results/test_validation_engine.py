"""Unit tests for query validation engine.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.expected_results.models import ValidationMode
from benchbox.core.validation.query_validation import QueryValidator

pytestmark = pytest.mark.fast


class TestQueryValidator:
    """Test QueryValidator class."""

    def test_validate_tpch_query_exact_match(self):
        """Test validation with exact match for TPC-H Q1."""
        validator = QueryValidator()
        result = validator.validate_query_result(
            benchmark_type="tpch",
            query_id="1",
            actual_row_count=4,
            scale_factor=1.0,
        )
        assert result.is_valid
        assert result.expected_row_count == 4
        assert result.actual_row_count == 4
        assert result.validation_mode == ValidationMode.EXACT

    def test_validate_tpch_query_mismatch(self):
        """Test validation with row count mismatch."""
        validator = QueryValidator()
        result = validator.validate_query_result(
            benchmark_type="tpch",
            query_id="1",
            actual_row_count=5,  # Wrong count
            scale_factor=1.0,
        )
        assert not result.is_valid
        assert result.expected_row_count == 4
        assert result.actual_row_count == 5
        assert result.difference == 1
        assert result.error_message is not None

    def test_validate_unknown_benchmark(self):
        """Test validation with unknown benchmark type."""
        validator = QueryValidator()
        result = validator.validate_query_result(
            benchmark_type="unknown_benchmark",
            query_id="1",
            actual_row_count=100,
            scale_factor=1.0,
        )
        # Should skip validation gracefully
        assert result.is_valid
        assert result.validation_mode == ValidationMode.SKIP
        assert result.warning_message is not None

    def test_validate_unknown_query(self):
        """Test validation with unknown query ID."""
        validator = QueryValidator()
        result = validator.validate_query_result(
            benchmark_type="tpch",
            query_id="999",  # Non-existent query
            actual_row_count=100,
            scale_factor=1.0,
        )
        # Should skip validation gracefully
        assert result.is_valid
        assert result.validation_mode == ValidationMode.SKIP
        assert result.warning_message is not None

    def test_validate_tpcds_query(self):
        """Test validation with TPC-DS query.

        TPC-DS queries use SKIP validation mode by default because queries are
        parameterized with random seeds. The answer files represent one specific
        parameterization, but benchmark runs may use different seeds.
        """
        validator = QueryValidator()
        result = validator.validate_query_result(
            benchmark_type="tpcds",
            query_id="1",
            actual_row_count=101,  # TPC-DS Q1 returns 101 rows at SF=1
            scale_factor=1.0,
        )
        assert result.is_valid
        assert result.validation_mode == ValidationMode.SKIP
        assert result.actual_row_count == 101
        # expected_row_count is available but validation is skipped
        assert result.warning_message is not None
        assert "SKIP" in result.warning_message or "skip" in result.warning_message
