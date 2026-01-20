"""Unit tests for expected results models.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.expected_results.models import (
    BenchmarkExpectedResults,
    ExpectedQueryResult,
    ValidationMode,
    ValidationResult,
)

pytestmark = pytest.mark.fast


class TestExpectedQueryResult:
    """Test ExpectedQueryResult model."""

    def test_exact_validation_mode(self):
        """Test EXACT validation mode with expected_row_count."""
        result = ExpectedQueryResult(
            query_id="1",
            expected_row_count=100,
            validation_mode=ValidationMode.EXACT,
        )
        assert result.query_id == "1"
        assert result.expected_row_count == 100
        assert result.validation_mode == ValidationMode.EXACT
        assert result.get_expected_count() == 100

    def test_range_validation_mode(self):
        """Test RANGE validation mode with min/max."""
        result = ExpectedQueryResult(
            query_id="2",
            expected_row_count_min=50,
            expected_row_count_max=150,
            validation_mode=ValidationMode.RANGE,
        )
        assert result.expected_row_count_min == 50
        assert result.expected_row_count_max == 150
        assert result.validation_mode == ValidationMode.RANGE

    def test_skip_validation_mode(self):
        """Test SKIP validation mode."""
        result = ExpectedQueryResult(
            query_id="3",
            validation_mode=ValidationMode.SKIP,
        )
        assert result.validation_mode == ValidationMode.SKIP
        assert result.get_expected_count() is None

    def test_formula_evaluation(self):
        """Test formula-based expected count calculation."""
        result = ExpectedQueryResult(
            query_id="4",
            row_count_formula="5",
            validation_mode=ValidationMode.EXACT,
        )
        assert result.get_expected_count(scale_factor=1.0) == 5
        assert result.get_expected_count(scale_factor=10.0) == 5

    def test_scale_dependent_formula(self):
        """Test scale-dependent formula."""
        result = ExpectedQueryResult(
            query_id="5",
            row_count_formula="SF * 100",
            validation_mode=ValidationMode.EXACT,
        )
        assert result.get_expected_count(scale_factor=1.0) == 100
        assert result.get_expected_count(scale_factor=10.0) == 1000

    def test_validation_missing_exact_count(self):
        """Test validation error when EXACT mode missing expected count."""
        with pytest.raises(ValueError, match="EXACT mode requires"):
            ExpectedQueryResult(
                query_id="6",
                validation_mode=ValidationMode.EXACT,
                # Missing expected_row_count and row_count_formula
            )

    def test_validation_missing_range(self):
        """Test validation error when RANGE mode missing min/max."""
        with pytest.raises(ValueError, match="RANGE mode requires"):
            ExpectedQueryResult(
                query_id="7",
                validation_mode=ValidationMode.RANGE,
                expected_row_count_min=10,
                # Missing expected_row_count_max
            )

    def test_validation_invalid_range(self):
        """Test validation error when min > max in RANGE mode."""
        with pytest.raises(ValueError, match="must be <="):
            ExpectedQueryResult(
                query_id="8",
                validation_mode=ValidationMode.RANGE,
                expected_row_count_min=100,
                expected_row_count_max=50,  # Invalid: min > max
            )


class TestBenchmarkExpectedResults:
    """Test BenchmarkExpectedResults model."""

    def test_creation_and_lookup(self):
        """Test creating benchmark results and looking up queries."""
        query_results = {
            "1": ExpectedQueryResult(
                query_id="1",
                expected_row_count=100,
                validation_mode=ValidationMode.EXACT,
            ),
            "2": ExpectedQueryResult(
                query_id="2",
                expected_row_count=200,
                validation_mode=ValidationMode.EXACT,
            ),
        }

        benchmark_results = BenchmarkExpectedResults(
            benchmark_name="test_benchmark",
            scale_factor=1.0,
            query_results=query_results,
        )

        assert benchmark_results.benchmark_name == "test_benchmark"
        assert benchmark_results.scale_factor == 1.0
        assert len(benchmark_results.query_results) == 2

        # Test lookup
        result1 = benchmark_results.get_expected_result("1")
        assert result1 is not None
        assert result1.expected_row_count == 100

        result2 = benchmark_results.get_expected_result("2")
        assert result2 is not None
        assert result2.expected_row_count == 200

        # Test missing query
        result3 = benchmark_results.get_expected_result("999")
        assert result3 is None


class TestValidationResult:
    """Test ValidationResult model."""

    def test_valid_result(self):
        """Test successful validation result."""
        result = ValidationResult(
            is_valid=True,
            query_id="1",
            expected_row_count=100,
            actual_row_count=100,
            validation_mode=ValidationMode.EXACT,
        )
        assert result.is_valid
        assert result.difference == 0
        assert result.difference_percent == 0.0

    def test_invalid_result_with_difference(self):
        """Test failed validation with difference calculation."""
        result = ValidationResult(
            is_valid=False,
            query_id="2",
            expected_row_count=100,
            actual_row_count=110,
            validation_mode=ValidationMode.EXACT,
            error_message="Row count mismatch",
        )
        assert not result.is_valid
        assert result.difference == 10
        assert result.difference_percent == 10.0
        assert result.error_message == "Row count mismatch"

    def test_validation_with_warning(self):
        """Test validation result with warning."""
        result = ValidationResult(
            is_valid=True,
            query_id="3",
            expected_row_count=None,
            actual_row_count=50,
            validation_mode=ValidationMode.SKIP,
            warning_message="No expected result available",
        )
        assert result.is_valid
        assert result.warning_message == "No expected result available"
        assert result.difference is None
        assert result.difference_percent is None
