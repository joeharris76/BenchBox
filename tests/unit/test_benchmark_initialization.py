"""Tests for consolidated benchmark initialization functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.base import BaseBenchmark

pytestmark = pytest.mark.fast


class MockImplementation:
    """Mock benchmark implementation class."""

    def __init__(self, scale_factor, output_dir, verbose=False, force_regenerate=False, **kwargs):
        self.scale_factor = scale_factor
        self.output_dir = output_dir
        self.verbose = verbose
        self.force_regenerate = force_regenerate
        self.kwargs = kwargs


class MockBenchmark(BaseBenchmark):
    """Test benchmark class for testing initialization consolidation."""

    def __init__(self, scale_factor=1.0, output_dir=None, **kwargs):
        self._validate_scale_factor_type(scale_factor)
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)
        self._initialize_benchmark_implementation(MockImplementation, scale_factor, output_dir, **kwargs)

    def generate_data(self):
        return []

    def get_queries(self):
        return {}

    def get_schema(self):
        return []

    def get_query(self, query_id, **kwargs):
        return f"SELECT {query_id};"


class MockBenchmarkInitializationConsolidation:
    """Test consolidated benchmark initialization functionality."""

    def test_validate_scale_factor_type_valid(self):
        """Test scale factor type validation with valid inputs."""
        benchmark = MockBenchmark()

        # Should not raise for valid types
        benchmark._validate_scale_factor_type(1.0)
        benchmark._validate_scale_factor_type(1)
        benchmark._validate_scale_factor_type(0.5)

    def test_validate_scale_factor_type_invalid(self):
        """Test scale factor type validation with invalid inputs."""
        benchmark = MockBenchmark()

        # Should raise TypeError for invalid types
        with pytest.raises(TypeError, match="scale_factor must be a number, got str"):
            benchmark._validate_scale_factor_type("1.0")

        with pytest.raises(TypeError, match="scale_factor must be a number, got list"):
            benchmark._validate_scale_factor_type([1.0])

        with pytest.raises(TypeError, match="scale_factor must be a number, got NoneType"):
            benchmark._validate_scale_factor_type(None)

    def test_initialize_benchmark_implementation(self):
        """Test common benchmark implementation initialization."""
        test_kwargs = {
            "custom_param": "value",
            "verbose": True,
            "force_regenerate": False,
        }

        benchmark = MockBenchmark(scale_factor=2.0, output_dir="/tmp/test", **test_kwargs)

        # Check that implementation was initialized correctly
        assert hasattr(benchmark, "_impl")
        assert isinstance(benchmark._impl, MockImplementation)
        assert benchmark._impl.scale_factor == 2.0
        assert benchmark._impl.output_dir == "/tmp/test"
        assert benchmark._impl.verbose is True
        assert benchmark._impl.force_regenerate is False
        assert benchmark._impl.kwargs == {"custom_param": "value"}

    def test_initialize_benchmark_implementation_with_defaults(self):
        """Test initialization with default verbose and force_regenerate."""
        test_kwargs = {"custom_param": "value"}

        benchmark = MockBenchmark(scale_factor=1.0, **test_kwargs)

        # Check defaults were applied
        assert benchmark._impl.verbose is False
        assert benchmark._impl.force_regenerate is False
        assert benchmark._impl.kwargs == {"custom_param": "value"}

    def test_initialize_benchmark_implementation_kwargs_extraction(self):
        """Test that verbose and force_regenerate are properly extracted from kwargs."""
        test_kwargs = {
            "verbose": True,
            "force_regenerate": True,
            "other_param": "should_remain",
            "another_param": 42,
        }

        benchmark = MockBenchmark(scale_factor=1.0, **test_kwargs)

        # Check that verbose and force_regenerate were extracted
        assert benchmark._impl.verbose is True
        assert benchmark._impl.force_regenerate is True

        # Check that other params remained in kwargs
        expected_remaining = {"other_param": "should_remain", "another_param": 42}
        assert benchmark._impl.kwargs == expected_remaining

    def test_scale_factor_type_validation_in_constructor(self):
        """Test that scale factor type validation is called during construction."""
        # Valid construction should work
        benchmark = MockBenchmark(scale_factor=1.0)
        assert benchmark.scale_factor == 1.0

        # Invalid construction should fail
        with pytest.raises(TypeError, match="scale_factor must be a number"):
            MockBenchmark(scale_factor="invalid")

    def test_consolidated_initialization_reduces_duplication(self):
        """Test that the consolidated pattern reduces code duplication."""
        # This test verifies that the helper methods work correctly
        # and can be used consistently across different benchmarks

        class AnotherMockBenchmark(BaseBenchmark):
            def __init__(self, scale_factor=1.0, output_dir=None, **kwargs):
                self._validate_scale_factor_type(scale_factor)
                super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)
                self._initialize_benchmark_implementation(MockImplementation, scale_factor, output_dir, **kwargs)

            def generate_data(self):
                return []

            def get_queries(self):
                return {}

            def get_schema(self):
                return []

            def get_query(self, query_id, **kwargs):
                return f"SELECT {query_id};"

        # Both benchmarks should behave identically
        benchmark1 = MockBenchmark(scale_factor=2.0, verbose=True, custom="param1")
        benchmark2 = AnotherMockBenchmark(scale_factor=2.0, verbose=True, custom="param2")

        # Same initialization pattern
        assert benchmark1._impl.scale_factor == benchmark2._impl.scale_factor
        assert benchmark1._impl.verbose == benchmark2._impl.verbose
        assert benchmark1._impl.force_regenerate == benchmark2._impl.force_regenerate

        # Different custom parameters preserved
        assert benchmark1._impl.kwargs == {"custom": "param1"}
        assert benchmark2._impl.kwargs == {"custom": "param2"}
