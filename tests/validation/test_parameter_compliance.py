"""Parameter compliance validation tests for TPC-H ultra-simplified implementation.

Tests that parameter handling complies with TPC-H specification.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox import TPCH
from benchbox.core.tpch.benchmark import TPCHBenchmark
from benchbox.core.tpch.queries import TPCHQueries


class TestTPCHParameterCompliance:
    """Test TPC-H parameter compliance with qgen."""

    def test_scale_factor_parameter_compliance(self):
        """Test scale factor parameter compliance."""
        tpch = TPCH()

        # Test valid scale factors
        valid_scale_factors = [0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0]

        for sf in valid_scale_factors:
            # Should generate without error
            sql = tpch.get_query(1, scale_factor=sf, seed=12345)
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_seed_parameter_compliance(self):
        """Test seed parameter compliance."""
        tpch = TPCH()

        # Test valid seed values
        valid_seeds = [1, 100, 10000, 2**31 - 1]

        for seed in valid_seeds:
            # Should generate without error
            sql = tpch.get_query(1, seed=seed)
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_query_id_parameter_compliance(self):
        """Test query ID parameter compliance."""
        tpch = TPCH()

        # Valid query IDs (1-22)
        for query_id in range(1, 23):
            sql = tpch.get_query(query_id, seed=54321)
            assert isinstance(sql, str)
            assert len(sql) > 50

        # Invalid query IDs should raise ValueError
        invalid_ids = [0, 23, 24, 25, -1, 100]
        for query_id in invalid_ids:
            with pytest.raises(ValueError) as exc_info:
                tpch.get_query(query_id)
            assert "Query ID must be 1-22" in str(exc_info.value)

    def test_parameter_type_validation(self):
        """Test parameter type validation."""
        tpch = TPCH()

        # Test valid types
        sql = tpch.get_query(1, seed=12345, scale_factor=0.01)
        assert isinstance(sql, str)

        # Test type coercion
        sql = tpch.get_query(1, seed=12345, scale_factor=0.01)  # float
        assert isinstance(sql, str)

        # Test integer query_id (correct type)
        sql = tpch.get_query(1, seed=12345)
        assert isinstance(sql, str)

    def test_parameter_inheritance_compliance(self):
        """Test parameter inheritance from benchmark instance."""
        # Create benchmark with specific scale factor
        benchmark = TPCHBenchmark(scale_factor=0.01)

        # Query without specifying scale factor should use inherited value
        sql = benchmark.get_query(1, seed=99999)
        assert isinstance(sql, str)
        assert len(sql) > 50

        # Query with explicit scale factor should override
        sql_override = benchmark.get_query(1, seed=99999, scale_factor=0.5)
        assert isinstance(sql_override, str)
        assert len(sql_override) > 50

    def test_parameter_boundary_conditions(self):
        """Test parameter boundary conditions."""
        tpch = TPCH()

        # Test minimum scale factor
        sql_min = tpch.get_query(1, scale_factor=0.01, seed=11111)
        assert isinstance(sql_min, str)

        # Test maximum practical scale factor
        sql_max = tpch.get_query(1, scale_factor=100.0, seed=11111)
        assert isinstance(sql_max, str)

        # Test minimum seed
        sql_seed_min = tpch.get_query(1, seed=1)
        assert isinstance(sql_seed_min, str)

        # Test maximum seed
        sql_seed_max = tpch.get_query(1, seed=2**31 - 1)
        assert isinstance(sql_seed_max, str)

    def test_parameter_combination_compliance(self):
        """Test various parameter combinations."""
        tpch = TPCH()

        # Test all valid combinations
        combinations = [
            {},  # No parameters
            {"seed": 12345},  # Seed only
            {"scale_factor": 0.1},  # Scale factor only
            {"seed": 54321, "scale_factor": 2.0},  # Both parameters
        ]

        for params in combinations:
            sql = tpch.get_query(1, **params)
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_deprecated_parameter_handling(self):
        """Test deprecated parameter handling compliance."""
        tpch = TPCH()

        # Test deprecated params=None (should work)
        sql = tpch.get_query(1, params=None, seed=33333)
        assert isinstance(sql, str)
        assert len(sql) > 50

        # Test mixed deprecated and new parameters
        sql_mixed = tpch.get_query(1, params=None, seed=44444, dialect="duckdb")
        assert isinstance(sql_mixed, str)
        assert len(sql_mixed) > 50

    def test_parameter_validation_across_interfaces(self):
        """Test parameter validation across different interfaces."""
        seed = 77777
        scale_factor = 0.01

        # Test through different interfaces
        tpch = TPCH()
        benchmark = TPCHBenchmark()
        queries = TPCHQueries()

        # All should handle parameters correctly
        sql_tpch = tpch.get_query(1, seed=seed, scale_factor=scale_factor)
        sql_benchmark = benchmark.get_query(1, seed=seed, scale_factor=scale_factor)
        sql_queries = queries.get_query(1, seed=seed, scale_factor=scale_factor)

        assert isinstance(sql_tpch, str)
        assert isinstance(sql_benchmark, str)
        assert isinstance(sql_queries, str)

    def test_parameter_persistence_across_calls(self):
        """Test that parameters don't persist across calls."""
        tpch = TPCH()

        # Generate with specific parameters
        sql1 = tpch.get_query(1, seed=11111, scale_factor=0.01)

        # Generate without parameters (should use defaults)
        sql2 = tpch.get_query(1)

        # Generate with different parameters
        sql3 = tpch.get_query(1, seed=22222, scale_factor=0.01)

        # All should be valid but potentially different
        assert isinstance(sql1, str)
        assert isinstance(sql2, str)
        assert isinstance(sql3, str)

    def test_default_parameter_behavior(self):
        """Test default parameter behavior."""
        tpch = TPCH()

        # No parameters should use qgen defaults
        sql_no_params = tpch.get_query(1)
        assert isinstance(sql_no_params, str)
        assert len(sql_no_params) > 50

        # Same call should be consistent (qgen should use same defaults)
        sql_no_params2 = tpch.get_query(1)
        assert sql_no_params == sql_no_params2

    def test_parameter_validation_error_messages(self):
        """Test parameter validation error messages."""
        tpch = TPCH()

        # Test invalid query ID error message
        with pytest.raises(ValueError) as exc_info:
            tpch.get_query(0)
        assert "Query ID must be 1-22" in str(exc_info.value)

        with pytest.raises(ValueError) as exc_info:
            tpch.get_query(25)
        assert "Query ID must be 1-22" in str(exc_info.value)

    def test_scale_factor_precision_handling(self):
        """Test scale factor precision handling."""
        tpch = TPCH()

        # Test various precision levels
        scale_factors = [0.001, 0.01, 0.1, 1.0, 1.5, 2.0, 10.0, 100.0]

        for sf in scale_factors:
            sql = tpch.get_query(1, scale_factor=sf, seed=88888)
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_seed_range_compliance(self):
        """Test seed range compliance."""
        tpch = TPCH()

        # Test various seed ranges
        seed_ranges = [
            [1, 100],  # Small seeds
            [1000, 10000],  # Medium seeds
            [100000, 1000000],  # Large seeds
            [2**30, 2**31 - 1],  # Maximum range
        ]

        for start, end in seed_ranges:
            # Test a few seeds in each range
            for seed in [start, (start + end) // 2, end]:
                sql = tpch.get_query(1, seed=seed)
                assert isinstance(sql, str)
                assert len(sql) > 50

    def test_parameter_immutability(self):
        """Test that parameters don't affect instance state."""
        tpch = TPCH(scale_factor=0.01)

        # Generate with different parameters
        tpch.get_query(1, scale_factor=0.01)
        tpch.get_query(1, scale_factor=0.02)

        # Instance scale factor should be what was initialized
        assert tpch.scale_factor == 0.01

        # Should still work with instance scale factor
        sql3 = tpch.get_query(1)
        assert isinstance(sql3, str)

    def test_parameter_forwarding_compliance(self):
        """Test parameter forwarding through the stack."""
        benchmark = TPCHBenchmark(scale_factor=0.01)

        # Test parameter forwarding through benchmark
        sql1 = benchmark.get_query(1, seed=12345)
        sql2 = benchmark.get_query(1, seed=12345)

        # Should be identical (same parameters)
        assert sql1 == sql2

        # Test with different parameters
        sql3 = benchmark.get_query(1, seed=54321, scale_factor=0.01)
        assert isinstance(sql3, str)
        assert len(sql3) > 50

    def test_tpch_specification_compliance(self):
        """Test compliance with TPC-H specification requirements."""
        tpch = TPCH()

        # TPC-H requires 22 queries
        queries = tpch.get_queries()
        assert len(queries) == 22

        # All query IDs should be present
        for query_id in range(1, 23):
            assert str(query_id) in queries

        # Each query should be valid
        for _query_id_str, sql in queries.items():
            assert isinstance(sql, str)
            assert len(sql) > 50
            assert "select" in sql.lower() or "with" in sql.lower()
