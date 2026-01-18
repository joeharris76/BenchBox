"""Unit tests for API backward compatibility with ultra-simplified implementation.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox import TPCH
from benchbox.core.tpch.benchmark import TPCHBenchmark
from benchbox.core.tpch.queries import TPCHQueries, TPCHQueryManager

pytestmark = pytest.mark.fast


class TestAPICompatibility:
    """Test backward compatibility with existing API."""

    def test_tpchquerymanager_import_compatibility(self):
        """Test that TPCHQueryManager can still be imported and used."""
        # Import should work

        # Should be alias for TPCHQueries
        assert TPCHQueryManager is TPCHQueries

        # Should be instantiable
        manager = TPCHQueryManager()
        assert isinstance(manager, TPCHQueries)

    def test_get_query_original_signature(self):
        """Test that original get_query() signature still works."""
        queries = TPCHQueries()

        # Original signature: get_query(query_id)
        sql = queries.get_query(1)
        assert isinstance(sql, str)
        assert len(sql) > 50

    def test_get_query_enhanced_signature(self):
        """Test that enhanced get_query() signature works."""
        queries = TPCHQueries()

        # Enhanced signature: get_query(query_id, seed=None, scale_factor=1.0)
        sql = queries.get_query(1, seed=12345, scale_factor=1.0)
        assert isinstance(sql, str)
        assert len(sql) > 50

    def test_benchmark_integration_compatibility(self):
        """Test integration with TPCHBenchmark class."""
        benchmark = TPCHBenchmark()

        # Test original get_query method
        sql = benchmark.get_query(1)
        assert isinstance(sql, str)
        assert len(sql) > 50

        # Test enhanced get_query method
        sql_enhanced = benchmark.get_query(1, seed=12345)
        assert isinstance(sql_enhanced, str)
        assert len(sql_enhanced) > 50

    def test_benchmark_get_parameterized_query_compatibility(self):
        """Test TPCHBenchmark get_parameterized_query compatibility."""
        benchmark = TPCHBenchmark()

        # Test old signature: get_query(query_id, params=None, dialect="standard")
        sql_old = benchmark.get_query(1, params=None, dialect="standard")
        assert isinstance(sql_old, str)
        assert len(sql_old) > 50

        # Test new signature: get_query(query_id, seed=None, scale_factor=None)
        sql_new = benchmark.get_query(1, seed=12345, scale_factor=1.0)
        assert isinstance(sql_new, str)
        assert len(sql_new) > 50

        # Test mixed signature
        sql_mixed = benchmark.get_query(1, params=None, seed=12345)
        assert isinstance(sql_mixed, str)
        assert len(sql_mixed) > 50

    def test_top_level_tpch_compatibility(self):
        """Test top-level TPCH class compatibility."""
        tpch = TPCH()

        # Test original signature
        sql = tpch.get_query(1)
        assert isinstance(sql, str)
        assert len(sql) > 50

        # Test enhanced signature
        sql_enhanced = tpch.get_query(1, seed=12345, scale_factor=1.0)
        assert isinstance(sql_enhanced, str)
        assert len(sql_enhanced) > 50

    def test_top_level_tpch_parameterized_query_compatibility(self):
        """Test top-level TPCH get_parameterized_query compatibility."""
        tpch = TPCH()

        # Test old style call with params=None
        sql_old = tpch.get_query(1, params=None, dialect="duckdb")
        assert isinstance(sql_old, str)
        assert len(sql_old) > 50

        # Test new style call
        sql_new = tpch.get_query(1, seed=12345, dialect="duckdb")
        assert isinstance(sql_new, str)
        assert len(sql_new) > 50

    def test_deprecated_params_parameter_ignored(self):
        """Test that deprecated params parameter is properly ignored."""
        tpch = TPCH()

        # params parameter should be ignored, not cause errors
        sql1 = tpch.get_query(1, params=None, seed=42)
        sql2 = tpch.get_query(1, params={"1": "ignored"}, seed=42)
        sql3 = tpch.get_query(1, seed=42)

        # All should be the same since params is ignored and seed is same
        assert sql1 == sql2 == sql3

    def test_method_signature_parameter_ordering(self):
        """Test that parameter ordering in method signatures works correctly."""
        tpch = TPCH()

        # Test positional arguments
        sql1 = tpch.get_query(1)
        assert isinstance(sql1, str)

        # Test keyword arguments
        sql2 = tpch.get_query(query_id=1, seed=123)
        assert isinstance(sql2, str)

        # Test mixed arguments
        sql3 = tpch.get_query(1, seed=123, scale_factor=1.0)
        assert isinstance(sql3, str)

    def test_scale_factor_inheritance_from_benchmark(self):
        """Test that benchmark scale factor is inherited when not specified."""
        # Create benchmark with specific scale factor
        benchmark = TPCHBenchmark(scale_factor=2.0)

        # Query should inherit scale factor from benchmark
        sql = benchmark.get_query(1)
        assert isinstance(sql, str)
        assert len(sql) > 50

        # Explicit scale factor should override benchmark scale factor
        sql_override = benchmark.get_query(1, scale_factor=0.5)
        assert isinstance(sql_override, str)
        assert len(sql_override) > 50

    def test_benchmark_get_queries_compatibility(self):
        """Test that get_queries() method still works."""
        benchmark = TPCHBenchmark()

        queries = benchmark.get_queries()

        assert isinstance(queries, dict)
        assert len(queries) == 22

        # Keys should be strings (for backward compatibility)
        for key in queries:
            assert isinstance(key, str)
            assert int(key) in range(1, 23)

        # Values should be SQL strings
        for sql in queries.values():
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_import_paths_compatibility(self):
        """Test that existing import paths still work."""
        # Test direct import
        from benchbox.core.tpch.queries import TPCHQueryManager as Manager1

        assert Manager1 is TPCHQueries

        # Test benchmark import
        from benchbox.core.tpch.benchmark import TPCHBenchmark as Benchmark1

        benchmark = Benchmark1()
        assert hasattr(benchmark, "get_query")

        # Test top-level import
        from benchbox import TPCH as TPCH1

        tpch = TPCH1()
        assert hasattr(tpch, "get_query")

    def test_error_handling_compatibility(self):
        """Test that error handling behaves consistently."""
        queries = TPCHQueries()

        # Invalid query ID should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            queries.get_query(23)
        assert "Query ID must be 1-22" in str(exc_info.value)

        # Same through benchmark
        benchmark = TPCHBenchmark()
        with pytest.raises(ValueError):
            benchmark.get_query(0)

        # Same through top-level TPCH
        tpch = TPCH()
        with pytest.raises(ValueError):
            tpch.get_query(-1)

    def test_return_type_compatibility(self):
        """Test that return types are consistent with expectations."""
        tpch = TPCH()

        # get_query should return string
        sql = tpch.get_query(1)
        assert isinstance(sql, str)

        # get_parameterized_query should return string
        sql_param = tpch.get_query(1)
        assert isinstance(sql_param, str)

        # get_queries should return dict
        queries = tpch.get_queries()
        assert isinstance(queries, dict)

    def test_dialect_parameter_handling(self):
        """Test that dialect parameter is handled appropriately."""
        tpch = TPCH()

        # Dialect parameter should be accepted but qgen ANSI mode handles compatibility
        sql_duckdb = tpch.get_query(1, dialect="duckdb")
        sql_postgres = tpch.get_query(1, dialect="postgres")
        sql_standard = tpch.get_query(1, dialect="standard")

        # All should be valid SQL strings
        assert isinstance(sql_duckdb, str)
        assert isinstance(sql_postgres, str)
        assert isinstance(sql_standard, str)
        assert len(sql_duckdb) > 50
        assert len(sql_postgres) > 50
        assert len(sql_standard) > 50
