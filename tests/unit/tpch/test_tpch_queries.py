"""Unit tests for TPCHQueries class - Ultra-simplified TPC-H interface.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import patch

import pytest

from benchbox.core.tpch.queries import TPCHQueries, TPCHQueryManager

# Mark all tests in this file as unit tests
pytestmark = [pytest.mark.unit, pytest.mark.fast]


class TestTPCHQueries:
    """Test TPCHQueries ultra-simplified interface."""

    def test_initialization_requires_qgen(self):
        """Test that initialization fails fast if qgen not available."""
        with patch("benchbox.core.tpch.queries.QGenBinary") as mock_qgen:
            mock_qgen.side_effect = RuntimeError("qgen binary required but not found")

            with pytest.raises(RuntimeError) as exc_info:
                TPCHQueries()

            assert "qgen binary required but not found" in str(exc_info.value)

    def test_successful_initialization(self):
        """Test successful initialization with qgen available."""
        # This should work in actual environment with qgen
        queries = TPCHQueries()
        assert queries.qgen is not None

    def test_get_query_all_valid_ids(self):
        """Test get_query for all 22 TPC-H queries."""
        queries = TPCHQueries()

        # Test all valid query IDs
        for query_id in range(1, 23):
            sql = queries.get_query(query_id)

            assert isinstance(sql, str)
            assert len(sql) > 50  # Should be substantial SQL
            assert "select" in sql.lower() or "with" in sql.lower()  # Valid SQL
            assert ":1" not in sql  # No parameter placeholders should remain

    def test_get_query_with_seed_determinism(self):
        """Test that same seed produces same results."""
        queries = TPCHQueries()

        # Test deterministic behavior
        sql1 = queries.get_query(1, seed=12345)
        sql2 = queries.get_query(1, seed=12345)

        assert sql1 == sql2

        # Test multiple queries with same seed
        results1 = [queries.get_query(i, seed=42) for i in range(1, 6)]
        results2 = [queries.get_query(i, seed=42) for i in range(1, 6)]

        assert results1 == results2

    def test_get_query_different_seeds(self):
        """Test that different seeds produce different results."""
        queries = TPCHQueries()

        sql_seed1 = queries.get_query(1, seed=111)
        sql_seed2 = queries.get_query(1, seed=222)

        # Different seeds should produce different parameters
        assert sql_seed1 != sql_seed2

    def test_get_query_with_scale_factor(self):
        """Test scale factor parameter functionality."""
        queries = TPCHQueries()

        # Test different scale factors
        sql_sf1 = queries.get_query(1, scale_factor=1.0)
        sql_sf01 = queries.get_query(1, scale_factor=0.1)

        assert isinstance(sql_sf1, str)
        assert isinstance(sql_sf01, str)
        assert len(sql_sf1) > 0
        assert len(sql_sf01) > 0

    def test_get_query_invalid_id_low(self):
        """Test error handling for query ID too low."""
        queries = TPCHQueries()

        with pytest.raises(ValueError) as exc_info:
            queries.get_query(0)

        assert "Query ID must be 1-22, got 0" in str(exc_info.value)

    def test_get_query_invalid_id_high(self):
        """Test error handling for query ID too high."""
        queries = TPCHQueries()

        with pytest.raises(ValueError) as exc_info:
            queries.get_query(23)

        assert "Query ID must be 1-22, got 23" in str(exc_info.value)

    def test_get_query_invalid_id_negative(self):
        """Test error handling for negative query ID."""
        queries = TPCHQueries()

        with pytest.raises(ValueError) as exc_info:
            queries.get_query(-1)

        assert "Query ID must be 1-22, got -1" in str(exc_info.value)

    def test_get_all_queries_batch(self):
        """Test batch generation of all queries."""
        queries = TPCHQueries()

        all_queries = queries.get_all_queries()

        # Should return dictionary with all 22 queries
        assert isinstance(all_queries, dict)
        assert len(all_queries) == 22

        # Check all query IDs are present
        for query_id in range(1, 23):
            assert query_id in all_queries
            assert isinstance(all_queries[query_id], str)
            assert len(all_queries[query_id]) > 50

    def test_get_all_queries_with_parameters(self):
        """Test batch generation with parameters."""
        queries = TPCHQueries()

        # Test with seed
        all_queries_seed = queries.get_all_queries(seed=777)
        assert len(all_queries_seed) == 22

        # Test with scale factor
        all_queries_sf = queries.get_all_queries(scale_factor=0.5)
        assert len(all_queries_sf) == 22

        # Test with both
        all_queries_both = queries.get_all_queries(seed=888, scale_factor=2.0)
        assert len(all_queries_both) == 22

    def test_tpchquerymanager_alias(self):
        """Test backward compatibility alias."""
        # TPCHQueryManager should be an alias for TPCHQueries
        assert TPCHQueryManager is TPCHQueries

        # Should be able to instantiate with alias
        manager = TPCHQueryManager()
        assert isinstance(manager, TPCHQueries)

        # Should have same functionality
        sql = manager.get_query(1)
        assert isinstance(sql, str)
        assert len(sql) > 50

    def test_parameter_combinations(self):
        """Test various parameter combinations."""
        queries = TPCHQueries()

        # Test all parameter combinations
        combinations = [
            {},  # No parameters
            {"seed": 123},  # Seed only
            {"scale_factor": 0.5},  # Scale factor only
            {"seed": 456, "scale_factor": 2.0},  # Both parameters
        ]

        for params in combinations:
            sql = queries.get_query(1, **params)
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_query_content_validation(self):
        """Test that generated queries contain expected content."""
        queries = TPCHQueries()

        # Test specific queries for expected content
        query1 = queries.get_query(1)
        assert "lineitem" in query1.lower()
        assert "l_shipdate" in query1.lower()

        query2 = queries.get_query(2)
        assert "part" in query2.lower()
        assert "supplier" in query2.lower()

        query3 = queries.get_query(3)
        assert "customer" in query3.lower()
        assert "orders" in query3.lower()

    def test_no_parameter_placeholders_in_output(self):
        """Test that no parameter placeholders remain in generated SQL."""
        queries = TPCHQueries()

        # Test several queries
        for query_id in [1, 5, 10, 15, 20]:
            sql = queries.get_query(query_id, seed=99)

            # Should not contain parameter placeholders
            assert ":1" not in sql
            assert ":2" not in sql
            assert ":3" not in sql
            assert ":4" not in sql
            assert ":5" not in sql

    def test_scale_factor_edge_cases(self):
        """Test scale factor edge cases."""
        queries = TPCHQueries()

        # Test very small scale factor
        sql_tiny = queries.get_query(1, scale_factor=0.01)
        assert isinstance(sql_tiny, str)
        assert len(sql_tiny) > 0

        # Test larger scale factor
        sql_large = queries.get_query(1, scale_factor=10.0)
        assert isinstance(sql_large, str)
        assert len(sql_large) > 0

    def test_seed_edge_cases(self):
        """Test seed edge cases."""
        queries = TPCHQueries()

        # Test various seed values
        seeds = [0, 1, 999999, 2147483647]  # Including max int

        for seed in seeds:
            sql = queries.get_query(1, seed=seed)
            assert isinstance(sql, str)
            assert len(sql) > 0

    def test_error_propagation_from_qgen(self):
        """Test that errors from qgen are properly propagated."""
        queries = TPCHQueries()

        # Mock qgen to raise an error
        with patch.object(queries.qgen, "generate") as mock_generate:
            mock_generate.side_effect = RuntimeError("Mock qgen error")

            with pytest.raises(RuntimeError):
                queries.get_query(1)

    def test_extract_rowcount_oracle_syntax(self):
        """Test extraction of rowcount from Oracle ROWNUM syntax."""
        from benchbox.core.tpch.queries import QGenBinary

        qgen = QGenBinary()

        # Test Oracle syntax: WHERE ROWNUM <= n
        assert qgen._extract_rowcount("SELECT * FROM orders WHERE ROWNUM <= 20;") == 20
        assert qgen._extract_rowcount("SELECT * FROM orders WHERE ROWNUM <= 100;") == 100
        assert qgen._extract_rowcount("select * from orders where rownum <= 10;") == 10  # lowercase

        # Test no rowcount
        assert qgen._extract_rowcount("SELECT * FROM orders;") is None

    def test_extract_rowcount_sqlserver_syntax(self):
        """Test extraction of rowcount from SQL Server SET ROWCOUNT syntax."""
        from benchbox.core.tpch.queries import QGenBinary

        qgen = QGenBinary()

        # Test SQL Server syntax: SET ROWCOUNT n
        assert qgen._extract_rowcount("SET ROWCOUNT 100\nGO\nSELECT * FROM orders;") == 100
        assert qgen._extract_rowcount("set rowcount 20\ngo\nselect * from orders;") == 20  # lowercase

    def test_extract_rowcount_informix_syntax(self):
        """Test extraction of rowcount from Informix FIRST syntax."""
        from benchbox.core.tpch.queries import QGenBinary

        qgen = QGenBinary()

        # Test Informix syntax: FIRST n
        assert qgen._extract_rowcount("SELECT FIRST 100 * FROM orders;") == 100
        assert qgen._extract_rowcount("select first 20 * from orders;") == 20  # lowercase

    def test_queries_with_limits_have_limit_clause(self):
        """Test that queries requiring row limits have LIMIT clause in output.

        According to TPC-H specification, these queries have :n directives with positive values:
        - Query 2: LIMIT 100
        - Query 3: LIMIT 10
        - Query 10: LIMIT 20
        - Query 18: LIMIT 100
        - Query 21: LIMIT 100

        Note: Query 15 has :n -1 (no limit), so it's excluded from this test.
        """
        queries = TPCHQueries()

        # Test queries that should have LIMIT clauses
        test_cases = [
            (2, 100),
            (3, 10),
            (10, 20),
            (18, 100),
            (21, 100),
        ]

        for query_id, expected_limit in test_cases:
            sql = queries.get_query(query_id, seed=42)
            sql_upper = sql.upper()

            # Should contain LIMIT clause
            assert "LIMIT" in sql_upper, f"Query {query_id} should have LIMIT clause"

            # Should contain the expected limit value
            assert f"LIMIT {expected_limit}" in sql_upper, (
                f"Query {query_id} should have LIMIT {expected_limit}, got:\n{sql}"
            )

            # Should NOT contain Oracle/SQL Server specific syntax
            assert "ROWNUM" not in sql_upper, f"Query {query_id} should not have ROWNUM"
            assert "SET ROWCOUNT" not in sql_upper, f"Query {query_id} should not have SET ROWCOUNT"

    def test_queries_without_limits_no_limit_clause(self):
        """Test that queries not requiring row limits don't have LIMIT clause.

        These queries either have no :n directive or have :n -1 (no limit):
        - Query 1: No limit (aggregates to 4 rows)
        - Query 4: No limit (aggregation)
        - Query 6: No limit (single aggregate)
        - Query 8: No limit (yearly aggregation)
        - Query 15: :n -1 (no limit, returns all matching rows)
        """
        queries = TPCHQueries()

        # Test queries that should NOT have LIMIT clauses
        test_cases = [1, 4, 6, 8, 11, 12, 14, 15, 16, 17, 19, 22]

        for query_id in test_cases:
            sql = queries.get_query(query_id, seed=42)
            sql_upper = sql.upper()

            # Should NOT contain LIMIT clause
            assert "LIMIT" not in sql_upper, f"Query {query_id} should not have LIMIT clause, got:\n{sql}"

    def test_limit_clause_determinism(self):
        """Test that LIMIT clauses are consistent across different seeds."""
        queries = TPCHQueries()

        # Query 10 should always have LIMIT 20, regardless of seed
        for seed in [1, 42, 999, 12345]:
            sql = queries.get_query(10, seed=seed)
            assert "LIMIT 20" in sql.upper(), f"Query 10 with seed {seed} should have LIMIT 20"

        # Query 3 should always have LIMIT 10, regardless of seed
        for seed in [1, 42, 999, 12345]:
            sql = queries.get_query(3, seed=seed)
            assert "LIMIT 10" in sql.upper(), f"Query 3 with seed {seed} should have LIMIT 10"

    def test_limit_clause_with_scale_factor(self):
        """Test that LIMIT clauses are present regardless of scale factor."""
        queries = TPCHQueries()

        # Query 10 should have LIMIT 20 at different scale factors
        for sf in [0.1, 1.0, 10.0]:
            sql = queries.get_query(10, scale_factor=sf)
            assert "LIMIT 20" in sql.upper(), f"Query 10 at SF={sf} should have LIMIT 20"

    def test_limit_clause_format(self):
        """Test that LIMIT clauses are properly formatted."""
        queries = TPCHQueries()

        sql = queries.get_query(10, seed=42)

        # Should end with "LIMIT 20;"
        assert sql.rstrip().endswith("LIMIT 20;") or sql.rstrip().endswith("limit 20;"), (
            f"Query 10 should end with LIMIT 20; Got:\n{sql[-100:]}"
        )
