"""Determinism validation tests for TPC-H ultra-simplified implementation.

Tests that qgen produces deterministic results with identical parameters.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from benchbox import TPCH
from benchbox.core.tpch.benchmark import TPCHBenchmark
from benchbox.core.tpch.queries import TPCHQueries


class TestTPCHDeterminism:
    """Test TPC-H deterministic behavior with qgen."""

    def test_same_seed_produces_identical_queries(self):
        """Test that same seed produces identical queries."""
        tpch = TPCH()
        seed = 12345

        # Generate same query multiple times with same seed
        queries = []
        for _ in range(3):
            sql = tpch.get_query(1, seed=seed)
            queries.append(sql)

        # All should be identical
        assert len(set(queries)) == 1, "Same seed should produce identical queries"

        # Verify it's actually a valid query
        assert isinstance(queries[0], str)
        assert len(queries[0]) > 50

    def test_different_seeds_produce_different_queries(self):
        """Test that different seeds produce different queries."""
        tpch = TPCH()

        # Generate same query with different seeds
        queries = []
        for seed in [111, 222, 333, 444, 555]:
            sql = tpch.get_query(1, seed=seed)
            queries.append(sql)

        # All should be different
        assert len(set(queries)) == 5, "Different seeds should produce different queries"

        # All should be valid queries
        for sql in queries:
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_determinism_across_query_manager_interfaces(self):
        """Test determinism across different query manager interfaces."""
        seed = 98765
        query_id = 5

        # through different interfaces
        tpch = TPCH()
        benchmark = TPCHBenchmark()
        queries = TPCHQueries()

        sql_tpch = tpch.get_query(query_id, seed=seed)
        sql_benchmark = benchmark.get_query(query_id, seed=seed)
        sql_queries = queries.get_query(query_id, seed=seed)

        # Normalize whitespace and case for comparison (different interfaces may format differently)
        def normalize_sql(sql):
            # Strip quotes around identifiers and normalize whitespace/case
            import re

            normalized = sql.strip()
            # Strip trailing semicolon
            normalized = normalized.rstrip(";")
            # Strip quotes around identifiers
            normalized = normalized.replace('"', "")
            # Normalize date literals: date '...' -> cast('...' as date)
            normalized = re.sub(r"date\s+'([^']+)'", r"cast('\1' as date)", normalized)
            # Normalize interval: interval '1' year -> interval '1 year'
            normalized = re.sub(r"interval\s+'(\d+)'\s+(\w+)", r"interval '\1 \2'", normalized)
            # Normalize whitespace and case
            return " ".join(normalized.split()).lower()

        # Should all be semantically identical (ignoring formatting)
        assert normalize_sql(sql_tpch) == normalize_sql(sql_benchmark) == normalize_sql(sql_queries)

    def test_determinism_with_scale_factor(self):
        """Test determinism with different scale factors."""
        seed = 55555
        query_id = 10

        # Same seed, different scale factors
        tpch = TPCH()

        # Generate multiple times with same parameters
        results = []
        for _ in range(3):
            sql = tpch.get_query(query_id, seed=seed, scale_factor=0.5)
            results.append(sql)

        # Should be identical
        assert len(set(results)) == 1

        # Different scale factors should still be deterministic
        sql_sf1 = tpch.get_query(query_id, seed=seed, scale_factor=1.0)
        sql_sf2 = tpch.get_query(query_id, seed=seed, scale_factor=2.0)

        # Each should be consistent
        assert sql_sf1 == tpch.get_query(query_id, seed=seed, scale_factor=1.0)
        assert sql_sf2 == tpch.get_query(query_id, seed=seed, scale_factor=2.0)

    def test_determinism_across_all_queries(self):
        """Test determinism across all 22 TPC-H queries."""
        seed = 77777
        tpch = TPCH()

        # Generate all queries twice with same seed
        queries_run1 = {}
        queries_run2 = {}

        for query_id in range(1, 23):
            queries_run1[query_id] = tpch.get_query(query_id, seed=seed)
            queries_run2[query_id] = tpch.get_query(query_id, seed=seed)

        # Each query should be identical across runs
        for query_id in range(1, 23):
            assert queries_run1[query_id] == queries_run2[query_id], f"Query {query_id} not deterministic"

    def test_determinism_with_batch_generation(self):
        """Test determinism with batch query generation."""
        seed = 88888
        tpch = TPCH()

        # Generate all queries in batch multiple times
        # Note: batch generation doesn't support seed parameter directly
        # but individual queries should still be deterministic

        # Test individual query determinism after batch operations
        tpch.get_queries()

        # Now test individual determinism
        for query_id in [1, 5, 10, 15, 20]:
            sql1 = tpch.get_query(query_id, seed=seed)
            sql2 = tpch.get_query(query_id, seed=seed)
            assert sql1 == sql2, f"Query {query_id} not deterministic after batch operations"

    def test_determinism_with_parameterized_queries(self):
        """Test determinism with parameterized query interface."""
        seed = 33333
        tpch = TPCH()

        # Test old-style parameterized query
        sql1 = tpch.get_query(1, params=None, seed=seed)
        sql2 = tpch.get_query(1, params=None, seed=seed)
        assert sql1 == sql2

        # Test new-style parameterized query
        sql3 = tpch.get_query(1, seed=seed, dialect="duckdb")
        sql4 = tpch.get_query(1, seed=seed, dialect="duckdb")
        assert sql3 == sql4

    def test_determinism_independence_across_instances(self):
        """Test that different instances maintain independent determinism."""
        seed = 11111
        query_id = 7

        # multiple instances
        tpch1 = TPCH()
        tpch2 = TPCH()
        tpch3 = TPCH(scale_factor=0.5)

        # Generate queries with same seed
        sql1 = tpch1.get_query(query_id, seed=seed)
        sql2 = tpch2.get_query(query_id, seed=seed)
        sql3 = tpch3.get_query(query_id, seed=seed, scale_factor=1.0)  # Override scale

        # All should be identical (same seed, same effective scale factor)
        assert sql1 == sql2 == sql3

    def test_determinism_with_concurrent_generation(self):
        """Test determinism with concurrent query generation."""
        import threading

        seed = 99999
        query_id = 12
        results = []

        def generate_query():
            tpch = TPCH()
            sql = tpch.get_query(query_id, seed=seed)
            results.append(sql)

        # Generate queries concurrently
        threads = []
        for _ in range(5):
            thread = threading.Thread(target=generate_query)
            threads.append(thread)
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # All results should be identical
        assert len(set(results)) == 1, "Concurrent generation should be deterministic"

    def test_determinism_with_error_recovery(self):
        """Test determinism after error conditions."""
        seed = 22222
        tpch = TPCH()

        # Generate valid query
        sql_before = tpch.get_query(1, seed=seed)

        # Cause an error
        try:
            tpch.get_query(25, seed=seed)  # Invalid query ID
        except ValueError:
            pass  # Expected

        # Generate same query again
        sql_after = tpch.get_query(1, seed=seed)

        # Should be identical
        assert sql_before == sql_after, "Determinism should survive error conditions"

    def test_random_seed_behavior(self):
        """Test behavior when no seed is provided."""
        tpch = TPCH()

        # Generate queries without seed multiple times
        queries = []
        for _ in range(5):
            sql = tpch.get_query(1)  # No seed
            queries.append(sql)

        # Without seed, qgen should still produce consistent results
        # (qgen uses internal default seeding)
        # We can't predict if they'll be identical, but they should be valid
        for sql in queries:
            assert isinstance(sql, str)
            assert len(sql) > 50

    def test_seed_boundary_conditions(self):
        """Test determinism with boundary seed values."""
        tpch = TPCH()
        query_id = 3

        # Test boundary seed values
        boundary_seeds = [0, 1, 2**31 - 1, 2**32 - 1]

        for seed in boundary_seeds:
            # Generate multiple times with same seed
            sql1 = tpch.get_query(query_id, seed=seed)
            sql2 = tpch.get_query(query_id, seed=seed)

            assert sql1 == sql2, f"Seed {seed} not deterministic"
            assert isinstance(sql1, str)
            assert len(sql1) > 50
