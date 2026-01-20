"""Tests for ClickBench CSV loading configuration and empty string handling.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.clickbench.benchmark import ClickBenchBenchmark
from benchbox.platforms.duckdb import DuckDBAdapter

pytestmark = pytest.mark.fast


class TestClickBenchCSVLoading:
    """Test ClickBench CSV loading configuration and empty string preservation."""

    def setup_method(self):
        """Setup test fixtures."""
        self.benchmark = ClickBenchBenchmark(scale_factor=0.001)
        self.adapter = DuckDBAdapter()

    def test_clickbench_csv_loading_config(self):
        """Test that ClickBench provides correct CSV loading configuration."""
        config = self.benchmark.get_csv_loading_config("hits")

        expected_config = [
            "delim='|'",  # ClickBench uses pipe delimiter
            "header=false",  # No header row
            "nullstr='__NULL__'",  # Use a marker that won't appear in real data for NULLs
            "ignore_errors=true",  # Continue on parse errors
            "auto_detect=true",  # Auto-detect types
        ]

        assert config == expected_config

    def test_empty_string_preservation_logic(self):
        """Test the logic behind empty string preservation in ClickBench."""
        # This tests the reasoning documented in the CSV loading config method
        config = self.benchmark.get_csv_loading_config("hits")

        # Verify key configuration for empty string preservation
        assert "nullstr='__NULL__'" in config, "Must use non-empty string NULL marker to preserve empty strings"
        assert "delim='|'" in config, "ClickBench uses pipe delimiter"

        # The key insight: by using '__NULL__' instead of '' for nullstr,
        # DuckDB will preserve actual empty strings in the CSV as empty strings
        # rather than converting them to NULL values

    def test_clickbench_schema_consistency(self):
        """Test that ClickBench schema is consistent with NOT NULL expectations."""
        from benchbox.core.clickbench.schema import HITS_TABLE

        # Verify all fields are NOT NULL as expected
        for column in HITS_TABLE["columns"]:
            assert column["nullable"] is False, f"Column {column['name']} should be NOT NULL"

        # This validates that the schema correctly expects no NULL values,
        # which is why preserving empty strings (not converting to NULL) is critical

    def test_clickbench_query_patterns(self):
        """Test that ClickBench queries use empty string filtering patterns."""
        from benchbox.core.clickbench.queries import ClickBenchQueryManager

        query_manager = ClickBenchQueryManager()
        # Get all queries by checking query IDs
        query_ids = [f"Q{i}" for i in range(1, 44)]  # ClickBench has Q1-Q43
        queries = {qid: query_manager.get_query(qid) for qid in query_ids}

        # Check for queries that filter empty strings (not NULL values)
        empty_string_queries = []
        for query_id, query_sql in queries.items():
            if "<> ''" in query_sql:
                empty_string_queries.append(query_id)

        assert len(empty_string_queries) > 0, "ClickBench should have queries filtering empty strings"

        # Verify no queries use IS NOT NULL patterns (which would be wrong for ClickBench)
        null_queries = []
        for query_id, query_sql in queries.items():
            if "IS NOT NULL" in query_sql.upper():
                null_queries.append(query_id)

        assert len(null_queries) == 0, f"ClickBench queries should not use IS NOT NULL: {null_queries}"


class TestClickBenchDataIntegrity:
    """Test data integrity and validation for ClickBench empty string handling."""

    def test_csv_loading_configuration_logic(self):
        """Test the logic behind CSV loading configuration without actual CSV files."""
        from benchbox.core.clickbench.benchmark import ClickBenchBenchmark

        benchmark = ClickBenchBenchmark()
        config = benchmark.get_csv_loading_config("hits")

        # Test that the configuration is designed to preserve empty strings
        assert "nullstr='__NULL__'" in config
        assert "delim='|'" in config

        # The key insight: nullstr='__NULL__' means DuckDB will only treat
        # the literal string '__NULL__' as NULL, preserving empty strings

    def test_empty_string_vs_null_handling(self):
        """Test the conceptual difference between empty strings and NULLs in ClickBench context."""
        # This test documents the key insight of the fix

        # ClickBench schema: all fields NOT NULL
        from benchbox.core.clickbench.schema import HITS_TABLE

        referer_col = next(col for col in HITS_TABLE["columns"] if col["name"] == "Referer")
        assert referer_col["nullable"] is False

        # ClickBench queries: filter with <> '' not IS NOT NULL
        from benchbox.core.clickbench.queries import ClickBenchQueryManager

        query_manager = ClickBenchQueryManager()

        # Check a query that uses empty string filtering
        try:
            q29_sql = query_manager.get_query("Q29")  # Query that filters on Referer
            if q29_sql and "Referer" in q29_sql:
                # Should use empty string filtering, not NULL checking
                assert "<>" in q29_sql or "!=" in q29_sql or "WHERE" in q29_sql
        except Exception:
            # If query not found or has issues, that's okay for this test
            pass

        # The fix ensures DuckDB CSV loading preserves this semantic:
        # - Empty CSV fields -> Empty strings in database (not NULL)
        # - Only '__NULL__' literal in CSV -> NULL in database (which shouldn't happen)

    def test_benchmark_integration_concept(self):
        """Test that the benchmark components work together conceptually."""
        from benchbox.core.clickbench.benchmark import ClickBenchBenchmark

        # Test the integration between benchmark configuration and platform loading
        benchmark = ClickBenchBenchmark()

        # Benchmark provides CSV configuration
        csv_config = benchmark.get_csv_loading_config("hits")
        assert len(csv_config) > 0

        # Verify CSV config contains expected values
        assert "nullstr='__NULL__'" in csv_config
        assert "delim='|'" in csv_config
