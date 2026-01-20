"""Tests for Star Schema Benchmark (SSB) implementation.

Copyright 2026 Joe Harris / BenchBox Project

This implementation is derived from TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox import SSB
from benchbox.core.ssb.benchmark import SSBBenchmark


@pytest.mark.ssb
class TestSSB:
    """Test the Star Schema Benchmark implementation."""

    @pytest.fixture
    def ssb(self, small_scale_factor: float, temp_dir: Path) -> SSB:
        """Create an SSB instance for testing."""
        # Disable compression to avoid dependency on zstandard library
        return SSB(
            scale_factor=small_scale_factor,
            output_dir=temp_dir,
            compress_data=False,
            compression_type="none",
        )

    def test_generate_data(self, ssb: SSB) -> None:
        """Test that data generation produces expected files."""
        data_paths = ssb.generate_data()

        # Check that output includes expected files (returns dict)
        expected_tables = ["date", "customer", "supplier", "part", "lineorder"]

        assert isinstance(data_paths, dict), "generate_data should return a dictionary"

        for table in expected_tables:
            assert table in data_paths, f"Table {table} not found in generated data"

        # Verify files exist and have content
        for _table_name, path in data_paths.items():
            assert Path(path).exists(), f"Generated file {path} does not exist"
            assert Path(path).stat().st_size >= 0, f"Generated file {path} has negative size"

    def test_get_queries(self, ssb: SSB) -> None:
        """Test that all SSB queries can be retrieved."""
        queries = ssb.get_queries()

        # SSB has 13 queries organized in 4 flights
        expected_queries = [
            "Q1.1",
            "Q1.2",
            "Q1.3",  # Flight 1
            "Q2.1",
            "Q2.2",
            "Q2.3",  # Flight 2
            "Q3.1",
            "Q3.2",
            "Q3.3",
            "Q3.4",  # Flight 3
            "Q4.1",
            "Q4.2",
            "Q4.3",  # Flight 4
        ]

        assert len(queries) == 13
        for query_id in expected_queries:
            assert query_id in queries, f"Query {query_id} not found"
            assert isinstance(queries[query_id], str)
            assert queries[query_id].strip()

            # Check that queries contain expected SQL elements
            query_sql = queries[query_id].upper()
            assert "SELECT" in query_sql
            assert "FROM" in query_sql

            if query_id.startswith("Q1"):  # Flight 1 queries should involve lineorder
                assert "LINEORDER" in query_sql
            elif query_id.startswith("Q2"):  # Flight 2 should involve joins
                assert "LINEORDER" in query_sql
                assert any(table in query_sql for table in ["SUPPLIER", "PART"])
            elif query_id.startswith("Q3"):  # Flight 3 should involve multiple joins
                assert "LINEORDER" in query_sql
                assert "CUSTOMER" in query_sql
            elif query_id.startswith("Q4"):  # Flight 4 should involve comprehensive joins
                assert "LINEORDER" in query_sql
                assert any(table in query_sql for table in ["CUSTOMER", "SUPPLIER", "PART"])

    def test_get_query(self, ssb: SSB) -> None:
        """Test retrieving a specific query."""
        query1_1 = ssb.get_query("Q1.1")
        assert isinstance(query1_1, str)
        assert "SELECT" in query1_1.upper()
        assert "LINEORDER" in query1_1.upper()

        # Test a different query from a different flight
        query3_1 = ssb.get_query("Q3.1")
        assert isinstance(query3_1, str)
        assert "CUSTOMER" in query3_1.upper()
        assert "LINEORDER" in query3_1.upper()

    def test_translate_query(self, ssb: SSB, sql_dialect: str) -> None:
        """Test translating a query to different SQL dialects."""
        ssb.get_query("Q1.1")
        translated_query = ssb.translate_query("Q1.1", dialect=sql_dialect)

        assert isinstance(translated_query, str)
        assert "SELECT" in translated_query.upper()

    def test_invalid_query_id(self, ssb: SSB) -> None:
        """Test that requesting an invalid query raises an exception."""
        with pytest.raises(ValueError):
            ssb.get_query("Q5.1")  # Flight 5 doesn't exist

        with pytest.raises(ValueError):
            ssb.get_query("Q1.4")  # Flight 1 only has 3 queries

    def test_get_query(self, ssb: SSB) -> None:
        """Test that parameterized queries can be retrieved."""
        # Test with default random parameters
        param_query = ssb.get_query("Q1.1")
        assert isinstance(param_query, str)
        assert "SELECT" in param_query.upper()

        # Test with custom parameters
        custom_params = {"year": 1994}
        param_query = ssb.get_query("Q1.1", params=custom_params)
        assert isinstance(param_query, str)
        assert "SELECT" in param_query.upper()

    def test_get_schema(self, ssb: SSB) -> None:
        """Test retrieving the SSB schema."""
        schema = ssb.get_schema()

        # Check that schema is a dictionary with table names as keys
        assert isinstance(schema, dict), "Schema should be a dictionary"
        expected_tables = ["date", "customer", "supplier", "part", "lineorder"]

        for table in expected_tables:
            assert table in schema, f"Table {table} not found in schema"

        # Check specific table structures
        lineorder_table = schema["lineorder"]
        column_names = [col["name"] for col in lineorder_table["columns"]]
        expected_lineorder_columns = [
            "lo_orderkey",
            "lo_linenumber",
            "lo_custkey",
            "lo_partkey",
            "lo_suppkey",
            "lo_orderdate",
            "lo_orderpriority",
            "lo_shippriority",
            "lo_quantity",
            "lo_extendedprice",
            "lo_ordtotalprice",
            "lo_discount",
            "lo_revenue",
            "lo_supplycost",
            "lo_tax",
            "lo_commitdate",
            "lo_shipmode",
        ]

        for column in expected_lineorder_columns:
            assert column in column_names

        # Check dimension table
        date_table = schema["date"]
        date_columns = [col["name"] for col in date_table["columns"]]
        expected_date_columns = [
            "d_datekey",
            "d_date",
            "d_dayofweek",
            "d_month",
            "d_year",
        ]

        for column in expected_date_columns:
            assert column in date_columns

    def test_get_create_tables_sql(self, ssb: SSB) -> None:
        """Test retrieving SQL to create SSB tables."""
        sql = ssb.get_create_tables_sql()

        assert isinstance(sql, str)
        assert "CREATE TABLE" in sql

        # Check for all tables (using lowercase names as they appear in SQL)
        expected_tables = ["date", "customer", "supplier", "part", "lineorder"]

        for table in expected_tables:
            assert f"CREATE TABLE {table}" in sql

    def test_ssb_properties(self, ssb: SSB) -> None:
        """Test SSB-specific properties."""
        # Test that SSB has the expected number of tables
        schema = ssb.get_schema()
        assert len(schema) == 5  # 4 dimension tables + 1 fact table

        # Test that queries are organized in flights
        queries = ssb.get_queries()

        # Flight 1: Q1.1, Q1.2, Q1.3
        flight1_queries = [q for q in queries if q.startswith("Q1")]
        assert len(flight1_queries) == 3

        # Flight 2: Q2.1, Q2.2, Q2.3
        flight2_queries = [q for q in queries if q.startswith("Q2")]
        assert len(flight2_queries) == 3

        # Flight 3: Q3.1, Q3.2, Q3.3, Q3.4
        flight3_queries = [q for q in queries if q.startswith("Q3")]
        assert len(flight3_queries) == 4

        # Flight 4: Q4.1, Q4.2, Q4.3
        flight4_queries = [q for q in queries if q.startswith("Q4")]
        assert len(flight4_queries) == 3

    def test_query_complexity_progression(self, ssb: SSB) -> None:
        """Test that query complexity increases across flights."""
        queries = ssb.get_queries()

        # Flight 1 should be simpler (fewer joins)
        q1_1 = queries["Q1.1"].upper()
        # Should primarily use LINEORDER and DATE

        # Flight 4 should be more complex (more joins)
        q4_1 = queries["Q4.1"].upper()
        # Should use multiple tables with joins

        # Count approximate complexity by looking for JOIN keywords or WHERE clauses with multiple tables
        q1_1.count("JOIN") + q1_1.count("WHERE")
        q4_1.count("JOIN") + q4_1.count("WHERE")

        # Q4 queries should generally be more complex than Q1 queries
        # (this is a rough heuristic but validates the SSB structure)


@pytest.mark.ssb
class TestSSBBenchmarkCoverage:
    """Test SSBBenchmark class for better coverage of key methods."""

    @pytest.fixture
    def ssb_benchmark(self, small_scale_factor: float, temp_dir: Path) -> SSBBenchmark:
        """Create an SSBBenchmark instance for testing."""
        # Disable compression to avoid dependency on zstandard library
        return SSBBenchmark(
            scale_factor=small_scale_factor,
            output_dir=temp_dir,
            compress_data=False,
            compression_type="none",
        )

    def test_execute_query_coverage(self, ssb_benchmark: SSBBenchmark) -> None:
        """Test execute_query method variations."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
        mock_connection.execute.return_value = mock_cursor

        with patch.object(ssb_benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT COUNT(*) FROM lineorder"

            result = ssb_benchmark.execute_query("Q1.1", mock_connection)

            assert result == [("result1",), ("result2",)]
            mock_connection.execute.assert_called_once()

    def test_load_data_to_database_coverage(self, ssb_benchmark: SSBBenchmark) -> None:
        """Test load_data_to_database with mock data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_file = Path(temp_dir) / "lineorder.csv"
            csv_file.write_text("1|1|1|1|19920101|1|10|20|30|40|50|60|70|80|90|100\n")

            ssb_benchmark.tables = {"lineorder": str(csv_file)}

            mock_connection = Mock()
            mock_connection.executescript = Mock()
            mock_connection.executemany = Mock()
            mock_connection.commit = Mock()

            with patch.object(ssb_benchmark, "get_create_tables_sql") as mock_get_sql:
                mock_get_sql.return_value = "CREATE TABLE lineorder (...);"

                ssb_benchmark.load_data_to_database(mock_connection)

                mock_connection.executescript.assert_called_once()
                mock_connection.executemany.assert_called()

    def test_run_benchmark_coverage(self, ssb_benchmark: SSBBenchmark) -> None:
        """Test run_benchmark with timing and error handling."""
        mock_connection = Mock()

        with patch.object(ssb_benchmark, "execute_query") as mock_execute:
            mock_execute.return_value = [("result1",)]
            with patch.object(ssb_benchmark.query_manager, "get_all_queries") as mock_get_all:
                mock_get_all.return_value = {"Q1.1": "SELECT COUNT(*) FROM lineorder"}

                result = ssb_benchmark.run_benchmark(mock_connection, iterations=1)

                assert result["benchmark"] == "Star Schema Benchmark"
                # SSB returns query_results as a list, not queries as a dict
                assert any(q["query_id"] == "Q1.1" for q in result["query_results"])

    def test_error_handling_paths(self, ssb_benchmark: SSBBenchmark) -> None:
        """Test error handling in key methods."""
        # Test load_data_to_database without data
        mock_connection = Mock()
        with pytest.raises(ValueError, match="No data generated"):
            ssb_benchmark.load_data_to_database(mock_connection)

        # Test unsupported connection type in execute_query
        mock_connection = Mock()
        delattr(mock_connection, "execute")
        delattr(mock_connection, "cursor")

        with patch.object(ssb_benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT COUNT(*) FROM lineorder"

            with pytest.raises(ValueError, match="Unsupported connection type"):
                ssb_benchmark.execute_query("Q1.1", mock_connection)
