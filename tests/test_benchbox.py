"""Tests for top-level BenchBox functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import unittest
from unittest import mock

import pytest

from benchbox import TPCH


@pytest.mark.unit
@pytest.mark.fast
class TestBenchBox(unittest.TestCase):
    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Initialize a TPC-H benchmark at scale factor 1 for testing
        with mock.patch("benchbox.core.tpch.generator.TPCHDataGenerator._find_or_build_dbgen") as mock_build:
            mock_build.return_value = "/tmp/dbgen"
            self.tpch = TPCH(scale_factor=0.01)

    @mock.patch("benchbox.core.tpch.benchmark.TPCHBenchmark.generate_data")
    def test_generate_data(self, mock_generate_data) -> None:
        """Test if data generation returns valid paths."""
        # Setup mock
        mock_paths = ["/tmp/data/customer.tbl", "/tmp/data/lineitem.tbl"]
        mock_generate_data.return_value = mock_paths

        # Call the method
        data_paths = self.tpch.generate_data()

        # Verify results
        self.assertIsInstance(data_paths, list)
        self.assertTrue(all(isinstance(path, str) for path in data_paths))
        mock_generate_data.assert_called_once()

    def test_get_queries(self) -> None:
        """Test if all benchmark queries are returned as a dictionary."""
        queries = self.tpch.get_queries()
        self.assertIsInstance(queries, dict)
        self.assertGreater(len(queries), 0)

    def test_get_query(self) -> None:
        """Test if a specific query is returned correctly."""
        query1 = self.tpch.get_query(1)
        self.assertIsInstance(query1, str)
        self.assertIn("SELECT", query1.upper())

    @mock.patch("sqlglot.transpile")
    @mock.patch("benchbox.utils.dialect_utils.normalize_dialect_for_sqlglot")
    def test_translate_query(self, mock_normalize, mock_transpile) -> None:
        """Test if a query is translated to a specific SQL dialect."""
        # Setup mock
        mock_query = "SELECT * FROM customer WHERE c_custkey = 1"
        mock_transpile.return_value = ["SELECT * FROM customer WHERE c_custkey = 1"]
        mock_normalize.return_value = "postgres"

        # Mock the get_query method to avoid actual query retrieval
        with mock.patch.object(self.tpch, "get_query", return_value=mock_query):
            translated_query = self.tpch.translate_query(1, dialect="postgres")

            # Verify results
            self.assertIsInstance(translated_query, str)
            self.assertIn("SELECT", translated_query.upper())
            mock_normalize.assert_called_once_with("postgres")
            mock_transpile.assert_called_once_with(mock_query, read="postgres", write="postgres", identify=True)


if __name__ == "__main__":
    unittest.main()
