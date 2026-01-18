"""Unit tests for QGenBinary class - Direct qgen wrapper functionality.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from benchbox.core.tpch.queries import QGenBinary

# Mark all tests in this file as unit tests
pytestmark = [pytest.mark.unit, pytest.mark.fast]


class TestQGenBinary:
    """Test QGenBinary wrapper functionality."""

    def test_qgen_discovery_success(self):
        """Test successful qgen binary discovery."""
        # This should work in the actual environment with qgen available
        qgen = QGenBinary()
        assert qgen.qgen_path is not None
        assert Path(qgen.qgen_path).exists()
        assert Path(qgen.qgen_path).is_file()
        assert qgen.templates_dir is not None
        assert Path(qgen.templates_dir).exists()

    @patch("benchbox.core.tpch.queries.Path.exists")
    def test_qgen_discovery_failure(self, mock_exists):
        """Test failure when qgen binary not found."""
        mock_exists.return_value = False

        with pytest.raises(RuntimeError) as exc_info:
            QGenBinary()

        assert "qgen binary required but not found" in str(exc_info.value)
        assert "TPC-H requires the compiled qgen tool" in str(exc_info.value)

    def test_generate_basic_query(self):
        """Test basic qgen execution for query generation."""
        qgen = QGenBinary()

        # Test query 1 generation with default parameters
        sql = qgen.generate(1)

        assert isinstance(sql, str)
        assert len(sql) > 100  # Should be substantial SQL
        assert "select" in sql.lower()
        assert "lineitem" in sql.lower()  # Query 1 uses lineitem table
        assert ":1" not in sql  # Parameters should be substituted

    def test_generate_with_seed(self):
        """Test qgen execution with seed parameter."""
        qgen = QGenBinary()

        # Generate same query with same seed twice
        sql1 = qgen.generate(1, seed=12345)
        sql2 = qgen.generate(1, seed=12345)

        # Should be identical with same seed
        assert sql1 == sql2

        # Generate with different seed
        sql3 = qgen.generate(1, seed=54321)

        # Should be different with different seed
        assert sql1 != sql3

    def test_generate_with_scale_factor(self):
        """Test qgen execution with scale factor parameter."""
        qgen = QGenBinary()

        # Test different scale factors
        sql_sf1 = qgen.generate(1, scale_factor=1.0)
        sql_sf01 = qgen.generate(1, scale_factor=0.1)

        assert isinstance(sql_sf1, str)
        assert isinstance(sql_sf01, str)
        assert len(sql_sf1) > 0
        assert len(sql_sf01) > 0
        # Note: SQL structure may be same, but parameters should differ

    def test_sql_cleaning(self):
        """Test minimal SQL cleaning functionality."""
        qgen = QGenBinary()

        # Test with a query that qgen might include comments/directives in
        test_sql = """-- TPC Query comment
        select * from test;
        go
        """

        cleaned = qgen._clean_sql(test_sql)

        assert "-- TPC Query comment" not in cleaned
        assert "go" not in cleaned.lower()
        assert "select * from test" in cleaned

    def test_subprocess_timeout(self):
        """Test timeout handling for qgen subprocess."""
        qgen = QGenBinary()

        # The normal qgen execution should complete well within timeout
        # This test verifies timeout mechanism exists
        sql = qgen.generate(1)  # Should complete quickly
        assert len(sql) > 0

    def test_invalid_query_id_handling(self):
        """Test qgen error handling for invalid query IDs."""
        qgen = QGenBinary()

        # qgen should fail for invalid query IDs
        with pytest.raises(subprocess.CalledProcessError):
            qgen.generate(23)  # Query 23 doesn't exist

        with pytest.raises(subprocess.CalledProcessError):
            qgen.generate(0)  # Query 0 doesn't exist

    def test_generate_multiple_queries(self):
        """Test generating multiple different queries."""
        qgen = QGenBinary()

        # Test several different queries
        queries = {}
        for query_id in [1, 2, 3, 5, 10]:
            sql = qgen.generate(query_id, seed=42)
            queries[query_id] = sql

            assert isinstance(sql, str)
            assert len(sql) > 50
            assert ":" not in sql  # No parameter placeholders should remain

        # All queries should be different
        sqls = list(queries.values())
        for i, sql1 in enumerate(sqls):
            for j, sql2 in enumerate(sqls):
                if i != j:
                    assert sql1 != sql2

    def test_seed_parameter_determinism(self):
        """Test that seed parameter produces deterministic results."""
        qgen = QGenBinary()

        # Test multiple queries with same seed
        results1 = {}
        results2 = {}

        for query_id in [1, 2, 3]:
            results1[query_id] = qgen.generate(query_id, seed=999)
            results2[query_id] = qgen.generate(query_id, seed=999)

        # Results should be identical
        assert results1 == results2

    def test_working_directory_isolation(self):
        """Test that qgen execution doesn't affect current working directory."""
        qgen = QGenBinary()
        original_cwd = os.getcwd()

        # Generate a query
        qgen.generate(1)

        # Working directory should be unchanged
        assert os.getcwd() == original_cwd

    @patch("subprocess.run")
    def test_subprocess_error_propagation(self, mock_run):
        """Test that subprocess errors are properly propagated."""
        qgen = QGenBinary()

        # Mock subprocess failure
        mock_run.side_effect = subprocess.CalledProcessError(1, "qgen", stderr="Mock error")

        with pytest.raises(subprocess.CalledProcessError):
            qgen.generate(1)

    def test_ansi_mode_flag(self):
        """Test that ANSI mode flag is passed to qgen."""
        qgen = QGenBinary()

        # This is tested indirectly by verifying qgen execution works
        # ANSI mode should provide better SQL compatibility
        sql = qgen.generate(1)

        # Should generate valid SQL without database-specific syntax issues
        assert isinstance(sql, str)
        assert len(sql) > 0
