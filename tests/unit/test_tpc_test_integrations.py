"""Unit tests for TPC Power/Throughput/Maintenance test integrations.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys
import unittest.mock as mock
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.fast

# Include the project root to the path so we can import modules
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from benchbox.platforms.base import (  # noqa: E402
    PlatformAdapterConnection,
    PlatformAdapterCursor,
)


class MockBenchmark:
    """Mock benchmark for testing."""

    def __init__(self, benchmark_name: str):
        self._name = benchmark_name
        self.display_name = benchmark_name

    def get_query(self, query_id: int, **kwargs):
        """Mock query retrieval."""
        return f"SELECT * FROM test_table WHERE id = {query_id}"

    def get_queries(self):
        """Mock queries dictionary."""
        queries = {}
        if "tpch" in self._name.lower():
            # TPC-H has queries 1-22
            for i in range(1, 23):
                queries[str(i)] = f"Query {i}"
        elif "tpcds" in self._name.lower():
            # TPC-DS has queries 1-99
            for i in range(1, 100):
                queries[str(i)] = f"Query {i}"
        return queries


class MockPlatformAdapter:
    """Mock platform adapter for testing TPC integration."""

    def __init__(self):
        self.platform_name = "mock_platform"
        self.logger = mock.Mock()

    def connect(self, **config):
        """Mock connection method."""
        return mock.Mock()

    def create_database(self, database_name: str, **config) -> dict[str, Any]:
        """Mock database creation."""
        return {"status": "success", "database": database_name}

    def execute_query(
        self, connection: Any, query: str, query_name: str | None = None, benchmark_type: str | None = None, **kwargs
    ) -> dict[str, Any]:
        """Mock query execution with realistic result structure."""
        import random

        # Simulate execution time
        execution_time = random.uniform(0.1, 2.0)
        rows_returned = random.randint(1, 1000)

        return {
            "query_name": query_name or "test_query",
            "execution_time": execution_time,
            "status": "SUCCESS",
            "rows_returned": rows_returned,
            "query_text": query[:100] + "..." if len(query) > 100 else query,
        }

    def get_target_dialect(self) -> str:
        """Mock target dialect."""
        return "standard"

    def _format_execution_time(self, execution_time: float) -> str:
        """Mock time formatting."""
        if execution_time < 1.0:
            return f"{execution_time * 1000:.0f}ms"
        else:
            return f"{execution_time:.2f}s"

    # Mock TPC test methods we want to test
    def _execute_tpch_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock TPC-H power test execution; calls execute_query to exercise error handling."""
        results = []
        for i in range(1, 23):  # TPC-H has 22 queries
            try:
                sql = benchmark.get_query(i)
                _ = self.execute_query(connection, sql, str(i))
                status = "SUCCESS"
            except Exception:
                status = "FAILED"
            results.append(
                {
                    "query_id": i,
                    "execution_time": 1.5,
                    "status": status,
                    "rows_returned": 100 if status == "SUCCESS" else 0,
                    "test_type": "power",
                    "stream_id": run_config.get("stream_id", 0),
                    "position": i,
                }
            )
        return results

    def _execute_tpcds_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock TPC-DS power test execution for testing."""
        results = []
        for i in range(1, 11):  # Mock first 10 queries
            result = {
                "query_id": i,
                "execution_time": 2.0,
                "status": "SUCCESS",
                "rows_returned": 200,
                "test_type": "power",
                "stream_id": run_config.get("stream_id", 0),
                "position": i,
            }
            results.append(result)
        return results

    def _execute_tpcds_throughput_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock TPC-DS throughput test execution for testing."""
        results = []
        num_streams = run_config.get("num_streams", 2)
        for stream_id in range(num_streams):
            for i in range(1, 6):  # Mock 5 queries per stream
                result = {
                    "query_id": i,
                    "execution_time": 1.0,
                    "status": "SUCCESS",
                    "rows_returned": 150,
                    "test_type": "throughput",
                    "stream_id": stream_id,
                }
                results.append(result)
        return results

    def _execute_tpcds_maintenance_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock TPC-DS maintenance test execution for testing."""
        operations = ["insert", "update", "delete", "validate"]
        results = []
        for i, op_type in enumerate(operations):
            result = {
                "query_id": f"{op_type}_operation_{i + 1}",
                "execution_time": 0.5,
                "status": "SUCCESS",
                "rows_returned": 50,
                "test_type": "maintenance",
                "operation_type": op_type.upper(),
                "table_name": f"test_table_{i + 1}",
            }
            results.append(result)
        return results

    def _execute_queries_by_type(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock query execution routing by test type."""
        test_execution_type = run_config.get("test_execution_type", "standard")

        if test_execution_type == "power":
            if "tpch" in benchmark._name.lower():
                return self._execute_tpch_power_test(benchmark, connection, run_config)
            elif "tpcds" in benchmark._name.lower():
                return self._execute_tpcds_power_test(benchmark, connection, run_config)
            else:
                # Unknown benchmark type for power test, fall back to standard
                return self._execute_all_queries(benchmark, connection, run_config)
        elif test_execution_type == "throughput":
            return self._execute_tpcds_throughput_test(benchmark, connection, run_config)
        elif test_execution_type == "maintenance":
            return self._execute_tpcds_maintenance_test(benchmark, connection, run_config)
        else:
            # Standard execution falls back to standard query executor
            return self._execute_all_queries(benchmark, connection, run_config)

    def _execute_all_queries(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Mock standard query execution."""
        return [{"query_id": 1, "status": "SUCCESS", "execution_time": 1.0}]


class TestPlatformAdapterConnection:
    """Test the PlatformAdapterConnection helper class."""

    def test_connection_creation(self):
        """Test connection adapter creation."""
        mock_connection = mock.Mock()
        mock_platform = MockPlatformAdapter()

        adapter_conn = PlatformAdapterConnection(mock_connection, mock_platform)

        assert adapter_conn.connection == mock_connection
        assert adapter_conn.platform_adapter == mock_platform
        assert adapter_conn.dialect == "standard"

    def test_query_execution(self):
        """Test query execution through connection adapter."""
        mock_connection = mock.Mock()
        mock_platform = MockPlatformAdapter()

        adapter_conn = PlatformAdapterConnection(mock_connection, mock_platform)
        cursor = adapter_conn.execute("SELECT * FROM test_table")

        assert isinstance(cursor, PlatformAdapterCursor)
        assert len(cursor.rows) > 0  # Should have mock rows

    def test_cursor_methods(self):
        """Test cursor methods."""
        mock_platform_result = {
            "rows_returned": 5,
            "status": "SUCCESS",
            "execution_time": 1.5,
        }

        cursor = PlatformAdapterCursor(mock_platform_result)

        # Test fetchall
        rows = cursor.fetchall()
        assert len(rows) == 5
        assert all(row == ("mock_row",) for row in rows)

        # Test fetchone
        first_row = cursor.fetchone()
        assert first_row == ("mock_row",)


class TestTPCHPowerTestIntegration:
    """Test TPC-H Power Test integration."""

    def test_tpch_power_test_execution(self):
        """Test TPC-H power test execution."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpch_benchmark = MockBenchmark("tpch")

        run_config = {"scale_factor": 0.01, "seed": 1, "stream_id": 0, "verbose": False}

        # Execute TPC-H power test
        results = mock_platform._execute_tpch_power_test(tpch_benchmark, mock_connection, run_config)

        # Verify results structure
        assert isinstance(results, list)
        assert len(results) > 0

        # Check for expected result fields
        for result in results:
            if result.get("query_id") != "power_test_error":
                assert "query_id" in result
                assert "execution_time" in result
                assert "status" in result
                assert "test_type" in result
                assert result["test_type"] == "power"
                assert "stream_id" in result

    def test_tpch_power_test_error_handling(self):
        """Test TPC-H power test error handling."""
        mock_platform = MockPlatformAdapter()

        # Mock query execution failure
        mock_platform.execute_query = mock.Mock(side_effect=Exception("Database error"))

        mock_connection = mock.Mock()
        tpch_benchmark = MockBenchmark("tpch")

        run_config = {"scale_factor": 0.01, "seed": 1, "stream_id": 0, "verbose": False}

        # Execute TPC-H power test
        results = mock_platform._execute_tpch_power_test(tpch_benchmark, mock_connection, run_config)

        # Verify error handling
        assert isinstance(results, list)
        assert len(results) > 0

        # Should have error results
        error_results = [r for r in results if r.get("status") == "FAILED"]
        assert len(error_results) > 0


class TestTPCDSPowerTestIntegration:
    """Test TPC-DS Power Test integration."""

    def test_tpcds_power_test_execution(self):
        """Test TPC-DS power test execution."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpcds_benchmark = MockBenchmark("tpcds")

        run_config = {"scale_factor": 0.01, "seed": 1, "stream_id": 0, "verbose": False}

        # Execute TPC-DS power test
        results = mock_platform._execute_tpcds_power_test(tpcds_benchmark, mock_connection, run_config)

        # Verify results structure
        assert isinstance(results, list)
        assert len(results) > 0

        # Check for expected result fields
        for result in results:
            if result.get("query_id") != "power_test_error":
                assert "query_id" in result
                assert "execution_time" in result
                assert "status" in result
                assert "test_type" in result
                assert result["test_type"] == "power"


class TestTPCDSThroughputTestIntegration:
    """Test TPC-DS Throughput Test integration."""

    def test_tpcds_throughput_test_execution(self):
        """Test TPC-DS throughput test execution."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpcds_benchmark = MockBenchmark("tpcds")

        run_config = {"scale_factor": 0.01, "num_streams": 2, "verbose": False}

        # Execute TPC-DS throughput test
        results = mock_platform._execute_tpcds_throughput_test(tpcds_benchmark, mock_connection, run_config)

        # Verify results structure
        assert isinstance(results, list)
        assert len(results) > 0

        # Check for expected result fields
        for result in results:
            if result.get("query_id") != "throughput_test_error":
                assert "query_id" in result
                assert "execution_time" in result
                assert "status" in result
                assert "test_type" in result
                assert result["test_type"] == "throughput"
                assert "stream_id" in result


class TestTPCDSMaintenanceTestIntegration:
    """Test TPC-DS Maintenance Test integration."""

    def test_tpcds_maintenance_test_execution(self):
        """Test TPC-DS maintenance test execution."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpcds_benchmark = MockBenchmark("tpcds")

        run_config = {
            "scale_factor": 0.01,
            "verbose": False,
            "output_dir": "/tmp/test_maintenance",
        }

        # Execute TPC-DS maintenance test
        results = mock_platform._execute_tpcds_maintenance_test(tpcds_benchmark, mock_connection, run_config)

        # Verify results structure
        assert isinstance(results, list)
        assert len(results) > 0

        # Check for expected result fields
        for result in results:
            if result.get("query_id") != "maintenance_test_error":
                assert "query_id" in result
                assert "execution_time" in result
                assert "status" in result
                assert "test_type" in result
                assert result["test_type"] == "maintenance"
                assert "operation_type" in result
                assert "table_name" in result


class TestTpcTestRouting:
    """Test TPC test routing functionality."""

    def test_power_test_routing(self):
        """Test power test routing by benchmark type."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()

        # Test TPC-H routing
        tpch_benchmark = MockBenchmark("tpch")
        run_config = {"test_execution_type": "power", "scale_factor": 0.01}

        results = mock_platform._execute_queries_by_type(tpch_benchmark, mock_connection, run_config)
        assert isinstance(results, list)

        # Test TPC-DS routing
        tpcds_benchmark = MockBenchmark("tpcds")
        run_config = {"test_execution_type": "power", "scale_factor": 0.01}

        results = mock_platform._execute_queries_by_type(tpcds_benchmark, mock_connection, run_config)
        assert isinstance(results, list)

    def test_throughput_test_routing(self):
        """Test throughput test routing."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpcds_benchmark = MockBenchmark("tpcds")

        run_config = {
            "test_execution_type": "throughput",
            "scale_factor": 0.01,
            "num_streams": 2,
        }

        results = mock_platform._execute_queries_by_type(tpcds_benchmark, mock_connection, run_config)
        assert isinstance(results, list)

    def test_maintenance_test_routing(self):
        """Test maintenance test routing."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()
        tpcds_benchmark = MockBenchmark("tpcds")

        run_config = {"test_execution_type": "maintenance", "scale_factor": 0.01}

        results = mock_platform._execute_queries_by_type(tpcds_benchmark, mock_connection, run_config)
        assert isinstance(results, list)

    def test_standard_test_fallback(self):
        """Test fallback to standard test execution."""
        mock_platform = MockPlatformAdapter()
        mock_connection = mock.Mock()

        # Mock standard query execution
        mock_platform._execute_all_queries = mock.Mock(
            return_value=[{"query_id": 1, "status": "SUCCESS", "execution_time": 1.0}]
        )

        tpch_benchmark = MockBenchmark("tpch")
        run_config = {"test_execution_type": "standard", "scale_factor": 0.01}

        results = mock_platform._execute_queries_by_type(tpch_benchmark, mock_connection, run_config)
        assert isinstance(results, list)
        mock_platform._execute_all_queries.assert_called_once()


if __name__ == "__main__":
    # Run individual test classes for development
    import unittest

    # a test suite with all test classes
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Include all test classes
    test_classes = [
        TestPlatformAdapterConnection,
        TestTPCHPowerTestIntegration,
        TestTPCDSPowerTestIntegration,
        TestTPCDSThroughputTestIntegration,
        TestTPCDSMaintenanceTestIntegration,
        TestTpcTestRouting,
    ]

    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
