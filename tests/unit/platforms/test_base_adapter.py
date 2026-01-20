"""Tests for base platform adapter functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

import pytest

from benchbox.core.config import QueryResult
from benchbox.platforms.base import (
    BenchmarkResults,
    ConnectionConfig,
    PlatformAdapter,
)

pytestmark = pytest.mark.fast


class MockPlatformAdapter(PlatformAdapter):
    """Mock platform adapter for testing."""

    def add_cli_arguments(self):
        pass

    @classmethod
    def from_config(cls, config: dict[str, Any]):
        return cls(**config)

    def get_target_dialect(self) -> str | None:
        return None

    def get_platform_info(self, connection=None):
        """Mock platform info implementation."""
        return {
            "platform_type": "mock",
            "platform_name": "Mock Platform",
            "platform_version": "1.0.0",
            "connection_mode": "test",
            "host": None,
            "port": None,
            "configuration": {},
            "client_library_version": "1.0.0",
            "embedded_library_version": None,
        }

    def create_connection(self, **connection_config):
        return Mock()

    def create_schema(self, benchmark, connection):
        return 0.1

    def load_data(self, benchmark, connection, data_dir):
        return {"table1": 100}, 0.5, None

    def configure_for_benchmark(self, connection, benchmark_type):
        pass

    def execute_query(self, connection, query, query_id):
        return {
            "query_id": query_id,
            "status": "SUCCESS",
            "execution_time": 0.1,
            "rows_returned": 10,
        }

    def apply_table_tunings(self, table_tuning, connection):
        """Mock table tuning implementation."""

    def generate_tuning_clause(self, table_tuning):
        """Mock tuning clause generation."""
        return ""

    def apply_unified_tuning(self, unified_config, connection):
        """Mock unified tuning implementation."""

    def apply_platform_optimizations(self, platform_config, connection):
        """Mock platform optimizations implementation."""

    def apply_constraint_configuration(self, primary_key_config, foreign_key_config, connection):
        """Mock constraint configuration implementation."""


class MockPlatformAdapterWithDialect(MockPlatformAdapter):
    """Mock platform adapter with dialect support for testing."""

    def get_target_dialect(self) -> str:
        return "mock_dialect"

    def get_platform_info(self, connection=None):
        """Mock platform info implementation with dialect support."""
        return {
            "platform_type": "mock_dialect",
            "platform_name": "Mock Platform with Dialect",
            "platform_version": "1.0.0",
            "connection_mode": "test",
            "host": None,
            "port": None,
            "configuration": {"dialect": "mock_dialect"},
            "client_library_version": "1.0.0",
            "embedded_library_version": None,
        }

    def run_power_test(self, benchmark, **kwargs):
        """Mock power test implementation."""
        return {
            "test_type": "power",
            "total_execution_time": 5.0,
            "query_count": 22,
            "successful_queries": 22,
            "failed_queries": 0,
            "query_results": [
                {
                    "query_id": f"q{i}",
                    "status": "SUCCESS",
                    "execution_time": 0.2,
                    "rows_returned": 100,
                }
                for i in range(1, 23)
            ],
            "geometric_mean": 0.2,
            "validation_status": "PASSED",
        }

    def run_throughput_test(self, benchmark, **kwargs):
        """Mock throughput test implementation."""
        stream_count = kwargs.get("stream_count", 2)
        return {
            "test_type": "throughput",
            "stream_count": stream_count,
            "total_execution_time": 10.0,
            "aggregate_query_time": 18.0,
            "queries_per_hour": 7920,
            "stream_results": [
                {
                    "stream_id": i,
                    "execution_time": 9.0,
                    "query_count": 22,
                    "successful_queries": 22,
                    "failed_queries": 0,
                }
                for i in range(stream_count)
            ],
            "validation_status": "PASSED",
        }

    def run_maintenance_test(self, benchmark, **kwargs):
        """Mock maintenance test implementation."""
        return {
            "test_type": "maintenance",
            "operations_executed": 4,
            "total_execution_time": 2.5,
            "operation_results": [
                {
                    "operation": "INSERT",
                    "status": "SUCCESS",
                    "execution_time": 0.5,
                    "rows_affected": 1000,
                },
                {
                    "operation": "UPDATE",
                    "status": "SUCCESS",
                    "execution_time": 0.8,
                    "rows_affected": 500,
                },
                {
                    "operation": "DELETE",
                    "status": "SUCCESS",
                    "execution_time": 0.7,
                    "rows_affected": 200,
                },
                {
                    "operation": "REFRESH",
                    "status": "SUCCESS",
                    "execution_time": 0.5,
                    "rows_affected": 0,
                },
            ],
            "concurrent_query_impact": 0.1,
            "data_integrity_status": "PASSED",
            "validation_status": "PASSED",
        }

    def apply_table_tunings(self, table_tuning, connection):
        """Mock table tuning implementation."""

    def generate_tuning_clause(self, table_tuning):
        """Mock tuning clause generation."""
        return ""

    def apply_unified_tuning(self, unified_config, connection):
        """Mock unified tuning implementation."""

    def apply_platform_optimizations(self, platform_config, connection):
        """Mock platform optimizations implementation."""

    def apply_constraint_configuration(self, primary_key_config, foreign_key_config, connection):
        """Mock constraint configuration implementation."""


class TestConnectionConfig:
    """Test ConnectionConfig functionality."""

    def test_default_values(self):
        """Test default connection configuration values."""
        config = ConnectionConfig()
        assert config.host is None
        assert config.port is None
        assert config.auth_method == "password"
        assert config.ssl_enabled is False
        assert config.pool_size == 5
        assert config.max_overflow == 10
        assert config.pool_timeout == 30

    def test_env_value_resolution(self):
        """Test environment variable resolution."""
        config = ConnectionConfig(password="${TEST_PASSWORD}")

        with patch.dict("os.environ", {"TEST_PASSWORD": "secret123"}):
            assert config.get_env_value("password") == "secret123"

    def test_env_value_no_expansion(self):
        """Test that regular values are not expanded."""
        config = ConnectionConfig(password="regular_password")
        assert config.get_env_value("password") == "regular_password"

    def test_env_value_missing_variable(self):
        """Test handling of missing environment variables."""
        config = ConnectionConfig(password="${MISSING_VAR}")
        assert config.get_env_value("password", "default") == "default"


class TestPlatformAdapter:
    """Test base PlatformAdapter functionality."""

    def test_initialization(self):
        """Test adapter initialization."""
        adapter = MockPlatformAdapter()
        adapter.config["test_param"] = "value"
        assert adapter.config["test_param"] == "value"
        assert adapter.connection is None
        assert adapter.connection_pool is None
        assert adapter.dialect is None

    def test_sql_translation_no_dialect(self):
        """Test SQL translation when no dialect is set."""
        adapter = MockPlatformAdapter()
        sql = "SELECT * FROM table"
        assert adapter.translate_sql(sql) == sql

    @patch("sqlglot.transpile")
    def test_sql_translation_with_dialect(self, mock_transpile):
        """Test SQL translation with dialect set."""
        adapter = MockPlatformAdapter()
        adapter._dialect = "postgresql"

        mock_transpile.return_value = ['SELECT * FROM "table"']

        result = adapter.translate_sql("SELECT * FROM table", "duckdb")
        mock_transpile.assert_called_once_with("SELECT * FROM table", read="duckdb", write="postgresql")
        # translate_sql adds semicolon at the end
        assert result == 'SELECT * FROM "table";'

    @patch("sqlglot.transpile")
    def test_sql_translation_error_handling(self, mock_transpile):
        """Test SQL translation error handling."""
        adapter = MockPlatformAdapter()
        adapter._dialect = "postgresql"

        mock_transpile.side_effect = Exception("Translation error")

        sql = "SELECT * FROM table"
        result = adapter.translate_sql(sql, "duckdb")
        assert result == sql  # Should return original SQL on error

    def test_connection_test_success(self):
        """Test successful connection test."""
        adapter = MockPlatformAdapter()
        assert adapter.test_connection() is True

    def test_connection_test_failure(self):
        """Test failed connection test."""
        adapter = MockPlatformAdapter()

        # Mock create_connection to raise an exception
        adapter.create_connection = Mock(side_effect=Exception("Connection failed"))

        assert adapter.test_connection() is False

    def test_get_connection_from_pool_no_pool(self):
        """Test getting connection when no pool exists."""
        adapter = MockPlatformAdapter()
        adapter.config["test_param"] = "value"
        connection = adapter.get_connection_from_pool()
        assert connection is not None

    def test_get_connection_from_pool_with_pool(self):
        """Test getting connection from pool."""
        adapter = MockPlatformAdapter()
        mock_pool = Mock()
        mock_connection = Mock()
        mock_pool.get_connection.return_value = mock_connection
        adapter.connection_pool = mock_pool

        connection = adapter.get_connection_from_pool()
        assert connection == mock_connection
        mock_pool.get_connection.assert_called_once()

    def test_execute_all_queries_with_dialect_support(self):
        """Test _execute_all_queries method with dialect support."""
        adapter = MockPlatformAdapterWithDialect()

        class DummyBenchmark:
            def __init__(self):
                self.calls = []

            def get_queries(self, dialect=None, base_dialect=None):
                self.calls.append((dialect, base_dialect))
                return {
                    "1": "SELECT * FROM table1 LIMIT 100",
                    "2": "SELECT * FROM table2 LIMIT 50",
                }

        dummy_benchmark = DummyBenchmark()
        mock_connection = Mock()
        run_config = {}

        with patch("rich.console.Console"):
            results = adapter._execute_all_queries(dummy_benchmark, mock_connection, run_config)

        assert isinstance(results, list)
        assert dummy_benchmark.calls
        dialect, base_dialect = dummy_benchmark.calls[0]
        assert dialect == "mock_dialect"

    def test_execute_all_queries_without_dialect_support(self):
        """Test _execute_all_queries method without dialect support."""
        adapter = MockPlatformAdapter()  # No get_target_dialect method

        # mock benchmark
        mock_benchmark = Mock()
        mock_benchmark.get_queries.return_value = {
            "1": "SELECT TOP 100 * FROM table1",
            "2": "SELECT TOP 50 * FROM table2",
        }

        # Mock connection
        mock_connection = Mock()

        # Mock run config
        run_config = {}

        with patch("rich.console.Console"):
            results = adapter._execute_all_queries(mock_benchmark, mock_connection, run_config)

        # Verify benchmark was called without dialect
        mock_benchmark.get_queries.assert_called_once()
        # Should not have been called with dialect parameter
        args, kwargs = mock_benchmark.get_queries.call_args
        assert "dialect" not in kwargs
        assert isinstance(results, list)

    def test_execute_all_queries_benchmark_no_dialect_parameter(self):
        """Test _execute_all_queries when benchmark doesn't support dialect parameter."""
        adapter = MockPlatformAdapterWithDialect()

        # mock benchmark without dialect support
        mock_benchmark = Mock()
        mock_benchmark.get_queries.return_value = {"1": "SELECT TOP 100 * FROM table1"}

        # Mock connection
        mock_connection = Mock()
        run_config = {}

        with patch("inspect.signature") as mock_signature:
            # Mock signature to indicate no dialect parameter
            mock_signature.return_value.parameters = {}

            with patch("rich.console.Console"):
                adapter._execute_all_queries(mock_benchmark, mock_connection, run_config)

            # Should call without dialect since benchmark doesn't support it
            mock_benchmark.get_queries.assert_called_once()
            args, kwargs = mock_benchmark.get_queries.call_args
            assert "dialect" not in kwargs

    def test_execute_all_queries_dialect_exception_handling(self):
        """Test _execute_all_queries handles dialect inspection exceptions gracefully."""
        adapter = MockPlatformAdapterWithDialect()

        # mock benchmark
        mock_benchmark = Mock()
        mock_benchmark.get_queries.return_value = {"1": "SELECT * FROM table1"}

        mock_connection = Mock()
        run_config = {}

        with patch("inspect.signature", side_effect=Exception("Signature error")):
            with patch("rich.console.Console"):
                adapter._execute_all_queries(mock_benchmark, mock_connection, run_config)

            # Should fallback to calling without dialect
            mock_benchmark.get_queries.assert_called_once()
            args, kwargs = mock_benchmark.get_queries.call_args
            assert "dialect" not in kwargs


class TestBenchmarkResults:
    """Test BenchmarkResults data class."""

    def test_benchmark_results_creation(self):
        """Test creating BenchmarkResults instance."""
        from datetime import datetime

        results = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="test_platform",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.5,
            total_queries=5,
            successful_queries=4,
            failed_queries=1,
            query_results=[],
            total_execution_time=8.0,
            average_query_time=2.0,
            data_loading_time=1.5,
            schema_creation_time=1.0,
            total_rows_loaded=1000,
            data_size_mb=10.5,
            table_statistics={"table1": 1000},
        )

        assert results.benchmark_name == "test_benchmark"
        assert results.platform == "test_platform"
        assert results.scale_factor == 1.0
        assert results.validation_status == "PASSED"  # Default value
        assert results.validation_details is None  # Default value


@pytest.fixture
def mock_benchmark():
    """Create a mock benchmark for testing."""
    benchmark = Mock()
    benchmark.scale_factor = 1.0
    benchmark._name = "test_benchmark"
    benchmark.output_dir = Path("/tmp/test_data")
    benchmark.generate_data = Mock()
    benchmark.get_queries = Mock(return_value={"q1": "SELECT 1", "q2": "SELECT 2"})
    benchmark.get_create_tables_sql = Mock(return_value="CREATE TABLE test (id INT)")
    # Include tables attribute that won't interfere with data generation phase
    benchmark.tables = None
    benchmark._impl = None
    # Mock create_enhanced_benchmark_result will be set per test
    benchmark.create_enhanced_benchmark_result = Mock()
    return benchmark


class TestPlatformAdapterWorkflow:
    """Test complete platform adapter workflow."""

    def test_run_benchmark_success(self, mock_benchmark, tmp_path):
        """Test successful benchmark execution."""
        from datetime import datetime

        adapter = MockPlatformAdapter()
        mock_benchmark.output_dir = tmp_path

        # Mock file system for data size calculation
        test_file = tmp_path / "test.csv"
        test_file.write_text("test,data\n1,value")

        # Mock create_enhanced_benchmark_result to return correct values
        mock_result = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="mock",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.0,
            total_queries=2,
            successful_queries=2,
            failed_queries=0,
            query_results=[],
            total_execution_time=0.2,
            average_query_time=0.1,
            data_loading_time=0.5,
            schema_creation_time=0.1,
            total_rows_loaded=100,
            data_size_mb=1.0,
            table_statistics={"test_table": 100},
        )
        mock_benchmark.create_enhanced_benchmark_result.return_value = mock_result

        result = adapter.run_benchmark(mock_benchmark)

        assert isinstance(result, BenchmarkResults)
        assert result.benchmark_name == "test_benchmark"
        assert result.platform == "mock"
        assert result.total_queries == 2
        assert result.successful_queries == 2
        assert result.failed_queries == 0

    def test_run_benchmark_with_query_subset(self, mock_benchmark, tmp_path):
        """Test benchmark execution with query subset."""
        from datetime import datetime

        adapter = MockPlatformAdapter()
        mock_benchmark.output_dir = tmp_path

        # Mock create_enhanced_benchmark_result to return correct values for subset
        mock_result = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="mock",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.0,
            total_queries=1,  # Only 1 query in subset
            successful_queries=1,
            failed_queries=0,
            query_results=[],
            total_execution_time=0.1,
            average_query_time=0.1,
            data_loading_time=0.5,
            schema_creation_time=0.1,
            total_rows_loaded=100,
            data_size_mb=1.0,
            table_statistics={"test_table": 100},
        )
        mock_benchmark.create_enhanced_benchmark_result.return_value = mock_result

        result = adapter.run_benchmark(mock_benchmark, query_subset=["q1"])

        assert result.total_queries == 1
        assert result.successful_queries == 1

    def test_run_benchmark_with_categories(self, mock_benchmark, tmp_path):
        """Test benchmark execution with categories."""
        from datetime import datetime

        adapter = MockPlatformAdapter()
        mock_benchmark.output_dir = tmp_path
        mock_benchmark.get_queries_by_category = Mock(return_value={"q1": "SELECT 1"})

        # Mock create_enhanced_benchmark_result to return correct values for categories
        mock_result = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="mock",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.0,
            total_queries=1,  # Only 1 query in category
            successful_queries=1,
            failed_queries=0,
            query_results=[],
            total_execution_time=0.1,
            average_query_time=0.1,
            data_loading_time=0.5,
            schema_creation_time=0.1,
            total_rows_loaded=100,
            data_size_mb=1.0,
            table_statistics={"test_table": 100},
        )
        mock_benchmark.create_enhanced_benchmark_result.return_value = mock_result

        result = adapter.run_benchmark(mock_benchmark, categories=["category1"])

        assert result.total_queries == 1
        mock_benchmark.get_queries_by_category.assert_called_once_with("category1")

    def test_run_benchmark_with_data_generation(self, mock_benchmark, tmp_path):
        """Test benchmark execution with data generation."""
        from datetime import datetime

        adapter = MockPlatformAdapter()
        mock_benchmark.output_dir = tmp_path
        mock_benchmark.tables = None  # Trigger data generation
        mock_benchmark._impl = None  # Make sure _impl also has no tables

        # Mock create_enhanced_benchmark_result
        mock_result = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="mock",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.0,
            total_queries=2,
            successful_queries=2,
            failed_queries=0,
            query_results=[],
            total_execution_time=0.2,
            average_query_time=0.1,
            data_loading_time=0.5,
            schema_creation_time=0.1,
            total_rows_loaded=100,
            data_size_mb=1.0,
            table_statistics={"test_table": 100},
        )
        mock_benchmark.create_enhanced_benchmark_result.return_value = mock_result

        result = adapter.run_benchmark(mock_benchmark)

        mock_benchmark.generate_data.assert_called_once()
        assert isinstance(result, BenchmarkResults)

    def test_run_benchmark_query_failure(self, mock_benchmark, tmp_path):
        """Test benchmark execution with query failures."""
        from datetime import datetime

        adapter = MockPlatformAdapter()
        mock_benchmark.output_dir = tmp_path

        # Mock execute_query to fail for one query
        def mock_execute_query(connection, query, query_id):
            if query_id == "q1":
                raise Exception("Query failed")
            return {
                "query_id": query_id,
                "status": "SUCCESS",
                "execution_time": 0.1,
                "rows_returned": 10,
            }

        adapter.execute_query = mock_execute_query

        # Mock create_enhanced_benchmark_result to return correct failure counts
        mock_result = BenchmarkResults(
            benchmark_name="test_benchmark",
            platform="mock",
            scale_factor=1.0,
            execution_id="test_001",
            timestamp=datetime.now(),
            duration_seconds=10.0,
            total_queries=2,
            successful_queries=1,  # 1 success, 1 failure
            failed_queries=1,
            query_results=[
                {"query_id": "q1", "status": "FAILED", "error": "Query failed"},
                {"query_id": "q2", "status": "SUCCESS"},
            ],
            total_execution_time=0.1,
            average_query_time=0.1,
            data_loading_time=0.5,
            schema_creation_time=0.1,
            total_rows_loaded=100,
            data_size_mb=1.0,
            table_statistics={"test_table": 100},
        )
        mock_benchmark.create_enhanced_benchmark_result.return_value = mock_result

        result = adapter.run_benchmark(mock_benchmark)

        assert result.total_queries == 2
        assert result.successful_queries == 1
        assert result.failed_queries == 1

        # Check that failed query has error information
        failed_query = next(q for q in result.query_results if q["status"] == "FAILED")
        assert "error" in failed_query
        assert failed_query["query_id"] == "q1"


def test_collect_resource_utilization_without_psutil(monkeypatch):
    adapter = MockPlatformAdapter()

    original_import = __import__

    def fake_import(name, *args, **kwargs):  # pragma: no cover - exercised during test
        if name == "psutil":
            raise ImportError("psutil not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", fake_import)

    snapshot = adapter._collect_resource_utilization()
    assert snapshot["available"] is False
    assert "cpu_count" in snapshot


def test_collect_resource_utilization_with_stub(monkeypatch):
    adapter = MockPlatformAdapter()

    mb = 1024 * 1024

    class DummyProcess:
        def __init__(self, *_args, **_kwargs):
            self.pid = 4321

        class _OneShot:
            def __init__(self, process):
                self._process = process

            def __enter__(self):  # pragma: no cover - trivial
                return self._process

            def __exit__(self, exc_type, exc, tb):  # pragma: no cover - trivial
                return False

        def oneshot(self):
            return DummyProcess._OneShot(self)

        def memory_full_info(self):
            return SimpleNamespace(rss=256 * mb, vms=512 * mb)

        def memory_percent(self):
            return 7.5

        def cpu_percent(self, interval=None):
            return 12.5

        def num_threads(self):
            return 8

        def num_fds(self):
            return 24

        def num_handles(self):  # pragma: no cover - platform specific
            return 30

        def io_counters(self):
            return SimpleNamespace(read_bytes=25 * mb, write_bytes=10 * mb, read_count=120, write_count=45)

        def open_files(self):
            return [SimpleNamespace(path="/tmp/query.sql")]

        def num_ctx_switches(self):
            return SimpleNamespace(voluntary=5, involuntary=1)

        def create_time(self):
            return 1_700_000_000.0

        def name(self):
            return "dummy-process"

        def status(self):
            return "running"

    def cpu_percent(interval=None, percpu=False):
        if percpu:
            return [20.0, 22.0]
        return 21.5

    dummy_psutil = SimpleNamespace(
        cpu_count=lambda: 16,
        virtual_memory=lambda: SimpleNamespace(total=64 * mb, available=40 * mb, percent=37.5),
        swap_memory=lambda: SimpleNamespace(total=8 * mb, used=2 * mb, percent=25.0),
        cpu_percent=cpu_percent,
        getloadavg=lambda: (0.5, 0.4, 0.3),
        cpu_freq=lambda: SimpleNamespace(current=3200.0),
        disk_usage=lambda path: SimpleNamespace(total=120 * mb, used=60 * mb, free=60 * mb, percent=50.0),
        disk_io_counters=lambda: SimpleNamespace(
            read_bytes=600 * mb,
            write_bytes=150 * mb,
            read_count=1200,
            write_count=300,
        ),
        net_io_counters=lambda: SimpleNamespace(
            bytes_sent=800 * mb,
            bytes_recv=400 * mb,
            packets_sent=500,
            packets_recv=250,
        ),
        boot_time=lambda: 1_699_000_000.0,
        Process=lambda pid=None: DummyProcess(),
        __version__="6.0.0",
    )

    monkeypatch.setitem(sys.modules, "psutil", dummy_psutil)

    snapshot = adapter._collect_resource_utilization()
    assert snapshot["available"] is True
    assert snapshot["psutil_version"] == "6.0.0"
    assert snapshot["cpu_count"] == 16
    assert snapshot["cpu_percent"] == pytest.approx(21.5)
    assert snapshot["cpu"]["per_cpu_percent"] == [20.0, 22.0]
    assert snapshot["memory"]["total_mb"] == pytest.approx(64.0)
    assert snapshot["memory"]["used_mb"] == pytest.approx(24.0)
    assert snapshot["disk_io"]["read_mb"] == pytest.approx(600.0)
    assert snapshot["network_io"]["sent_mb"] == pytest.approx(800.0)
    assert snapshot["process_memory_mb"] == pytest.approx(256.0)
    assert snapshot["process_cpu_percent"] == pytest.approx(12.5)
    assert snapshot["process"]["num_threads"] == 8
    assert snapshot["process"]["io_counters"]["write_mb"] == pytest.approx(10.0)
    assert snapshot["process"]["context_switches"]["involuntary"] == 1


def test_summarize_performance_characteristics():
    adapter = MockPlatformAdapter()

    results = [
        QueryResult(
            query_id="q1",
            query_name="Q1",
            sql_text="SELECT 1",
            execution_time_ms=100.0,
            rows_returned=10,
            status="SUCCESS",
        ),
        QueryResult(
            query_id="q2",
            query_name="Q2",
            sql_text="SELECT 2",
            execution_time_ms=200.0,
            rows_returned=5,
            status="ERROR",
            error_message="boom",
        ),
    ]

    summary = adapter._summarize_performance_characteristics(
        results,
        total_duration=5.0,
        total_rows_loaded=123,
    )

    assert summary["total_queries"] == 2
    assert summary["successful_queries"] == 1
    assert summary["failed_queries"] == 1
    assert summary["rows_returned_total"] == 15
    assert summary["average_query_time_ms"] == pytest.approx(150.0)
    assert summary["average_success_query_time_ms"] == pytest.approx(100.0)
    assert summary["throughput_qps"] == pytest.approx(0.4)
    assert summary["successful_throughput_qps"] == pytest.approx(0.2)
    assert summary["rows_returned_per_second"] == pytest.approx(3.0)
    assert summary["rows_returned_average"] == pytest.approx(7.5)
    assert summary["total_rows_loaded"] == 123
    assert summary["success_rate"] == pytest.approx(0.5)
    assert summary["execution_time_stats"]["all"]["seconds"]["max"] == pytest.approx(0.2)
    assert summary["execution_time_stats"]["successful"]["milliseconds"]["min"] == pytest.approx(100.0)
    assert summary["rows_returned_stats"]["successful"]["min"] == pytest.approx(10.0)
    assert summary["error_breakdown"] == {"boom": 1}
    assert summary["failure_samples"][0]["status"] == "ERROR"
    assert summary["failure_samples"][0]["rows_returned"] == 5


def test_summarize_performance_characteristics_failure_samples_limited():
    adapter = MockPlatformAdapter()

    error_results = [
        QueryResult(
            query_id=f"q{i}",
            query_name=f"Q{i}",
            sql_text=f"SELECT {i}",
            execution_time_ms=50.0 + i,
            rows_returned=0,
            status="ERROR",
            error_message=f"err{i}",
        )
        for i in range(6)
    ]

    summary = adapter._summarize_performance_characteristics(
        error_results,
        total_duration=12.0,
        total_rows_loaded=0,
    )

    assert summary["failed_queries"] == 6
    assert len(summary["failure_samples"]) == 5
    assert summary["error_breakdown"]["err5"] == 1
    assert summary["throughput_qps"] == pytest.approx(0.5)


class TestConsolidatedFunctionality:
    """Test consolidated functionality added to base platform adapter."""

    def test_tpc_methods_consolidation(self):
        """Test that TPC methods are properly consolidated in base class."""
        adapter = MockPlatformAdapter()
        mock_benchmark = Mock()
        mock_connection = Mock()

        # Test that TPC methods exist and can be called
        assert hasattr(adapter, "run_power_test")
        assert hasattr(adapter, "run_throughput_test")
        assert hasattr(adapter, "run_maintenance_test")

        # Test that run_throughput_test delegates to run_power_test when no TPC benchmark
        with patch.object(adapter, "run_power_test") as mock_run_power_test:
            mock_run_power_test.return_value = {"test": "throughput"}

            # Mock hasattr to ensure benchmark doesn't have run_throughput_test method
            with patch("builtins.hasattr", return_value=False):
                # Benchmark without run_throughput_test method should delegate to run_power_test
                result = adapter.run_throughput_test(mock_benchmark, connection=mock_connection, test_param="value")
                mock_run_power_test.assert_called_with(mock_benchmark, connection=mock_connection, test_param="value")
                assert result == {"test": "throughput"}

    def test_create_schema_with_tuning_helper(self):
        """Test the _create_schema_with_tuning helper method."""
        adapter = MockPlatformAdapterWithDialect()
        mock_benchmark = Mock()

        # Test with tuning configuration
        with patch.object(adapter, "get_effective_tuning_configuration") as mock_get_tuning:
            with patch.object(adapter, "get_target_dialect", return_value="postgres"):
                with patch.object(adapter, "translate_sql", return_value="translated_sql"):
                    mock_tuning_config = Mock()
                    mock_get_tuning.return_value = mock_tuning_config
                    mock_benchmark.get_create_tables_sql.return_value = "original_sql"

                    result = adapter._create_schema_with_tuning(mock_benchmark)

                    mock_benchmark.get_create_tables_sql.assert_called_with(
                        dialect="postgres", tuning_config=mock_tuning_config
                    )
                    adapter.translate_sql.assert_called_with("original_sql", "duckdb")
                    assert result == "translated_sql"

        # Test fallback to legacy signature
        adapter2 = MockPlatformAdapterWithDialect()
        mock_benchmark2 = Mock()

        with patch.object(adapter2, "get_effective_tuning_configuration") as mock_get_tuning:
            with patch.object(adapter2, "get_target_dialect", return_value="postgres"):
                with patch.object(adapter2, "translate_sql", return_value="fallback_sql"):
                    mock_tuning_config = Mock()
                    mock_get_tuning.return_value = mock_tuning_config

                    # Make the new signature raise TypeError, then return success
                    mock_benchmark2.get_create_tables_sql.side_effect = [
                        TypeError("Signature not supported"),
                        "fallback_sql_original",
                    ]

                    result = adapter2._create_schema_with_tuning(mock_benchmark2)

                    # Should call twice - first with new signature (fails), then fallback
                    assert mock_benchmark2.get_create_tables_sql.call_count == 2
                    calls = mock_benchmark2.get_create_tables_sql.call_args_list
                    # First call with new signature
                    assert calls[0].kwargs == {
                        "dialect": "postgres",
                        "tuning_config": mock_tuning_config,
                    }
                    # Second call without arguments (fallback)
                    assert calls[1].args == ()
                    assert calls[1].kwargs == {}

                    adapter2.translate_sql.assert_called_with("fallback_sql_original", "duckdb")
                    assert result == "fallback_sql"

    def test_get_constraint_configuration_helper(self):
        """Test the _get_constraint_configuration helper method."""
        adapter = MockPlatformAdapter()

        # Test with tuning configuration
        with patch.object(adapter, "get_effective_tuning_configuration") as mock_get_tuning:
            mock_tuning_config = Mock()
            mock_tuning_config.primary_keys.enabled = True
            mock_tuning_config.foreign_keys.enabled = False
            mock_get_tuning.return_value = mock_tuning_config

            primary_keys, foreign_keys = adapter._get_constraint_configuration()

            assert primary_keys is True
            assert foreign_keys is False

        # Test without tuning configuration
        with patch.object(adapter, "get_effective_tuning_configuration", return_value=None):
            primary_keys, foreign_keys = adapter._get_constraint_configuration()

            assert primary_keys is False
            assert foreign_keys is False

    def test_log_constraint_configuration_helper(self):
        """Test the _log_constraint_configuration helper method."""
        adapter = MockPlatformAdapter()

        # Test logging with constraints enabled
        with patch.object(adapter.logger, "info") as mock_info:
            with patch.object(adapter.logger, "debug") as mock_debug:
                adapter._log_constraint_configuration(True, True)

                info_messages = [args[0] for args, _ in mock_info.call_args_list]
                assert any("Primary key constraints" in msg for msg in info_messages)
                assert any("Foreign key constraints" in msg for msg in info_messages)
                mock_debug.assert_called_with(
                    "Schema constraints from tuning config: primary_keys=True, foreign_keys=True"
                )

        # Test logging with no constraints
        with patch.object(adapter.logger, "info") as mock_info:
            with patch.object(adapter.logger, "debug") as mock_debug:
                adapter._log_constraint_configuration(False, False)

                mock_info.assert_not_called()
                debug_messages = [args[0] for args, _ in mock_debug.call_args_list]
                assert any("No constraints enabled" in msg for msg in debug_messages)
                assert any(
                    "Schema constraints from tuning config: primary_keys=False, foreign_keys=False" in msg
                    for msg in debug_messages
                )

    def test_schema_creation_no_translation_needed(self):
        """Test schema creation when source and target dialect are the same."""
        adapter = MockPlatformAdapterWithDialect()
        mock_benchmark = Mock()

        with patch.object(adapter, "get_effective_tuning_configuration", return_value=None):
            with patch.object(adapter, "get_target_dialect", return_value="duckdb"):
                with patch.object(adapter, "translate_sql") as mock_translate:
                    mock_benchmark.get_create_tables_sql.return_value = "schema_sql"

                    result = adapter._create_schema_with_tuning(mock_benchmark, source_dialect="duckdb")

                    # Should not call translate_sql when dialects match
                    mock_translate.assert_not_called()
                    assert result == "schema_sql"
