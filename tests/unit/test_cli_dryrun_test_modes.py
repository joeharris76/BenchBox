"""Test TPC-DS Test Mode CLI Dry Run Output functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

from benchbox.cli.benchmarks import BenchmarkConfig
from benchbox.cli.database import DatabaseConfig
from benchbox.cli.dryrun import DryRunExecutor
from benchbox.cli.system import SystemProfile
from benchbox.core.dryrun import DryRunExecutor as CoreDryRunExecutor

pytestmark = pytest.mark.fast


@pytest.fixture
def mock_benchmark_config():
    """Mock benchmark configuration."""
    config = BenchmarkConfig(
        name="tpcds",
        display_name="TPC-DS",
        scale_factor=0.1,
        test_execution_type="standard",
    )
    return config


@pytest.fixture
def mock_system_profile():
    """Mock system profile."""
    from datetime import datetime

    profile = SystemProfile(
        os_name="Linux",
        os_version="5.4.0",
        architecture="x86_64",
        cpu_model="Intel Core i7",
        cpu_cores_physical=4,
        cpu_cores_logical=8,
        memory_total_gb=16.0,
        memory_available_gb=14.0,
        python_version="3.13.5",
        disk_space_gb=500.0,
        timestamp=datetime.now(),
    )
    return profile


@pytest.fixture
def mock_database_config():
    """Mock database configuration."""
    config = DatabaseConfig(type="duckdb", name="test.db")
    return config


@pytest.fixture
def mock_tpcds_benchmark():
    """Mock TPC-DS benchmark."""
    benchmark = Mock()
    benchmark._name = "tpcds"
    benchmark.scale_factor = 0.1
    benchmark.get_queries.return_value = {
        "1": "SELECT * FROM store_sales;",
        "2": "SELECT * FROM store_returns;",
        "3": "SELECT * FROM catalog_sales;",
    }

    # Mock get_all_queries method to return empty - this method is used by _estimate_runtime
    benchmark.get_all_queries = Mock(return_value={})

    # Mock get_create_tables_sql method - this is used for schema generation
    benchmark.get_create_tables_sql = Mock(return_value="CREATE TABLE test_table (id INT);")

    # Mock query manager
    query_manager = Mock()
    benchmark.query_manager = query_manager

    return benchmark


class TestDryRunTestModes:
    """Test dry run functionality for different test execution modes."""

    def test_standard_execution_mode(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test dry run with standard execution mode."""
        mock_benchmark_config.test_execution_type = "standard"

        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)
            result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify standard execution
        assert result.benchmark_config["test_execution_type"] == "standard"
        assert result.query_preview["test_execution_type"] == "standard"
        assert "Standard sequential execution" in result.query_preview["execution_context"]
        assert len(result.queries) == 3  # Standard queries

    def test_power_test_execution_mode(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test dry run with TPC-DS power test execution mode."""
        mock_benchmark_config.test_execution_type = "power"

        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)

            # Mock the real test execution method to return expected power test format
            with patch.object(executor, "_extract_queries_via_real_test_execution") as mock_extract:
                mock_extract.return_value = {
                    "Position_01_Query_42": "SELECT * FROM web_sales WHERE ws_sold_date_sk = ?;",
                    "Position_02_Query_7": "SELECT * FROM store_sales WHERE ss_sold_date_sk = ?;",
                    "Position_03_Query_23": "SELECT * FROM catalog_sales WHERE cs_sold_date_sk = ?;",
                }

                result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify power test execution
        assert result.benchmark_config["test_execution_type"] == "power"
        assert result.query_preview["test_execution_type"] == "power"
        assert "PowerTest stream permutation" in result.query_preview["execution_context"]

        # Verify stream ordering is shown
        query_keys = list(result.queries.keys())
        assert any("Position_01_Query_42" in key for key in query_keys), (
            f"Expected Position_01_Query_42 in {query_keys}"
        )
        assert any("Position_02_Query_7" in key for key in query_keys), f"Expected Position_02_Query_7 in {query_keys}"
        assert any("Position_03_Query_23" in key for key in query_keys), (
            f"Expected Position_03_Query_23 in {query_keys}"
        )

    def test_throughput_test_execution_mode(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test dry run with TPC-DS throughput test execution mode."""
        mock_benchmark_config.test_execution_type = "throughput"

        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)

            with patch.object(executor, "_extract_queries_via_real_test_execution") as mock_extract:
                mock_extract.return_value = {
                    "Stream_1_Position_01_Query_42": "SELECT * FROM web_sales WHERE ws_sold_date_sk = ?;",
                    "Stream_1_Position_02_Query_7": "SELECT * FROM store_sales WHERE ss_sold_date_sk = ?;",
                    "Stream_1_Position_03_Query_23": "SELECT * FROM catalog_sales WHERE cs_sold_date_sk = ?;",
                    "Stream_2_Position_01_Query_42": "SELECT * FROM web_sales WHERE ws_sold_date_sk = ?;",
                    "Stream_2_Position_02_Query_7": "SELECT * FROM store_sales WHERE ss_sold_date_sk = ?;",
                    "Stream_2_Position_03_Query_23": "SELECT * FROM catalog_sales WHERE cs_sold_date_sk = ?;",
                }

                result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify throughput test execution
        assert result.benchmark_config["test_execution_type"] == "throughput"
        assert result.query_preview["test_execution_type"] == "throughput"
        assert "ThroughputTest (4 concurrent streams" in result.query_preview["execution_context"]

        # Verify concurrent stream execution is shown
        query_keys = list(result.queries.keys())
        assert any("Stream_1_" in key for key in query_keys), f"Expected Stream_1_ in {query_keys}"
        assert any("Stream_2_" in key for key in query_keys), f"Expected Stream_2_ in {query_keys}"

    def test_maintenance_test_execution_mode(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test dry run with TPC-DS maintenance test execution mode."""
        mock_benchmark_config.test_execution_type = "maintenance"

        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)

            with patch.object(executor, "_extract_queries_via_real_test_execution") as mock_extract:
                mock_extract.return_value = {
                    "RF1_INSERT_catalog_sales": "INSERT INTO catalog_sales VALUES (...);",
                    "RF1_UPDATE_catalog_sales": "UPDATE catalog_sales SET cs_quantity = ? WHERE ...;",
                    "RF2_DELETE_catalog_sales": "DELETE FROM catalog_sales WHERE ...;",
                    "MAINTENANCE_OPERATION": "REFRESH MATERIALIZED VIEW catalog_sales_summary;",
                }

                result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify maintenance test execution
        assert result.benchmark_config["test_execution_type"] == "maintenance"
        assert result.query_preview["test_execution_type"] == "maintenance"
        assert "MaintenanceTest (data operations: INSERT/UPDATE/DELETE)" in result.query_preview["execution_context"]

        # Verify maintenance operations are shown instead of queries
        query_keys = list(result.queries.keys())
        assert any("RF1_INSERT" in key for key in query_keys), f"Expected RF1_INSERT in {query_keys}"
        assert any("RF1_UPDATE" in key for key in query_keys), f"Expected RF1_UPDATE in {query_keys}"
        assert any("RF2_DELETE" in key for key in query_keys), f"Expected RF2_DELETE in {query_keys}"
        assert any("MAINTENANCE" in key for key in query_keys), f"Expected MAINTENANCE in {query_keys}"

        # Verify operations contain actual SQL/descriptions
        for key, operation in result.queries.items():
            assert "INSERT" in operation or "UPDATE" in operation or "DELETE" in operation or "REFRESH" in operation, (
                f"Expected SQL operation in {key}: {operation}"
            )

    def test_tpch_power_test_execution_mode(self, mock_system_profile, mock_database_config, tmp_path):
        """Test dry run with TPC-H power test execution mode."""
        mock_benchmark_config = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=0.1,
            test_execution_type="power",
        )

        # Mock TPC-H benchmark
        mock_tpch_benchmark = Mock()
        mock_tpch_benchmark._name = "tpch"
        mock_tpch_benchmark.scale_factor = 0.1
        mock_tpch_benchmark.get_queries.return_value = {
            "1": "SELECT * FROM lineitem;",
            "2": "SELECT * FROM orders;",
            "3": "SELECT * FROM customer;",
        }
        # Mock get_all_queries method to return empty - this method is used by _estimate_runtime
        mock_tpch_benchmark.get_all_queries = Mock(return_value={})

        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpch_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)

            with patch.object(executor, "_extract_queries_via_real_test_execution") as mock_extract:
                mock_extract.return_value = {
                    "Position_01_Query_3": "SELECT * FROM customer;",
                    "Position_02_Query_1": "SELECT * FROM lineitem;",
                    "Position_03_Query_2": "SELECT * FROM orders;",
                }

                result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify TPC-H power test execution
        assert result.benchmark_config["test_execution_type"] == "power"
        assert result.query_preview["test_execution_type"] == "power"
        assert "TPC-H PowerTest stream permutation" in result.query_preview["execution_context"]

        # Verify TPC-H stream ordering is shown
        query_keys = list(result.queries.keys())
        assert any("Position_01_Query_3" in key for key in query_keys), f"Expected Position_01_Query_3 in {query_keys}"
        assert any("Position_02_Query_1" in key for key in query_keys), f"Expected Position_02_Query_1 in {query_keys}"

    def test_display_query_preview_formatting(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test that query preview formatting works correctly for different test modes."""
        with (
            patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
            patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
            patch("benchbox.platforms.get_platform_adapter"),
        ):
            executor = DryRunExecutor(tmp_path)

            assert executor._format_test_execution_type("power") == "PowerTest (Stream Permutation)"
            assert executor._format_test_execution_type("throughput") == "ThroughputTest (Concurrent Streams)"
            assert executor._format_test_execution_type("maintenance") == "MaintenanceTest (Data Operations)"
            assert executor._format_test_execution_type("standard") == "Standard (Sequential)"

            assert executor._get_preview_title("power", 99) == "PowerTest Stream Execution Preview"
            assert executor._get_preview_title("throughput", 40) == "ThroughputTest Concurrent Stream Preview"
            assert executor._get_preview_title("maintenance", 6) == "MaintenanceTest Operations Preview"
            assert executor._get_preview_title("standard", 99) == "Query Preview"

            assert (
                executor._format_query_display_title("Position_01_Query_42", "power") == "Stream Position 01 Query 42"
            )
            assert (
                executor._format_query_display_title("Stream_1_Position_01_Query_42", "throughput")
                == "Stream 1 Position 01 Query 42"
            )
            assert (
                executor._format_query_display_title("RF1_INSERT_catalog_sales", "maintenance")
                == "Operation RF1_INSERT_catalog_sales"
            )

            assert executor._format_panel_title("Position_01_Query_42", "power") == "Stream Position 01: Query 42"
            assert (
                executor._format_panel_title("Stream_1_Position_01_Query_42", "throughput")
                == "Stream 1 Position 01: Query 42"
            )
            assert (
                executor._format_panel_title("RF1_INSERT_catalog_sales", "maintenance")
                == "Maintenance Operation: RF1_INSERT_catalog_sales"
            )

    def test_configuration_summary_shows_test_execution_type(
        self,
        mock_benchmark_config,
        mock_system_profile,
        mock_database_config,
        mock_tpcds_benchmark,
        tmp_path,
    ):
        """Test that configuration summary shows test execution type correctly."""
        mock_benchmark_config.test_execution_type = "power"

        with patch("benchbox.core.tpcds.streams.create_standard_streams"):
            with (
                patch.object(CoreDryRunExecutor, "_get_benchmark_instance", return_value=mock_tpcds_benchmark),
                patch.object(CoreDryRunExecutor, "_get_platform_config", return_value={"type": "duckdb"}),
                patch("benchbox.platforms.get_platform_adapter"),
            ):
                executor = DryRunExecutor(tmp_path)
                result = executor.execute_dry_run(mock_benchmark_config, mock_system_profile, mock_database_config)

        # Verify the result contains power test configuration instead of checking console output
        assert result.benchmark_config["test_execution_type"] == "power"
        assert result.query_preview["test_execution_type"] == "power"
        assert "PowerTest stream permutation" in result.query_preview["execution_context"]

        # Verify the formatting methods work correctly
        assert executor._format_test_execution_type("power") == "PowerTest (Stream Permutation)"
