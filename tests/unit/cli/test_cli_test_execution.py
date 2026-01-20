from tests.conftest import make_benchmark_results

"""Tests for CLI test execution functionality (Power, Throughput, Maintenance tests).

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from benchbox.cli.main import cli
from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.platforms.base import BenchmarkResults

pytestmark = pytest.mark.medium  # CLI execution tests spawn subprocesses (2-5s)


@pytest.mark.medium
class TestCLITestExecution:
    """Test CLI test execution functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_power_phase_validation(self):
        """Test that power phase works correctly."""
        with (
            patch("benchbox.cli.main.BenchmarkManager") as mock_manager,
            patch("benchbox.cli.main.DatabaseManager") as mock_db_manager,
            patch("benchbox.cli.execution.BenchmarkExecutor") as mock_executor,
            patch("benchbox.cli.main.SystemProfiler") as mock_profiler,
        ):
            # Configure mocks
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            mock_manager_instance.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)}}

            mock_db_manager_instance = Mock()
            mock_db_manager.return_value = mock_db_manager_instance

            # Create a proper mock database config with options dict
            mock_db_config = Mock()
            mock_db_config.type = "duckdb"
            mock_db_config.options = {}
            mock_db_manager_instance.create_config.return_value = mock_db_config

            mock_executor_instance = Mock()
            mock_executor.return_value = mock_executor_instance
            mock_executor_instance.execute_benchmark.return_value = Mock()

            # Mock system profiler
            mock_profiler_instance = Mock()
            mock_profiler.return_value = mock_profiler_instance
            mock_profiler_instance.get_system_profile.return_value = Mock()

            # Mock config manager
            with patch("benchbox.cli.main.ConfigManager") as mock_config:
                mock_config_instance = Mock()
                mock_config.return_value = mock_config_instance
                mock_config_instance.validate_config.return_value = True
                mock_config_instance.load_unified_tuning_config.return_value = None

                result = self.runner.invoke(
                    cli,
                    [
                        "run",
                        "--platform",
                        "duckdb",
                        "--benchmark",
                        "tpch",
                        "--scale",
                        "0.01",
                        "--phases",
                        "power",
                        "--non-interactive",
                    ],
                )

        # Should succeed (exit code 0) and properly handle the power phase
        assert result.exit_code == 0
        # Look for evidence that the command ran successfully
        assert "running tpch on duckdb" in result.output.lower() or "power test execution" in result.output.lower()

    def test_throughput_phase_validation(self):
        """Test that throughput phase works correctly."""
        with (
            patch("benchbox.cli.main.BenchmarkManager") as mock_manager,
            patch("benchbox.cli.main.DatabaseManager") as mock_db_manager,
            patch("benchbox.cli.execution.BenchmarkExecutor") as mock_executor,
            patch("benchbox.cli.main.SystemProfiler") as mock_profiler,
        ):
            # Configure mocks
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            mock_manager_instance.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)}}

            mock_db_manager_instance = Mock()
            mock_db_manager.return_value = mock_db_manager_instance

            # Create a proper mock database config with options dict
            mock_db_config = Mock()
            mock_db_config.type = "duckdb"
            mock_db_config.options = {}
            mock_db_manager_instance.create_config.return_value = mock_db_config

            mock_executor_instance = Mock()
            mock_executor.return_value = mock_executor_instance
            mock_executor_instance.execute_benchmark.return_value = Mock()

            # Mock system profiler
            mock_profiler_instance = Mock()
            mock_profiler.return_value = mock_profiler_instance
            mock_profiler_instance.get_system_profile.return_value = Mock()

            # Mock config manager
            with patch("benchbox.cli.main.ConfigManager") as mock_config:
                mock_config_instance = Mock()
                mock_config.return_value = mock_config_instance
                mock_config_instance.validate_config.return_value = True
                mock_config_instance.load_unified_tuning_config.return_value = None

                result = self.runner.invoke(
                    cli,
                    [
                        "run",
                        "--platform",
                        "duckdb",
                        "--benchmark",
                        "tpch",
                        "--scale",
                        "0.01",
                        "--phases",
                        "throughput",
                        "--non-interactive",
                    ],
                )

        # Should succeed and properly handle the throughput phase
        assert result.exit_code == 0
        # Look for evidence that the command ran successfully
        assert "running tpch on duckdb" in result.output.lower() or "throughput test execution" in result.output.lower()

    def test_maintenance_phase_validation(self):
        """Test that maintenance phase works correctly."""
        with (
            patch("benchbox.cli.main.BenchmarkManager") as mock_manager,
            patch("benchbox.cli.main.DatabaseManager") as mock_db_manager,
            patch("benchbox.cli.execution.BenchmarkExecutor") as mock_executor,
            patch("benchbox.cli.main.SystemProfiler") as mock_profiler,
        ):
            # Configure mocks
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            mock_manager_instance.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)}}

            mock_db_manager_instance = Mock()
            mock_db_manager.return_value = mock_db_manager_instance

            # Create a proper mock database config with options dict
            mock_db_config = Mock()
            mock_db_config.type = "duckdb"
            mock_db_config.options = {}
            mock_db_manager_instance.create_config.return_value = mock_db_config

            mock_executor_instance = Mock()
            mock_executor.return_value = mock_executor_instance
            mock_executor_instance.execute_benchmark.return_value = Mock()

            # Mock system profiler
            mock_profiler_instance = Mock()
            mock_profiler.return_value = mock_profiler_instance
            mock_profiler_instance.get_system_profile.return_value = Mock()

            # Mock config manager
            with patch("benchbox.cli.main.ConfigManager") as mock_config:
                mock_config_instance = Mock()
                mock_config.return_value = mock_config_instance
                mock_config_instance.validate_config.return_value = True
                mock_config_instance.load_unified_tuning_config.return_value = None

                result = self.runner.invoke(
                    cli,
                    [
                        "run",
                        "--platform",
                        "duckdb",
                        "--benchmark",
                        "tpch",
                        "--scale",
                        "0.01",
                        "--phases",
                        "maintenance",
                        "--non-interactive",
                    ],
                )

        # Should succeed and properly handle the maintenance phase
        assert result.exit_code == 0
        # Look for evidence that the command ran successfully
        assert (
            "running tpch on duckdb" in result.output.lower() or "maintenance test execution" in result.output.lower()
        )

    def test_combined_phases_validation(self):
        """Test that combined phases work correctly."""
        with (
            patch("benchbox.cli.main.BenchmarkManager") as mock_manager,
            patch("benchbox.cli.main.DatabaseManager") as mock_db_manager,
            patch("benchbox.cli.execution.BenchmarkExecutor") as mock_executor,
            patch("benchbox.cli.main.SystemProfiler") as mock_profiler,
        ):
            # Configure mocks
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            mock_manager_instance.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)}}

            mock_db_manager_instance = Mock()
            mock_db_manager.return_value = mock_db_manager_instance
            # Create a proper mock database config with options dict
            mock_db_config = Mock()
            mock_db_config.type = "duckdb"
            mock_db_config.options = {}
            mock_db_manager_instance.create_config.return_value = mock_db_config

            mock_executor_instance = Mock()
            mock_executor.return_value = mock_executor_instance
            mock_executor_instance.execute_benchmark.return_value = Mock()

            # Mock system profiler
            mock_profiler_instance = Mock()
            mock_profiler.return_value = mock_profiler_instance
            mock_profiler_instance.get_system_profile.return_value = Mock()

            # Mock config manager
            with patch("benchbox.cli.main.ConfigManager") as mock_config:
                mock_config_instance = Mock()
                mock_config.return_value = mock_config_instance
                mock_config_instance.validate_config.return_value = True
                mock_config_instance.load_unified_tuning_config.return_value = None

                result = self.runner.invoke(
                    cli,
                    [
                        "run",
                        "--platform",
                        "duckdb",
                        "--benchmark",
                        "tpch",
                        "--scale",
                        "0.01",
                        "--phases",
                        "power,throughput",
                        "--non-interactive",
                    ],
                )

        # Should succeed and properly handle the combined phases
        assert result.exit_code == 0
        # Look for evidence that the command ran successfully
        assert "running tpch on duckdb" in result.output.lower() or "combined test execution" in result.output.lower()

    def test_phase_requires_benchmark(self):
        """Test that providing phases without required benchmark is rejected."""
        result = self.runner.invoke(cli, ["run", "--phases", "power,throughput"], catch_exceptions=False)
        assert result.exit_code != 0
        assert "benchmark" in result.output.lower() or "required" in result.output.lower()

    def test_benchmark_config_test_execution_type_setting(self):
        """Test that BenchmarkConfig gets correct test execution type."""
        # Test power test
        config = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="power")
        assert config.test_execution_type == "power"

        # Test throughput test
        config = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="throughput")
        assert config.test_execution_type == "throughput"

        # Test maintenance test
        config = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="maintenance")
        assert config.test_execution_type == "maintenance"

        # Test combined test
        config = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="combined")
        assert config.test_execution_type == "combined"

        # Test default (standard)
        config = BenchmarkConfig(name="tpch", display_name="TPC-H")
        assert config.test_execution_type == "standard"

    def test_no_tuning_disables_constraints(self):
        """Test that `--tuning notuning` properly disables constraints."""
        with (
            patch("benchbox.cli.main.BenchmarkManager") as mock_manager,
            patch("benchbox.cli.main.DatabaseManager") as mock_db_manager,
            patch("benchbox.cli.execution.BenchmarkExecutor") as mock_executor,
            patch("benchbox.cli.main.SystemProfiler") as mock_profiler,
        ):
            # Configure mocks
            mock_manager_instance = Mock()
            mock_manager.return_value = mock_manager_instance
            mock_manager_instance.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)}}

            mock_db_manager_instance = Mock()
            mock_db_manager.return_value = mock_db_manager_instance
            # Create a proper mock database config with options dict
            mock_db_config = Mock()
            mock_db_config.type = "duckdb"
            mock_db_config.options = {}
            mock_db_manager_instance.create_config.return_value = mock_db_config

            mock_executor_instance = Mock()
            mock_executor.return_value = mock_executor_instance
            mock_executor_instance.execute_benchmark.return_value = Mock()

            # Mock system profiler
            mock_profiler_instance = Mock()
            mock_profiler.return_value = mock_profiler_instance
            mock_profiler_instance.get_system_profile.return_value = Mock()

            # Mock config manager
            with patch("benchbox.cli.main.ConfigManager") as mock_config:
                mock_config_instance = Mock()
                mock_config.return_value = mock_config_instance
                mock_config_instance.validate_config.return_value = True
                mock_config_instance.load_unified_tuning_config.return_value = None

                result = self.runner.invoke(
                    cli,
                    [
                        "run",
                        "--platform",
                        "duckdb",
                        "--benchmark",
                        "tpch",
                        "--scale",
                        "0.01",
                        "--tuning",
                        "notuning",
                        "--non-interactive",
                    ],
                )

        # Should succeed when properly configured with notuning mode
        assert result.exit_code == 0
        # The benchmark should run successfully with no-tuning configuration
        assert "benchmark completed" in result.output.lower() or "passed" in result.output.lower()


class TestCLIOrchestrator:
    """Test CLI orchestrator test execution delegation."""

    def setup_method(self):
        """Set up test environment."""
        from benchbox.cli.orchestrator import BenchmarkOrchestrator

        self.orchestrator = BenchmarkOrchestrator()

    @patch("benchbox.cli.orchestrator.get_platform_adapter")
    def test_orchestrator_power_test_delegation(self, mock_get_adapter):
        """Test that orchestrator delegates power test correctly via run_benchmark."""
        from benchbox.cli.benchmarks import BenchmarkConfig
        from benchbox.cli.orchestrator import BenchmarkOrchestrator
        from benchbox.core.results.models import BenchmarkResults

        orchestrator = BenchmarkOrchestrator()
        cfg = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=0.01,
            test_execution_type="power",
        )
        sys_prof = Mock()
        db_cfg = DatabaseConfig(
            type="duckdb",
            name="test",
            options={},
        )
        # Mock benchmark instance
        with patch.object(BenchmarkOrchestrator, "_get_benchmark_instance") as mock_bench:
            from pathlib import Path

            mock_bench.return_value = Mock(_name="tpch", scale_factor=0.01)
            # Provide pre-generated tables to skip generation
            mock_bench.return_value.tables = {"customer": Path("customer.tbl")}
            mock_bench.return_value.generate_data = Mock(return_value={"customer": Path("customer.tbl")})
            mock_bench.return_value.get_data_source_benchmark.return_value = None
            # Mock directory manager
            orchestrator.directory_manager.get_datagen_path = Mock(return_value=Path("/tmp/test_datagen"))
            orchestrator.directory_manager.get_database_path = Mock(return_value=Path("/tmp/test_db.duckdb"))
            # Mock adapter and result
            mock_adapter = Mock()
            from datetime import datetime

            mock_adapter.run_benchmark.return_value = BenchmarkResults(
                benchmark_name="TPC-H",
                platform="duckdb",
                scale_factor=0.01,
                execution_id="x",
                timestamp=datetime.now(),
                duration_seconds=1.0,
                query_definitions={},
                execution_phases={},
                total_queries=1,
                successful_queries=1,
                failed_queries=0,
                total_execution_time=1.0,
                average_query_time=1.0,
                test_execution_type="power",
                power_at_size=111.0,
            )
            mock_get_adapter.return_value = mock_adapter
            result = orchestrator.execute_benchmark(cfg, sys_prof, db_cfg)
        assert mock_adapter.run_benchmark.called
        assert result.power_at_size == 111.0

    @patch("benchbox.cli.orchestrator.get_platform_adapter")
    def test_orchestrator_throughput_test_delegation(self, mock_get_adapter):
        """Test that orchestrator delegates throughput test correctly via run_benchmark."""
        from benchbox.cli.benchmarks import BenchmarkConfig
        from benchbox.cli.orchestrator import BenchmarkOrchestrator
        from benchbox.core.results.models import BenchmarkResults

        orchestrator = BenchmarkOrchestrator()
        cfg = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=0.01,
            test_execution_type="throughput",
        )
        sys_prof = Mock()
        db_cfg = DatabaseConfig(
            type="duckdb",
            name="test",
            options={},
        )
        with patch.object(BenchmarkOrchestrator, "_get_benchmark_instance") as mock_bench:
            from pathlib import Path

            mock_bench.return_value = Mock(_name="tpch", scale_factor=0.01)
            mock_bench.return_value.tables = {"orders": Path("orders.tbl")}
            mock_bench.return_value.generate_data = Mock(return_value={"orders": Path("orders.tbl")})
            mock_bench.return_value.get_data_source_benchmark.return_value = None
            # Mock directory manager
            orchestrator.directory_manager.get_datagen_path = Mock(return_value=Path("/tmp/test_datagen"))
            orchestrator.directory_manager.get_database_path = Mock(return_value=Path("/tmp/test_db.duckdb"))
            mock_adapter = Mock()
            from datetime import datetime

            mock_adapter.run_benchmark.return_value = BenchmarkResults(
                benchmark_name="TPC-H",
                platform="duckdb",
                scale_factor=0.01,
                execution_id="y",
                timestamp=datetime.now(),
                duration_seconds=1.0,
                query_definitions={},
                execution_phases={},
                total_queries=1,
                successful_queries=1,
                failed_queries=0,
                total_execution_time=1.0,
                average_query_time=1.0,
                test_execution_type="throughput",
                throughput_at_size=222.0,
            )
            mock_get_adapter.return_value = mock_adapter
            result = orchestrator.execute_benchmark(cfg, sys_prof, db_cfg)
        assert mock_adapter.run_benchmark.called
        assert result.throughput_at_size == 222.0

    @patch("benchbox.cli.orchestrator.get_platform_adapter")
    def test_orchestrator_maintenance_test_delegation(self, mock_get_adapter):
        """Test that orchestrator delegates maintenance test correctly via run_benchmark."""
        from benchbox.cli.benchmarks import BenchmarkConfig
        from benchbox.cli.orchestrator import BenchmarkOrchestrator
        from benchbox.core.results.models import BenchmarkResults

        orchestrator = BenchmarkOrchestrator()
        cfg = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=0.01,
            test_execution_type="maintenance",
        )
        sys_prof = Mock()
        db_cfg = DatabaseConfig(
            type="duckdb",
            name="test",
            options={},
        )
        with patch.object(BenchmarkOrchestrator, "_get_benchmark_instance") as mock_bench:
            from pathlib import Path

            mock_bench.return_value = Mock(_name="tpch", scale_factor=0.01)
            mock_bench.return_value.tables = {"lineitem": Path("lineitem.tbl")}
            mock_bench.return_value.generate_data = Mock(return_value={"lineitem": Path("lineitem.tbl")})
            mock_bench.return_value.get_data_source_benchmark.return_value = None
            # Mock directory manager
            orchestrator.directory_manager.get_datagen_path = Mock(return_value=Path("/tmp/test_datagen"))
            orchestrator.directory_manager.get_database_path = Mock(return_value=Path("/tmp/test_db.duckdb"))
            mock_adapter = Mock()
            from datetime import datetime

            mock_adapter.run_benchmark.return_value = BenchmarkResults(
                benchmark_name="TPC-H",
                platform="duckdb",
                scale_factor=0.01,
                execution_id="z",
                timestamp=datetime.now(),
                duration_seconds=1.0,
                query_definitions={},
                execution_phases={},
                total_queries=1,
                successful_queries=1,
                failed_queries=0,
                total_execution_time=1.0,
                average_query_time=1.0,
                test_execution_type="maintenance",
            )
            mock_get_adapter.return_value = mock_adapter
            result = orchestrator.execute_benchmark(cfg, sys_prof, db_cfg)
        assert mock_adapter.run_benchmark.called
        assert result.test_execution_type == "maintenance"

    def test_tpc_metrics_passthrough(self):
        """Verify TPC metrics are adapter-provided and passed through in platform results."""
        from datetime import datetime

        from benchbox.core.results.models import BenchmarkResults

        res = BenchmarkResults(
            benchmark_name="TPC-H",
            platform="duckdb",
            scale_factor=0.01,
            execution_id="m1",
            timestamp=datetime.now(),
            duration_seconds=1.0,
            query_definitions={},
            execution_phases={},
            total_queries=22,
            successful_queries=22,
            failed_queries=0,
            total_execution_time=1.0,
            average_query_time=0.1,
            test_execution_type="power",
            power_at_size=123.0,
        )
        assert res.test_execution_type == "power"
        assert res.power_at_size == 123.0

    def test_tpc_metrics_none_when_not_provided(self):
        """When adapter doesn't provide metrics, fields remain unset/None."""
        from datetime import datetime

        from benchbox.core.results.models import BenchmarkResults

        res = BenchmarkResults(
            benchmark_name="TPC-DS",
            platform="duckdb",
            scale_factor=0.1,
            execution_id="m2",
            timestamp=datetime.now(),
            duration_seconds=2.0,
            query_definitions={},
            execution_phases={},
            total_queries=10,
            successful_queries=8,
            failed_queries=2,
            total_execution_time=2.0,
            average_query_time=0.2,
        )
        assert getattr(res, "power_at_size", None) is None
        assert getattr(res, "throughput_at_size", None) is None


class TestResultHandling:
    """Test TPC result handling and metrics."""

    def test_benchmark_results_tpc_fields(self):
        """Test that BenchmarkResults includes TPC fields."""
        result = BenchmarkResults(
            benchmark_name="TPC-H",
            platform="duckdb",
            scale_factor=0.001,
            execution_id="test-id",
            timestamp=datetime.now(),
            duration_seconds=120.0,
            total_queries=5,
            successful_queries=5,
            failed_queries=0,
            query_results=[],
            total_execution_time=100.0,
            average_query_time=20.0,
            data_loading_time=10.0,
            schema_creation_time=5.0,
            total_rows_loaded=1000,
            data_size_mb=50.0,
            table_statistics={"orders": 100},
            test_execution_type="power",
            power_at_size=1500.0,
            geometric_mean_execution_time=18.5,
        )

        # Verify TPC fields are properly set
        assert result.test_execution_type == "power"
        assert result.power_at_size == 1500.0
        assert result.throughput_at_size is None  # Not set for power test
        assert result.geometric_mean_execution_time == 18.5

    def test_cli_benchmark_results_tpc_fields(self):
        """Test that CLI BenchmarkResults includes TPC fields."""
        from benchbox.cli.types import BenchmarkResults

        result = make_benchmark_results(
            benchmark_id="tpch-001",
            benchmark_name="TPC-H",
            scale_factor=0.001,
            test_execution_type="throughput",
            throughput_at_size=2500.0,
            geometric_mean_execution_time=15.2,
        )

        # Verify TPC fields are properly set
        assert isinstance(result, BenchmarkResults)
        assert result.test_execution_type == "throughput"
        assert result.power_at_size is None  # Not set for throughput test
        assert result.throughput_at_size == 2500.0
        assert result.geometric_mean_execution_time == 15.2


if __name__ == "__main__":
    pytest.main([__file__])
