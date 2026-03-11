from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from benchbox.cli.main import cli

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def _mock_result() -> SimpleNamespace:
    return SimpleNamespace(
        validation_status="PASSED",
        execution_id="exec123",
        query_results=[],
        validation_details={},
    )


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Click command mock.patch requires Python 3.11+ for attribute access",
)
def test_direct_and_non_interactive_direct_use_shared_execution_and_export_helpers() -> None:
    runner = CliRunner()

    with (
        patch("benchbox.cli.commands.run.DatabaseManager") as mock_db_mgr,
        patch("benchbox.cli.commands.run.BenchmarkManager") as mock_bench_mgr,
        patch("benchbox.cli.commands.run.SystemProfiler") as mock_profiler,
        patch("benchbox.cli.commands.run.BenchmarkOrchestrator") as mock_orchestrator_cls,
        patch("benchbox.cli.commands.run._execute_orchestrated_run", return_value=_mock_result()) as mock_execute,
        patch(
            "benchbox.cli.commands.run._export_orchestrated_result",
            return_value={"json": "/tmp/result.json"},
        ) as mock_export,
    ):
        mock_bench_mgr.return_value.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (1, 2)}}
        mock_profiler.return_value.get_system_profile.return_value = Mock()
        mock_db_cfg = Mock()
        mock_db_cfg.type = "duckdb"
        mock_db_cfg.options = {}
        mock_db_mgr.return_value.create_config.return_value = mock_db_cfg
        mock_orchestrator_cls.return_value.directory_manager.results_dir = "/tmp/results"

        result_direct = runner.invoke(
            cli,
            ["run", "--platform", "duckdb", "--benchmark", "tpch", "--scale", "0.01"],
        )
        assert result_direct.exit_code == 0

        result_non_interactive = runner.invoke(
            cli,
            [
                "run",
                "--platform",
                "duckdb",
                "--benchmark",
                "tpch",
                "--scale",
                "0.01",
                "--non-interactive",
            ],
        )
        assert result_non_interactive.exit_code == 0

        # Both direct variants should route through the shared helpers.
        assert mock_execute.call_count == 2
        assert mock_export.call_count == 2


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Click command mock.patch requires Python 3.11+ for attribute access",
)
def test_load_only_uses_shared_execution_and_export_helpers() -> None:
    runner = CliRunner()

    with (
        patch("benchbox.cli.commands.run.DatabaseManager") as mock_db_mgr,
        patch("benchbox.cli.commands.run.BenchmarkManager") as mock_bench_mgr,
        patch("benchbox.cli.commands.run.SystemProfiler") as mock_profiler,
        patch("benchbox.cli.commands.run.BenchmarkOrchestrator") as mock_orchestrator_cls,
        patch("benchbox.cli.commands.run._execute_orchestrated_run", return_value=_mock_result()) as mock_execute,
        patch(
            "benchbox.cli.commands.run._export_orchestrated_result",
            return_value={"json": "/tmp/result.json"},
        ) as mock_export,
    ):
        mock_bench_mgr.return_value.benchmarks = {"tpch": {"display_name": "TPC-H", "estimated_time_range": (1, 2)}}
        mock_profiler.return_value.get_system_profile.return_value = Mock()
        mock_db_cfg = Mock()
        mock_db_cfg.type = "duckdb"
        mock_db_cfg.options = {}
        mock_db_mgr.return_value.create_config.return_value = mock_db_cfg
        mock_orchestrator_cls.return_value.directory_manager.results_dir = "/tmp/results"

        result = runner.invoke(
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
                "load",
            ],
        )
        assert result.exit_code == 0
        assert mock_execute.call_count == 1
        assert mock_export.call_count == 1
