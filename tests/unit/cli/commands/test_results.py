from __future__ import annotations

import json
from importlib import import_module
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from benchbox.cli.commands.results import results

pytestmark = pytest.mark.fast


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


def test_results_group_without_subcommand_shows_summary(cli_runner, monkeypatch):
    results_module = import_module("benchbox.cli.commands.results")
    exporter = MagicMock()
    monkeypatch.setattr(results_module, "ResultExporter", lambda: exporter)

    result = cli_runner.invoke(results, [])
    assert result.exit_code == 0
    exporter.show_results_summary.assert_called_once_with()


def test_show_cli_reconstructs_legacy_command(cli_runner, tmp_path):
    result_file = tmp_path / "result.json"
    result_file.write_text(
        json.dumps(
            {
                "platform": "duckdb",
                "benchmark_name": "tpch",
                "scale_factor": 0.01,
                "run": {"query_subset": ["Q1", "Q2"]},
            }
        )
    )

    result = cli_runner.invoke(results, ["show-cli", str(result_file)])
    assert result.exit_code == 0
    assert "benchbox run --platform duckdb --benchmark tpch --scale 0.01" in result.output
    assert "--queries Q1,Q2" in result.output


def test_show_cli_reports_missing_fields(cli_runner, tmp_path):
    result_file = tmp_path / "bad.json"
    result_file.write_text(json.dumps({"platform": "duckdb"}))

    result = cli_runner.invoke(results, ["show-cli", str(result_file)])
    assert result.exit_code == 0
    assert "Could not extract required fields" in result.output


def test_show_cli_reports_invalid_json(cli_runner, tmp_path):
    result_file = tmp_path / "invalid.json"
    result_file.write_text("{not valid json")

    result = cli_runner.invoke(results, ["show-cli", str(result_file)])
    assert result.exit_code == 0
    assert "Invalid JSON file" in result.output
