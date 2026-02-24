from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from benchbox.cli.commands.run import _export_orchestrated_result

pytestmark = pytest.mark.fast


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Click command mock.patch requires Python 3.11+ for attribute access",
)
def test_export_helper_applies_directory_manager_naming_and_output_dir() -> None:
    result = SimpleNamespace(execution_id="abc123", output_filename=None)

    result_path = Path("/tmp/results/tpch_sf0_01_duckdb_power_abc123_20260220_000000.json")
    orchestrator = Mock()
    orchestrator.directory_manager.get_result_path.return_value = result_path
    orchestrator.directory_manager.results_dir = Path("/tmp/results")

    with patch("benchbox.cli.commands.run.ResultExporter") as exporter_cls:
        exporter = exporter_cls.return_value
        exporter.export_result.return_value = {"json": str(result_path)}

        exported = _export_orchestrated_result(
            orchestrator=orchestrator,
            result=result,
            benchmark="tpch",
            scale=0.01,
            platform_label="duckdb",
            mode_label="power",
            quiet=False,
            export_formats=["json"],
        )

    assert exported["json"] == str(result_path)
    assert result.output_filename == result_path.name
    assert exporter.output_dir == orchestrator.directory_manager.results_dir
    exporter.export_result.assert_called_once_with(result, ["json"])


@pytest.mark.skipif(
    sys.version_info < (3, 11),
    reason="Click command mock.patch requires Python 3.11+ for attribute access",
)
def test_export_helper_defaults_to_json_format() -> None:
    result = SimpleNamespace(execution_id="abc123", output_filename=None)
    result_path = Path("/tmp/results/out.json")

    orchestrator = Mock()
    orchestrator.directory_manager.get_result_path.return_value = result_path
    orchestrator.directory_manager.results_dir = Path("/tmp/results")

    with patch("benchbox.cli.commands.run.ResultExporter") as exporter_cls:
        exporter = exporter_cls.return_value
        exporter.export_result.return_value = {"json": str(result_path)}
        _export_orchestrated_result(
            orchestrator=orchestrator,
            result=result,
            benchmark="tpch",
            scale=0.01,
            platform_label="duckdb",
            mode_label="power",
            quiet=False,
            export_formats=None,
        )

    exporter.export_result.assert_called_once_with(result, ["json"])
