import pytest
from click.testing import CliRunner

from benchbox.cli.main import cli

pytestmark = pytest.mark.fast


def test_quiet_mode_suppresses_all_output():
    runner = CliRunner()
    # Use data-only to avoid requiring any external database dependencies
    result = runner.invoke(
        cli,
        [
            "run",
            "--platform",
            "duckdb",
            "--benchmark",
            "tpch",
            "--phases",
            "generate",
            "--quiet",
        ],
    )
    assert result.exit_code == 0, result.output
    assert result.output.strip() == ""


def test_default_mode_outputs_messages():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "run",
            "--platform",
            "duckdb",
            "--benchmark",
            "tpch",
            "--phases",
            "generate",
        ],
    )
    assert result.exit_code == 0
    # Expect some visible output (e.g., initialization line)
    assert "Initializing" in result.output or "Data-only" in result.output
