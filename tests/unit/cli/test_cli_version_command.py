"""Tests for the --version flag on the BenchBox CLI."""

import pytest
from click.testing import CliRunner

from benchbox.cli.main import cli

pytestmark = pytest.mark.fast


def test_cli_version_flag_displays_version_report():
    """Running benchbox --version should show the formatted version report."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert "BenchBox Version:" in result.output
    assert "Version Consistency:" in result.output
    assert "Python Version:" in result.output
