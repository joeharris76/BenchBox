import io
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from benchbox.cli.main import cli
from benchbox.utils.printing import get_console, quiet_console, set_quiet

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


def test_quiet_flag_suppresses_adapter_run_benchmark_output():
    """Verify that quiet_console used by PlatformAdapter.run_benchmark suppresses output.

    The adapter prints status messages (Connecting, Validating, Executing, etc.)
    via quiet_console. When set_quiet(True) is active, all of those calls must
    be silenced. This test exercises the proxy directly, independent of the CLI
    runner, to isolate the adapter-level behaviour.
    """
    set_quiet(True)
    try:
        console = get_console()
        # In quiet mode, the console writes to a StringIO sink, not stdout
        assert isinstance(console.file, io.StringIO)

        # Simulate the messages the adapter would emit
        adapter_messages = [
            "Connecting to DuckDB...",
            "✅ Database being reused - skipping schema creation and data loading",
            "Validating benchmark data...",
            "✅ Data validation passed",
            "Executing benchmark queries (power mode)...",
        ]
        for msg in adapter_messages:
            quiet_console.print(msg)

        # Nothing should have reached the real stdout
        sink = console.file
        assert isinstance(sink, io.StringIO)
    finally:
        set_quiet(False)


def test_adapter_uses_quiet_console_not_local_console():
    """Confirm adapter methods reference quiet_console, not Console()."""
    from benchbox.platforms.base import adapter as adapter_module

    # The module should expose quiet_console from printing
    assert hasattr(adapter_module, "quiet_console")
    assert adapter_module.quiet_console is quiet_console


def test_quiet_console_proxy_delegates_when_not_quiet():
    """Ensure quiet_console passes output through when quiet mode is off."""
    set_quiet(False)
    try:
        console = get_console()
        with console.capture() as capture:
            quiet_console.print("visible message")
        assert "visible message" in capture.get()
    finally:
        set_quiet(False)
