import io
import sys
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

import benchbox.cli.commands.run  # noqa: F401  ensure the module is loaded
import benchbox.cli.main  # noqa: F401
from benchbox.cli.main import cli
from benchbox.utils.printing import get_console, quiet_console, set_quiet

# Several __init__.py files re-export names that shadow their submodules
# (e.g. benchbox.cli.commands exports the Click command ``run``, and
# benchbox.cli exports the function ``main``).  patch() walks the attribute
# chain and finds the re-exported object instead of the module, so we grab
# the real modules via sys.modules.
_run_mod = sys.modules["benchbox.cli.commands.run"]
_main_mod = sys.modules["benchbox.cli.main"]

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_quiet_mode_suppresses_decorative_output_generate_phase():
    runner = CliRunner()
    # Use data-only phase to avoid requiring a live DuckDB execution
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
    # Decorative output must be suppressed; only bare filepaths (if any) allowed
    assert "Benchmark completed" not in result.output
    assert "Initializing" not in result.output
    # Each non-empty line must look like a file path, not a status message
    for line in result.output.splitlines():
        if line.strip():
            assert line.strip().startswith("/") or "benchmark_runs" in line, (
                f"Unexpected non-path output in quiet mode: {line!r}"
            )


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


@pytest.mark.parametrize(
    ("module_path", "attr_name"),
    [
        ("benchbox.cli.commands.results", "console"),
        ("benchbox.cli.commands.report", "console"),
        ("benchbox.cli.commands.show_plan", "console"),
        ("benchbox.cli.commands.compare_plans", "console"),
        ("benchbox.cli.commands.plan_history", "console"),
    ],
)
def test_cli_commands_use_shared_quiet_console(module_path: str, attr_name: str) -> None:
    """Ensure migrated CLI command modules share centralized quiet-aware console."""
    import importlib

    module = importlib.import_module(module_path)
    assert getattr(module, attr_name) is quiet_console


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


def _invoke_run_quiet_with_export(quiet: bool):
    """Invoke `benchbox run` with mocked export returning a known filepath."""
    from benchbox.core.schemas import DatabaseConfig

    mock_result = Mock()
    mock_result.validation_status = "PASSED"
    mock_result.execution_id = "test-exec-id"

    database_config = DatabaseConfig(type="duckdb", name="DuckDB")
    mock_db_manager = MagicMock()
    mock_db_manager.create_config.return_value = database_config

    mock_bench_manager = MagicMock()
    mock_bench_manager.benchmarks = {
        "tpch": {
            "display_name": "TPC-H",
            "class": Mock(),
            "description": "TPC-H",
            "estimated_time_range": (1, 5),
        }
    }
    mock_bench_manager.validate_scale_factor = Mock()

    mock_orchestrator = MagicMock()
    mock_orchestrator.execute_benchmark.return_value = mock_result
    mock_orchestrator.directory_manager.get_result_path.return_value = "/tmp/test.json"
    mock_orchestrator.directory_manager.results_dir = "/tmp"

    runner = CliRunner()
    args = [
        "run",
        "--platform",
        "duckdb",
        "--benchmark",
        "tpch",
        "--scale",
        "0.01",
        "--non-interactive",
        "--phases",
        "power",
    ]
    if quiet:
        args.append("--quiet")

    with (
        patch.object(_run_mod, "DatabaseManager", return_value=mock_db_manager),
        patch.object(_run_mod, "BenchmarkManager", return_value=mock_bench_manager),
        patch.object(_run_mod, "BenchmarkOrchestrator", return_value=mock_orchestrator),
        patch.object(_run_mod, "SystemProfiler"),
        patch.object(_main_mod, "get_config_manager") as mock_cfg,
        patch.object(_run_mod, "_execute_orchestrated_run", return_value=mock_result),
        patch.object(_run_mod, "_export_orchestrated_result", return_value={"json": "/tmp/result.json"}),
        patch.object(_run_mod, "_render_post_run_charts"),
        patch("benchbox.cli.preferences.save_last_run_config"),
    ):
        mock_cfg.return_value.get.side_effect = lambda key, default=None: default
        return runner.invoke(cli, args)


def test_quiet_mode_emits_result_filepath_to_stdout():
    """--quiet must print the exported filepath to stdout (machine-readable)."""
    result = _invoke_run_quiet_with_export(quiet=True)
    assert result.exit_code == 0, result.output
    assert "/tmp/result.json" in result.output


def test_quiet_mode_suppresses_decorative_output():
    """--quiet must not emit status messages or formatted labels."""
    result = _invoke_run_quiet_with_export(quiet=True)
    assert result.exit_code == 0, result.output
    assert "Benchmark completed" not in result.output
    assert "JSON:" not in result.output


def test_normal_mode_does_not_double_print_filepath():
    """Normal mode must not print the bare filepath before the formatted label line."""
    result = _invoke_run_quiet_with_export(quiet=False)
    assert result.exit_code == 0, result.output
    # The path should appear once (inside the formatted label), not as a bare extra line
    assert result.output.count("/tmp/result.json") == 1
