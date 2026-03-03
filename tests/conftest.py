"""Common test fixtures for BenchBox.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import fcntl
import os
import sys
import tempfile
import time
import warnings
from collections.abc import Generator, Iterable
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

import pytest

# Sphinx 11 deprecations in third-party extensions (sphinx_tags, myst_parser, ablog, napoleon).
# Guarded because older Sphinx versions (e.g. on Python 3.10) lack this class,
# and --strict-config in pytest.ini would abort on an unresolvable category.
try:
    from sphinx.deprecation import RemovedInSphinx11Warning

    warnings.filterwarnings("ignore", category=RemovedInSphinx11Warning)
except (ImportError, AttributeError):
    pass

from benchbox.core.results.models import BenchmarkResults

# zstandard is a runtime dependency (always available)
ZSTD_AVAILABLE = True

# Register fixture plugins - this must come before any imports from those modules
# to allow pytest to rewrite assertions in the fixture modules
pytest_plugins = [
    "tests.fixtures.database_fixtures",
    "tests.fixtures.test_data_fixtures",
    "tests.fixtures.result_dict_fixtures",
]

# ── Parallel test run mutual exclusion ──────────────────────────────────────
_TEST_LOCK_PATH = Path.home() / ".benchbox" / "test.lock"
_test_lock_fd: int | None = None  # Kept open to hold the flock for the session lifetime.


def _should_acquire_test_lock(config: pytest.Config) -> bool:
    """Return True when this process should compete for the parallel run lock.

    Skips lock acquisition for xdist worker subprocesses (only the controller
    process locks), when parallelism is disabled (-n 0 / no numprocesses), or
    when BENCHBOX_SKIP_TEST_LOCK is set in the environment.
    """
    if hasattr(config, "workerinput"):
        return False  # xdist worker — the controller holds the lock on our behalf
    if os.environ.get("BENCHBOX_SKIP_TEST_LOCK"):
        return False  # explicit opt-out (e.g. intentional concurrent debug runs)
    try:
        n = config.option.numprocesses
    except AttributeError:
        return False  # xdist not installed or numprocesses not yet registered
    # Lock for '-n auto' (string) or any explicit positive worker count.
    # bool(0) is False so n != 0 would be redundant — bool(n) is sufficient.
    # Assumes numprocesses is None, 0, "auto", or a positive int (pytest-xdist contract).
    return bool(n)


def pytest_configure(config) -> None:
    """Configure pytest with enhanced test organization and optimization settings."""
    global _test_lock_fd

    # Acquire exclusive lock to prevent concurrent parallel test runs from
    # competing for CPU. Only the controller process (not xdist workers) locks.
    if _should_acquire_test_lock(config):
        _TEST_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(_TEST_LOCK_PATH), os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0), 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            # Another parallel run holds the lock — fail fast with a clear message.
            try:
                holder_info = _TEST_LOCK_PATH.read_text().strip()
            except OSError:
                holder_info = "(could not read lock file)"
            os.close(fd)
            # Use os._exit() rather than sys.exit(): pytest_configure is called
            # before the session loop so SystemExit bubbles up as INTERNALERROR.
            # os._exit() terminates the process immediately with the given code.
            sys.stderr.write(
                f"\n\033[91m[benchbox] BLOCKED: A parallel test run is already active.\033[0m\n"
                f"  Lock file : {_TEST_LOCK_PATH}\n"
                f"  Holder    : {holder_info}\n\n"
                f"  Options:\n"
                f"    \u2022 Wait for the other run to finish and retry.\n"
                f"    \u2022 Kill the other run, then retry.\n"
                f"    \u2022 Run single-threaded (no lock):  pytest -n 0 ...\n"
                f"    \u2022 Bypass lock (dangerous):         BENCHBOX_SKIP_TEST_LOCK=1 pytest ...\n\n"
            )
            sys.stderr.flush()
            os._exit(1)
        # Write diagnostic info so other processes can identify the lock holder.
        # ftruncate is safe here: O_RDWR opens at position 0, so the subsequent
        # write lands at offset 0 without needing an explicit seek.
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        cmd = " ".join(sys.argv[:4])
        try:
            os.ftruncate(fd, 0)
            os.write(fd, f"pid:{os.getpid()} started:{started} cmd:{cmd}\n".encode())
        except OSError:
            pass
        _test_lock_fd = fd  # Keep fd open to maintain the lock for the whole session.

    # Configure DuckDB for optimal testing performance
    config.option.duckdb_memory_limit = "2GB"
    config.option.duckdb_threads = 2  # Limit for CI environments
    config.option.duckdb_enable_extensions = True

    # Markers are defined in pytest.ini - no need to register here


def pytest_unconfigure(config) -> None:
    """Release the parallel run lock when the session ends."""
    global _test_lock_fd
    if _test_lock_fd is not None:
        try:
            fcntl.flock(_test_lock_fd, fcntl.LOCK_UN)
            os.close(_test_lock_fd)
        except OSError:
            pass
        _test_lock_fd = None


def make_benchmark_results(
    *,
    benchmark_id: str = "test-benchmark",
    benchmark_name: str = "Test Benchmark",
    execution_id: str = "test-exec",
    platform: str = "cli",
    scale_factor: float = 0.01,
    duration_seconds: float = 0.0,
    timestamp: Optional[datetime] = None,
    total_queries: int = 0,
    successful_queries: int = 0,
    failed_queries: int = 0,
    query_results: Optional[Iterable[Any]] = None,
    validation_status: str = "UNKNOWN",
    validation_details: Optional[dict[str, Any]] = None,
    execution_metadata: Optional[dict[str, Any]] = None,
    summary_metrics: Optional[dict[str, Any]] = None,
    query_subset: Optional[list[str]] = None,
    concurrency_level: Optional[int] = None,
    **extras: Any,
) -> BenchmarkResults:
    """Create a fully-populated BenchmarkResults instance for tests."""

    field_names = set(BenchmarkResults.__dataclass_fields__.keys())

    init_kwargs: dict[str, Any] = {
        "benchmark_name": benchmark_name,
        "platform": platform,
        "scale_factor": scale_factor,
        "execution_id": execution_id,
        "timestamp": timestamp or datetime.now(),
        "duration_seconds": duration_seconds,
        "total_queries": total_queries,
        "successful_queries": successful_queries,
        "failed_queries": failed_queries,
        "query_results": list(query_results or []),
        "validation_status": validation_status,
        "validation_details": validation_details or {},
        "execution_metadata": execution_metadata or {},
    }

    # Pass through known dataclass fields supplied via extras.
    for key, value in extras.items():
        if key in field_names:
            init_kwargs[key] = value

    result = BenchmarkResults(**init_kwargs)

    # Attach optional metadata used by legacy CLI tests.
    result._benchmark_id_override = benchmark_id
    result.summary_metrics = summary_metrics or {}
    if query_subset is not None:
        result.query_subset = query_subset
    if concurrency_level is not None:
        result.concurrency_level = concurrency_level

    # Allow additional loose attributes (e.g., benchmark_version) expected by tests.
    for key, value in extras.items():
        if key not in field_names:
            setattr(result, key, value)

    return result


@pytest.fixture
def make_results():
    """Factory fixture for creating BenchmarkResults with sensible defaults.

    Usage::

        def test_something(make_results):
            result = make_results(platform="snowflake", total_queries=22)
    """
    return make_benchmark_results


def pytest_runtest_setup(item) -> None:
    """Set up test-specific configurations based on markers."""
    # Set timeouts based on speed markers
    if item.get_closest_marker("fast"):
        item.config.option.timeout = 30
    elif item.get_closest_marker("medium"):
        item.config.option.timeout = 120
    elif item.get_closest_marker("slow"):
        item.config.option.timeout = 600

    # Skip tests based on environment
    if item.get_closest_marker("skip_ci") and os.environ.get("CI"):
        pytest.skip("Skipped in CI environment")

    if item.get_closest_marker("local_only") and os.environ.get("CI"):
        pytest.skip("Local-only test skipped in CI")


# Paths with known slow tests (>10s) - should be marked "slow" not "fast"
# These patterns are matched against the test path string
SLOW_SPEED_PATTERNS = frozenset(
    {
        # Databricks adapter tests have heavy import overhead (~300s)
        "tests/unit/platforms/databricks/test_databricks_df_adapter.py",
        # Platform info tests load all platform modules (~300s)
        "tests/unit/platforms/test_platform_info.py",
        # CLI power/throughput tests spawn subprocesses (~10-15s)
        "tests/unit/cli/test_cli_power_throughput.py",
        # TPC-DS integration tests run dsdgen at SF 1.0 (minimum, takes several minutes)
        "tests/integration/test_tpcds_query_generation.py",
        # Sphinx docs build takes several minutes
        "tests/docs/",
    }
)

# Paths with medium-speed tests (1-10s) - should be marked "medium" not "fast"
MEDIUM_SPEED_PATTERNS = frozenset(
    {
        # TPC data generation tests invoke binaries (2-5s per test)
        "tests/unit/core/tpch/test_tpch_stdout_datagen.py",
        "tests/unit/core/tpcds/test_tpcds_stdout_datagen.py",
        # CLI execution tests spawn subprocesses (2-5s per test)
        "tests/unit/cli/test_cli_test_execution.py",
        "tests/unit/cli/test_cli_main.py",
        "tests/unit/examples/",
        # Concurrency tests have sleep/wait time
        "tests/unit/core/concurrency/",
        # TPC-DI ETL sources generate data
        "tests/unit/core/tpcdi/test_etl_sources.py",
        # TPC-DS OBT queries parse SQL in DuckDB
        "tests/unit/core/tpcds_obt/test_tpcds_obt_queries.py",
        # System info tests profile the machine
        "tests/unit/utils/test_system_info.py",
        # Error handling tests have concurrent file access
        "tests/unit/test_error_handling.py",
        # Release infrastructure tests invoke CLI entry points
        "tests/unit/test_release_infrastructure.py",
        # Query generation preflight invokes dsqgen
        "tests/unit/test_query_generation_preflight.py",
        # BigQuery staging root tests
        "tests/unit/platforms/test_bigquery_staging_root.py",
        # Plan capture tests have timeouts
        "tests/unit/platforms/test_plan_capture_errors.py",
        # Base benchmark tests generate data
        "tests/test_base.py",
        # TPC-DS enhanced tests
        "tests/unit/test_tpcds_enhanced.py",
        # CLI exceptions tests with validation
        "tests/unit/cli/test_exceptions.py",
    }
)

# Marker names that are known to create high CPU/memory or import pressure when
# fanned out across many xdist workers.
# Tests in these categories are forced into the serial lane in `make test-all`.
# Keep this focused on tests that materially increase CPU/memory/import pressure.
RESOURCE_HEAVY_MARKERS = frozenset(
    {
        "slow",
        "performance",
        "live_integration",
        "cloud_import",
        "requires_table_formats",
    }
)

# Extremely expensive suites are routed to explicit stress lane.
# They remain covered, but are excluded from default `make test-all` runs.
STRESS_PATH_PATTERNS = frozenset(
    {
        "tests/performance/",
    }
)


def _get_speed_marker_for_path(test_path: Path) -> str | None:
    """Determine speed marker based on test path patterns.

    Returns:
        'slow' for slow tests (>10s), 'medium' for medium tests (1-10s),
        None to use default behavior.
    """
    path_str = str(test_path)

    # Check slow patterns first (highest priority)
    for pattern in SLOW_SPEED_PATTERNS:
        if pattern in path_str:
            return "slow"

    # Check medium patterns
    for pattern in MEDIUM_SPEED_PATTERNS:
        if pattern in path_str:
            return "medium"

    return None


def pytest_collection_modifyitems(config, items) -> None:
    """Apply directory-based defaults for markers with speed-aware classification."""
    for item in items:
        test_path = Path(str(item.fspath))
        path_str = str(test_path)
        marker_names = {marker.name for marker in item.iter_markers()}

        if "stress" not in marker_names:
            for pattern in STRESS_PATH_PATTERNS:
                if pattern in path_str:
                    item.add_marker(pytest.mark.stress)
                    marker_names.add("stress")
                    break

        # Directory-driven test categories
        if "unit" not in marker_names and "integration" not in marker_names and "performance" not in marker_names:
            if any(part == "unit" for part in test_path.parts):
                item.add_marker(pytest.mark.unit)
                marker_names.add("unit")
            elif any(part == "integration" for part in test_path.parts):
                item.add_marker(pytest.mark.integration)
                marker_names.add("integration")
            elif any(part == "performance" for part in test_path.parts):
                item.add_marker(pytest.mark.performance)
                marker_names.add("performance")

        # Speed markers with pattern-based classification
        # Always assign one of fast/medium/slow even when stress is present.
        if not {"fast", "medium", "slow"} & marker_names:
            # First check if this path matches known slow/medium patterns
            speed_marker = _get_speed_marker_for_path(test_path)

            if speed_marker == "slow":
                item.add_marker(pytest.mark.slow)
                marker_names.add("slow")
            elif speed_marker == "medium":
                item.add_marker(pytest.mark.medium)
                marker_names.add("medium")
            elif "unit" in marker_names:
                # Default: unit tests are fast unless pattern-matched above
                item.add_marker(pytest.mark.fast)
                marker_names.add("fast")
            elif "integration" in marker_names:
                # Integration tests are medium by default
                item.add_marker(pytest.mark.medium)
                marker_names.add("medium")
            elif "performance" in marker_names:
                item.add_marker(pytest.mark.slow)
                marker_names.add("slow")
            else:
                item.add_marker(pytest.mark.medium)
                marker_names.add("medium")

        # Route all existing slow tests into the explicit stress lane.
        # This preserves coverage while keeping default `make test-all` lean.
        if "slow" in marker_names and "stress" not in marker_names:
            item.add_marker(pytest.mark.stress)
            marker_names.add("stress")

        if marker_names & RESOURCE_HEAVY_MARKERS and "resource_heavy" not in marker_names:
            item.add_marker(pytest.mark.resource_heavy)


@pytest.fixture(autouse=True)
def _provide_fake_duckdb(monkeypatch):
    """Provide a lightweight duckdb stub when the optional dependency is missing."""
    from benchbox.platforms import duckdb as duckdb_module  # import late to honour patching

    if getattr(duckdb_module, "duckdb", None) is not None:
        yield
        return

    class _FakeCursor:
        def __init__(self):
            self.statements: list[str] = []

        def execute(self, sql: str):
            self.statements.append(sql)
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            return None

        def fetchmany(self, size=None):  # pragma: no cover - interface completeness
            return []

        def close(self):
            return None

    class _FakeDuckDBModule:
        __version__ = "0.0-test"

        @staticmethod
        def connect(_database_path: str):
            return _FakeCursor()

    monkeypatch.setattr(duckdb_module, "duckdb", _FakeDuckDBModule(), raising=False)
    yield


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def available_compression_type() -> str:
    """Return a compression type that is available on this system.

    Returns 'zstd' if zstandard is installed, otherwise 'gzip'.
    This is useful for tests that need compression but don't specifically
    require zstd.
    """
    return "zstd" if ZSTD_AVAILABLE else "gzip"


@pytest.fixture
def zstd_available() -> bool:
    """Return whether zstandard library is available."""
    return ZSTD_AVAILABLE


# Scale factor fixtures are now consolidated in benchmark_fixtures.py
# These are kept for backward compatibility but deprecated
@pytest.fixture
def small_scale_factor() -> float:
    """Return a small scale factor for quick testing.

    DEPRECATED: Use scale_factor fixture from benchmark_fixtures.py instead.
    """
    return 1.0


@pytest.fixture
def medium_scale_factor() -> float:
    """Return a medium scale factor for more thorough testing.

    DEPRECATED: Use scale_factor fixture from benchmark_fixtures.py instead.
    """
    return 1.0


@pytest.fixture(params=["sqlite", "postgres", "mysql", "bigquery", "snowflake"])
def sql_dialect(request) -> str:
    """Parameterized fixture for testing different SQL dialects."""
    return request.param


@pytest.fixture
def mock_environment_variables() -> Generator[None, None, None]:
    """Set up environment variables for testing and restore them afterwards."""
    original_env = os.environ.copy()
    try:
        # Set environment variables for testing
        os.environ["BENCHBOX_DATA_DIR"] = "/tmp/benchbox-test-data"
        yield
    finally:
        # Restore original environment variables
        os.environ.clear()
        os.environ.update(original_env)


def pytest_sessionstart(session) -> None:
    """Create test databases before the test session starts."""
    # Skip database setup for unit-only test runs
    markers = set()
    for item in session.items:
        for mark in item.iter_markers():
            markers.add(mark.name)

    if markers and "integration" not in markers and "database" not in markers:
        return

    import subprocess
    import sys
    from pathlib import Path

    # the test databases directory
    test_db_dir = Path(__file__).parent / "databases"
    test_db_dir.mkdir(exist_ok=True)

    # Run the database creation script
    create_script = test_db_dir / "create_test_databases.py"
    if create_script.exists():
        try:
            result = subprocess.run(
                [sys.executable, str(create_script)],
                capture_output=True,
                text=True,
                cwd=str(Path(__file__).parent.parent),
            )
            if result.returncode != 0:
                print(f"Warning: Failed to create test databases: {result.stderr}")
        except Exception as e:
            print(f"Warning: Error creating test databases: {e}")


def pytest_sessionfinish(session, exitstatus) -> None:
    """Clean up test databases after the test session ends."""
    from pathlib import Path

    # the test databases directory
    test_db_dir = Path(__file__).parent / "databases"

    # Strip all .duckdb files
    if test_db_dir.exists():
        for db_file in test_db_dir.glob("*.duckdb"):
            try:
                db_file.unlink()
            except Exception as e:
                print(f"Warning: Could not remove {db_file}: {e}")


def pytest_terminal_summary(terminalreporter, exitstatus) -> None:
    """Emit a WARN (non-failing) when total coverage is below 80%.

    This reads the `.coverage` data file and computes overall coverage using
    the coverage.py API to avoid relying on pytest-cov's fail-under behavior.
    It does not fail the test run; it only prints a prominent warning line.
    """
    try:
        import io

        import coverage

        threshold = 80.0

        # Load existing coverage data written by pytest-cov
        cov = coverage.Coverage(data_file=".coverage", config_file=".coveragerc_core")
        cov.load()

        buf = io.StringIO()
        total = cov.report(ignore_errors=True, file=buf)  # returns float percent

        if total < threshold:
            terminalreporter.write_sep(
                "-",
                f"WARNING: Test coverage {total:.2f}% is below threshold {threshold:.0f}%",
            )
    except Exception as e:  # pragma: no cover - best-effort warning path
        # If coverage data/lib isn't available, don't break the test run.
        terminalreporter.write_line(f"Note: Coverage warning check skipped: {e}")


@pytest.fixture
def duckdb_memory_db():
    """Create an in-memory DuckDB database connection."""
    import duckdb

    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture(autouse=True)
def mock_platform_dependency_checks():
    """Provide default dependency stubs for cloud adapters in unit tests."""

    targets = [
        "benchbox.platforms.bigquery.check_platform_dependencies",
        "benchbox.platforms.databricks.adapter.check_platform_dependencies",
        "benchbox.platforms.clickhouse.check_platform_dependencies",
        "benchbox.platforms.snowflake.check_platform_dependencies",
        "benchbox.platforms.redshift.check_platform_dependencies",
    ]

    with ExitStack() as stack:
        for target in targets:
            try:
                stack.enter_context(patch(target, return_value=(True, [])))
            except AttributeError:
                # Skip patching if the attribute doesn't exist (e.g., databricks is now a package)
                pass

        try:
            from benchbox.platforms.snowflake import SnowflakeAdapter

            SnowflakeAdapter.add_cli_arguments = staticmethod(lambda parser: None)
            # Don't stub from_config - we want to test the real implementation
        except ImportError:
            pass

        try:
            from benchbox.platforms.redshift import RedshiftAdapter

            RedshiftAdapter.add_cli_arguments = staticmethod(lambda parser: None)
            # Don't stub from_config - we want to test the real implementation
        except ImportError:
            pass

        yield


@pytest.fixture
def cli_benchmark_mocks():
    """Pre-configured mocks for CLI benchmark testing.

    This fixture provides a standard set of mocks for testing CLI benchmark
    commands without spawning subprocesses or invoking real platform adapters.
    Use this to reduce CLI test execution time from ~19s to <5s.

    Returns:
        Dictionary with mock objects for BenchmarkManager, DatabaseManager,
        SystemProfiler, ConfigManager, and BenchmarkOrchestrator.

    Example:
        def test_cli_run_command(cli_benchmark_mocks, cli_runner):
            result = cli_runner.invoke(cli, ["run", "--platform", "duckdb", ...])
            assert result.exit_code == 0
    """
    with (
        patch("benchbox.cli.main.BenchmarkManager") as mock_main_manager,
        patch("benchbox.cli.main.DatabaseManager") as mock_main_db_manager,
        patch("benchbox.cli.main.SystemProfiler") as mock_main_profiler,
        patch("benchbox.cli.main.ConfigManager") as mock_config,
        patch("benchbox.cli.main.get_config_manager") as mock_get_config_manager,
        patch("benchbox.cli.orchestrator.BenchmarkOrchestrator") as mock_orchestrator,
        patch("benchbox.cli.commands.run.BenchmarkManager") as mock_run_manager,
        patch("benchbox.cli.commands.run.DatabaseManager") as mock_run_db_manager,
        patch("benchbox.cli.commands.run.SystemProfiler") as mock_run_profiler,
        patch("benchbox.cli.commands.run.BenchmarkOrchestrator") as mock_run_orchestrator,
        patch("benchbox.cli.commands.run._execute_orchestrated_run") as mock_execute_orchestrated_run,
        patch("benchbox.cli.commands.run._export_orchestrated_result") as mock_export_orchestrated_result,
        patch("benchbox.cli.commands.run._render_post_run_charts"),
        patch("benchbox.cli.preferences.save_last_run_config"),
    ):
        # Configure BenchmarkManager
        mock_manager_instance = type("MockBenchmarkManager", (), {})()
        mock_manager_instance.benchmarks = {
            "tpch": {"display_name": "TPC-H", "estimated_time_range": (2, 10)},
            "tpcds": {"display_name": "TPC-DS", "estimated_time_range": (5, 30)},
        }
        mock_manager_instance.set_verbosity = lambda *_args, **_kwargs: None
        mock_manager_instance.validate_scale_factor = lambda *_args, **_kwargs: None
        mock_main_manager.return_value = mock_manager_instance
        mock_run_manager.return_value = mock_manager_instance

        # Configure DatabaseManager
        mock_db_manager_instance = type("MockDatabaseManager", (), {})()
        mock_db_manager_instance.set_verbosity = lambda *_args, **_kwargs: None
        mock_db_config = type(
            "MockDbConfig",
            (),
            {
                "type": "duckdb",
                "options": {},
                "driver_version_actual": None,
                "driver_version_resolved": None,
            },
        )()
        mock_db_manager_instance.create_config = lambda *_args, **_kwargs: mock_db_config
        mock_main_db_manager.return_value = mock_db_manager_instance
        mock_run_db_manager.return_value = mock_db_manager_instance

        # Configure SystemProfiler
        mock_system_profile = type(
            "MockSystemProfile",
            (),
            {"cpu_cores_logical": 4, "memory_total_gb": 8},
        )()
        mock_profiler_instance = type("MockProfiler", (), {})()
        mock_profiler_instance.get_system_profile = lambda: mock_system_profile
        mock_main_profiler.return_value = mock_profiler_instance
        mock_run_profiler.return_value = mock_profiler_instance

        # Configure ConfigManager
        mock_config_instance = type("MockConfigManager", (), {})()
        mock_config_instance.config_path = Path("benchbox.yaml")
        mock_config_instance.validate_config = lambda: True
        mock_config_instance.load_unified_tuning_config = lambda *_args, **_kwargs: None
        mock_config_instance.get = lambda _key, default=None: default
        mock_config.return_value = mock_config_instance
        mock_get_config_manager.return_value = mock_config_instance

        # Configure BenchmarkOrchestrator
        mock_orchestrator_instance = type("MockOrchestrator", (), {})()
        mock_orchestrator_instance.set_verbosity = lambda *_args, **_kwargs: None
        mock_orchestrator_instance.set_custom_output_dir = lambda *_args, **_kwargs: None
        mock_orchestrator.return_value = mock_orchestrator_instance
        mock_run_orchestrator.return_value = mock_orchestrator_instance

        # Configure result execution helpers
        mock_result = type(
            "MockResult",
            (),
            {"validation_status": "PASSED", "execution_id": "mock-exec-id", "query_results": []},
        )()
        mock_execute_orchestrated_run.return_value = mock_result
        mock_export_orchestrated_result.return_value = {"json": "benchmark_runs/results/mock-exec-id.json"}

        yield {
            "manager": mock_run_manager,
            "db_manager": mock_run_db_manager,
            "profiler": mock_run_profiler,
            "config": mock_config,
            "orchestrator": mock_run_orchestrator,
        }


@pytest.fixture
def cli_runner():
    """Provide a Click CLI test runner.

    This fixture creates a CliRunner instance for testing Click CLI commands.
    Use in conjunction with cli_benchmark_mocks for fast CLI testing.

    Returns:
        click.testing.CliRunner instance
    """
    from click.testing import CliRunner

    return CliRunner()
