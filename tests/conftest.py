"""Common test fixtures for BenchBox.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
import tempfile
from collections.abc import Generator, Iterable
from contextlib import ExitStack
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from unittest.mock import patch

import pytest

from benchbox.core.results.models import BenchmarkResults

# Check for optional compression library availability
try:
    import zstandard

    ZSTD_AVAILABLE = True
except ImportError:
    ZSTD_AVAILABLE = False

# Register fixture plugins - this must come before any imports from those modules
# to allow pytest to rewrite assertions in the fixture modules
pytest_plugins = [
    "tests.fixtures.database_fixtures",
    "tests.fixtures.test_data_fixtures",
]


def pytest_configure(config) -> None:
    """Configure pytest with enhanced test organization and optimization settings."""
    # Configure DuckDB for optimal testing performance
    config.option.duckdb_memory_limit = "2GB"
    config.option.duckdb_threads = 2  # Limit for CI environments
    config.option.duckdb_enable_extensions = True

    # Markers are defined in pytest.ini - no need to register here


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

    # Skip tests that require zstd when it's not available
    if item.get_closest_marker("requires_zstd") and not ZSTD_AVAILABLE:
        pytest.skip("zstandard library not installed")


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
        # Cloud storage tests have network-like operations
        "tests/unit/utils/test_cloud_storage.py",
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
        marker_names = {marker.name for marker in item.iter_markers()}

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


@pytest.fixture
def tpch_c_tools_path() -> Path:
    """Return path to TPC-H C tools if available."""
    project_root = Path(__file__).parent.parent
    return project_root / "_sources" / "tpc-h" / "dbgen"


@pytest.fixture
def skip_if_no_c_tools() -> None:
    """Skip test if TPC-H C tools are not available."""
    # NOTE: This fixture is obsolete but preserved for backward compatibility
    # Ultra-simplified implementation assumes qgen is always available
    pytest.skip("C tools compatibility testing removed in ultra-simplified implementation")


@pytest.fixture
def c_compatible_test_config() -> dict[str, Any]:
    """Configuration for C-compatible tests."""
    return {
        "timeout": 60,
        "max_retries": 3,
        "comparison_threshold": 0.85,
        "ignore_whitespace": True,
        "ignore_case": True,
        "verbose": False,
    }


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
