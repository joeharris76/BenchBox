"""Common test fixtures for BenchBox.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

# ── Limit library-internal parallelism ────────────────────────────────────
# Must be set BEFORE importing any native library (polars, numpy, etc.).
# With pytest-xdist each worker is a separate process; libraries that default
# to using all CPU cores (polars, DuckDB, BLAS, OpenMP) multiply effective
# parallelism by the worker count, causing CPU oversubscription and machine
# lock-ups on developer workstations.
import os
from types import FrameType

os.environ.setdefault("POLARS_MAX_THREADS", "2")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
# DuckDB ignores env vars; patched in pytest_configure below.

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


def _is_xdist_remote_exec_namespace(globals_dict: dict[str, Any]) -> bool:
    """Return True for the live xdist remote.py ``__channelexec__`` globals."""
    return globals_dict.get("__name__") == "__channelexec__" and callable(globals_dict.get("worker_title"))


def _suppress_xdist_worker_title(start_frame: FrameType | None = None) -> bool:
    """Replace xdist's live ``worker_title`` when its exec frame is on the stack."""
    import inspect

    frame = start_frame or inspect.currentframe()
    if frame is None:
        return False
    if start_frame is None:
        frame = frame.f_back

    try:
        while frame is not None:
            if _is_xdist_remote_exec_namespace(frame.f_globals):
                frame.f_globals["worker_title"] = lambda title: None
                return True
            frame = frame.f_back
    finally:
        del frame  # avoid reference cycle

    return False


def pytest_configure(config) -> None:
    """Configure pytest with enhanced test organization and optimization settings."""
    global _test_lock_fd

    # Suppress setproctitle on macOS to prevent launchservicesd CPU storm.
    #
    # Root cause: xdist calls setproctitle() twice per test (running/idle)
    # via xdist.remote.worker_title().  At ~200 calls/second this triggers
    # macOS launchservicesd to rebuild its process registry continuously,
    # consuming 200%+ CPU and ~900 MB RSS — the actual root cause of
    # the macOS beachball during parallel test runs.
    #
    # In xdist workers, remote.py runs in an execnet __channelexec__
    # namespace, so `import xdist.remote` loads a different module object
    # than the one executing.  We walk the call stack to find the real
    # xdist exec namespace and patch its worker_title there.
    if sys.platform == "darwin" and hasattr(config, "workerinput"):
        _suppress_xdist_worker_title()

    # Acquire exclusive lock to prevent concurrent parallel test runs from
    # competing for CPU. Only the controller process (not xdist workers) locks.
    if _should_acquire_test_lock(config):
        _TEST_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(_TEST_LOCK_PATH), os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0), 0o644)
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (BlockingIOError, OSError):
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

    # Limit DuckDB internal threads.  DuckDB ignores environment variables;
    # the only reliable method is passing config={'threads': N} to connect().
    # Monkey-patch duckdb.connect so ALL connections created during tests
    # default to 2 threads (preserving explicit overrides).
    try:
        import duckdb as _duckdb_mod

        _original_duckdb_connect = _duckdb_mod.connect

        def _limited_duckdb_connect(*args, **kwargs):
            cfg = kwargs.get("config") or {}
            if isinstance(cfg, dict) and "threads" not in cfg:
                cfg["threads"] = "2"
                kwargs["config"] = cfg
            return _original_duckdb_connect(*args, **kwargs)

        _duckdb_mod.connect = _limited_duckdb_connect
    except ImportError:
        pass


def pytest_unconfigure(config) -> None:
    """Release the parallel run lock when the session ends."""
    global _test_lock_fd
    if _test_lock_fd is not None:
        try:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(_test_lock_fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

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
