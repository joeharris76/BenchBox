"""Run TPC-H on StarRocks (self-hosted cluster).

StarRocks is an open-source MPP (Massively Parallel Processing) OLAP database
engine, graduated from the Linux Foundation. It delivers sub-second query
latency on large-scale datasets using columnar storage, vectorized execution,
and distributed hash partitioning. StarRocks connects via the MySQL protocol
and uses Stream Load for high-throughput data ingestion over HTTP.

Prerequisites:
    1. Running StarRocks cluster with Frontend (FE) and Backend (BE) nodes
    2. FE MySQL protocol port accessible (default: 9030)
    3. BE HTTP port accessible for Stream Load (default: 8040)
    4. Database user with CREATE/DROP/INSERT/SELECT privileges

Required environment variables:
    STARROCKS_HOST           StarRocks FE hostname or IP address

Optional environment variables:
    STARROCKS_PORT           MySQL protocol port (default: 9030)
    STARROCKS_USER           Database username (default: root)
    STARROCKS_PASSWORD       Database password (default: "")
    STARROCKS_DATABASE       Target database name (default: benchbox)
    STARROCKS_HTTP_PORT      BE HTTP port for Stream Load (default: 8040)

Installation:
    uv add benchbox --extra starrocks

Usage:
    export STARROCKS_HOST=192.168.1.100

    python examples/getting_started/sql/starrocks_tpch.py

    # Preview without execution
    python examples/getting_started/sql/starrocks_tpch.py --dry-run ./preview
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from benchbox.cli.orchestrator import BenchmarkOrchestrator
from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.core.system import SystemProfiler
from benchbox.examples import execute_example_dry_run

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "starrocks"


def _require_env(var_name: str) -> str:
    """Require a StarRocks environment variable.

    StarRocks connection details should be provided via environment variables,
    especially passwords. Never hardcode credentials in scripts.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set StarRocks connection details before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for StarRocks.

    StarRocks Concepts:

    1. MYSQL PROTOCOL
       - StarRocks FE exposes a MySQL-compatible wire protocol on port 9030
       - Standard MySQL clients and drivers (PyMySQL) connect directly
       - SQL syntax is largely MySQL-compatible with OLAP extensions

    2. STREAM LOAD
       - High-throughput data ingestion via HTTP PUT to BE nodes
       - Sends CSV/JSON data directly to Backend HTTP port (default: 8040)
       - Supports parallel loading across multiple BE nodes
       - BenchBox uses Stream Load automatically during the load phase

    3. COLUMNAR STORAGE
       - Data stored in columnar format for analytical query performance
       - Duplicate Key model used for benchmark tables (no deduplication)
       - Automatic compaction and compression for storage efficiency

    4. DISTRIBUTED EXECUTION
       - Queries distributed across Backend (BE) nodes via MPP engine
       - Hash partitioning distributes data for parallel scans
       - Pipeline execution engine with vectorized operators
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    database_config = DatabaseConfig(
        type="starrocks",
        name="starrocks_tpch",
        options={
            # FE host (required) - Frontend node hostname
            "host": _require_env("STARROCKS_HOST"),
            # MySQL protocol port (optional, default: 9030)
            "port": int(os.getenv("STARROCKS_PORT", "9030")),
            # Authentication (optional, StarRocks defaults to root with no password)
            "username": os.getenv("STARROCKS_USER", "root"),
            "password": os.getenv("STARROCKS_PASSWORD", ""),
            # Target database (optional, default: benchbox)
            "database": os.getenv("STARROCKS_DATABASE", "benchbox"),
            # BE HTTP port for Stream Load ingestion (optional, default: 8040)
            "http_port": int(os.getenv("STARROCKS_HTTP_PORT", "8040")),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on StarRocks.

    StarRocks provides:
    - Sub-second OLAP query latency via vectorized execution
    - MPP distributed query engine across multiple BE nodes
    - MySQL protocol compatibility for standard tooling
    - Stream Load for high-throughput parallel data ingestion
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="starrocks_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on StarRocks.")
        print()
        print("Cluster requirements:")
        print("- FE (Frontend) node accessible on MySQL port (default: 9030)")
        print("- BE (Backend) node(s) accessible on HTTP port (default: 8040)")
        print("- Stream Load requires direct HTTP access to BE nodes")
        print()
        print("Before running:")
        print("1. Verify StarRocks cluster is healthy: mysql -h $STARROCKS_HOST -P 9030 -u root")
        print("2. Check backends are alive: SHOW BACKENDS;")
        print("3. Database will be created automatically if it doesn't exist")
        print("4. Review query translations in generated SQL files")
        return

    profiler = SystemProfiler()
    system_profile = profiler.get_system_profile()

    orchestrator = BenchmarkOrchestrator(base_dir=str(_OUTPUT_DIR))

    result = orchestrator.execute_benchmark(
        config=benchmark_config,
        system_profile=system_profile,
        database_config=database_config,
        phases_to_run=["generate", "load", "power"],
    )

    print(f"Benchmark completed at scale factor {scale_factor} with {result.successful_queries} successful queries")
    print(f"Total runtime (s): {result.total_execution_time:.2f}")
    print()
    print("Results saved to:", _OUTPUT_DIR)
    print()
    print("Next steps:")
    print("- Review results.json for detailed query timings")
    print("- Check StarRocks FE web UI (port 8030) for query profiles")
    print("- Try larger scale factors for distributed performance testing")
    print("- Compare with other OLAP platforms (ClickHouse, DuckDB)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on StarRocks")
    parser.add_argument("--scale", type=float, default=0.01, help="Benchmark scale factor")
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview without executing; write artifacts to OUTPUT_DIR.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_example(
        scale_factor=args.scale,
        dry_run_output=Path(args.dry_run) if args.dry_run else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
