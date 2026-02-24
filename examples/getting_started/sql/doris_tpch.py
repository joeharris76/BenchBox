"""Run TPC-H on Apache Doris (self-hosted cluster).

Apache Doris is a high-performance real-time analytical database based on MPP
(Massively Parallel Processing) architecture. Originally developed at Baidu as
Palo, it graduated as an Apache Top-Level Project in 2022. Doris connects via
the MySQL protocol (port 9030) and uses Stream Load for high-throughput data
ingestion over HTTP (port 8030). It supports multiple table models (Duplicate,
Aggregate, Unique, Primary Key) and provides a vectorized execution engine
for analytical workloads.

Prerequisites:
    1. Running Apache Doris cluster with Frontend (FE) and Backend (BE) nodes
    2. FE MySQL protocol port accessible (default: 9030)
    3. FE HTTP port accessible for Stream Load (default: 8030)
    4. Database user with CREATE/DROP/INSERT/SELECT privileges

Required environment variables:
    DORIS_HOST               Doris FE hostname or IP address

Optional environment variables:
    DORIS_PORT               MySQL protocol port (default: 9030)
    DORIS_USER               Database username (default: root)
    DORIS_PASSWORD           Database password (default: "")
    DORIS_DATABASE           Target database name (default: benchbox)
    DORIS_HTTP_PORT          FE HTTP port for Stream Load (default: 8030)

Installation:
    uv add benchbox --extra doris

Docker quick-start:
    docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 \\
        apache/doris:doris-all-in-one-2.1

    mysql -h 127.0.0.1 -P 9030 -u root -e "SELECT 1"

Managed cloud options:
    - VeloDB Cloud (velodb.io) - fully managed Doris by core contributors
    - SelectDB Cloud (selectdb.com) - enterprise managed Doris
    - ApsaraDB for SelectDB - managed Doris on Alibaba Cloud

Usage:
    export DORIS_HOST=192.168.1.100

    python examples/getting_started/sql/doris_tpch.py

    # Preview without execution
    python examples/getting_started/sql/doris_tpch.py --dry-run ./preview
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "doris"


def _require_env(var_name: str) -> str:
    """Require a Doris environment variable.

    Doris connection details should be provided via environment variables,
    especially passwords. Never hardcode credentials in scripts.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set Doris connection details before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Apache Doris.

    Apache Doris Concepts:

    1. MYSQL PROTOCOL
       - Doris FE exposes a MySQL-compatible wire protocol on port 9030
       - Standard MySQL clients and drivers (PyMySQL) connect directly
       - SQL syntax is largely MySQL-compatible with OLAP extensions

    2. STREAM LOAD
       - High-throughput data ingestion via HTTP PUT to the FE node
       - Sends CSV data to the FE HTTP port (default: 8030)
       - FE routes data to the appropriate BE nodes for ingestion
       - BenchBox uses Stream Load automatically when requests is installed

    3. TABLE MODELS
       - Duplicate Key: all rows preserved (used for benchmarks)
       - Aggregate: rows merged by aggregate functions
       - Unique Key: last write wins deduplication
       - Primary Key: real-time updates with merge-on-read

    4. DISTRIBUTED EXECUTION
       - Queries distributed across Backend (BE) nodes via MPP engine
       - Hash partitioning distributes data for parallel scans
       - Vectorized execution engine (Doris 2.0+) for columnar processing
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    database_config = DatabaseConfig(
        type="doris",
        name="doris_tpch",
        options={
            # FE host (required) - Frontend node hostname
            "host": _require_env("DORIS_HOST"),
            # MySQL protocol port (optional, default: 9030)
            "port": int(os.getenv("DORIS_PORT", "9030")),
            # Authentication (optional, Doris defaults to root with no password)
            "username": os.getenv("DORIS_USER", "root"),
            "password": os.getenv("DORIS_PASSWORD", ""),
            # Target database (optional, default: benchbox)
            "database": os.getenv("DORIS_DATABASE", "benchbox"),
            # FE HTTP port for Stream Load ingestion (optional, default: 8030)
            "http_port": int(os.getenv("DORIS_HTTP_PORT", "8030")),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Apache Doris.

    Apache Doris provides:
    - Sub-second OLAP query latency via vectorized execution engine
    - MPP distributed query engine across multiple BE nodes
    - MySQL protocol compatibility for standard tooling
    - Stream Load for high-throughput data ingestion via FE HTTP API
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="doris_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Apache Doris.")
        print()
        print("Cluster requirements:")
        print("- FE (Frontend) node accessible on MySQL port (default: 9030)")
        print("- FE (Frontend) node accessible on HTTP port (default: 8030) for Stream Load")
        print("- BE (Backend) node(s) running and registered with FE")
        print()
        print("Before running:")
        print("1. Verify Doris cluster is healthy: mysql -h $DORIS_HOST -P 9030 -u root")
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
    print("- Check Doris FE web UI (port 8030) for query profiles")
    print("- Try larger scale factors for distributed performance testing")
    print("- Compare with other OLAP platforms (StarRocks, ClickHouse, DuckDB)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Apache Doris")
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
