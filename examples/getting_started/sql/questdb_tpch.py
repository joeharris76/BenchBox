"""Run TPC-H on QuestDB (self-hosted time-series database).

QuestDB is a high-performance open-source time-series database optimized for
fast ingestion and SQL queries. It supports the PostgreSQL wire protocol for
query execution and a REST API for efficient bulk data import. BenchBox uses
psycopg2 over the PG wire protocol (port 8812) for schema DDL and query
execution, and the REST API /imp endpoint (port 9000) for CSV data loading.

QuestDB Concepts:

1. POSTGRESQL WIRE PROTOCOL (port 8812)
   - QuestDB exposes a PostgreSQL-compatible wire protocol on port 8812
   - Standard PG clients and drivers (psycopg2) connect directly
   - Autocommit mode is required; multi-statement transactions are not supported
   - SQL syntax is largely PostgreSQL-compatible with time-series extensions

2. REST API (port 9000)
   - /imp endpoint for bulk CSV data import (HTTP POST with multipart upload)
   - /exec endpoint for SQL query execution and row count verification
   - Supports optional TLS (HTTPS) for encrypted communication
   - Primary data loading path for BenchBox benchmarks

3. INFLUXDB LINE PROTOCOL (port 9009)
   - High-throughput ingestion protocol for time-series data
   - Millions of rows per second for streaming workloads
   - Not used by BenchBox, but available for custom ingestion

4. TIME-SERIES DESIGN
   - Designated timestamp columns enable time-based partitioning
   - Automatic partitioning by HOUR, DAY, MONTH, or YEAR
   - SYMBOL type for low-cardinality strings (stored as integers internally)
   - No foreign key or primary key constraints
   - Single database per instance (no CREATE DATABASE support)

Docker Quick Start:

    docker run -p 9000:9000 -p 8812:8812 -p 9009:9009 -p 9003:9003 \\
        questdb/questdb:latest

    # Verify PG wire protocol
    psql -h 127.0.0.1 -p 8812 -U admin -d qdb -c "SELECT 1"

    # Verify REST API
    curl http://localhost:9000/exec?query=SELECT%201

Prerequisites:
    1. Running QuestDB instance (Docker or native installation)
    2. PG wire protocol port accessible (default: 8812)
    3. REST API HTTP port accessible (default: 9000)
    4. psycopg2-binary and requests packages installed

Required environment variables:
    QUESTDB_HOST             QuestDB server hostname or IP address

Optional environment variables:
    QUESTDB_PG_PORT          PostgreSQL wire protocol port (default: 8812)
    QUESTDB_HTTP_PORT        REST API HTTP port (default: 9000)

Installation:
    uv add benchbox --extra questdb

Usage:
    export QUESTDB_HOST=localhost

    python examples/getting_started/sql/questdb_tpch.py

    # Preview without execution
    python examples/getting_started/sql/questdb_tpch.py --dry-run ./preview
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "questdb"


def _require_env(var_name: str) -> str:
    """Require a QuestDB environment variable.

    QuestDB connection details should be provided via environment variables.
    Never hardcode credentials in scripts.
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set QuestDB connection details before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for QuestDB.

    QuestDB Concepts:

    1. PG WIRE PROTOCOL
       - QuestDB exposes a PostgreSQL-compatible wire protocol on port 8812
       - Standard PG drivers (psycopg2) connect directly in autocommit mode
       - Schema DDL and query execution go through this protocol

    2. REST API IMPORT
       - Bulk CSV data loading via HTTP POST to /imp endpoint (port 9000)
       - Supports delimiter detection, durability settings, and table targeting
       - Falls back to COPY FROM STDIN via PG wire protocol if unavailable

    3. TIME-SERIES OPTIMIZATIONS
       - Designated timestamp columns and time-based partitioning
       - SYMBOL type for low-cardinality string columns
       - No foreign key or primary key constraints

    4. SINGLE DATABASE
       - QuestDB uses one database per instance (default: qdb)
       - No CREATE DATABASE or DROP DATABASE support
       - Database name identifies the instance, not a user-created schema
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    database_config = DatabaseConfig(
        type="questdb",
        name="questdb_tpch",
        options={
            # QuestDB host (required) - server hostname
            "host": _require_env("QUESTDB_HOST"),
            # PG wire protocol port (optional, default: 8812)
            "pg_port": int(os.getenv("QUESTDB_PG_PORT", "8812")),
            # REST API HTTP port (optional, default: 9000)
            "http_port": int(os.getenv("QUESTDB_HTTP_PORT", "9000")),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on QuestDB.

    QuestDB provides:
    - High-performance time-series storage with columnar design
    - PostgreSQL wire protocol for standard SQL tooling
    - REST API for efficient bulk data import
    - Automatic time-based partitioning for time-series workloads
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="questdb_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on QuestDB.")
        print()
        print("QuestDB requirements:")
        print("- PG wire protocol accessible on port 8812")
        print("- REST API accessible on port 9000 (for CSV import)")
        print("- Autocommit mode is used automatically (no transactions)")
        print()
        print("Before running:")
        print("1. Verify QuestDB is running: psql -h $QUESTDB_HOST -p 8812 -U admin -d qdb")
        print("2. Test REST API: curl http://$QUESTDB_HOST:9000/exec?query=SELECT%201")
        print("3. Foreign key constraints will be stripped automatically")
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
    print("- Check QuestDB web console at http://<host>:9000 for query analysis")
    print("- Try larger scale factors for performance testing")
    print("- Compare with other platforms (DuckDB, ClickHouse, TimescaleDB)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on QuestDB")
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
