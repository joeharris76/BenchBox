"""Run TPC-H power test benchmark against PostgreSQL.

This example demonstrates BenchBox's TPC-H benchmark on PostgreSQL, one of
the most widely deployed relational databases. PostgreSQL provides excellent
SQL standards compliance and is commonly used as a baseline for comparing
analytical query performance across platforms.

The benchmark includes 22 queries covering various SQL patterns:
joins, aggregations, subqueries, and complex predicates.

Requirements:
    - PostgreSQL 12+ server running and accessible
    - psycopg2-binary package installed

Usage (from repository root):
    python examples/getting_started/local/postgresql_tpch_power.py
    python examples/getting_started/local/postgresql_tpch_power.py --scale 0.1
    python examples/getting_started/local/postgresql_tpch_power.py --host db.example.com --port 5432
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.platforms.postgresql import PostgreSQLAdapter
from benchbox.tpch import TPCH

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "postgresql"


def run_example(
    scale_factor: float = 0.01,
    *,
    host: str = "localhost",
    port: int = 5432,
    database: str = "benchbox_tpch",
    username: str = "postgres",
    password: str | None = None,
    schema: str = "public",
    work_mem: str = "256MB",
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data and execute the TPC-H power test using PostgreSQL."""
    # Ensure output directory exists for storing generated data and results
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Dry-run mode: Preview what would be executed without actually running queries
    if dry_run_output is not None:
        benchmark_config = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=scale_factor,
            options={
                "force_regenerate": force_regenerate,
            },
            test_execution_type="power",
        )

        database_config = DatabaseConfig(
            type="postgresql",
            name="postgresql_tpch_power",
            options={
                "host": host,
                "port": port,
                "database": database,
                "username": username,
                "password": password,
                "schema": schema,
                "work_mem": work_mem,
                "force_recreate": force_regenerate,
            },
        )

        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="postgresql_tpch_power",
        )
        return

    # Normal execution mode

    # Step 1: Create TPC-H benchmark object
    # TPC-H includes 22 queries and 8 tables (customer, orders, lineitem, etc.)
    benchmark = TPCH(
        scale_factor=scale_factor,  # 0.01 = ~10MB, 1.0 = ~1GB, 10.0 = ~10GB
        output_dir=_OUTPUT_ROOT / f"tpch_sf_{scale_factor}",
        force_regenerate=force_regenerate,
        verbose=False,
    )

    # Step 2: Generate TPC-H data files (.tbl format)
    # Creates 8 files: customer.tbl, orders.tbl, lineitem.tbl, etc.
    # Files are cached - subsequent runs skip generation unless force_regenerate=True
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} data files under {benchmark.output_dir}")

    # Step 3: Create PostgreSQL adapter
    # COPY command is used for efficient bulk loading
    adapter = PostgreSQLAdapter(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        schema=schema,
        work_mem=work_mem,  # Increase for better sort/hash performance
        force_recreate=force_regenerate,
    )

    # Step 4: Run benchmark
    # This does several things:
    #   1. Creates database schema (8 tables with proper types)
    #   2. Loads data from .tbl files using COPY command
    #   3. Runs ANALYZE on tables for optimizer statistics
    #   4. Executes all 22 TPC-H queries sequentially (power test)
    #   5. Collects timing and result metrics
    results = adapter.run_benchmark(benchmark, test_execution_type="power")

    # Step 5: Display results
    print("TPC-H power test on PostgreSQL complete!")
    print(f"Queries executed: {results.total_queries}")  # Should be 22 for full TPC-H
    print(f"Successful queries: {results.successful_queries}")
    print(f"Total execution time (s): {results.total_execution_time:.2f}")

    # Display PostgreSQL-specific info
    platform_info = adapter.get_platform_info()
    if "platform_version" in platform_info:
        print(f"PostgreSQL version: {platform_info['platform_version']}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPC-H benchmark on PostgreSQL (power test)")
    parser.add_argument(
        "--scale",
        type=float,
        default=0.01,
        help="Benchmark scale factor (0.01=~10MB, 0.1=~100MB, 1.0=~1GB)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="PostgreSQL server hostname",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=5432,
        help="PostgreSQL server port",
    )
    parser.add_argument(
        "--database",
        type=str,
        default="benchbox_tpch",
        help="Database name (will be created if it doesn't exist)",
    )
    parser.add_argument(
        "--username",
        type=str,
        default="postgres",
        help="PostgreSQL username",
    )
    parser.add_argument(
        "--password",
        type=str,
        default=None,
        help="PostgreSQL password (or use PGPASSWORD env var)",
    )
    parser.add_argument(
        "--schema",
        type=str,
        default="public",
        help="Schema to use for benchmark tables",
    )
    parser.add_argument(
        "--work-mem",
        type=str,
        default="256MB",
        help="PostgreSQL work_mem setting for sorts/hashes",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data and recreate database even if they exist",
    )
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview the run plan without executing queries",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_example(
        scale_factor=args.scale,
        host=args.host,
        port=args.port,
        database=args.database,
        username=args.username,
        password=args.password,
        schema=args.schema,
        work_mem=args.work_mem,
        force_regenerate=args.force,
        dry_run_output=Path(args.dry_run) if args.dry_run else None,
    )
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
