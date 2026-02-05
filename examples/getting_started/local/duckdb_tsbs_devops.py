"""Run TSBS DevOps time-series benchmark against DuckDB.

This example demonstrates BenchBox's TSBS DevOps benchmark, which simulates
infrastructure monitoring workloads typical of DevOps and observability platforms.
Based on the official TSBS implementation by Timescale, it generates realistic
time-series data representing CPU, memory, disk, and network metrics.

The benchmark includes 18 queries covering common monitoring patterns:
single-host lookups, aggregations, threshold alerts, and dashboard queries.

Usage (from repository root):
    python examples/getting_started/local/duckdb_tsbs_devops.py
    python examples/getting_started/local/duckdb_tsbs_devops.py --scale 0.1
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.platforms.duckdb import DuckDBAdapter
from benchbox.tsbs_devops import TSBSDevOps

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "duckdb"


def run_example(
    scale_factor: float = 0.01,
    *,
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data and execute the TSBS DevOps benchmark using DuckDB."""
    # Ensure output directory exists for storing generated data and results
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Dry-run mode: Preview what would be executed without actually running queries
    if dry_run_output is not None:
        benchmark_config = BenchmarkConfig(
            name="tsbs_devops",
            display_name="TSBS DevOps",
            scale_factor=scale_factor,
            options={
                "force_regenerate": force_regenerate,
            },
            test_execution_type="power",
        )

        database_config = DatabaseConfig(
            type="duckdb",
            name="duckdb_tsbs_devops",
            options={
                "database_path": ":memory:",
                "force_recreate": force_regenerate,
            },
        )

        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="duckdb_tsbs_devops",
        )
        return

    # Normal execution mode

    # Step 1: Create TSBS DevOps benchmark object
    # Tables: tags (host metadata), cpu, mem, disk, net (metric tables)
    # Queries: 18 queries covering monitoring patterns
    benchmark = TSBSDevOps(
        scale_factor=scale_factor,  # 0.01 = 10 hosts, 1.0 = 100 hosts, 10.0 = 1000 hosts
        output_dir=_OUTPUT_ROOT / f"tsbs_devops_sf_{scale_factor}",
        force_regenerate=force_regenerate,
        verbose=False,
    )

    # Step 2: Generate time-series data
    # Creates realistic patterns:
    #   - Diurnal CPU patterns (higher during business hours)
    #   - Memory growth with periodic GC drops
    #   - Disk I/O bursts (5% chance of 10x spike)
    #   - Network errors (~0.1% rate)
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} data files under {benchmark.output_dir}")

    # Step 3: Create DuckDB adapter
    adapter = DuckDBAdapter(database_path=":memory:", force_recreate=force_regenerate)

    # Step 4: Run benchmark
    # Executes 18 TSBS DevOps queries:
    #   - Single host: cpu metrics for specific host
    #   - Aggregation: max CPU across all hosts
    #   - GroupBy: time-bucketed aggregations
    #   - Threshold: alert-style filters (CPU > 90%, low memory)
    #   - Lastpoint: most recent metrics per host (dashboard queries)
    results = adapter.run_benchmark(benchmark, test_execution_type="power")

    # Step 5: Display results
    print("TSBS DevOps benchmark complete!")
    print(f"Queries executed: {results.total_queries}")  # 18 queries
    print(f"Successful queries: {results.successful_queries}")
    print(f"Total execution time (s): {results.total_execution_time:.2f}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TSBS DevOps time-series benchmark on DuckDB")
    parser.add_argument(
        "--scale",
        type=float,
        default=0.01,
        help="Scale factor (0.01=10 hosts, 0.1=10 hosts, 1.0=100 hosts)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data even if files already exist",
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
        force_regenerate=args.force,
        dry_run_output=Path(args.dry_run) if args.dry_run else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
