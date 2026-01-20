"""Run NYC Taxi OLAP benchmark against DuckDB.

This example demonstrates BenchBox's NYC Taxi benchmark, which uses real-world
NYC TLC taxi trip data for analytical workloads. The benchmark includes 25 OLAP
queries covering temporal, geographic, and financial analytics patterns.

Unlike synthetic benchmarks, NYC Taxi data exhibits realistic distributions,
seasonal patterns, and geographic clustering - making it ideal for testing
real-world query performance.

Usage (from repository root):
    python examples/getting_started/local/duckdb_nyctaxi.py
    python examples/getting_started/local/duckdb_nyctaxi.py --scale 0.1
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.nyctaxi import NYCTaxi
from benchbox.platforms.duckdb import DuckDBAdapter

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "duckdb"


def run_example(
    scale_factor: float = 0.01,
    *,
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data and execute the NYC Taxi benchmark using DuckDB."""
    # Ensure output directory exists for storing generated data and results
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Dry-run mode: Preview what would be executed without actually running queries
    if dry_run_output is not None:
        benchmark_config = BenchmarkConfig(
            name="nyctaxi",
            display_name="NYC Taxi",
            scale_factor=scale_factor,
            options={
                "force_regenerate": force_regenerate,
            },
            test_execution_type="power",
        )

        database_config = DatabaseConfig(
            type="duckdb",
            name="duckdb_nyctaxi",
            options={
                "database_path": ":memory:",
                "force_recreate": force_regenerate,
            },
        )

        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="duckdb_nyctaxi",
        )
        return

    # Normal execution mode

    # Step 1: Create NYC Taxi benchmark object
    # Includes 25 queries across 9 categories (temporal, geographic, financial, etc.)
    # Data: trips fact table (~30M rows at SF=1) + taxi_zones dimension (265 rows)
    benchmark = NYCTaxi(
        scale_factor=scale_factor,  # 0.01 = ~300K trips, 1.0 = ~30M trips
        output_dir=_OUTPUT_ROOT / f"nyctaxi_sf_{scale_factor}",
        force_regenerate=force_regenerate,
        verbose=False,
    )

    # Step 2: Generate NYC Taxi data
    # Downloads real TLC data when available, or generates synthetic data as fallback
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} data files under {benchmark.output_dir}")

    # Step 3: Create DuckDB adapter
    adapter = DuckDBAdapter(database_path=":memory:", force_recreate=force_regenerate)

    # Step 4: Run benchmark
    # Executes all 25 NYC Taxi queries:
    #   - Temporal: trips-per-hour, trips-per-day, hourly-revenue
    #   - Geographic: top-pickup-zones, zone-pairs, borough-summary
    #   - Financial: total-revenue, tip-analysis, payment-analysis
    #   - And more...
    results = adapter.run_benchmark(benchmark, test_execution_type="power")

    # Step 5: Display results
    print("NYC Taxi benchmark complete!")
    print(f"Queries executed: {results.total_queries}")  # 25 queries
    print(f"Successful queries: {results.successful_queries}")
    print(f"Total execution time (s): {results.total_execution_time:.2f}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="NYC Taxi OLAP benchmark on DuckDB")
    parser.add_argument(
        "--scale",
        type=float,
        default=0.01,
        help="Scale factor (0.01=300K rows, 0.1=3M rows, 1.0=30M rows)",
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
