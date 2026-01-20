"""Run a minimal TPC-DS power test against DuckDB.

This example mirrors the TPC-H script but targets TPC-DS, demonstrating
that the same minimal workflow works for larger benchmark suites. It
keeps configuration surface area minimal while exercising the full
power-phase query set.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.platforms.duckdb import DuckDBAdapter
from benchbox.tpcds import TPCDS

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "duckdb"


def run_example(
    scale_factor: float = 0.01,
    *,
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data and execute the TPC-DS power test on DuckDB."""
    # Ensure output directory exists for storing generated data and results
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Dry-run mode: Preview execution without running actual queries
    # Useful for validating TPC-DS configuration (which is more complex than TPC-H)
    if dry_run_output is not None:
        # Create benchmark configuration for TPC-DS
        benchmark_config = BenchmarkConfig(
            name="tpcds",
            display_name="TPC-DS",
            scale_factor=scale_factor,
            options={
                "force_regenerate": force_regenerate,
            },
            test_execution_type="power",  # Sequential query execution
        )

        # Create database configuration
        database_config = DatabaseConfig(
            type="duckdb",
            name="duckdb_tpcds_power",
            options={
                "database_path": ":memory:",  # In-memory for fast execution
                "force_recreate": force_regenerate,
            },
        )

        # Generate dry-run artifacts (SQL files, execution plan, etc.)
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="duckdb_tpcds_power",
        )
        return

    # Normal execution mode: Run the full TPC-DS benchmark

    # Step 1: Create TPC-DS benchmark object
    # TPC-DS is more complex than TPC-H:
    #   - 99 queries (vs 22 for TPC-H)
    #   - 24 tables (vs 8 for TPC-H)
    #   - More complex queries with advanced SQL features
    #   - Longer execution time (typically 4-5x slower than TPC-H at same scale)
    benchmark = TPCDS(
        scale_factor=scale_factor,  # 0.01 = ~5MB, 1.0 = ~500MB, 10.0 = ~5GB
        output_dir=_OUTPUT_ROOT / f"tpcds_sf_{scale_factor}",
        force_regenerate=force_regenerate,  # Reuse existing data if available
        verbose=False,  # Reduce console output
    )

    # Step 2: Generate TPC-DS data files (.dat format)
    # Creates 24 files including:
    #   - Catalog returns/sales (e-commerce transactions)
    #   - Store sales/returns (retail transactions)
    #   - Web sales/returns (online transactions)
    #   - Customer, item, warehouse, promotion tables
    # Files are cached for subsequent runs
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} data files under {benchmark.output_dir}")

    # Step 3: Create DuckDB adapter
    # Handles schema creation, data loading, and query execution
    adapter = DuckDBAdapter(database_path=":memory:", force_recreate=force_regenerate)

    # Step 4: Run TPC-DS power test
    # This executes all 99 TPC-DS queries sequentially:
    #   1. Creates schema (24 tables with complex relationships)
    #   2. Loads data from .dat files
    #   3. Runs all 99 queries one by one
    #   4. Collects performance metrics
    # Note: TPC-DS queries are more complex than TPC-H, testing:
    #   - Multi-table joins (up to 10+ tables)
    #   - Window functions and ranking
    #   - Complex aggregations and grouping
    #   - Subqueries and CTEs
    results = adapter.run_benchmark(benchmark, test_execution_type="power")

    # Step 5: Display results
    print("Power test complete!")
    print(f"Queries executed: {results.total_queries}")  # Should be 99 for full TPC-DS
    print(f"Successful queries: {results.successful_queries}")
    print(f"Total execution time (s): {results.total_execution_time:.2f}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPC-DS DuckDB power test (minimal example)")
    parser.add_argument(
        "--scale", type=float, default=0.01, help="Benchmark scale factor (minimum 0.01; increase for larger runs)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerate data even if files already exist and recreate the DuckDB database",
    )
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="OUTPUT_DIR",
        help="Preview the run plan without executing queries; artifacts are written to OUTPUT_DIR.",
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
