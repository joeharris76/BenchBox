"""Run a minimal TPC-H power test against DuckDB.

This "Hello World" example focuses on a single feature: generating the
TPC-H dataset at a small scale and running the standard power test using
DuckDB's in-memory engine. The script keeps configuration to an absolute
minimum so new users can see successful end-to-end execution quickly.

Usage (from repository root):
    python examples/getting_started/local/duckdb_tpch_power.py
    python examples/getting_started/local/duckdb_tpch_power.py --scale 0.1
"""

from __future__ import annotations

import argparse
from pathlib import Path

from benchbox.core.config import BenchmarkConfig, DatabaseConfig
from benchbox.examples import execute_example_dry_run
from benchbox.platforms.duckdb import DuckDBAdapter
from benchbox.tpch import TPCH

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_ROOT = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "duckdb"


def run_example(
    scale_factor: float = 0.01,
    *,
    force_regenerate: bool = False,
    dry_run_output: Path | None = None,
) -> None:
    """Generate data and execute the TPC-H power test using DuckDB."""
    # Ensure output directory exists for storing generated data and results
    _OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)

    # Dry-run mode: Preview what would be executed without actually running queries
    # This is useful for validating configuration before committing to expensive runs
    if dry_run_output is not None:
        # Create benchmark configuration object (used by dry-run system)
        benchmark_config = BenchmarkConfig(
            name="tpch",
            display_name="TPC-H",
            scale_factor=scale_factor,
            options={
                "force_regenerate": force_regenerate,
            },
            test_execution_type="power",  # Power test = sequential query execution
        )

        # Create database configuration object
        database_config = DatabaseConfig(
            type="duckdb",
            name="duckdb_tpch_power",
            options={
                "database_path": ":memory:",  # In-memory = fast but no persistence
                "force_recreate": force_regenerate,
            },
        )

        # Generate dry-run artifacts (SQL files, execution plan, etc.)
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="duckdb_tpch_power",
        )
        return

    # Normal execution mode: Actually run the benchmark

    # Step 1: Create benchmark object
    # TPC-H includes 22 queries and 8 tables (customer, orders, lineitem, etc.)
    benchmark = TPCH(
        scale_factor=scale_factor,  # 0.01 = ~10MB, 1.0 = ~1GB, 10.0 = ~10GB
        output_dir=_OUTPUT_ROOT / f"tpch_sf_{scale_factor}",  # Data files stored here
        force_regenerate=force_regenerate,  # If False, reuses existing data files
        verbose=False,  # Reduce console output for cleaner demo
    )

    # Step 2: Generate TPC-H data files (.tbl format)
    # This creates 8 files: customer.tbl, orders.tbl, lineitem.tbl, etc.
    # Files are cached - subsequent runs skip generation unless force_regenerate=True
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} data files under {benchmark.output_dir}")

    # Step 3: Create database adapter
    # DuckDB adapter handles loading data and executing queries
    # ":memory:" means database lives in RAM (fast, but lost after script ends)
    adapter = DuckDBAdapter(database_path=":memory:", force_recreate=force_regenerate)

    # Step 4: Run benchmark
    # This does several things:
    #   1. Creates database schema (8 tables with proper types)
    #   2. Loads data from .tbl files into tables
    #   3. Executes all 22 TPC-H queries sequentially (power test)
    #   4. Collects timing and result metrics
    results = adapter.run_benchmark(benchmark, test_execution_type="power")

    # Step 5: Display results
    print("Power test complete!")
    print(f"Queries executed: {results.total_queries}")  # Should be 22 for full TPC-H
    print(f"Successful queries: {results.successful_queries}")  # Ideally same as total
    print(f"Total execution time (s): {results.total_execution_time:.2f}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TPC-H DuckDB power test (minimal example)")
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
