"""Run TPC-H on Firebolt Core (local Docker deployment).

Firebolt Core is the free, self-hosted version of Firebolt's vectorized query
engine. It runs in Docker and provides the same analytics performance as the
cloud version without requiring cloud credentials or incurring costs.

Prerequisites:
    1. Docker installed and running
    2. Firebolt Core container running on port 3473:
       docker run -i --rm \
         --ulimit memlock=8589934592:8589934592 \
         --security-opt seccomp=unconfined \
         -p 127.0.0.1:3473:3473 \
         -v ./firebolt-data:/firebolt-core/volume \
         ghcr.io/firebolt-db/firebolt-core:preview-rc

    3. Firebolt SDK installed:
       uv add benchbox --extra firebolt

Configuration:
    No authentication required for Core mode - just needs the local URL.

Usage:
    # Start Firebolt Core container first (see above)

    # Run benchmark
    python examples/getting_started/cloud/firebolt_core_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/firebolt_core_tpch.py --dry-run ./preview
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "firebolt_core"


def _get_firebolt_url() -> str:
    """Get the Firebolt Core URL.

    Firebolt Core runs locally and doesn't require authentication.
    The default URL is http://localhost:3473.
    """
    return os.getenv("FIREBOLT_URL", "http://localhost:3473")


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Firebolt Core.

    Firebolt Core Concepts:

    1. DEPLOYMENT
       - Runs locally in Docker container
       - Port 3473 is the default query endpoint
       - Data persists in mounted volume
       - Same query engine as cloud version

    2. NO AUTHENTICATION
       - Core mode doesn't require credentials
       - Just needs the local URL endpoint
       - Perfect for development and testing

    3. DATABASE
       - Database is created automatically if it doesn't exist
       - Tables are created within the database
       - Data is stored in the mounted volume

    4. QUERY ENGINE
       - Vectorized execution (SIMD optimized)
       - PostgreSQL-compatible SQL dialect
       - Columnar storage for analytics
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    database_config = DatabaseConfig(
        type="firebolt",
        name="firebolt_core_tpch",
        options={
            # Core mode connection (no auth required)
            "url": _get_firebolt_url(),
            # Database name (created if doesn't exist)
            "database": os.getenv("FIREBOLT_DATABASE", "benchbox"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Firebolt Core.

    Firebolt Core is ideal for:
    - Local development and testing
    - CI/CD pipelines with Docker
    - Learning Firebolt SQL without cloud costs
    - Comparing local vs cloud performance
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="firebolt_core_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running.")
        print()
        print("Firebolt Core benefits:")
        print("- Free: No cloud costs")
        print("- Fast: Same vectorized engine as cloud")
        print("- Local: Data stays on your machine")
        print("- Portable: Docker-based deployment")
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
    print("- Review results.json for query timings")
    print("- Compare with cloud version using firebolt_cloud_tpch.py")
    print("- Try TPC-DS: --benchmark tpcds")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Firebolt Core (local Docker)")
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
