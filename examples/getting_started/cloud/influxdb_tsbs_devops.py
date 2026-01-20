"""Run TSBS DevOps benchmark on InfluxDB 3.x.

InfluxDB 3.x is a time series database built on the FDAP stack (Apache Arrow,
DataFusion, Parquet) with native SQL support via FlightSQL protocol.

This example demonstrates running the TSBS (Time Series Benchmark Suite)
DevOps workload against InfluxDB for time series query performance testing.

Deployment Modes:
    - Cloud: InfluxDB Cloud managed service (default)
    - Core: Self-hosted InfluxDB Core (OSS)

Prerequisites:
    1. InfluxDB 3.x instance (Cloud or Core)
    2. Authentication token with read/write permissions
    3. Database (bucket) created

Required environment variables:
    INFLUXDB_TOKEN    Authentication token

Optional environment variables (with defaults):
    INFLUXDB_HOST     Server hostname (default: localhost)
    INFLUXDB_PORT     Server port (default: 8086)
    INFLUXDB_ORG      Organization name
    INFLUXDB_DATABASE Database name (default: benchbox)

Installation:
    uv add benchbox --extra influxdb

Usage - InfluxDB Cloud:
    export INFLUXDB_TOKEN=your_token
    export INFLUXDB_HOST=us-east-1-1.aws.cloud2.influxdata.com
    export INFLUXDB_ORG=your-org
    export INFLUXDB_DATABASE=benchmarks

    python examples/getting_started/cloud/influxdb_tsbs_devops.py

Usage - InfluxDB Core (local Docker):
    # Start InfluxDB Core
    docker run -d --name influxdb -p 8086:8086 \\
      -e DOCKER_INFLUXDB_INIT_MODE=setup \\
      -e DOCKER_INFLUXDB_INIT_USERNAME=admin \\
      -e DOCKER_INFLUXDB_INIT_PASSWORD=password123 \\
      -e DOCKER_INFLUXDB_INIT_ORG=benchbox \\
      -e DOCKER_INFLUXDB_INIT_BUCKET=benchmarks \\
      -e DOCKER_INFLUXDB_INIT_ADMIN_TOKEN=my-token \\
      influxdb:3.0

    export INFLUXDB_TOKEN=my-token
    python examples/getting_started/cloud/influxdb_tsbs_devops.py --mode core --no-ssl

Preview without execution:
    python examples/getting_started/cloud/influxdb_tsbs_devops.py --dry-run ./preview
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "influxdb"


def _get_env(var_name: str, default: str | None = None) -> str | None:
    """Get environment variable with optional default."""
    return os.getenv(var_name, default)


def _require_env(var_name: str) -> str:
    """Require an environment variable.

    InfluxDB credentials should NEVER be hardcoded. Always use
    environment variables or a secrets manager.
    """
    value = os.getenv(var_name)
    if not value:
        raise ValueError(
            f"Missing required environment variable: {var_name}\nSet it with: export {var_name}=your_value"
        )
    return value


def main() -> None:
    """Run TSBS DevOps benchmark on InfluxDB."""
    parser = argparse.ArgumentParser(
        description="Run TSBS DevOps benchmark on InfluxDB 3.x",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--mode",
        choices=["cloud", "core"],
        default="cloud",
        help="InfluxDB deployment mode (default: cloud)",
    )
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Scale factor for TSBS data generation (default: 1.0)",
    )
    parser.add_argument(
        "--no-ssl",
        action="store_true",
        help="Disable SSL/TLS (for local InfluxDB Core)",
    )
    parser.add_argument(
        "--dry-run",
        type=str,
        metavar="PATH",
        help="Preview benchmark without execution, output to PATH",
    )
    args = parser.parse_args()

    # Get configuration from environment
    token = _require_env("INFLUXDB_TOKEN")
    host = _get_env("INFLUXDB_HOST", "localhost")
    port = int(_get_env("INFLUXDB_PORT", "8086"))
    org = _get_env("INFLUXDB_ORG")
    database = _get_env("INFLUXDB_DATABASE", "benchbox")
    ssl = not args.no_ssl

    # Handle dry-run mode
    if args.dry_run:
        from benchbox.examples import execute_example_dry_run

        execute_example_dry_run(
            dry_run_path=args.dry_run,
            platform_name="influxdb",
            benchmark_name="tsbs-devops",
            scale_factor=args.scale,
            platform_config={
                "host": host,
                "port": port,
                "token": "***",  # Redacted for dry-run
                "org": org or "(not set)",
                "database": database,
                "mode": args.mode,
                "ssl": ssl,
            },
        )
        return

    # Import BenchBox components
    from benchbox.cli.orchestrator import BenchmarkOrchestrator
    from benchbox.core.config import BenchmarkConfig, DatabaseConfig
    from benchbox.core.system import SystemProfiler

    # Check InfluxDB availability
    from benchbox.platforms.influxdb import INFLUXDB_AVAILABLE, InfluxDBAdapter

    if not INFLUXDB_AVAILABLE:
        raise ImportError("InfluxDB client not installed. Run: uv add influxdb3-python")

    # Create adapter
    print(f"Connecting to InfluxDB ({args.mode} mode)...")
    adapter = InfluxDBAdapter(
        host=host,
        port=port,
        token=token,
        org=org,
        database=database,
        mode=args.mode,
        ssl=ssl,
    )

    # Configure benchmark
    db_config = DatabaseConfig(
        platform=adapter,
        output_dir=_OUTPUT_DIR,
    )

    bench_config = BenchmarkConfig(
        benchmark_type="tsbs_devops",
        scale_factor=args.scale,
        phases=["power"],  # Time series queries
    )

    # Create orchestrator
    orchestrator = BenchmarkOrchestrator(
        database_config=db_config,
        benchmark_config=bench_config,
        system_profiler=SystemProfiler(),
    )

    # Run benchmark
    print(f"Running TSBS DevOps benchmark at scale {args.scale}...")
    print(f"  Host: {host}:{port}")
    print(f"  Database: {database}")
    print(f"  Mode: {args.mode}")
    print(f"  SSL: {ssl}")
    print()

    results = orchestrator.run()

    # Print summary
    print("\n" + "=" * 60)
    print("Benchmark Complete!")
    print("=" * 60)
    print(f"Output directory: {_OUTPUT_DIR}")
    if hasattr(results, "manifest_path"):
        print(f"Results manifest: {results.manifest_path}")


if __name__ == "__main__":
    main()
