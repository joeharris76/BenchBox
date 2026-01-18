"""Run TPC-H on AWS Athena (serverless query service).

Athena is AWS's serverless interactive query service built on Trino. It queries
data directly from S3 without requiring infrastructure management. You only pay
for the data scanned by each query.

Prerequisites:
    1. AWS account with Athena access
    2. S3 bucket for data staging and query results
    3. IAM permissions for Athena and S3 operations
    4. AWS credentials configured (CLI, environment, or IAM role)

Required environment variables:
    ATHENA_S3_STAGING_DIR   S3 path for data staging (e.g., s3://bucket/benchbox/)

Optional environment variables:
    AWS_REGION              AWS region (default: us-east-1)
    AWS_PROFILE             AWS CLI profile name
    ATHENA_WORKGROUP        Athena workgroup (default: primary)
    ATHENA_DATABASE         Glue database name (default: benchbox)
    ATHENA_OUTPUT_LOCATION  S3 path for query results (defaults to staging dir)

Installation:
    uv add benchbox --extra athena

Usage:
    export ATHENA_S3_STAGING_DIR=s3://your-bucket/benchbox/
    export AWS_REGION=us-east-1

    python examples/getting_started/cloud/athena_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/athena_tpch.py --dry-run ./preview

Cost Estimation:
    Athena charges $5 per TB of data scanned.
    - TPC-H SF=0.01 (~10MB): ~$0.0001 per query
    - TPC-H SF=1.0 (~1GB): ~$0.005 per query
    - Full benchmark (22 queries): multiply by 22
    Using Parquet format reduces data scanned by ~10x vs CSV.
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "athena"


def _require_env(var_name: str) -> str:
    """Require an AWS/Athena environment variable.

    AWS credentials should be configured via:
    - AWS CLI: aws configure
    - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    - IAM role (on EC2/ECS/Lambda)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set AWS/Athena configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Athena.

    AWS Athena Concepts:

    1. SERVERLESS ARCHITECTURE
       - No servers to manage
       - Automatic scaling based on query complexity
       - Pay only for data scanned
       - Queries run on shared infrastructure

    2. S3 INTEGRATION
       - Data lives in S3 (your bucket)
       - Query results written to S3
       - Staging dir for benchmark data upload
       - Supports Parquet, ORC, CSV, JSON formats

    3. GLUE DATA CATALOG
       - Metadata store for tables
       - Database = container for tables
       - Tables point to S3 locations
       - Automatically used by Athena

    4. WORKGROUPS
       - Resource isolation and cost tracking
       - Query limits and settings
       - Result encryption options
       - Default workgroup: "primary"

    5. COST MODEL
       - $5 per TB of data scanned
       - Parquet format reduces scans ~10x
       - Partitioning reduces scans further
       - No charge for failed queries
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    staging_dir = _require_env("ATHENA_S3_STAGING_DIR")

    database_config = DatabaseConfig(
        type="athena",
        name="athena_tpch",
        options={
            # S3 configuration (required)
            "s3_staging_dir": staging_dir,
            "s3_output_location": os.getenv("ATHENA_OUTPUT_LOCATION") or f"{staging_dir.rstrip('/')}/results/",
            # AWS region
            "region": os.getenv("AWS_REGION", "us-east-1"),
            # Athena configuration
            "workgroup": os.getenv("ATHENA_WORKGROUP", "primary"),
            "database": os.getenv("ATHENA_DATABASE", "benchbox"),
            # AWS profile (optional, uses default credential chain if not set)
            "aws_profile": os.getenv("AWS_PROFILE"),
            # Data format for better performance and lower costs
            "default_format": "PARQUET",
            "compression": "SNAPPY",
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on AWS Athena.

    Athena is ideal for:
    - Ad-hoc analytics on S3 data lakes
    - Serverless operation (no infrastructure)
    - Pay-per-query cost model
    - Integration with AWS ecosystem
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="athena_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Athena.")
        print()
        print("Cost estimation (Athena charges $5/TB scanned):")
        print(f"- TPC-H SF={scale_factor}:")
        data_size_mb = scale_factor * 1000
        cost_csv = data_size_mb / 1024 / 1024 * 5 * 22
        cost_parquet = cost_csv / 10
        print(f"  - CSV format: ~${cost_csv:.4f} for full benchmark")
        print(f"  - Parquet format: ~${cost_parquet:.4f} for full benchmark (recommended)")
        print()
        print("Before running:")
        print("1. Verify S3 bucket exists and is accessible")
        print("2. Check IAM permissions for Athena and S3")
        print("3. Review query translations in generated SQL files")
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
    print("- Check Athena console for query history and data scanned")
    print("- Compare with other platforms using result_analysis.py")
    print("- Clean up S3 data: aws s3 rm --recursive s3://bucket/benchbox/")
    print()
    print("Cost optimization tips:")
    print("- Use Parquet format (already configured) for ~10x cost reduction")
    print("- Partition large tables by date or category")
    print("- Use workgroup query limits to prevent runaway costs")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on AWS Athena")
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
