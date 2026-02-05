"""Run TPC-H on AWS Glue (serverless managed Spark).

AWS Glue is a fully managed ETL service that runs Apache Spark for distributed
data processing. Unlike interactive query services, Glue executes benchmark
queries as batch jobs, making it ideal for large-scale analytics workloads.

Key Features:
    - Serverless Spark execution with automatic scaling
    - Pay-per-use DPU-hour billing (~$0.44/DPU-hour)
    - Native integration with Glue Data Catalog
    - Optimized for ETL and batch analytics workloads
    - Supports Spark 3.x with AWS enhancements

Prerequisites:
    1. AWS account with Glue access
    2. S3 bucket for data staging and job scripts
    3. IAM role with Glue and S3 permissions
    4. AWS credentials configured (CLI, environment, or IAM role)

Required environment variables:
    GLUE_S3_STAGING_DIR   S3 path for data staging (e.g., s3://bucket/benchbox/)
    GLUE_JOB_ROLE         IAM role ARN for Glue job execution

Optional environment variables:
    AWS_REGION            AWS region (default: us-east-1)
    AWS_PROFILE           AWS CLI profile name
    GLUE_DATABASE         Glue database name (default: benchbox)
    GLUE_WORKER_TYPE      Worker type: G.1X, G.2X, G.025X, etc. (default: G.1X)
    GLUE_NUM_WORKERS      Number of workers (default: 2)
    GLUE_VERSION          Glue version: 3.0, 4.0 (default: 4.0)

Installation:
    uv add benchbox --extra glue

Usage:
    export GLUE_S3_STAGING_DIR=s3://your-bucket/benchbox/
    export GLUE_JOB_ROLE=arn:aws:iam::123456789012:role/GlueBenchmarkRole

    python examples/getting_started/cloud/glue_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/glue_tpch.py --dry-run ./preview

Cost Estimation:
    Glue charges ~$0.44 per DPU-hour.
    - G.1X worker = 4 vCPU, 16GB RAM = 1 DPU
    - G.2X worker = 8 vCPU, 32GB RAM = 2 DPU
    - Minimum billing: 1 minute per job

    TPC-H estimates (2 workers, G.1X):
    - SF=0.01 (~10MB): ~$0.03 for full benchmark
    - SF=1.0 (~1GB): ~$0.30 for full benchmark
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "glue"


def _require_env(var_name: str) -> str:
    """Require an AWS/Glue environment variable.

    AWS credentials should be configured via:
    - AWS CLI: aws configure
    - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    - IAM role (on EC2/ECS/Lambda)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set AWS/Glue configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for AWS Glue.

    AWS Glue Concepts:

    1. SERVERLESS SPARK
       - Runs Apache Spark in serverless mode
       - Automatic cluster provisioning and scaling
       - No infrastructure management required
       - Optimized for ETL and batch analytics

    2. DPU (DATA PROCESSING UNITS)
       - Unit of compute capacity
       - G.1X: 4 vCPU, 16GB = 1 DPU (~$0.44/hour)
       - G.2X: 8 vCPU, 32GB = 2 DPU (~$0.88/hour)
       - G.025X: 0.25 DPU for development
       - Billed per second, 1-minute minimum

    3. GLUE DATA CATALOG
       - Centralized metadata repository
       - Compatible with Hive metastore
       - Tables point to S3 locations
       - Shared across Glue, Athena, EMR

    4. JOB EXECUTION
       - Jobs are batch processes (not interactive)
       - Python or Spark scripts
       - Results written to S3
       - Automatic retry on failure

    5. S3 INTEGRATION
       - Data stored in S3 buckets
       - Job scripts uploaded to S3
       - Query results written to S3
       - Supports Parquet, ORC, CSV, JSON
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    staging_dir = _require_env("GLUE_S3_STAGING_DIR")
    job_role = _require_env("GLUE_JOB_ROLE")

    database_config = DatabaseConfig(
        type="glue",
        name="glue_tpch",
        options={
            # Required S3 and IAM configuration
            "s3_staging_dir": staging_dir,
            "job_role": job_role,
            # AWS region
            "region": os.getenv("AWS_REGION", "us-east-1"),
            # Glue configuration
            "database": os.getenv("GLUE_DATABASE", "benchbox"),
            "worker_type": os.getenv("GLUE_WORKER_TYPE", "G.1X"),
            "number_of_workers": int(os.getenv("GLUE_NUM_WORKERS", "2")),
            "glue_version": os.getenv("GLUE_VERSION", "4.0"),
            # AWS profile (optional, uses default credential chain if not set)
            "aws_profile": os.getenv("AWS_PROFILE"),
            # Data format for optimal performance
            "default_format": "parquet",
            "compression": "snappy",
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on AWS Glue.

    AWS Glue is ideal for:
    - Large-scale ETL and batch analytics
    - Serverless Spark execution
    - Integration with Glue Data Catalog
    - Cost-effective processing of S3 data lakes
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="glue_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on AWS Glue.")
        print()
        print("Cost estimation (Glue charges ~$0.44/DPU-hour):")
        print(f"- TPC-H SF={scale_factor}:")
        num_workers = int(os.getenv("GLUE_NUM_WORKERS", "2"))
        worker_type = os.getenv("GLUE_WORKER_TYPE", "G.1X")
        dpu_per_worker = 2.0 if worker_type == "G.2X" else 1.0
        total_dpu = num_workers * dpu_per_worker
        # Rough estimate: 1-2 minutes per query at small scale
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        hours = (22 * minutes_per_query) / 60
        cost = hours * total_dpu * 0.44
        print(f"  - {num_workers}x {worker_type} workers: ~${cost:.2f} for full benchmark")
        print()
        print("Before running:")
        print("1. Verify S3 bucket exists and is accessible")
        print("2. Check IAM role has Glue and S3 permissions")
        print("3. Review query translations in generated SQL files")
        print("4. Monitor job progress in AWS Glue console")
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
    print("- Check Glue console for job history and DPU usage")
    print("- Compare with other platforms using result_analysis.py")
    print("- Clean up S3 data: aws s3 rm --recursive s3://bucket/benchbox/")
    print()
    print("Cost optimization tips:")
    print("- Use G.025X workers for development/testing")
    print("- Increase workers for larger scale factors")
    print("- Monitor DPU utilization in Glue metrics")
    print("- Use Spark UI to identify bottlenecks")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on AWS Glue")
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
