"""Run TPC-H on Amazon EMR Serverless (serverless managed Spark).

Amazon EMR Serverless is AWS's serverless Spark service with automatic scaling
and sub-second startup times. Unlike EMR on EC2, there are no clusters to manage -
resources are provisioned automatically based on workload.

Key Features:
    - Serverless: No clusters to manage, automatic scaling
    - Fast startup: Sub-second cold starts with pre-initialized capacity
    - Cost-effective: Pay only for vCPU-hours and memory-GB-hours used
    - Integrated: Native S3 and Glue Data Catalog integration

Prerequisites:
    1. AWS account with EMR Serverless access
    2. EMR Serverless application (or permission to create one)
    3. S3 bucket for data staging and job scripts
    4. IAM execution role with EMR Serverless and S3 permissions
    5. AWS credentials configured (CLI, environment, or IAM role)

Required environment variables:
    EMR_S3_STAGING_DIR      S3 path for data staging (e.g., s3://bucket/benchbox/)
    EMR_EXECUTION_ROLE_ARN  IAM role ARN for job execution

Optional environment variables:
    EMR_APPLICATION_ID      Existing EMR Serverless application ID
    AWS_REGION              AWS region (default: us-east-1)
    EMR_DATABASE            Glue database name (default: benchbox)
    EMR_RELEASE_LABEL       EMR release label (default: emr-7.0.0)
    EMR_CREATE_APPLICATION  Set to "true" to create new application

Installation:
    uv add benchbox --extra emr-serverless

Usage:
    export EMR_S3_STAGING_DIR=s3://your-bucket/benchbox/
    export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::123456789012:role/EMRServerlessRole
    export EMR_APPLICATION_ID=00f12345abc67890  # Or set EMR_CREATE_APPLICATION=true

    python examples/getting_started/cloud/emr_serverless_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/emr_serverless_tpch.py --dry-run ./preview

Cost Estimation:
    EMR Serverless charges per vCPU-hour (~$0.052624) and memory-GB-hour (~$0.0057785).
    - TPC-H SF=0.01 (~10MB): ~$0.02 for full benchmark
    - TPC-H SF=1.0 (~1GB): ~$0.20 for full benchmark
    Pre-initialized capacity is charged even when idle.
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "emr_serverless"


def _require_env(var_name: str) -> str:
    """Require an AWS/EMR environment variable.

    AWS credentials should be configured via:
    - AWS CLI: aws configure
    - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
    - IAM role (on EC2/ECS/Lambda)
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set AWS/EMR configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for EMR Serverless.

    EMR Serverless Concepts:

    1. SERVERLESS ARCHITECTURE
       - No clusters to manage
       - Automatic resource provisioning
       - Resources scale based on workload
       - Pay only for resources used

    2. APPLICATIONS
       - Container for job runs
       - Can be pre-created or created on-demand
       - Application ID needed for job submission
       - Auto-start and auto-stop configuration

    3. PRE-INITIALIZED CAPACITY
       - Optional warm workers for sub-second startup
       - Charged even when idle
       - Best for latency-sensitive workloads
       - Configure per worker type

    4. JOB RUNS
       - Each query submitted as a job run
       - Results written to S3
       - vCPU and memory tracked per run
       - Automatic retry on failure

    5. COST MODEL
       - vCPU-hour: ~$0.052624
       - Memory GB-hour: ~$0.0057785
       - Pre-initialized: charged when idle
       - No minimum billing duration
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    staging_dir = _require_env("EMR_S3_STAGING_DIR")
    execution_role = _require_env("EMR_EXECUTION_ROLE_ARN")

    database_config = DatabaseConfig(
        type="emr-serverless",
        name="emr_serverless_tpch",
        options={
            # Required configuration
            "s3_staging_dir": staging_dir,
            "execution_role_arn": execution_role,
            # Application configuration
            "application_id": os.getenv("EMR_APPLICATION_ID"),
            "create_application": os.getenv("EMR_CREATE_APPLICATION", "").lower() == "true",
            # AWS configuration
            "region": os.getenv("AWS_REGION", "us-east-1"),
            "database": os.getenv("EMR_DATABASE", "benchbox"),
            "release_label": os.getenv("EMR_RELEASE_LABEL", "emr-7.0.0"),
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on Amazon EMR Serverless.

    EMR Serverless is ideal for:
    - Serverless Spark with automatic scaling
    - Interactive analytics with fast startup
    - Cost-effective processing (pay-per-use)
    - Integration with AWS ecosystem
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="emr_serverless_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on EMR Serverless.")
        print()
        print("Cost estimation (EMR Serverless pricing):")
        print(f"- TPC-H SF={scale_factor}:")
        # Rough estimate: 4 vCPU, 16GB memory per query, 1-2 minutes per query
        vcpu_per_query = 4
        memory_gb_per_query = 16
        minutes_per_query = 2 if scale_factor >= 1.0 else 1
        hours_per_query = minutes_per_query / 60
        cost_per_query = (vcpu_per_query * hours_per_query * 0.052624) + (
            memory_gb_per_query * hours_per_query * 0.0057785
        )
        total_cost = cost_per_query * 22
        print(f"  - Estimated: ~${total_cost:.2f} for full benchmark (22 queries)")
        print()
        print("Before running:")
        print("1. Verify S3 bucket exists and is accessible")
        print("2. Check IAM execution role has correct permissions")
        print("3. Ensure EMR Serverless is available in your region")
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
    print("- Review results.json for query timings and resource usage")
    print("- Check EMR Serverless console for job history")
    print("- Compare with other platforms using result_analysis.py")
    print("- Clean up S3 data: aws s3 rm --recursive s3://bucket/benchbox/")
    print()
    print("Cost optimization tips:")
    print("- Use pre-initialized capacity for latency-sensitive workloads")
    print("- Disable pre-initialized capacity when not needed")
    print("- Monitor vCPU and memory usage to right-size")
    print("- Use Spark UI for query optimization")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on Amazon EMR Serverless")
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
