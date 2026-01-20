"""Run TPC-H on GCP Dataproc (managed Spark clusters).

GCP Dataproc is Google Cloud's managed Apache Spark and Hadoop service. It provides
both persistent clusters (for ongoing workloads) and ephemeral clusters (created
per-job and deleted automatically).

Key Features:
    - Managed Spark clusters with auto-scaling
    - Per-second billing (1-minute minimum)
    - Preemptible VMs for up to 80% cost savings
    - Native GCS integration
    - Hive Metastore support

Prerequisites:
    1. GCP project with Dataproc API enabled
    2. GCS bucket for data staging
    3. Service account with Dataproc and GCS permissions
    4. Application Default Credentials configured

Required environment variables:
    DATAPROC_PROJECT_ID      GCP project ID
    DATAPROC_GCS_STAGING_DIR GCS path for data staging (e.g., gs://bucket/benchbox/)

Optional environment variables:
    DATAPROC_REGION          GCP region (default: us-central1)
    DATAPROC_CLUSTER_NAME    Dataproc cluster name (auto-generated if not set)
    DATAPROC_DATABASE        Hive database name (default: benchbox)
    DATAPROC_MASTER_TYPE     Master VM type (default: n2-standard-4)
    DATAPROC_WORKER_TYPE     Worker VM type (default: n2-standard-4)
    DATAPROC_NUM_WORKERS     Number of workers (default: 2)
    DATAPROC_USE_PREEMPTIBLE Use preemptible workers (default: false)
    DATAPROC_EPHEMERAL       Create ephemeral cluster per job (default: false)

Installation:
    uv add benchbox --extra dataproc

Usage:
    export DATAPROC_PROJECT_ID=my-project-id
    export DATAPROC_GCS_STAGING_DIR=gs://your-bucket/benchbox/

    python examples/getting_started/cloud/dataproc_tpch.py

    # Preview without execution
    python examples/getting_started/cloud/dataproc_tpch.py --dry-run ./preview

Cost Estimation:
    Dataproc pricing is based on VM costs + Dataproc premium (~$0.01/vCPU-hour).
    - n2-standard-4: ~$0.20/hour per node
    - 2-node cluster (1 master + 2 workers): ~$0.60/hour
    - Preemptible workers: ~$0.04/hour per node (80% savings)
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "dataproc"


def _require_env(var_name: str) -> str:
    """Require a GCP/Dataproc environment variable.

    GCP credentials should be configured via:
    - gcloud auth application-default login
    - Service account key: GOOGLE_APPLICATION_CREDENTIALS
    - Compute Engine/GKE default service account
    """
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(
            f"Missing environment variable {var_name}. Set GCP/Dataproc configuration before running this example."
        )
    return value


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    """Build benchmark and database configurations for Dataproc.

    GCP Dataproc Concepts:

    1. CLUSTER MODES
       - Persistent: Long-running cluster reused across jobs
       - Ephemeral: Created per job, deleted after completion
       - Single-node: For development/testing

    2. MACHINE TYPES
       - Standard: n2-standard-4 (4 vCPU, 16GB)
       - High-memory: n2-highmem-4 (4 vCPU, 32GB)
       - Compute-optimized: c2-standard-4 (4 vCPU, 16GB)

    3. PREEMPTIBLE VMs
       - Up to 80% cheaper than regular VMs
       - Can be interrupted anytime
       - Best for fault-tolerant workloads
       - Mix with regular workers for reliability

    4. HIVE METASTORE
       - Default local metastore on master node
       - Dataproc Metastore: Managed Hive-compatible service
       - Shared across clusters

    5. COST OPTIMIZATION
       - Use preemptible workers for non-critical data
       - Auto-scaling policies
       - Delete clusters when not in use
       - Right-size VMs for workload
    """
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    project_id = _require_env("DATAPROC_PROJECT_ID")
    staging_dir = _require_env("DATAPROC_GCS_STAGING_DIR")

    database_config = DatabaseConfig(
        type="dataproc",
        name="dataproc_tpch",
        options={
            # Required GCP configuration
            "project_id": project_id,
            "gcs_staging_dir": staging_dir,
            # Cluster configuration
            "region": os.getenv("DATAPROC_REGION", "us-central1"),
            "cluster_name": os.getenv("DATAPROC_CLUSTER_NAME"),
            "database": os.getenv("DATAPROC_DATABASE", "benchbox"),
            # Machine types
            "master_machine_type": os.getenv("DATAPROC_MASTER_TYPE", "n2-standard-4"),
            "worker_machine_type": os.getenv("DATAPROC_WORKER_TYPE", "n2-standard-4"),
            "num_workers": int(os.getenv("DATAPROC_NUM_WORKERS", "2")),
            # Cost optimization
            "use_preemptible": os.getenv("DATAPROC_USE_PREEMPTIBLE", "").lower() == "true",
            "ephemeral_cluster": os.getenv("DATAPROC_EPHEMERAL", "").lower() == "true",
        },
    )

    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    """Execute TPC-H benchmark on GCP Dataproc.

    GCP Dataproc is ideal for:
    - Large-scale Spark workloads
    - Integration with GCP data services (BigQuery, GCS, Bigtable)
    - Cost-effective processing with preemptible VMs
    - Existing Hadoop/Spark ecosystem tools
    """
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="dataproc_tpch",
        )
        print()
        print("Dry-run complete! Review SQL files before running on Dataproc.")
        print()
        print("Cost estimation (n2-standard-4 VMs):")
        print(f"- TPC-H SF={scale_factor}:")
        num_workers = int(os.getenv("DATAPROC_NUM_WORKERS", "2"))
        total_nodes = 1 + num_workers  # 1 master + workers
        use_preemptible = os.getenv("DATAPROC_USE_PREEMPTIBLE", "").lower() == "true"
        if use_preemptible:
            cost_per_hour = 0.20 + (num_workers * 0.04)  # Master regular, workers preemptible
        else:
            cost_per_hour = total_nodes * 0.20
        # Rough estimate: 30 mins for small SF, 2 hours for SF=1
        hours = 0.5 if scale_factor < 1.0 else 2.0
        cost = hours * cost_per_hour
        print(f"  - {num_workers} workers, ~{hours:.1f} hours: ~${cost:.2f}")
        if not use_preemptible:
            print(f"  - With preemptible workers: ~${hours * (0.20 + num_workers * 0.04):.2f}")
        print()
        print("Before running:")
        print("1. Verify GCS bucket exists and is accessible")
        print("2. Check service account has Dataproc and GCS permissions")
        print("3. Ensure Dataproc API is enabled in your project")
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
    print("- Review results.json for query timings")
    print("- Check Dataproc console for job history and resource usage")
    print("- Compare with other platforms using result_analysis.py")
    print("- Clean up GCS data: gsutil rm -r gs://bucket/benchbox/")
    print()
    print("Cost optimization tips:")
    print("- Use preemptible workers (80% cheaper) for fault-tolerant workloads")
    print("- Use ephemeral clusters to avoid idle cluster costs")
    print("- Right-size VMs based on workload characteristics")
    print("- Enable auto-scaling for variable workloads")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on GCP Dataproc")
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
