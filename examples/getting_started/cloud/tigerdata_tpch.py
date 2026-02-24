"""Run TPC-H on TigerData cloud (TimescaleDB managed service).

TigerData is the managed cloud offering for TimescaleDB. BenchBox uses the
`timescaledb:cloud` deployment mode, which applies SSL defaults and skips
managed-database create/drop behavior.

Required environment configuration (choose one approach):

Option A: Service URL (recommended)
    TIGERDATA_SERVICE_URL     Full URL (postgres://user:pass@host:port/db?sslmode=require)

Option B: Individual values
    TIGERDATA_HOST            Cloud hostname
    TIGERDATA_PASSWORD        Password

Optional environment variables:
    TIGERDATA_USER            Username (default: tsdbadmin)
    TIGERDATA_PORT            Port (default: 5432)
    TIGERDATA_DATABASE        Database (default: tsdb)

Backward-compatible fallback variables are still accepted:
    TIMESCALE_SERVICE_URL, TIMESCALE_HOST, TIMESCALE_PASSWORD,
    TIMESCALE_USER, TIMESCALE_PORT, TIMESCALE_DATABASE

Usage:
    export TIGERDATA_HOST=abc123.rc8ft3nbrw.tsdb.cloud.timescale.com
    export TIGERDATA_PASSWORD=your-password
    python examples/getting_started/cloud/tigerdata_tpch.py

    # Dry-run preview
    python examples/getting_started/cloud/tigerdata_tpch.py --dry-run ./preview
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
_OUTPUT_DIR = _PROJECT_ROOT / "benchmark_runs" / "getting_started" / "tigerdata"


def _env(primary: str, fallback: str) -> str | None:
    return os.getenv(primary) or os.getenv(fallback)


def _build_configs(scale_factor: float) -> tuple[BenchmarkConfig, DatabaseConfig]:
    benchmark_config = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=scale_factor,
        test_execution_type="power",
        options={"enable_preflight_validation": False},
    )

    service_url = _env("TIGERDATA_SERVICE_URL", "TIMESCALE_SERVICE_URL")
    host = _env("TIGERDATA_HOST", "TIMESCALE_HOST")
    password = _env("TIGERDATA_PASSWORD", "TIMESCALE_PASSWORD")

    if not service_url and (not host or not password):
        raise RuntimeError(
            "Missing TigerData cloud connection settings. Configure either:\n"
            "  TIGERDATA_SERVICE_URL (or TIMESCALE_SERVICE_URL fallback)\n"
            "or:\n"
            "  TIGERDATA_HOST + TIGERDATA_PASSWORD\n"
            "  (TIMESCALE_HOST/TIMESCALE_PASSWORD fallback supported)."
        )

    options: dict[str, object] = {"deployment_mode": "cloud"}
    if service_url:
        options["service_url"] = service_url
    else:
        options["host"] = host
        options["password"] = password
        options["username"] = _env("TIGERDATA_USER", "TIMESCALE_USER") or "tsdbadmin"
        options["port"] = int(_env("TIGERDATA_PORT", "TIMESCALE_PORT") or "5432")
        options["database"] = _env("TIGERDATA_DATABASE", "TIMESCALE_DATABASE") or "tsdb"

    database_config = DatabaseConfig(
        type="timescaledb",
        name="tigerdata_tpch",
        options=options,
    )
    return benchmark_config, database_config


def run_example(scale_factor: float = 0.01, *, dry_run_output: Path | None = None) -> None:
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    benchmark_config, database_config = _build_configs(scale_factor)

    if dry_run_output is not None:
        execute_example_dry_run(
            benchmark_config=benchmark_config,
            database_config=database_config,
            output_dir=Path(dry_run_output),
            filename_prefix="tigerdata_tpch",
        )
        print("Dry-run complete. Review generated SQL before executing against TigerData.")
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

    print(f"TPC-H completed at scale {scale_factor} with {result.successful_queries} successful queries")
    print(f"Total runtime (s): {result.total_execution_time:.2f}")
    print(f"Results saved to: {_OUTPUT_DIR}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H on TigerData cloud")
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
    run_example(scale_factor=args.scale, dry_run_output=Path(args.dry_run) if args.dry_run else None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
