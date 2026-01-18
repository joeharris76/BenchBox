"""Result file loading and discovery utilities for schema v2.0.

This module provides functionality to load and reconstruct BenchmarkResults
from exported JSON files, enabling result re-export and analysis without
re-running benchmarks.

IMPORTANT: Only schema v2.0 files are supported. Legacy v1.x files are rejected.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from benchbox.core.results.models import BenchmarkResults

logger = logging.getLogger(__name__)


class ResultLoadError(Exception):
    """Raised when a result file cannot be loaded or parsed."""


class UnsupportedSchemaError(ResultLoadError):
    """Raised when the result file has an unsupported schema version."""


def find_latest_result(
    directory: Path | str,
    benchmark: str | None = None,
    platform: str | None = None,
) -> Path | None:
    """Find the most recent result file in a directory.

    Args:
        directory: Directory to search for result files.
        benchmark: Optional benchmark name filter (e.g., "tpch", "tpcds").
        platform: Optional platform name filter (e.g., "duckdb", "databricks").

    Returns:
        Path to most recent result file, or None if no results found.

    Note:
        Only v2.0 schema files are considered. Companion files (.plans.json,
        .tuning.json) are excluded from the search.
    """
    directory_path = Path(directory) if isinstance(directory, str) else directory

    if not directory_path.exists() or not directory_path.is_dir():
        return None

    # Find all JSON files, excluding companion files
    result_files = [
        f
        for f in directory_path.glob("*.json")
        if not f.name.endswith(".plans.json") and not f.name.endswith(".tuning.json")
    ]

    if not result_files:
        return None

    candidates: list[tuple[Path, datetime]] = []
    for filepath in result_files:
        try:
            with open(filepath) as f:
                data = json.load(f)

            # Only consider v2.0 files
            version = data.get("version")
            if version != "2.0":
                continue

            # Extract metadata for filtering (v2.0 format)
            file_benchmark = data.get("benchmark", {}).get("id", "")
            file_platform = data.get("platform", {}).get("name", "")
            file_timestamp = data.get("run", {}).get("timestamp", "")

            # Apply filters
            if benchmark and benchmark.lower() not in file_benchmark.lower():
                continue
            if platform and platform.lower() not in file_platform.lower():
                continue

            # Parse timestamp for sorting
            try:
                timestamp_dt = datetime.fromisoformat(file_timestamp)
            except (ValueError, TypeError):
                timestamp_dt = datetime.fromtimestamp(filepath.stat().st_mtime)

            candidates.append((filepath, timestamp_dt))

        except (json.JSONDecodeError, OSError):
            continue

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[1], reverse=True)
    return candidates[0][0]


def load_result_file(filepath: Path | str) -> tuple[BenchmarkResults, dict[str, Any]]:
    """Load a result JSON file and reconstruct BenchmarkResults.

    Args:
        filepath: Path to result JSON file.

    Returns:
        Tuple of (BenchmarkResults object, raw JSON dict).

    Raises:
        ResultLoadError: If file cannot be loaded or parsed.
        UnsupportedSchemaError: If schema version is not v2.0.
        FileNotFoundError: If file does not exist.

    Note:
        Only schema v2.0 files are supported. Legacy v1.x files will raise
        UnsupportedSchemaError.
    """
    filepath_obj = Path(filepath) if isinstance(filepath, str) else filepath

    if not filepath_obj.exists():
        raise FileNotFoundError(f"Result file not found: {filepath}")

    try:
        with open(filepath_obj) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ResultLoadError(f"Invalid JSON in result file: {e}") from e
    except OSError as e:
        raise ResultLoadError(f"Failed to read result file: {e}") from e

    # Check schema version - only v2.0 is supported
    version = data.get("version")
    if version != "2.0":
        legacy_version = data.get("schema_version", version)
        raise UnsupportedSchemaError(
            f"Unsupported schema version: {legacy_version}. "
            f"Only schema v2.0 is supported. "
            f"Please re-export the result using the current version of BenchBox."
        )

    # Load companion files if they exist
    plans_data = _load_companion_file(filepath_obj, ".plans.json")
    tuning_data = _load_companion_file(filepath_obj, ".tuning.json")

    # Reconstruct BenchmarkResults
    try:
        result = reconstruct_benchmark_results(data, plans_data, tuning_data)
    except Exception as e:
        raise ResultLoadError(f"Failed to reconstruct BenchmarkResults: {e}") from e

    return result, data


def _load_companion_file(main_file: Path, suffix: str) -> dict[str, Any] | None:
    """Load a companion file if it exists."""
    companion_path = main_file.with_suffix("").with_suffix(suffix)
    if not companion_path.exists():
        # Try alternate naming: main.plans.json instead of main.json -> main.plans.json
        companion_path = Path(str(main_file).replace(".json", suffix))

    if companion_path.exists():
        try:
            with open(companion_path) as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            logger.debug(f"Could not load companion file {companion_path}: {e}")

    return None


def reconstruct_benchmark_results(
    data: dict[str, Any],
    plans_data: dict[str, Any] | None = None,
    tuning_data: dict[str, Any] | None = None,
) -> BenchmarkResults:
    """Reconstruct a BenchmarkResults object from v2.0 JSON schema.

    This function reverses the transformation performed by build_result_payload()
    in the schema module, mapping JSON keys back to BenchmarkResults dataclass
    attributes.

    Args:
        data: Result data in v2.0 JSON format.
        plans_data: Optional plans companion file data.
        tuning_data: Optional tuning companion file data.

    Returns:
        Fully reconstructed BenchmarkResults object.

    Raises:
        KeyError: If required fields are missing.
        ValueError: If data cannot be parsed correctly.
    """
    # Extract top-level sections
    run_section = data.get("run", {})
    benchmark_section = data.get("benchmark", {})
    platform_section = data.get("platform", {})
    summary_section = data.get("summary", {})
    environment_section = data.get("environment", {})
    tables_section = data.get("tables", {})
    cost_section = data.get("cost", {})

    # Parse timestamp
    timestamp_str = run_section.get("timestamp", "")
    try:
        timestamp = datetime.fromisoformat(timestamp_str)
    except (ValueError, TypeError):
        timestamp = datetime.now()

    # Extract query results from compact format
    queries_list = data.get("queries", [])
    query_results = _reconstruct_query_results(queries_list, data.get("errors", []))

    # Extract timing metrics from summary
    timing_section = summary_section.get("timing", {})
    queries_counts = summary_section.get("queries", {})

    total_execution_time = timing_section.get("total_ms", 0.0) / 1000.0
    average_query_time = timing_section.get("avg_ms", 0.0) / 1000.0

    # Extract data loading stats
    data_section = summary_section.get("data", {})
    data_loading_time = data_section.get("load_time_ms", 0.0) / 1000.0
    total_rows_loaded = data_section.get("rows_loaded", 0)

    # Extract TPC metrics
    tpc_section = summary_section.get("tpc_metrics", {})
    power_at_size = tpc_section.get("power_at_size")
    throughput_at_size = tpc_section.get("throughput_at_size")
    qphh_at_size = tpc_section.get("qphh_at_size")

    # Geometric mean can be in either timing section (computed) or tpc_metrics (explicit)
    geometric_mean_ms = timing_section.get("geometric_mean_ms") or tpc_section.get("geometric_mean_ms")
    geometric_mean_execution_time = geometric_mean_ms / 1000.0 if geometric_mean_ms else None

    # Extract table statistics
    table_statistics = {}
    for table_name, stats in tables_section.items():
        table_statistics[table_name] = {
            "rows": stats.get("rows"),
            "load_time_ms": stats.get("load_ms"),
        }

    # Reconstruct platform info
    platform_info = {
        "name": platform_section.get("name"),
        "version": platform_section.get("version"),
        "variant": platform_section.get("variant"),
    }
    if platform_section.get("config"):
        platform_info.update(platform_section["config"])

    # Extract tuning info
    tunings_applied = None
    tuning_source_file = None
    tuning_config_hash = None
    tuning_validation_status = None

    tuning_summary = platform_section.get("tuning", {})
    if tuning_summary:
        tuning_source_file = "yaml" if tuning_summary.get("source") == "yaml" else None
        tuning_config_hash = tuning_summary.get("hash")

    if tuning_data:
        tunings_applied = tuning_data.get("clauses", {})
        tuning_source_file = tuning_data.get("source_file")
        tuning_config_hash = tuning_data.get("hash")
        tuning_validation_status = tuning_data.get("validation_status")

    # Reconstruct system profile from environment
    system_profile = {}
    if environment_section:
        if environment_section.get("os"):
            os_parts = environment_section["os"].split(" ", 1)
            system_profile["os_type"] = os_parts[0]
            system_profile["os_release"] = os_parts[1] if len(os_parts) > 1 else ""
        if environment_section.get("arch"):
            system_profile["architecture"] = environment_section["arch"]
        if environment_section.get("cpu_count"):
            system_profile["cpu_count"] = environment_section["cpu_count"]
        if environment_section.get("memory_gb"):
            system_profile["memory_gb"] = environment_section["memory_gb"]
        if environment_section.get("python"):
            system_profile["python_version"] = environment_section["python"]
        if environment_section.get("machine_id"):
            system_profile["machine_id"] = environment_section["machine_id"]

    # Extract cost summary
    cost_summary = None
    if cost_section:
        cost_summary = {
            "total_cost": cost_section.get("total_usd"),
            "cost_model": cost_section.get("model", "estimated"),
        }

    # Query plans from companion file
    query_plans_captured = 0
    plan_capture_failures = 0
    if plans_data:
        query_plans_captured = plans_data.get("plans_captured", 0)
        plan_capture_failures = plans_data.get("capture_failures", 0)

    # Create BenchmarkResults object
    result = BenchmarkResults(
        # Core identification
        benchmark_name=benchmark_section.get("name", "Unknown"),
        platform=platform_section.get("name", "Unknown"),
        scale_factor=benchmark_section.get("scale_factor", 1.0),
        execution_id=run_section.get("id", ""),
        timestamp=timestamp,
        duration_seconds=run_section.get("total_duration_ms", 0) / 1000.0,
        # Query counts
        total_queries=queries_counts.get("total", 0),
        successful_queries=queries_counts.get("passed", 0),
        failed_queries=queries_counts.get("failed", 0),
        query_results=query_results,
        # Timing metrics
        total_execution_time=total_execution_time,
        average_query_time=average_query_time,
        data_loading_time=data_loading_time,
        # Data metrics
        total_rows_loaded=total_rows_loaded,
        table_statistics=table_statistics,
        # TPC metrics
        power_at_size=power_at_size,
        throughput_at_size=throughput_at_size,
        qphh_at_size=qphh_at_size,
        geometric_mean_execution_time=geometric_mean_execution_time,
        # Metadata
        test_execution_type=benchmark_section.get("mode", "standard"),
        validation_status=summary_section.get("validation", "PASSED"),
        system_profile=system_profile,
        # Configuration
        query_subset=run_section.get("query_subset"),
        # Platform info
        platform_info=platform_info,
        # Tuning
        tunings_applied=tunings_applied,
        tuning_source_file=tuning_source_file,
        tuning_config_hash=tuning_config_hash,
        tuning_validation_status=tuning_validation_status,
        # Plans
        query_plans_captured=query_plans_captured,
        plan_capture_failures=plan_capture_failures,
        # Cost
        cost_summary=cost_summary,
        # Benchmark metadata
        _benchmark_id_override=benchmark_section.get("id"),
    )

    return result


def _reconstruct_query_results(
    queries_list: list[dict[str, Any]],
    errors_list: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Reconstruct query results from compact v2.0 format.

    Converts from compact format:
        {"id": "Q1", "ms": 632.9, "rows": 100}

    To internal format:
        {"query_id": "Q1", "execution_time_ms": 632.9, "rows_returned": 100, "status": "SUCCESS"}
    """
    results: list[dict[str, Any]] = []

    # Process successful queries
    for q in queries_list:
        result: dict[str, Any] = {
            "query_id": q.get("id"),
            "execution_time_ms": q.get("ms"),
            "rows_returned": q.get("rows"),
            "status": "SUCCESS",
        }
        if q.get("iter"):
            result["iteration"] = q["iter"]
        if q.get("stream"):
            result["stream_id"] = q["stream"]
        results.append(result)

    # Add failed queries from errors
    for error in errors_list:
        if error.get("phase") == "query":
            results.append(
                {
                    "query_id": error.get("query_id"),
                    "status": "FAILED",
                    "error_type": error.get("type"),
                    "error_message": error.get("message"),
                }
            )

    return results


__all__ = [
    "find_latest_result",
    "load_result_file",
    "reconstruct_benchmark_results",
    "ResultLoadError",
    "UnsupportedSchemaError",
]
