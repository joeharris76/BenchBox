"""Schema v2.0 utilities for benchmark result export.

This module provides construction and validation of the BenchBox result export format
(schema version 2.0). All exporters and downstream tooling should rely on these helpers
to ensure the canonical layout stays consistent.

Schema v2.0 Design Principles:
1. Single Source of Truth - No duplication
2. Progressive Detail - Summary first, then details
3. Omit Empty - No null placeholders, no unused sections
4. Clear Separation - Identity / Config / Results / Phases
5. Flat Where Possible - Reduce nesting depth
6. Consistent Units - All times in milliseconds
"""

from __future__ import annotations

import logging
import statistics
from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from benchbox.core.results.models import BenchmarkResults

SCHEMA_VERSION = "2.0"

logger = logging.getLogger(__name__)


def _normalize_query_result(qr: Any) -> dict[str, Any]:
    """Normalize a query result entry to a dictionary.

    Handles dict, dataclass, Pydantic model, or object with attributes.
    """
    if isinstance(qr, dict):
        return qr
    if is_dataclass(qr) and not isinstance(qr, type):
        return asdict(qr)
    # Handle Pydantic models (have model_dump method)
    if hasattr(qr, "model_dump"):
        return qr.model_dump()
    # Handle older Pydantic models (have dict method)
    if hasattr(qr, "dict"):
        return qr.dict()
    # Fallback: try to extract common attributes
    result: dict[str, Any] = {}
    for attr in (
        "query_id",
        "id",
        "status",
        "execution_time_ms",
        "execution_time",
        "rows_returned",
        "iteration",
        "stream_id",
        "error_message",
        "error",
        "error_type",
        "query_plan",
        "plan_fingerprint",
    ):
        if hasattr(qr, attr):
            val = getattr(qr, attr)
            if val is not None:
                result[attr] = val
    return result


class SchemaV2ValidationError(ValueError):
    """Raised when schema v2.0 validation fails."""


class SchemaV2Validator:
    """Validates schema v2.0 structure.

    Required keys: version, run, benchmark, platform, summary, queries
    Optional keys: environment, tables, errors, cost, export
    """

    REQUIRED_KEYS = ("version", "run", "benchmark", "platform", "summary", "queries")
    OPTIONAL_KEYS = ("environment", "tables", "errors", "cost", "export", "tuning")

    RUN_REQUIRED = ("id", "timestamp", "total_duration_ms", "query_time_ms")
    BENCHMARK_REQUIRED = ("id", "name", "scale_factor")
    PLATFORM_REQUIRED = ("name",)
    SUMMARY_REQUIRED = ("queries", "timing")

    def validate(self, payload: dict[str, Any]) -> None:
        """Raise ``SchemaV2ValidationError`` when the payload lacks required structure."""
        # Check top-level keys
        missing_top = [key for key in self.REQUIRED_KEYS if key not in payload]
        if missing_top:
            raise SchemaV2ValidationError(f"schema v2.0 payload missing keys: {missing_top}")

        # Validate version
        if payload.get("version") != SCHEMA_VERSION:
            raise SchemaV2ValidationError(
                f"invalid schema version {payload.get('version')} (expected {SCHEMA_VERSION})"
            )

        # Validate run block
        run = payload.get("run", {})
        if not isinstance(run, Mapping):
            raise SchemaV2ValidationError("run block must be a mapping")
        missing_run = [key for key in self.RUN_REQUIRED if key not in run]
        if missing_run:
            raise SchemaV2ValidationError(f"run block missing keys: {missing_run}")

        # Validate benchmark block
        benchmark = payload.get("benchmark", {})
        if not isinstance(benchmark, Mapping):
            raise SchemaV2ValidationError("benchmark block must be a mapping")
        missing_benchmark = [key for key in self.BENCHMARK_REQUIRED if key not in benchmark]
        if missing_benchmark:
            raise SchemaV2ValidationError(f"benchmark block missing keys: {missing_benchmark}")

        # Validate platform block
        platform = payload.get("platform", {})
        if not isinstance(platform, Mapping):
            raise SchemaV2ValidationError("platform block must be a mapping")
        missing_platform = [key for key in self.PLATFORM_REQUIRED if key not in platform]
        if missing_platform:
            raise SchemaV2ValidationError(f"platform block missing keys: {missing_platform}")

        # Validate summary block
        summary = payload.get("summary", {})
        if not isinstance(summary, Mapping):
            raise SchemaV2ValidationError("summary block must be a mapping")
        missing_summary = [key for key in self.SUMMARY_REQUIRED if key not in summary]
        if missing_summary:
            raise SchemaV2ValidationError(f"summary block missing keys: {missing_summary}")

        # Validate queries is a list
        queries = payload.get("queries")
        if not isinstance(queries, list):
            raise SchemaV2ValidationError("queries must be a list")

        # Check for unexpected top-level keys
        unexpected = set(payload.keys()) - set(self.REQUIRED_KEYS) - set(self.OPTIONAL_KEYS)
        if unexpected:
            raise SchemaV2ValidationError(f"schema v2.0 payload contains unexpected keys: {sorted(unexpected)}")


def build_result_payload(result: BenchmarkResults) -> dict[str, Any]:
    """Build v2.0 result payload from BenchmarkResults.

    Args:
        result: A BenchmarkResults instance from the lifecycle runner.

    Returns:
        A dictionary conforming to schema v2.0 and ready for JSON serialization.

    The compact query format:
        {"id": "Q1", "ms": 632.9, "rows": 100}
        {"id": "1", "ms": 189.2, "rows": 4, "iter": 1}
        {"id": "11", "ms": 376.8, "rows": 91, "stream": 1}
    """
    # Extract query timing data
    query_times_ms: list[float] = []
    queries_list: list[dict[str, Any]] = []
    failed_count = 0
    errors_list: list[dict[str, Any]] = []

    # Determine if we have multi-iteration or multi-stream runs
    has_iterations = False
    has_streams = False
    iterations_set: set[int] = set()
    streams_set: set[int] = set()

    # Normalize all query results to dictionaries first
    normalized_results = [_normalize_query_result(qr) for qr in (result.query_results or [])]

    for qr in normalized_results:
        iteration = qr.get("iteration")
        stream_id = qr.get("stream_id")
        if iteration is not None and iteration != 1:
            has_iterations = True
            iterations_set.add(iteration)
        if iteration == 1:
            iterations_set.add(1)
        if stream_id is not None and stream_id != 1:
            has_streams = True
            streams_set.add(stream_id)
        if stream_id == 1:
            streams_set.add(1)

    for qr in normalized_results:
        query_id = qr.get("query_id", qr.get("id", ""))
        status = qr.get("status", "UNKNOWN")

        # Get execution time in ms
        exec_time_ms = qr.get("execution_time_ms")
        exec_time = qr.get("execution_time")
        if exec_time_ms is None and exec_time is not None:
            exec_time_ms = exec_time * 1000

        rows = qr.get("rows_returned")

        if status == "SUCCESS" and exec_time_ms is not None:
            query_times_ms.append(exec_time_ms)

            # Build compact query entry
            entry: dict[str, Any] = {"id": str(query_id)}
            entry["ms"] = round(exec_time_ms, 1)
            if rows is not None:
                entry["rows"] = rows

            # Add optional fields only when non-default
            iteration = qr.get("iteration")
            if has_iterations and iteration is not None:
                entry["iter"] = iteration

            stream_id = qr.get("stream_id")
            if has_streams and stream_id is not None:
                entry["stream"] = stream_id

            queries_list.append(entry)
        else:
            failed_count += 1
            # Add to errors array
            error_entry = {
                "phase": "query",
                "query_id": str(query_id),
            }
            error_type = (
                qr.get("error_type") or qr.get("error_message", "").split(":")[0]
                if qr.get("error_message")
                else "UnknownError"
            )
            error_entry["type"] = error_type or "UnknownError"
            error_entry["message"] = qr.get("error_message") or qr.get("error") or "Query failed"
            errors_list.append(error_entry)

    # Compute timing statistics
    total_queries = len(queries_list) + failed_count
    successful_queries = len(queries_list)

    timing_stats = _compute_timing_stats(query_times_ms)

    # Build summary block
    summary: dict[str, Any] = {
        "queries": {
            "total": total_queries,
            "passed": successful_queries,
            "failed": failed_count,
        },
        "timing": timing_stats,
    }

    # Add data loading stats if available
    if result.total_rows_loaded or result.data_loading_time:
        data_stats: dict[str, Any] = {}
        if result.total_rows_loaded:
            data_stats["rows_loaded"] = result.total_rows_loaded
        if result.data_loading_time:
            data_stats["load_time_ms"] = round(result.data_loading_time * 1000, 1)
        if data_stats:
            summary["data"] = data_stats

    # Add validation status
    if result.validation_status:
        summary["validation"] = result.validation_status.lower()

    # Add TPC metrics if available
    tpc_metrics = _build_tpc_metrics(result)
    if tpc_metrics:
        summary["tpc_metrics"] = tpc_metrics

    # Build run block
    run: dict[str, Any] = {
        "id": result.execution_id,
        "timestamp": result.timestamp.isoformat() if result.timestamp else datetime.now().isoformat(),
        "total_duration_ms": round(result.duration_seconds * 1000),
        "query_time_ms": round(sum(query_times_ms)),
    }

    # Add optional run fields
    if len(iterations_set) > 1:
        run["iterations"] = max(iterations_set)
    if len(streams_set) > 1:
        run["streams"] = max(streams_set)
    if result.query_subset:
        run["query_subset"] = result.query_subset

    # Build benchmark block
    benchmark: dict[str, Any] = {
        "id": result.benchmark_id,
        "name": result.benchmark_name,
        "scale_factor": result.scale_factor,
    }
    if result.test_execution_type and result.test_execution_type != "standard":
        benchmark["mode"] = result.test_execution_type

    # Build platform block
    platform: dict[str, Any] = {"name": result.platform}
    if result.platform_info:
        version = result.platform_info.get("version")
        if version:
            platform["version"] = version
        variant = result.platform_info.get("variant")
        if variant:
            platform["variant"] = variant

        # Add platform config (cleaned)
        config = _extract_platform_config(result.platform_info)
        if config:
            platform["config"] = config

    # Add tuning summary if available
    tuning = _build_tuning_summary(result)
    if tuning:
        platform["tuning"] = tuning

    # Build environment block
    environment = _build_environment_block(result.system_profile)

    # Build tables block (compact)
    tables = _build_tables_block(result.table_statistics)

    # Add table loading errors if present
    if result.execution_phases:
        table_errors = _extract_table_errors(result.execution_phases)
        errors_list.extend(table_errors)

    # Build the payload
    payload: dict[str, Any] = {
        "version": SCHEMA_VERSION,
        "run": run,
        "benchmark": benchmark,
        "platform": platform,
        "summary": summary,
        "queries": queries_list,
    }

    # Add optional sections (only if non-empty)
    if environment:
        payload["environment"] = environment
    if tables:
        payload["tables"] = tables
    if errors_list:
        payload["errors"] = errors_list

    # Add cost summary if available
    if result.cost_summary:
        cost_block: dict[str, Any] = {}
        if "total_cost" in result.cost_summary:
            cost_block["total_usd"] = result.cost_summary["total_cost"]
        cost_block["model"] = result.cost_summary.get("cost_model", "estimated")
        if cost_block:
            payload["cost"] = cost_block

    return payload


def build_plans_payload(result: BenchmarkResults) -> dict[str, Any] | None:
    """Build companion plans file payload.

    Returns None if no plans were captured.

    Args:
        result: A BenchmarkResults instance.

    Returns:
        Dictionary for plans companion file, or None if no plans.
    """
    if not result.query_plans_captured or result.query_plans_captured == 0:
        return None

    plans_by_query: dict[str, Any] = {}
    errors_list: list[dict[str, Any]] = []

    for qr in result.query_results or []:
        query_id = str(qr.get("query_id", qr.get("id", "")))
        query_plan = qr.get("query_plan")
        plan_fingerprint = qr.get("plan_fingerprint")
        capture_time = qr.get("plan_capture_time_ms")

        if query_plan is not None:
            plan_entry: dict[str, Any] = {}
            if plan_fingerprint:
                plan_entry["fingerprint"] = plan_fingerprint
            if capture_time:
                plan_entry["capture_time_ms"] = round(capture_time, 1)

            # Serialize the plan
            if is_dataclass(query_plan):
                plan_entry["plan"] = asdict(query_plan)
            elif isinstance(query_plan, dict):
                plan_entry["plan"] = query_plan
            elif hasattr(query_plan, "to_dict"):
                plan_entry["plan"] = query_plan.to_dict()
            else:
                plan_entry["plan"] = str(query_plan)

            plans_by_query[query_id] = plan_entry

    # Add plan capture errors
    for error in result.plan_capture_errors or []:
        errors_list.append(
            {
                "query_id": error.get("query_id", "unknown"),
                "error": error.get("error", "Unknown error"),
            }
        )

    if not plans_by_query and not errors_list:
        return None

    return {
        "version": SCHEMA_VERSION,
        "run_id": result.execution_id,
        "plans_captured": len(plans_by_query),
        "capture_failures": result.plan_capture_failures or 0,
        "queries": plans_by_query,
        "errors": errors_list if errors_list else None,
    }


def build_tuning_payload(result: BenchmarkResults) -> dict[str, Any] | None:
    """Build companion tuning file payload.

    Returns None if no tuning was applied.

    Args:
        result: A BenchmarkResults instance.

    Returns:
        Dictionary for tuning companion file, or None if no tuning.
    """
    if not result.tunings_applied:
        return None

    tuning_applied = result.tunings_applied or {}
    if not tuning_applied:
        return None

    payload: dict[str, Any] = {
        "version": SCHEMA_VERSION,
        "run_id": result.execution_id,
    }

    # Source information
    if result.tuning_source_file:
        payload["source_file"] = result.tuning_source_file
    payload["source"] = "yaml" if result.tuning_source_file else "auto"

    # Hash for comparison
    if result.tuning_config_hash:
        payload["hash"] = result.tuning_config_hash

    # Validation status
    if result.tuning_validation_status:
        payload["validation_status"] = result.tuning_validation_status.lower()

    # Clauses breakdown
    clauses: dict[str, Any] = {}
    if "indexes" in tuning_applied:
        clauses["indexes"] = tuning_applied["indexes"]
    if "statistics" in tuning_applied:
        clauses["statistics"] = tuning_applied["statistics"]
    if "configuration" in tuning_applied:
        clauses["configuration"] = tuning_applied["configuration"]

    if clauses:
        payload["clauses"] = clauses

    return payload


def _compute_timing_stats(times_ms: list[float]) -> dict[str, Any]:
    """Compute timing statistics from a list of query times in milliseconds."""
    if not times_ms:
        return {
            "total_ms": 0,
            "avg_ms": 0,
            "min_ms": 0,
            "max_ms": 0,
        }

    total_ms = sum(times_ms)
    avg_ms = total_ms / len(times_ms)
    min_ms = min(times_ms)
    max_ms = max(times_ms)

    stats: dict[str, Any] = {
        "total_ms": round(total_ms, 1),
        "avg_ms": round(avg_ms, 1),
        "min_ms": round(min_ms, 1),
        "max_ms": round(max_ms, 1),
    }

    # Compute geometric mean
    if all(t > 0 for t in times_ms):
        try:
            geo_mean = statistics.geometric_mean(times_ms)
            stats["geometric_mean_ms"] = round(geo_mean, 1)
        except (statistics.StatisticsError, ValueError):
            pass

    # Compute percentiles and stdev if we have enough samples
    if len(times_ms) >= 3:
        try:
            stdev = statistics.stdev(times_ms)
            stats["stdev_ms"] = round(stdev, 1)
        except statistics.StatisticsError:
            pass

    if len(times_ms) >= 10:
        sorted_times = sorted(times_ms)
        n = len(sorted_times)

        # p90
        p90_idx = int(n * 0.90)
        stats["p90_ms"] = round(sorted_times[min(p90_idx, n - 1)], 1)

        # p95
        p95_idx = int(n * 0.95)
        stats["p95_ms"] = round(sorted_times[min(p95_idx, n - 1)], 1)

        # p99
        p99_idx = int(n * 0.99)
        stats["p99_ms"] = round(sorted_times[min(p99_idx, n - 1)], 1)

    return stats


def _build_tpc_metrics(result: BenchmarkResults) -> dict[str, Any] | None:
    """Build TPC metrics block if available."""
    metrics: dict[str, Any] = {}

    if result.power_at_size is not None:
        metrics["power_at_size"] = result.power_at_size
    if result.throughput_at_size is not None:
        metrics["throughput_at_size"] = result.throughput_at_size
    if result.qphh_at_size is not None:
        metrics["qphh_at_size"] = result.qphh_at_size
    if result.geometric_mean_execution_time is not None:
        metrics["geometric_mean_ms"] = round(result.geometric_mean_execution_time * 1000, 1)

    return metrics if metrics else None


def _build_tuning_summary(result: BenchmarkResults) -> dict[str, Any] | None:
    """Build tuning summary for platform block."""
    if not result.tunings_applied:
        return None

    summary: dict[str, Any] = {}

    # Determine source
    if result.tuning_source_file:
        summary["source"] = "yaml"
    else:
        summary["source"] = "auto"

    # Add hash for comparison
    if result.tuning_config_hash:
        summary["hash"] = result.tuning_config_hash

    # Count clauses applied
    clauses_count = 0
    tuning = result.tunings_applied or {}
    for key in ("indexes", "statistics", "configuration"):
        if key in tuning:
            val = tuning[key]
            if isinstance(val, (list, dict)):
                clauses_count += len(val)

    if clauses_count > 0:
        summary["clauses_applied"] = clauses_count

    return summary if summary else None


def _build_environment_block(system_profile: dict[str, Any] | None) -> dict[str, Any]:
    """Build environment block from system profile."""
    if not system_profile:
        return {}

    env: dict[str, Any] = {}

    # OS info
    os_type = system_profile.get("os_type") or system_profile.get("os")
    os_release = system_profile.get("os_release", "")
    if os_type:
        env["os"] = f"{os_type} {os_release}".strip() if os_release else os_type

    # Architecture
    arch = system_profile.get("architecture") or system_profile.get("arch")
    if arch:
        env["arch"] = arch

    # CPU
    cpu_count = system_profile.get("cpu_count")
    if cpu_count:
        env["cpu_count"] = cpu_count

    # Memory
    memory_gb = system_profile.get("memory_gb")
    if memory_gb:
        env["memory_gb"] = memory_gb

    # Python version
    python_version = system_profile.get("python_version")
    if python_version:
        env["python"] = python_version

    # Machine ID (anonymized)
    machine_id = system_profile.get("machine_id") or system_profile.get("anonymous_machine_id")
    if machine_id:
        env["machine_id"] = machine_id

    return env


def _build_tables_block(table_statistics: dict[str, Any] | None) -> dict[str, Any]:
    """Build compact tables block."""
    if not table_statistics:
        return {}

    tables: dict[str, Any] = {}
    for table_name, stats in table_statistics.items():
        if isinstance(stats, dict):
            entry: dict[str, Any] = {}
            if "rows" in stats:
                entry["rows"] = stats["rows"]
            elif "rows_loaded" in stats:
                entry["rows"] = stats["rows_loaded"]
            if "load_time_ms" in stats:
                entry["load_ms"] = round(stats["load_time_ms"], 1)
            elif "load_ms" in stats:
                entry["load_ms"] = round(stats["load_ms"], 1)
            if entry:
                tables[table_name] = entry
        elif isinstance(stats, int):
            # Simple row count
            tables[table_name] = {"rows": stats}

    return tables


def _extract_table_errors(execution_phases: Any) -> list[dict[str, Any]]:
    """Extract table loading errors from execution phases."""
    errors: list[dict[str, Any]] = []

    if execution_phases is None:
        return errors

    # Handle dataclass or dict
    if is_dataclass(execution_phases):
        phases = asdict(execution_phases)
    elif isinstance(execution_phases, dict):
        phases = execution_phases
    else:
        return errors

    setup = phases.get("setup", {})
    if not setup:
        return errors

    data_loading = setup.get("data_loading", {})
    if not data_loading:
        return errors

    per_table = data_loading.get("per_table_stats", {})
    for table_name, stats in (per_table or {}).items():
        if isinstance(stats, dict):
            error_type = stats.get("error_type")
            error_msg = stats.get("error_message")
            if error_type or error_msg:
                errors.append(
                    {
                        "phase": "data_loading",
                        "table": table_name,
                        "type": error_type or "LoadError",
                        "message": error_msg or "Table loading failed",
                    }
                )

    return errors


def _extract_platform_config(platform_info: dict[str, Any]) -> dict[str, Any]:
    """Extract relevant platform configuration, excluding version and variant."""
    if not platform_info:
        return {}

    exclude_keys = {"version", "variant", "name", "platform", "adapter_name", "adapter_version"}
    config: dict[str, Any] = {}

    for key, value in platform_info.items():
        if key.lower() not in exclude_keys and value is not None:
            # Skip empty values
            if isinstance(value, str) and not value:
                continue
            if isinstance(value, (list, dict)) and not value:
                continue
            config[key] = value

    return config
