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

from benchbox.core.results.builder import normalize_benchmark_id
from benchbox.core.results.query_normalizer import normalize_query_id

if TYPE_CHECKING:
    from benchbox.core.results.models import BenchmarkResults

SCHEMA_VERSION = "2.1"

logger = logging.getLogger(__name__)

CANONICAL_KEY_ORDER = [
    "version",
    "run",
    "benchmark",
    "platform",
    "config",
    "summary",
    "phases",
    "queries",
    "tables",
    "validation",
    "cost",
    "execution",
    "environment",
    "export",
    "errors",
]

QUERY_KEY_ORDER = ["id", "ms", "rows", "iter", "stream", "run_type", "status"]
CONFIG_KEY_ORDER = [
    "compression",
    "seed",
    "phases",
    "query_subset",
    "parallelism",
    "tuning_mode",
    "tuning_config",
    "platform_options",
    "mode",
    "test_type",
]
PHASE_KEY_ORDER = [
    "data_generation",
    "schema_creation",
    "data_loading",
    "validation",
    "power_test",
    "throughput_test",
]


def order_dict(d: dict[str, Any], key_order: list[str]) -> dict[str, Any]:
    """Return dict with keys ordered for stable JSON diffs."""
    from collections import OrderedDict

    ordered: OrderedDict[str, Any] = OrderedDict()
    for key in key_order:
        if key in d:
            ordered[key] = d[key]
    for key in sorted(d.keys()):
        if key not in ordered:
            ordered[key] = d[key]
    return ordered


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
        "execution_time_seconds",
        "execution_time_ms",
        "rows_returned",
        "iteration",
        "stream_id",
        "error_message",
        "run_type",
        "error",
        "error_type",
        "query_plan",
        "plan_fingerprint",
        "dataframe_skip_summary",
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
    OPTIONAL_KEYS = ("environment", "tables", "errors", "cost", "export", "tuning", "execution", "config", "phases")

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

        # Validate version (accept 2.0 and 2.1 as valid)
        valid_versions = ("2.0", "2.1")
        if payload.get("version") not in valid_versions:
            raise SchemaV2ValidationError(
                f"invalid schema version {payload.get('version')} (expected one of {valid_versions})"
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
    iterations_set: set[int] = set()
    streams_set: set[int] = set()

    # Normalize all query results to dictionaries first
    normalized_results = [_normalize_query_result(qr) for qr in (result.query_results or [])]

    for qr in normalized_results:
        iteration = qr.get("iteration", 1)
        stream_id = qr.get("stream_id", 0)
        if iteration is not None:
            iterations_set.add(int(iteration))
        if stream_id is not None:
            streams_set.add(int(stream_id))

    for qr in normalized_results:
        raw_id = qr.get("query_id") or qr.get("id") or qr.get("query") or ""
        query_id = normalize_query_id(raw_id)
        status = qr.get("status", "UNKNOWN")

        # Get execution time in ms, preferring canonical seconds key.
        exec_time_ms = qr.get("execution_time_ms")
        exec_time_seconds = qr.get("execution_time_seconds")
        if exec_time_ms is None and exec_time_seconds is not None:
            exec_time_ms = float(exec_time_seconds) * 1000.0

        rows = qr.get("rows_returned") or qr.get("rows") or qr.get("result_count")
        iteration = int(qr.get("iteration", 1))
        stream_id = int(qr.get("stream_id", 0))
        run_type = qr.get("run_type")
        if not run_type:
            run_type = "warmup" if iteration == 0 else "measurement"

        if status == "SUCCESS":
            if exec_time_ms is not None and run_type == "measurement":
                query_times_ms.append(exec_time_ms)
        else:
            failed_count += 1
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

        entry: dict[str, Any] = {"id": str(query_id)}
        if exec_time_ms is not None:
            entry["ms"] = round(exec_time_ms, 1)
        if rows is not None:
            entry["rows"] = rows
        entry["iter"] = iteration
        entry["stream"] = stream_id
        entry["run_type"] = run_type
        entry["status"] = status

        queries_list.append(order_dict(entry, QUERY_KEY_ORDER))

    # Compute timing statistics
    measurement_queries = [q for q in queries_list if q.get("run_type") == "measurement"]
    total_queries = len(measurement_queries)
    successful_queries = len([q for q in measurement_queries if q.get("status") == "SUCCESS"])
    failed_count = total_queries - successful_queries

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

    # Always include iterations and streams
    run["iterations"] = max(iterations_set) if iterations_set else 1
    run["streams"] = max(streams_set) if streams_set else 1
    if result.query_subset:
        run["query_subset"] = result.query_subset

    # Build benchmark block
    benchmark_name = _shorten_benchmark_name(result.benchmark_name)
    benchmark: dict[str, Any] = {
        "id": result.benchmark_id,
        "name": benchmark_name,
        "scale_factor": result.scale_factor,
        "test_type": result.test_execution_type,
    }

    # Build platform block
    platform_name = str(result.platform).replace(" (DataFrame)", "")
    platform: dict[str, Any] = {"name": platform_name}
    if result.platform_info:
        version = result.platform_info.get("platform_version") or result.platform_info.get("version")
        client_version = result.platform_info.get("client_library_version")
        platform["version"] = version or "unknown"
        platform["client_version"] = client_version or "unknown"
        variant = result.platform_info.get("variant")
        if variant:
            platform["variant"] = variant

        # Add platform config (cleaned)
        config = _extract_platform_config(result.platform_info)
        if config:
            platform["config"] = config
    if "version" not in platform:
        platform["version"] = "unknown"
    if "client_version" not in platform:
        platform["client_version"] = "unknown"

    # Add tuning summary if available
    tuning = _build_tuning_summary(result)
    if tuning:
        platform["tuning"] = tuning

    # Build environment block
    environment = _build_environment_block(result.system_profile)

    # Build tables block (compact)
    tables = _build_tables_block(result.table_statistics)

    # Build config and phases blocks
    config_block = _build_config_block(result)
    phases_block = _build_phases_block(result)

    # Add table loading errors if present
    if result.execution_phases:
        table_errors = _extract_table_errors(result.execution_phases)
        errors_list.extend(table_errors)

    # Build the payload
    payload: dict[str, Any] = {
        "version": SCHEMA_VERSION,
        "run": order_dict(
            run, ["id", "timestamp", "total_duration_ms", "query_time_ms", "iterations", "streams", "query_subset"]
        ),
        "benchmark": order_dict(benchmark, ["id", "name", "scale_factor", "test_type"]),
        "platform": order_dict(platform, ["name", "version", "client_version", "variant", "config", "tuning"]),
        "config": order_dict(config_block, CONFIG_KEY_ORDER),
        "summary": order_dict(summary, ["queries", "timing", "data", "validation", "tpc_metrics"]),
        "phases": order_dict(phases_block, PHASE_KEY_ORDER),
        "queries": queries_list,
    }

    # Add optional sections (only if non-empty)
    if environment:
        payload["environment"] = environment
    if tables:
        payload["tables"] = tables
    if errors_list:
        errors_list.sort(key=lambda e: (e.get("phase", ""), e.get("query_id", ""), e.get("type", "")))
        payload["errors"] = errors_list

    # Add cost summary if available
    if result.cost_summary:
        cost_block: dict[str, Any] = {}
        if "total_cost" in result.cost_summary:
            cost_block["total_usd"] = result.cost_summary["total_cost"]
        cost_block["model"] = result.cost_summary.get("cost_model", "estimated")
        if cost_block:
            payload["cost"] = cost_block

    # Add execution context for reproducibility (v2.1+)
    if result.execution_context:
        exec_block: dict[str, Any] = {}
        ctx = result.execution_context

        # Always include entry point
        if ctx.get("entry_point"):
            exec_block["entry_point"] = ctx["entry_point"]

        # Timestamp
        if ctx.get("invocation_timestamp"):
            exec_block["timestamp"] = ctx["invocation_timestamp"]

        # Phases (only if non-default)
        phases = ctx.get("phases")
        if phases and phases != ["power"]:
            exec_block["phases"] = phases

        # Seed (only if set)
        if ctx.get("seed") is not None:
            exec_block["seed"] = ctx["seed"]

        # Compression (only if enabled)
        if ctx.get("compression_enabled"):
            compression = {"type": ctx.get("compression_type", "none")}
            if ctx.get("compression_level"):
                compression["level"] = ctx["compression_level"]
            exec_block["compression"] = compression

        # Mode (always sql or dataframe)
        mode_value = None
        if ctx.get("mode"):
            mode_value = ctx["mode"]
        elif isinstance(result.execution_metadata, Mapping):
            mode_value = result.execution_metadata.get("mode")
        elif result.platform_info:
            mode_value = result.platform_info.get("execution_mode")
        if mode_value:
            exec_block["mode"] = mode_value

        # Official TPC mode
        if ctx.get("official"):
            exec_block["official"] = True

        # Validation mode
        if ctx.get("validation_mode"):
            exec_block["validation_mode"] = ctx["validation_mode"]

        # Force flags
        force_flags = []
        if ctx.get("force_datagen"):
            force_flags.append("datagen")
        if ctx.get("force_upload"):
            force_flags.append("upload")
        if force_flags:
            exec_block["force"] = force_flags

        # Query subset (also in run block, but include here for complete context)
        if ctx.get("query_subset"):
            exec_block["query_subset"] = ctx["query_subset"]

        # Plan capture
        if ctx.get("capture_plans"):
            exec_block["capture_plans"] = True
        if ctx.get("strict_plan_capture"):
            exec_block["strict_plan_capture"] = True

        # Non-interactive
        if ctx.get("non_interactive"):
            exec_block["non_interactive"] = True

        # Tuning
        if ctx.get("tuning_mode"):
            exec_block["tuning_mode"] = ctx["tuning_mode"]

        if exec_block:
            payload["execution"] = exec_block

    if "execution" not in payload:
        exec_block: dict[str, Any] = {}
        mode_value = None
        if isinstance(result.execution_metadata, Mapping):
            mode_value = result.execution_metadata.get("mode")
        if not mode_value and result.platform_info:
            mode_value = result.platform_info.get("execution_mode")
        if mode_value:
            exec_block["mode"] = mode_value
        if exec_block:
            payload["execution"] = exec_block

    return order_dict(payload, CANONICAL_KEY_ORDER)


def _shorten_benchmark_name(name: str) -> str:
    if name.lower().endswith(" benchmark"):
        return name[: -len(" benchmark")]
    return name


def _build_config_block(result: BenchmarkResults) -> dict[str, Any]:
    config: dict[str, Any] = {}
    ctx = result.execution_context or {}
    run_cfg = {}
    if isinstance(result.execution_metadata, Mapping):
        run_cfg = result.execution_metadata.get("run_config") or {}

    compression = None
    if ctx.get("compression_type") or run_cfg.get("compression"):
        if ctx.get("compression_type"):
            compression = {"type": ctx.get("compression_type"), "level": ctx.get("compression_level")}
        else:
            compression = run_cfg.get("compression")
    if compression:
        config["compression"] = compression

    if ctx.get("seed") is not None:
        config["seed"] = ctx.get("seed")
    elif run_cfg.get("seed") is not None:
        config["seed"] = run_cfg.get("seed")

    if ctx.get("phases"):
        config["phases"] = ctx.get("phases")
    elif run_cfg.get("phases"):
        config["phases"] = run_cfg.get("phases")

    if ctx.get("query_subset"):
        config["query_subset"] = ctx.get("query_subset")
    elif run_cfg.get("query_subset"):
        config["query_subset"] = run_cfg.get("query_subset")

    if run_cfg.get("platform_options"):
        config["platform_options"] = run_cfg.get("platform_options")

    if run_cfg.get("tuning_mode"):
        config["tuning_mode"] = run_cfg.get("tuning_mode")
    if run_cfg.get("tuning_config"):
        config["tuning_config"] = run_cfg.get("tuning_config")

    # Execution mode and test type for reproducibility
    exec_mode = None
    if isinstance(result.execution_metadata, Mapping):
        exec_mode = result.execution_metadata.get("mode")
    if not exec_mode and result.platform_info:
        exec_mode = result.platform_info.get("execution_mode")
    if exec_mode:
        config["mode"] = exec_mode
    # test_type is in benchmark block, not needed here

    return config


def _build_phases_block(result: BenchmarkResults) -> dict[str, Any]:
    phases: dict[str, Any] = {}
    standard = [
        "data_generation",
        "schema_creation",
        "data_loading",
        "validation",
        "power_test",
        "throughput_test",
    ]

    setup = getattr(result.execution_phases, "setup", None) if result.execution_phases else None
    if setup:
        if setup.data_generation:
            phases["data_generation"] = {
                "status": setup.data_generation.status,
                "duration_ms": setup.data_generation.duration_ms,
            }
        if setup.schema_creation:
            phases["schema_creation"] = {
                "status": setup.schema_creation.status,
                "duration_ms": setup.schema_creation.duration_ms,
            }
        if setup.data_loading:
            phases["data_loading"] = {
                "status": setup.data_loading.status,
                "duration_ms": setup.data_loading.duration_ms,
            }
        if setup.validation:
            phases["validation"] = {
                "status": setup.validation.row_count_validation,
                "duration_ms": setup.validation.duration_ms,
            }

    if result.execution_phases and result.execution_phases.power_test:
        phases["power_test"] = {
            "status": "COMPLETED",
            "duration_ms": result.execution_phases.power_test.duration_ms,
        }
    if result.execution_phases and result.execution_phases.throughput_test:
        phases["throughput_test"] = {
            "status": "COMPLETED",
            "duration_ms": result.execution_phases.throughput_test.duration_ms,
        }

    if isinstance(result.execution_metadata, Mapping):
        phase_status = result.execution_metadata.get("phase_status") or {}
        for name, status in phase_status.items():
            if name not in phases:
                phases[name] = status

    for phase in standard:
        if phase not in phases:
            phases[phase] = {"status": "NOT_RUN"}

    return phases


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
    if result.qph_at_size is not None:
        # Output with benchmark-specific key name based on benchmark_name
        benchmark_id = normalize_benchmark_id(result.benchmark_name or "")
        if benchmark_id == "tpcds":
            metrics["qphds_at_size"] = result.qph_at_size
        else:
            metrics["qphh_at_size"] = result.qph_at_size
    # Note: geometric_mean_ms is in summary.timing (computed from query times),
    # not here. TPC metrics block contains only TPC-specific metrics.

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
    for table_name in sorted(table_statistics.keys()):
        stats = table_statistics[table_name]
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

    exclude_keys = {
        "version",
        "variant",
        "name",
        "platform",
        "adapter_name",
        "adapter_version",
        "platform_version",
        "client_library_version",
        "client_version",
        "platform_name",
        "platform_type",
    }
    config: dict[str, Any] = {}

    def extract_recursive(d: dict[str, Any]) -> None:
        """Recursively extract config, flattening nested 'configuration' keys."""
        for key, value in d.items():
            if key.lower() in exclude_keys or value is None:
                continue
            # Skip empty values
            if isinstance(value, str) and not value:
                continue
            if isinstance(value, (list, dict)) and not value:
                continue
            # Flatten nested configuration
            if key == "configuration" and isinstance(value, dict):
                extract_recursive(value)
            else:
                config[key] = value

    extract_recursive(platform_info)
    return config
