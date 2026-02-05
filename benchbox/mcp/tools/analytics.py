"""Analytics tools for BenchBox MCP server.

Provides tools for result comparison, regression detection, and performance trends.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.core.results.exporter import ResultExporter
from benchbox.core.results.query_normalizer import normalize_query_id
from benchbox.mcp.errors import ErrorCode, make_error, make_not_found_error
from benchbox.utils.printing import get_quiet_console

logger = logging.getLogger(__name__)

# Tool annotations for read-only analytics tools
ANALYTICS_READONLY_ANNOTATIONS = ToolAnnotations(
    title="Read-only analytics tool",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Default results directory
DEFAULT_RESULTS_DIR = Path("benchmark_runs/results")


def register_analytics_tools(mcp: FastMCP) -> None:
    """Register analytics tools with the MCP server."""

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def analyze_results(
        analysis: str = "compare",
        file1: str | None = None,
        file2: str | None = None,
        platform: str | None = None,
        benchmark: str | None = None,
        threshold_percent: float = 10.0,
        metric: str = "geometric_mean",
        group_by: str = "platform",
        limit: int = 10,
    ) -> dict[str, Any]:
        """Analyze benchmark results.

        Args:
            analysis: Analysis type: 'compare', 'regressions', 'trends', 'aggregate'
            file1: Baseline result file (for 'compare')
            file2: Comparison result file (for 'compare')
            platform: Filter by platform name
            benchmark: Filter by benchmark name
            threshold_percent: Change threshold for regressions (default: 10%)
            metric: Metric for trends: geometric_mean, p50, p95, p99, total_time
            group_by: Grouping for aggregate: platform, benchmark, date
            limit: Max runs to analyze (default: 10)

        Returns:
            Analysis results based on the selected type.
        """
        analysis_lower = analysis.lower()

        if analysis_lower == "compare":
            if not file1 or not file2:
                return make_error(
                    ErrorCode.VALIDATION_ERROR,
                    "compare analysis requires file1 and file2 parameters",
                    suggestion="Provide both file1 and file2 for comparison",
                )
            return _compare_results_impl(file1, file2, threshold_percent)

        elif analysis_lower == "regressions":
            return _detect_regressions_impl(platform, benchmark, threshold_percent, limit)

        elif analysis_lower == "trends":
            return _get_performance_trends_impl(platform, benchmark, metric, limit)

        elif analysis_lower == "aggregate":
            return _aggregate_results_impl(platform, benchmark, group_by)

        else:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Invalid analysis type: {analysis}",
                details={"valid_types": ["compare", "regressions", "trends", "aggregate"]},
            )

    @mcp.tool(annotations=ANALYTICS_READONLY_ANNOTATIONS)
    def get_query_plan(
        result_file: str,
        query_id: str,
        format: str = "tree",
    ) -> dict[str, Any]:
        """Get query execution plan from benchmark results.

        Args:
            result_file: Result file containing query plans
            query_id: Query identifier (e.g., '1', 'Q1', 'q05')
            format: Output format: 'tree', 'json', 'summary'

        Returns:
            Query plan in the requested format.
        """
        valid_formats = ["tree", "json", "summary"]
        format_lower = format.lower()
        if format_lower not in valid_formats:
            return make_error(
                ErrorCode.VALIDATION_INVALID_FORMAT,
                f"Invalid format: {format}",
                details={"valid_formats": valid_formats},
            )

        results_dir = DEFAULT_RESULTS_DIR
        file_path = results_dir / result_file

        if not file_path.exists():
            if not result_file.endswith(".json"):
                file_path = results_dir / (result_file + ".json")

        if not file_path.exists():
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file not found: {result_file}",
                details={"requested_file": result_file},
            )

        try:
            with open(file_path) as f:
                data = json.load(f)

            normalized_id = normalize_query_id(query_id)

            query_exec = None
            for query_result in data.get("queries", []):
                qid = query_result.get("id", "")
                if normalize_query_id(qid) == normalized_id:
                    query_exec = query_result
                    break

            if not query_exec:
                available_ids = [str(q.get("id", "")) for q in data.get("queries", []) if q.get("id")]
                return make_not_found_error("query", query_id, available=sorted(set(available_ids))[:20])

            plans_path = file_path.with_suffix("").with_suffix(".plans.json")
            if not plans_path.exists():
                plans_path = Path(str(file_path).replace(".json", ".plans.json"))

            if not plans_path.exists():
                return {
                    "status": "no_plan",
                    "query_id": normalized_id,
                    "message": "No query plan captured for this query",
                    "suggestion": "Run benchmark with --capture-plans flag",
                    "query_info": {"runtime_ms": query_exec.get("ms"), "status": query_exec.get("status")},
                }

            with open(plans_path) as plans_handle:
                plans_data = json.load(plans_handle)

            query_plan_entry = plans_data.get("queries", {}).get(normalized_id)
            if not query_plan_entry or "plan" not in query_plan_entry:
                return {
                    "status": "no_plan",
                    "query_id": normalized_id,
                    "message": "No query plan captured for this query",
                    "query_info": {"runtime_ms": query_exec.get("ms"), "status": query_exec.get("status")},
                }

            query_plan = query_plan_entry.get("plan")
            response: dict[str, Any] = {
                "status": "success",
                "query_id": normalized_id,
                "runtime_ms": query_exec.get("ms"),
            }

            if format_lower == "json":
                response["plan"] = query_plan
            elif format_lower == "summary":
                response["summary"] = _extract_plan_summary(query_plan)
            else:
                response["plan_tree"] = _format_plan_tree(query_plan)

            return response

        except json.JSONDecodeError as e:
            return make_error(
                ErrorCode.RESOURCE_INVALID_FORMAT,
                f"Invalid JSON in result file: {e}",
                details={"file": result_file, "parse_error": str(e)},
            )
        except Exception as e:
            logger.exception(f"Failed to get query plan: {e}")
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Failed to get query plan: {e}",
                details={"exception_type": type(e).__name__},
            )


def _compare_results_impl(file1: str, file2: str, threshold_percent: float) -> dict[str, Any]:
    """Compare two benchmark runs."""
    results_dir = DEFAULT_RESULTS_DIR
    exporter = ResultExporter(anonymize=False, console=get_quiet_console())

    def resolve_result_path(filename: str) -> Path | None:
        path = results_dir / filename
        if not path.exists() and not filename.endswith(".json"):
            path = results_dir / (filename + ".json")
        if not path.exists():
            return None
        return path

    path1 = resolve_result_path(file1)
    path2 = resolve_result_path(file2)

    if path1 is None:
        return make_error(
            ErrorCode.RESOURCE_NOT_FOUND,
            f"Baseline file not found: {file1}",
            details={"file_type": "baseline", "requested_file": file1},
        )
    if path2 is None:
        return make_error(
            ErrorCode.RESOURCE_NOT_FOUND,
            f"Comparison file not found: {file2}",
            details={"file_type": "comparison", "requested_file": file2},
        )

    comparison = exporter.compare_results(path1, path2)
    if "error" in comparison:
        return make_error(
            ErrorCode.INTERNAL_ERROR,
            comparison.get("error", "Failed to compare results"),
            details={
                "baseline_loaded": comparison.get("baseline_loaded"),
                "current_loaded": comparison.get("current_loaded"),
            },
        )

    regressions: list[str] = []
    improvements: list[str] = []
    stable: list[str] = []

    for entry in comparison.get("query_comparisons", []):
        change_pct = entry.get("change_percent", 0)
        if change_pct > threshold_percent:
            regressions.append(entry.get("query_id"))
        elif change_pct < -threshold_percent:
            improvements.append(entry.get("query_id"))
        else:
            stable.append(entry.get("query_id"))

    comparison["summary"] = {
        "total_queries_compared": len(comparison.get("query_comparisons", [])),
        "regressions": len(regressions),
        "improvements": len(improvements),
        "stable": len(stable),
        "threshold_percent": threshold_percent,
    }
    comparison["regressions"] = [q for q in regressions if q]
    comparison["improvements"] = [q for q in improvements if q]

    return comparison


def _detect_regressions_impl(
    platform: str | None,
    benchmark: str | None,
    threshold_percent: float,
    lookback_runs: int,
) -> dict[str, Any]:
    """Detect performance regressions across recent runs."""
    results_dir = DEFAULT_RESULTS_DIR

    if not results_dir.exists():
        return {"status": "no_data", "message": f"No results directory found at {results_dir}", "regressions": []}

    result_files = [
        path
        for path in results_dir.glob("*.json")
        if not path.name.endswith(".plans.json") and not path.name.endswith(".tuning.json")
    ]
    if len(result_files) < 2:
        return {
            "status": "insufficient_data",
            "message": f"Need at least 2 benchmark runs for comparison, found {len(result_files)}",
            "regressions": [],
        }

    result_files = sorted(result_files, key=lambda p: p.stat().st_mtime, reverse=True)

    runs: list[dict[str, Any]] = []
    for file_path in result_files[: lookback_runs * 2]:
        try:
            with open(file_path) as f:
                data = json.load(f)

            run_platform = data.get("platform", {}).get("name", "unknown")
            benchmark_block = data.get("benchmark", {}) if isinstance(data.get("benchmark"), dict) else {}
            run_benchmark = benchmark_block.get("id", "unknown")

            if platform and platform.lower() not in run_platform.lower():
                continue
            if benchmark and benchmark.lower() not in run_benchmark.lower():
                continue

            runs.append(
                {
                    "file": file_path.name,
                    "path": str(file_path),
                    "platform": run_platform,
                    "benchmark": run_benchmark,
                    "scale_factor": benchmark_block.get("scale_factor"),
                    "timestamp": data.get("run", {}).get("timestamp", file_path.stat().st_mtime),
                    "data": data,
                }
            )

            if len(runs) >= lookback_runs:
                break

        except Exception as e:
            logger.warning(f"Could not parse result file {file_path}: {e}")
            continue

    if len(runs) < 2:
        return {
            "status": "insufficient_data",
            "message": f"Need at least 2 comparable runs, found {len(runs)} matching filters",
            "filters_applied": {"platform": platform, "benchmark": benchmark},
            "regressions": [],
        }

    newer_run = runs[0]
    older_run = runs[1]

    def extract_timings(run_data: dict) -> dict[str, float]:
        timings: dict[str, float] = {}
        for query in run_data.get("queries", []):
            if query.get("run_type") != "measurement":
                continue
            qid = str(query.get("id", ""))
            runtime = query.get("ms")
            if qid and runtime is not None:
                timings[qid] = float(runtime)
        return timings

    older_timings = extract_timings(older_run["data"])
    newer_timings = extract_timings(newer_run["data"])

    regressions: list[dict[str, Any]] = []
    improvements: list[dict[str, Any]] = []
    stable: list[str] = []

    all_queries = set(older_timings.keys()) | set(newer_timings.keys())
    for qid in sorted(all_queries):
        old_time = older_timings.get(qid)
        new_time = newer_timings.get(qid)

        if old_time is not None and new_time is not None and old_time > 0:
            delta_ms = new_time - old_time
            delta_pct = (delta_ms / old_time) * 100

            if delta_pct > threshold_percent:
                regressions.append(
                    {
                        "query_id": qid,
                        "baseline_ms": round(old_time, 2),
                        "current_ms": round(new_time, 2),
                        "delta_ms": round(delta_ms, 2),
                        "delta_percent": round(delta_pct, 1),
                        "severity": _classify_regression_severity(delta_pct),
                    }
                )
            elif delta_pct < -threshold_percent:
                improvements.append(
                    {
                        "query_id": qid,
                        "baseline_ms": round(old_time, 2),
                        "current_ms": round(new_time, 2),
                        "delta_ms": round(delta_ms, 2),
                        "delta_percent": round(delta_pct, 1),
                    }
                )
            else:
                stable.append(qid)

    regressions.sort(key=lambda r: r["delta_percent"], reverse=True)

    total_old = sum(older_timings.values())
    total_new = sum(newer_timings.values())
    total_delta_pct = ((total_new - total_old) / total_old * 100) if total_old > 0 else 0

    return {
        "status": "completed",
        "comparison": {
            "baseline": {
                "file": older_run["file"],
                "platform": older_run["platform"],
                "benchmark": older_run["benchmark"],
                "timestamp": older_run["timestamp"],
            },
            "current": {
                "file": newer_run["file"],
                "platform": newer_run["platform"],
                "benchmark": newer_run["benchmark"],
                "timestamp": newer_run["timestamp"],
            },
        },
        "summary": {
            "total_queries": len(all_queries),
            "regressions": len(regressions),
            "improvements": len(improvements),
            "stable": len(stable),
            "total_runtime_delta_percent": round(total_delta_pct, 1),
            "threshold_percent": threshold_percent,
        },
        "regressions": regressions,
        "improvements": improvements[:5],
    }


def _get_performance_trends_impl(
    platform: str | None,
    benchmark: str | None,
    metric: str,
    limit: int,
) -> dict[str, Any]:
    """Get performance trends over multiple benchmark runs."""
    valid_metrics = ["geometric_mean", "p50", "p95", "p99", "total_time"]
    metric_lower = metric.lower()
    if metric_lower not in valid_metrics:
        return make_error(
            ErrorCode.VALIDATION_ERROR,
            f"Invalid metric: {metric}",
            details={"valid_metrics": valid_metrics},
        )

    results_dir = DEFAULT_RESULTS_DIR
    if not results_dir.exists():
        return {"status": "no_data", "message": f"No results directory found at {results_dir}", "trends": []}

    result_files = [
        path
        for path in results_dir.glob("*.json")
        if not path.name.endswith(".plans.json") and not path.name.endswith(".tuning.json")
    ]
    result_files = sorted(result_files, key=lambda p: p.stat().st_mtime, reverse=True)

    runs: list[dict[str, Any]] = []
    for file_path in result_files:
        if len(runs) >= limit:
            break

        try:
            with open(file_path) as f:
                data = json.load(f)

            run_platform = data.get("platform", {}).get("name", "unknown")
            benchmark_block = data.get("benchmark", {}) if isinstance(data.get("benchmark"), dict) else {}
            run_benchmark = benchmark_block.get("id", "unknown")

            if platform and platform.lower() not in run_platform.lower():
                continue
            if benchmark and benchmark.lower() not in run_benchmark.lower():
                continue

            timings: list[float] = []
            for query in data.get("queries", []):
                if query.get("run_type") != "measurement":
                    continue
                runtime = query.get("ms")
                if runtime is not None and runtime > 0:
                    timings.append(float(runtime))

            if not timings:
                continue

            metric_value = _calculate_metric(timings, metric_lower)

            timestamp = data.get("run", {}).get("timestamp")
            if timestamp:
                try:
                    if isinstance(timestamp, str):
                        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    else:
                        ts = datetime.fromtimestamp(timestamp)
                    timestamp_str = ts.isoformat()
                except Exception:
                    timestamp_str = str(timestamp)
            else:
                timestamp_str = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()

            runs.append(
                {
                    "file": file_path.name,
                    "platform": run_platform,
                    "benchmark": run_benchmark,
                    "scale_factor": benchmark_block.get("scale_factor"),
                    "timestamp": timestamp_str,
                    "query_count": len(timings),
                    "metric": metric_lower,
                    "value": round(metric_value, 2),
                }
            )

        except Exception as e:
            logger.warning(f"Could not parse result file {file_path}: {e}")
            continue

    if not runs:
        return {
            "status": "no_matching_data",
            "message": "No benchmark runs match the specified filters",
            "filters_applied": {"platform": platform, "benchmark": benchmark},
            "trends": [],
        }

    runs.reverse()

    if len(runs) >= 2:
        first_value = runs[0]["value"]
        last_value = runs[-1]["value"]
        if first_value > 0:
            trend_pct = ((last_value - first_value) / first_value) * 100
            if trend_pct < -5:
                trend_direction = "improving"
            elif trend_pct > 5:
                trend_direction = "degrading"
            else:
                trend_direction = "stable"
        else:
            trend_direction = "unknown"
            trend_pct = 0
    else:
        trend_direction = "insufficient_data"
        trend_pct = 0

    return {
        "status": "success",
        "metric": metric_lower,
        "filters_applied": {"platform": platform, "benchmark": benchmark, "limit": limit},
        "summary": {
            "run_count": len(runs),
            "first_run": runs[0]["timestamp"] if runs else None,
            "last_run": runs[-1]["timestamp"] if runs else None,
            "trend_direction": trend_direction,
            "trend_percent": round(trend_pct, 1),
        },
        "data_points": runs,
    }


def _aggregate_results_impl(
    platform: str | None,
    benchmark: str | None,
    group_by: str,
) -> dict[str, Any]:
    """Aggregate multiple benchmark results into summary statistics."""
    valid_group_by = ["platform", "benchmark", "date"]
    group_by_lower = group_by.lower()
    if group_by_lower not in valid_group_by:
        return make_error(
            ErrorCode.VALIDATION_ERROR,
            f"Invalid group_by: {group_by}",
            details={"valid_options": valid_group_by},
        )

    results_dir = DEFAULT_RESULTS_DIR
    if not results_dir.exists():
        return {"status": "no_data", "message": f"No results directory found at {results_dir}", "aggregates": {}}

    result_files = [
        path
        for path in results_dir.glob("*.json")
        if not path.name.endswith(".plans.json") and not path.name.endswith(".tuning.json")
    ]

    groups: dict[str, list[dict[str, Any]]] = {}

    for file_path in result_files:
        try:
            with open(file_path) as f:
                data = json.load(f)

            run_platform = data.get("platform", {}).get("name", "unknown")
            benchmark_block = data.get("benchmark", {}) if isinstance(data.get("benchmark"), dict) else {}
            run_benchmark = benchmark_block.get("id", "unknown")

            if platform and platform.lower() not in run_platform.lower():
                continue
            if benchmark and benchmark.lower() not in run_benchmark.lower():
                continue

            if group_by_lower == "platform":
                group_key = run_platform
            elif group_by_lower == "benchmark":
                group_key = run_benchmark
            else:
                timestamp = data.get("run", {}).get("timestamp", file_path.stat().st_mtime)
                if isinstance(timestamp, str):
                    try:
                        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        group_key = ts.strftime("%Y-%m-%d")
                    except Exception:
                        group_key = timestamp[:10] if len(timestamp) >= 10 else "unknown"
                else:
                    group_key = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")

            timings: list[float] = []
            for query in data.get("queries", []):
                if query.get("run_type") != "measurement":
                    continue
                runtime = query.get("ms")
                if runtime is not None and runtime > 0:
                    timings.append(float(runtime))

            if not timings:
                continue

            if group_key not in groups:
                groups[group_key] = []

            groups[group_key].append(
                {
                    "file": file_path.name,
                    "timings": timings,
                    "total_time": sum(timings),
                    "query_count": len(timings),
                    "scale_factor": benchmark_block.get("scale_factor"),
                }
            )

        except Exception as e:
            logger.warning(f"Could not parse result file {file_path}: {e}")
            continue

    if not groups:
        return {
            "status": "no_matching_data",
            "message": "No benchmark runs match the specified filters",
            "filters_applied": {"platform": platform, "benchmark": benchmark},
            "aggregates": {},
        }

    aggregates: dict[str, dict[str, Any]] = {}
    for group_key, runs in sorted(groups.items()):
        all_timings = [t for run in runs for t in run["timings"]]
        total_times = [run["total_time"] for run in runs]

        aggregates[group_key] = {
            "run_count": len(runs),
            "total_queries": len(all_timings),
            "query_stats": {
                "mean_ms": round(sum(all_timings) / len(all_timings), 2) if all_timings else 0,
                "std_ms": round(_std_dev(all_timings), 2) if len(all_timings) > 1 else 0,
                "min_ms": round(min(all_timings), 2) if all_timings else 0,
                "max_ms": round(max(all_timings), 2) if all_timings else 0,
                "p50_ms": round(_percentile(all_timings, 50), 2) if all_timings else 0,
                "p95_ms": round(_percentile(all_timings, 95), 2) if all_timings else 0,
            },
            "run_stats": {
                "mean_total_ms": round(sum(total_times) / len(total_times), 2) if total_times else 0,
                "std_total_ms": round(_std_dev(total_times), 2) if len(total_times) > 1 else 0,
                "min_total_ms": round(min(total_times), 2) if total_times else 0,
                "max_total_ms": round(max(total_times), 2) if total_times else 0,
            },
            "files": [run["file"] for run in runs],
        }

    return {
        "status": "success",
        "group_by": group_by_lower,
        "filters_applied": {"platform": platform, "benchmark": benchmark},
        "summary": {
            "total_groups": len(aggregates),
            "total_runs": sum(a["run_count"] for a in aggregates.values()),
        },
        "aggregates": aggregates,
    }


def _extract_plan_summary(plan: dict) -> dict[str, Any]:
    """Extract summary statistics from a query plan."""
    summary = {
        "operator_count": 0,
        "estimated_rows": None,
        "estimated_cost": None,
        "join_count": 0,
        "scan_count": 0,
    }

    def count_operators(node: dict | list) -> None:
        if isinstance(node, dict):
            summary["operator_count"] += 1
            op_type = node.get("type", node.get("operator", "")).lower()
            if "join" in op_type:
                summary["join_count"] += 1
            if "scan" in op_type or "read" in op_type:
                summary["scan_count"] += 1
            if "rows" in node:
                if summary["estimated_rows"] is None:
                    summary["estimated_rows"] = node["rows"]
            if "cost" in node:
                if summary["estimated_cost"] is None:
                    summary["estimated_cost"] = node["cost"]
            for v in node.values():
                count_operators(v)
        elif isinstance(node, list):
            for item in node:
                count_operators(item)

    count_operators(plan)
    return summary


def _format_plan_tree(plan: dict, indent: int = 0) -> str:
    """Format a query plan as a readable tree string."""
    lines = []
    prefix = "  " * indent

    if isinstance(plan, dict):
        op_type = plan.get("type") or plan.get("operator") or plan.get("name") or "Node"
        lines.append(f"{prefix}├── {op_type}")

        for key in ["table", "alias", "condition", "rows", "cost"]:
            if key in plan:
                lines.append(f"{prefix}│   {key}: {plan[key]}")

        children = plan.get("children") or plan.get("inputs") or plan.get("plans") or []
        if isinstance(children, list):
            for child in children:
                lines.append(_format_plan_tree(child, indent + 1))
        elif isinstance(children, dict):
            lines.append(_format_plan_tree(children, indent + 1))

    return "\n".join(lines)


def _classify_regression_severity(delta_pct: float) -> str:
    """Classify regression severity based on percentage change."""
    if delta_pct >= 100:
        return "critical"
    elif delta_pct >= 50:
        return "high"
    elif delta_pct >= 25:
        return "medium"
    else:
        return "low"


def _calculate_metric(timings: list[float], metric: str) -> float:
    """Calculate the specified performance metric from query timings."""
    if not timings:
        return 0

    if metric == "geometric_mean":
        log_sum = sum(math.log(t) for t in timings if t > 0)
        return math.exp(log_sum / len(timings)) if timings else 0
    elif metric == "p50":
        return _percentile(timings, 50)
    elif metric == "p95":
        return _percentile(timings, 95)
    elif metric == "p99":
        return _percentile(timings, 99)
    elif metric == "total_time":
        return sum(timings)
    else:
        return sum(timings) / len(timings)


def _percentile(data: list[float], p: float) -> float:
    """Calculate the p-th percentile of the data."""
    if not data:
        return 0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_data) else f
    if f == c:
        return sorted_data[f]
    return sorted_data[f] * (c - k) + sorted_data[c] * (k - f)


def _std_dev(data: list[float]) -> float:
    """Calculate standard deviation."""
    if len(data) < 2:
        return 0
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / (len(data) - 1)
    return math.sqrt(variance)
