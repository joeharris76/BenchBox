"""Visualization tools for BenchBox MCP server.

Provides tools for generating charts and visualizations from benchmark results.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.mcp.errors import ErrorCode, make_error
from benchbox.utils.dependencies import get_package_install_message

logger = logging.getLogger(__name__)

# Tool annotations for read-only visualization info tools
VIZ_READONLY_ANNOTATIONS = ToolAnnotations(
    title="Visualization information",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Tool annotations for chart generation (creates files)
VIZ_GENERATE_ANNOTATIONS = ToolAnnotations(
    title="Generate visualization",
    readOnlyHint=False,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)

# Default output directories
DEFAULT_CHARTS_DIR = Path("benchmark_runs/charts")
DEFAULT_RESULTS_DIR = Path("benchmark_runs/results")

# Supported chart types
CHART_TYPE_DESCRIPTIONS: dict[str, str] = {
    "performance_bar": "Bar chart comparing total runtime across platforms",
    "distribution_box": "Box plot showing query execution time distribution",
    "query_heatmap": "Heatmap comparing per-query execution times across platforms",
    "cost_scatter": "Scatter plot of cost vs performance (requires cost data)",
    "time_series": "Line chart showing performance trends over time",
}


def _check_visualization_dependencies() -> tuple[bool, str | None]:
    """Check if visualization dependencies are available."""
    try:
        import plotly  # noqa: F401

        return True, None
    except ImportError:
        return False, "Visualization dependencies not installed. " + get_package_install_message("plotly", "")


def _resolve_result_path(result_file: str) -> Path | None:
    """Resolve a result file path within the results directory."""
    if ".." in result_file or result_file.startswith("/") or result_file.startswith("\\"):
        return None

    path = DEFAULT_RESULTS_DIR / result_file
    if path.exists():
        try:
            results_dir_resolved = DEFAULT_RESULTS_DIR.resolve()
            path_resolved = path.resolve()
            if not str(path_resolved).startswith(str(results_dir_resolved)):
                return None
        except Exception:
            return None
        return path

    if not result_file.endswith(".json"):
        path = DEFAULT_RESULTS_DIR / (result_file + ".json")
        if path.exists():
            try:
                results_dir_resolved = DEFAULT_RESULTS_DIR.resolve()
                path_resolved = path.resolve()
                if not str(path_resolved).startswith(str(results_dir_resolved)):
                    return None
            except Exception:
                return None
            return path

    return None


def _validate_output_dir(output_dir: str | None) -> tuple[Path, dict[str, Any] | None]:
    """Validate and resolve an output directory path."""
    if output_dir is None:
        return DEFAULT_CHARTS_DIR, None

    if ".." in output_dir or output_dir.startswith("/") or output_dir.startswith("\\"):
        return DEFAULT_CHARTS_DIR, make_error(
            ErrorCode.VALIDATION_ERROR,
            "Invalid output directory path",
            details={"path": output_dir},
            suggestion="Use a relative path without '..' components",
        )

    charts_dir = DEFAULT_CHARTS_DIR / output_dir

    try:
        charts_dir_base_resolved = DEFAULT_CHARTS_DIR.resolve()
        charts_dir_resolved = charts_dir.resolve()
        if not str(charts_dir_resolved).startswith(str(charts_dir_base_resolved)):
            return DEFAULT_CHARTS_DIR, make_error(
                ErrorCode.VALIDATION_ERROR,
                "Output path escapes allowed directory",
                details={"path": output_dir},
            )
    except Exception:
        pass

    return charts_dir, None


def register_visualization_tools(mcp: FastMCP) -> None:
    """Register visualization tools with the MCP server."""

    @mcp.tool(annotations=VIZ_READONLY_ANNOTATIONS)
    def suggest_charts(result_files: str) -> dict[str, Any]:
        """Analyze results and suggest appropriate chart types.

        Args:
            result_files: Comma-separated list of result filenames

        Returns:
            Chart suggestions with data profile and primary recommendation.
        """
        available, error_msg = _check_visualization_dependencies()
        if not available:
            return make_error(
                ErrorCode.DEPENDENCY_MISSING,
                "Visualization dependencies not available",
                details={"missing": "plotly"},
                suggestion=error_msg,
            )

        file_list = [f.strip() for f in result_files.split(",") if f.strip()]
        if not file_list:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                "No result files provided",
                suggestion="Provide comma-separated list of result filenames",
            )

        resolved_paths: list[Path] = []
        missing_files: list[str] = []

        for result_file in file_list:
            path = _resolve_result_path(result_file)
            if path:
                resolved_paths.append(path)
            else:
                missing_files.append(result_file)

        if missing_files:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file(s) not found: {', '.join(missing_files)}",
                details={"missing_files": missing_files},
            )

        if not resolved_paths:
            return make_error(ErrorCode.VALIDATION_ERROR, "No valid result files found")

        from benchbox.core.visualization.exceptions import VisualizationError
        from benchbox.core.visualization.result_plotter import ResultPlotter

        try:
            plotter = ResultPlotter.from_sources(resolved_paths)
            results = plotter.results

            num_results = len(results)
            platforms = sorted({r.platform for r in results})
            has_cost_data = any(r.cost_total is not None for r in results)
            has_query_data = any(r.queries for r in results)
            has_timestamps = any(r.timestamp is not None for r in results)
            total_queries = sum(len(r.queries) for r in results)

            suggestions: list[dict[str, str]] = []

            suggestions.append(
                {
                    "chart_type": "performance_bar",
                    "reason": "Compare total runtime across platforms/configurations",
                    "priority": "high",
                }
            )

            if has_query_data:
                suggestions.append(
                    {
                        "chart_type": "distribution_box",
                        "reason": f"Show query time distribution ({total_queries} queries)",
                        "priority": "high" if total_queries > 5 else "medium",
                    }
                )

            if num_results >= 2 and has_query_data:
                suggestions.append(
                    {
                        "chart_type": "query_heatmap",
                        "reason": f"Compare per-query performance across {len(platforms)} platform(s)",
                        "priority": "high" if num_results == 2 else "medium",
                    }
                )

            if has_cost_data:
                suggestions.append(
                    {
                        "chart_type": "cost_scatter",
                        "reason": "Plot cost vs performance trade-offs",
                        "priority": "high",
                    }
                )

            if num_results >= 3 and has_timestamps:
                suggestions.append(
                    {
                        "chart_type": "time_series",
                        "reason": f"Show performance trends over {num_results} runs",
                        "priority": "medium",
                    }
                )

            if num_results >= 2 and has_query_data:
                primary = "query_heatmap"
                primary_reason = "Best for comparing per-query performance"
            elif has_query_data:
                primary = "distribution_box"
                primary_reason = "Best for understanding query time distribution"
            else:
                primary = "performance_bar"
                primary_reason = "Best general-purpose comparison chart"

            return {
                "suggestions": suggestions,
                "primary": {"chart_type": primary, "reason": primary_reason},
                "data_profile": {
                    "result_count": num_results,
                    "platforms": platforms,
                    "total_queries": total_queries,
                    "has_cost_data": has_cost_data,
                    "has_timestamps": has_timestamps,
                },
                "source_files": file_list,
            }

        except VisualizationError as e:
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Could not analyze results: {e}",
                details={"result_files": file_list},
            )
        except Exception as e:
            logger.exception(f"Chart suggestion failed: {e}")
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Chart suggestion failed: {e}",
                details={"exception_type": type(e).__name__},
            )

    @mcp.tool(annotations=VIZ_GENERATE_ANNOTATIONS)
    def generate_chart(
        result_files: str,
        chart_type: str = "performance_bar",
        template: str | None = None,
        output_dir: str | None = None,
    ) -> dict[str, Any]:
        """Generate chart(s) from benchmark results.

        Args:
            result_files: Comma-separated list of result filenames
            chart_type: Chart type: performance_bar, distribution_box, query_heatmap, cost_scatter, time_series
            template: Template for chart sets: default, flagship, head_to_head, trends, cost_optimization
            output_dir: Custom output directory (relative to charts dir)

        Returns:
            Chart generation status with file paths.
        """
        available, error_msg = _check_visualization_dependencies()
        if not available:
            return make_error(
                ErrorCode.DEPENDENCY_MISSING,
                "Visualization dependencies not available",
                details={"missing": "plotly"},
                suggestion=error_msg,
            )

        file_list = [f.strip() for f in result_files.split(",") if f.strip()]
        if not file_list:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                "No result files provided",
                suggestion="Provide comma-separated list of result filenames",
            )

        # Validate chart type if no template
        if template is None and chart_type not in CHART_TYPE_DESCRIPTIONS:
            return make_error(
                ErrorCode.VALIDATION_ERROR,
                f"Unknown chart type: {chart_type}",
                details={"valid_chart_types": list(CHART_TYPE_DESCRIPTIONS.keys())},
            )

        # Validate template if provided
        chart_template = None
        if template is not None:
            from benchbox.core.visualization.templates import get_template, list_templates

            try:
                chart_template = get_template(template)
            except Exception:
                available_templates = [t.name for t in list_templates()]
                return make_error(
                    ErrorCode.VALIDATION_ERROR,
                    f"Unknown template: {template}",
                    details={"available_templates": available_templates},
                )

        charts_dir, error = _validate_output_dir(output_dir)
        if error is not None:
            return error

        resolved_paths: list[Path] = []
        missing_files: list[str] = []

        for result_file in file_list:
            path = _resolve_result_path(result_file)
            if path:
                resolved_paths.append(path)
            else:
                missing_files.append(result_file)

        if missing_files:
            return make_error(
                ErrorCode.RESOURCE_NOT_FOUND,
                f"Result file(s) not found: {', '.join(missing_files)}",
                details={"missing_files": missing_files},
            )

        if not resolved_paths:
            return make_error(ErrorCode.VALIDATION_ERROR, "No valid result files found")

        from benchbox.core.visualization.exceptions import VisualizationDependencyError, VisualizationError
        from benchbox.core.visualization.result_plotter import ResultPlotter

        try:
            plotter = ResultPlotter.from_sources(resolved_paths)
            charts_dir.mkdir(parents=True, exist_ok=True)

            if template is not None and chart_template is not None:
                # Generate chart set using template
                exports = plotter.generate_all_charts(
                    output_dir=charts_dir,
                    template_name=template,
                    smart=False,
                )

                charts_info: list[dict[str, Any]] = []
                for ct, format_paths in exports.items():
                    for fmt, path in format_paths.items():
                        charts_info.append(
                            {
                                "chart_type": ct,
                                "format": fmt,
                                "file_path": str(path),
                                "file_name": path.name,
                            }
                        )

                return {
                    "status": "generated",
                    "template": template,
                    "template_description": chart_template.description,
                    "source_files": file_list,
                    "output_dir": str(charts_dir),
                    "charts": charts_info,
                    "chart_count": len(charts_info),
                }
            else:
                # Generate single chart
                fmt = "html"
                exports = plotter.generate_all_charts(
                    output_dir=charts_dir,
                    formats=[fmt],
                    chart_types=[chart_type],
                    smart=False,
                )

                if chart_type not in exports:
                    return make_error(
                        ErrorCode.INTERNAL_ERROR,
                        f"Chart type '{chart_type}' could not be generated",
                        details={"chart_type": chart_type, "reason": "Insufficient data for this chart type"},
                    )

                chart_exports = exports[chart_type]
                if fmt not in chart_exports:
                    return make_error(
                        ErrorCode.INTERNAL_ERROR,
                        f"Format '{fmt}' was not exported",
                        details={"format": fmt},
                    )

                file_path = chart_exports[fmt]
                platforms = sorted({r.platform for r in plotter.results})

                return {
                    "status": "generated",
                    "chart_type": chart_type,
                    "format": fmt,
                    "file_path": str(file_path),
                    "file_name": file_path.name,
                    "source_files": file_list,
                    "platforms_compared": platforms if len(file_list) > 1 else None,
                }

        except VisualizationDependencyError as e:
            return make_error(
                ErrorCode.DEPENDENCY_MISSING,
                str(e),
                details={"package": e.package},
                suggestion=e.advice,
            )
        except VisualizationError as e:
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Visualization error: {e}",
                details={"chart_type": chart_type, "result_files": file_list},
            )
        except Exception as e:
            logger.exception(f"Chart generation failed: {e}")
            return make_error(
                ErrorCode.INTERNAL_ERROR,
                f"Chart generation failed: {e}",
                details={"exception_type": type(e).__name__},
            )
