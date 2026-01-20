"""Discovery tools for BenchBox MCP server.

Provides tools for discovering available platforms, benchmarks, and system information.
These are read-only tools that help users understand what BenchBox can do.

This module uses the public BenchBox API (benchbox.*, benchbox.platforms.*)
and the core benchmark registry for all discovery operations.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from benchbox.core.benchmark_registry import (
    BENCHMARK_METADATA,
    get_benchmark_class,
)

logger = logging.getLogger(__name__)

# Tool annotations for read-only discovery tools
READONLY_ANNOTATIONS = ToolAnnotations(
    title="Read-only discovery tool",
    readOnlyHint=True,
    destructiveHint=False,
    idempotentHint=True,
    openWorldHint=False,
)


def register_discovery_tools(mcp: FastMCP) -> None:
    """Register discovery tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """

    @mcp.tool(annotations=READONLY_ANNOTATIONS)
    def list_platforms() -> dict[str, Any]:
        """List all available database platforms.

        Returns information about each platform including:
        - Name and display name
        - Category (analytical, cloud, embedded, dataframe)
        - Whether it's currently available (dependencies installed)
        - Required configuration fields
        - Supported execution modes (SQL, DataFrame)

        Returns:
            Dictionary with platform information.
        """
        from benchbox.core.platform_registry import PlatformRegistry

        platforms = []
        all_metadata = PlatformRegistry.get_all_platform_metadata()

        for name, metadata in all_metadata.items():
            capabilities = metadata.get("capabilities", {})
            info = PlatformRegistry.get_platform_info(name)

            platform_data = {
                "name": name,
                "display_name": metadata.get("display_name", name),
                "description": metadata.get("description", ""),
                "category": metadata.get("category", "unknown"),
                "available": info.available if info else False,
                "recommended": metadata.get("recommended", False),
                "supports_sql": capabilities.get("supports_sql", False),
                "supports_dataframe": capabilities.get("supports_dataframe", False),
                "default_mode": capabilities.get("default_mode", "sql"),
                "installation_command": metadata.get("installation_command", ""),
            }
            platforms.append(platform_data)

        # Sort by recommended first, then by name
        platforms.sort(key=lambda p: (not p["recommended"], p["name"]))

        return {
            "platforms": platforms,
            "count": len(platforms),
            "summary": {
                "available": sum(1 for p in platforms if p["available"]),
                "sql_platforms": sum(1 for p in platforms if p["supports_sql"]),
                "dataframe_platforms": sum(1 for p in platforms if p["supports_dataframe"]),
                "recommended": sum(1 for p in platforms if p["recommended"]),
            },
        }

    @mcp.tool(annotations=READONLY_ANNOTATIONS)
    def list_benchmarks() -> dict[str, Any]:
        """List all available benchmarks.

        Returns information about each benchmark including:
        - Name and description
        - Number of queries
        - Supported scale factors
        - Supported phases (load, power, throughput, maintenance)
        - DataFrame support status

        Returns:
            Dictionary with benchmark information.
        """
        benchmarks = []
        for name, meta in BENCHMARK_METADATA.items():
            benchmark_data = {
                "name": name,
                "display_name": meta.get("display_name", name),
                "description": meta.get("description", f"{name} benchmark"),
                "category": meta.get("category", "unknown"),
                "query_count": meta.get("num_queries", 0),
                "query_description": meta.get("query_description", ""),
                "scale_factors": {
                    "default": meta.get("default_scale", 0.01),
                    "options": meta.get("scale_options", [0.01, 0.1, 1, 10]),
                    "minimum": meta.get("min_scale", 0.01),
                },
                "complexity": meta.get("complexity", "Medium"),
                "estimated_time_minutes": meta.get("estimated_time_range", (1, 5)),
                "supports_streams": meta.get("supports_streams", False),
                "dataframe_support": meta.get("supports_dataframe", False),
            }
            benchmarks.append(benchmark_data)

        # Group by category
        categories: dict[str, list[str]] = {}
        for bm in benchmarks:
            cat = bm["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(bm["name"])

        return {
            "benchmarks": benchmarks,
            "count": len(benchmarks),
            "categories": categories,
        }

    @mcp.tool(annotations=READONLY_ANNOTATIONS)
    def get_benchmark_info(benchmark: str) -> dict[str, Any]:
        """Get detailed information about a specific benchmark.

        Args:
            benchmark: Name of the benchmark (e.g., 'tpch', 'tpcds')

        Returns:
            Detailed benchmark information including queries and schema.
        """
        benchmark_lower = benchmark.lower()

        # Check if benchmark exists in metadata
        if benchmark_lower not in BENCHMARK_METADATA:
            return {
                "error": f"Benchmark '{benchmark}' not found",
                "available_benchmarks": list(BENCHMARK_METADATA.keys()),
            }

        meta = BENCHMARK_METADATA[benchmark_lower]

        # Try to load the benchmark class for more details
        queries: list[dict[str, Any]] = []
        tables: list[str] = []
        try:
            benchmark_class = get_benchmark_class(benchmark_lower)
            if benchmark_class is not None:
                # Instantiate with minimal config to get query info
                bm = benchmark_class(scale_factor=0.01)

                # Get query IDs from get_queries() method (public API)
                if hasattr(bm, "get_queries"):
                    all_queries = bm.get_queries()
                    for qid in all_queries:
                        query_info: dict[str, Any] = {"id": str(qid)}
                        queries.append(query_info)

                # Get tables
                if hasattr(bm, "tables"):
                    tables = list(bm.tables)
        except Exception as e:
            logger.debug(f"Could not instantiate benchmark {benchmark}: {e}")

        return {
            "name": benchmark_lower,
            "display_name": meta.get("display_name", benchmark_lower),
            "description": meta.get("description", f"{benchmark} benchmark"),
            "category": meta.get("category", "unknown"),
            "queries": {
                "count": meta.get("num_queries", len(queries)),
                "ids": [q["id"] for q in queries][:30],  # Limit for readability
                "details": queries[:20],
                "truncated": len(queries) > 20,
            },
            "schema": {
                "tables": tables,
                "table_count": len(tables),
            },
            "scale_factors": {
                "default": meta.get("default_scale", 0.01),
                "options": meta.get("scale_options", [0.01, 0.1, 1, 10]),
                "minimum": meta.get("min_scale", 0.01),
            },
            "complexity": meta.get("complexity", "Medium"),
            "estimated_time_minutes": meta.get("estimated_time_range", (1, 5)),
            "supports_streams": meta.get("supports_streams", False),
            "dataframe_support": meta.get("supports_dataframe", False),
        }

    @mcp.tool(annotations=READONLY_ANNOTATIONS)
    def system_profile() -> dict[str, Any]:
        """Get system profile information.

        Returns information about the system including:
        - CPU cores and type
        - Available memory
        - Disk space
        - Python version and key package versions
        - BenchBox version

        Useful for determining appropriate scale factors and configurations.

        Returns:
            System profile information.
        """
        import platform

        import psutil

        import benchbox

        # Get disk space for common directories
        disk_usage = {}
        for path, name in [("/", "root"), ("/tmp", "temp")]:
            try:
                usage = psutil.disk_usage(path)
                disk_usage[name] = {
                    "path": path,
                    "total_gb": round(usage.total / (1024**3), 2),
                    "free_gb": round(usage.free / (1024**3), 2),
                    "used_percent": usage.percent,
                }
            except Exception:
                pass

        # Get memory info
        memory = psutil.virtual_memory()

        # Get package versions
        package_versions = {}
        for pkg in ["polars", "pandas", "duckdb", "pyarrow"]:
            try:
                mod = __import__(pkg)
                package_versions[pkg] = getattr(mod, "__version__", "unknown")
            except ImportError:
                package_versions[pkg] = "not installed"

        return {
            "cpu": {
                "cores": psutil.cpu_count(logical=False) or 1,
                "threads": psutil.cpu_count(logical=True) or 1,
                "architecture": platform.machine(),
            },
            "memory": {
                "total_gb": round(memory.total / (1024**3), 2),
                "available_gb": round(memory.available / (1024**3), 2),
                "used_percent": memory.percent,
            },
            "disk": disk_usage,
            "python": {
                "version": platform.python_version(),
                "implementation": platform.python_implementation(),
            },
            "packages": package_versions,
            "benchbox": {
                "version": getattr(benchbox, "__version__", "unknown"),
            },
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
            },
            "recommendations": {
                "max_scale_factor": _recommend_max_scale_factor(memory.available),
                "notes": [
                    "Scale factor 0.01 requires ~10MB RAM",
                    "Scale factor 1 requires ~1GB RAM",
                    "Scale factor 10 requires ~10GB RAM",
                ],
            },
        }


def _recommend_max_scale_factor(available_bytes: int) -> float:
    """Recommend maximum scale factor based on available memory.

    Args:
        available_bytes: Available memory in bytes

    Returns:
        Recommended maximum scale factor
    """
    available_gb = available_bytes / (1024**3)

    if available_gb >= 64:
        return 100
    elif available_gb >= 16:
        return 10
    elif available_gb >= 4:
        return 1
    elif available_gb >= 1:
        return 0.1
    else:
        return 0.01


def register_dependency_tools(mcp: FastMCP) -> None:
    """Register dependency checking tools with the MCP server.

    Args:
        mcp: The FastMCP server instance to register tools with.
    """

    @mcp.tool(annotations=READONLY_ANNOTATIONS)
    def check_dependencies(
        platform: str | None = None,
        verbose: bool = False,
    ) -> dict[str, Any]:
        """Check platform dependencies and installation status.

        Returns dependency status for platforms, including missing packages
        and installation commands. Use this to diagnose why a platform isn't available.

        Args:
            platform: Specific platform to check (optional, checks all if not provided)
            verbose: Include detailed package information

        Returns:
            Dependency status with missing packages and install commands.

        Example:
            check_dependencies()  # Check all platforms
            check_dependencies(platform="databricks")  # Check specific platform
            check_dependencies(platform="snowflake", verbose=True)  # Detailed info
        """
        from benchbox.utils.dependencies import (
            DATAFRAME_DEPENDENCY_GROUPS,
            DEPENDENCY_GROUPS,
            check_platform_dependencies,
            get_install_command,
        )

        # Combine SQL and DataFrame dependency groups
        all_groups = {**DEPENDENCY_GROUPS, **DATAFRAME_DEPENDENCY_GROUPS}

        # Filter to specific platform if requested
        if platform:
            platform_lower = platform.lower()
            # Handle -df suffix for DataFrame platforms
            base_platform = platform_lower.replace("-df", "")

            # Try exact match first, then base platform
            if platform_lower in all_groups:
                all_groups = {platform_lower: all_groups[platform_lower]}
            elif base_platform in all_groups:
                all_groups = {base_platform: all_groups[base_platform]}
            else:
                return {
                    "error": f"Unknown platform: {platform}",
                    "available_platforms": sorted(
                        [k for k in all_groups.keys() if k not in ("all", "cloud", "dataframe-all")]
                    ),
                    "suggestion": "Use check_dependencies() to see all platforms",
                }

        results: dict[str, Any] = {
            "platforms": {},
            "summary": {
                "total": 0,
                "available": 0,
                "missing_dependencies": 0,
            },
        }

        for name, info in all_groups.items():
            # Skip meta-groups in summary unless specifically requested
            if name in ("all", "cloud", "dataframe-all") and not platform:
                continue

            available, missing = check_platform_dependencies(name, info.packages)

            platform_status: dict[str, Any] = {
                "available": available,
                "description": info.description,
            }

            if not available:
                platform_status["missing_packages"] = missing
                platform_status["install_command"] = get_install_command(name)
                results["summary"]["missing_dependencies"] += 1
            else:
                results["summary"]["available"] += 1

            if verbose:
                platform_status["required_packages"] = list(info.packages)
                platform_status["use_cases"] = info.use_cases
                platform_status["supported_platforms"] = info.platforms

            results["platforms"][name] = platform_status
            results["summary"]["total"] += 1

        # Add installation recommendations
        results["recommendations"] = []

        if results["summary"]["missing_dependencies"] > 0:
            results["recommendations"].append("Install missing dependencies with: uv add benchbox --extra <platform>")
            results["recommendations"].append("For all cloud platforms: uv add benchbox --extra cloud")
            results["recommendations"].append("For everything: uv add benchbox --extra all")

        # Add quick status for single platform check
        if platform and len(results["platforms"]) == 1:
            platform_info = list(results["platforms"].values())[0]
            results["quick_status"] = {
                "platform": platform,
                "available": platform_info["available"],
                "action_required": not platform_info["available"],
            }
            if not platform_info["available"]:
                results["quick_status"]["install_command"] = platform_info.get("install_command")

        return results
