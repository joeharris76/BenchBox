"""Discovery tools for BenchBox MCP server.

Provides tools for discovering available platforms, benchmarks, and system information.
These are read-only tools that help users understand what BenchBox can do.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import logging
from typing import Any

from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

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
        from benchbox.cli.benchmarks import BenchmarkManager

        manager = BenchmarkManager()
        benchmark_metadata = manager.benchmarks

        benchmarks = []
        for name, meta in benchmark_metadata.items():
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
                "dataframe_support": name in ("tpch", "tpcds"),
            }
            benchmarks.append(benchmark_data)

        # Group by category
        categories = {}
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
        from benchbox.cli.benchmarks import BenchmarkManager
        from benchbox.core.benchmark_loader import get_benchmark_class

        manager = BenchmarkManager()
        benchmark_lower = benchmark.lower()

        # Check if benchmark exists in metadata
        if benchmark_lower not in manager.benchmarks:
            return {
                "error": f"Benchmark '{benchmark}' not found",
                "available_benchmarks": list(manager.benchmarks.keys()),
            }

        meta = manager.benchmarks[benchmark_lower]

        # Try to load the benchmark class for more details
        queries = []
        tables = []
        try:
            benchmark_class = get_benchmark_class(benchmark_lower)
            # Instantiate with minimal config to get query info
            bm = benchmark_class(scale_factor=0.01)

            # Try different methods to get query IDs
            query_ids = []
            if hasattr(bm, "query_manager") and hasattr(bm.query_manager, "get_all_queries"):
                all_queries = bm.query_manager.get_all_queries()
                query_ids = list(all_queries.keys())
            elif hasattr(bm, "get_query_ids"):
                query_ids = bm.get_query_ids()

            for qid in query_ids:
                query_info = {"id": qid}
                if hasattr(bm, "get_query"):
                    try:
                        q = bm.get_query(qid)
                        if hasattr(q, "description"):
                            query_info["description"] = q.description
                    except Exception:
                        pass
                queries.append(query_info)

            if hasattr(bm, "get_tables"):
                tables = bm.get_tables()
            elif hasattr(bm, "tables"):
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
            "dataframe_support": benchmark_lower in ("tpch", "tpcds"),
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
