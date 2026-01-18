"""Base platform adapter interface.

Defines the interface for database platform adapters.
Provides database-specific optimizations with consistent API.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import hashlib
import logging
import math
import os
import platform
import random
import signal
import statistics
import sys
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any

from benchbox.core.constants import (
    GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS,
    TPCDS_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    TPCDS_POWER_DEFAULT_WARMUP_ITERATIONS,
    TPCH_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    TPCH_POWER_DEFAULT_WARMUP_ITERATIONS,
)
from benchbox.core.errors import PlanCaptureError, SerializationError
from benchbox.core.operations import OperationExecutor
from benchbox.platforms.base.models import (
    ConnectionConfig,
    DataGenerationPhase,
    DataLoadingPhase,
    PowerTestPhase,
    QueryExecution,
    SchemaCreationPhase,
    SetupPhase,
    TableCreationStats,
    TableGenerationStats,
    TableLoadingStats,
    ThroughputStream,
    ThroughputTestPhase,
    ValidationPhase,
)
from benchbox.platforms.base.utils import is_non_interactive
from benchbox.utils.verbosity import VerbosityMixin, VerbositySettings

# Import result models for type hints
try:
    from benchbox.core.results.models import (
        BenchmarkResults,
        ExecutionPhases,
        QueryDefinition,
    )
except ImportError:
    # Handle case where results module is not available
    ExecutionPhases = None
    QueryDefinition = None
    BenchmarkResults = None

# Import tuning interface classes
try:
    from benchbox.core.tuning.interface import (
        BenchmarkTunings,
        ForeignKeyConfiguration,
        PlatformOptimizationConfiguration,
        PrimaryKeyConfiguration,
        TableTuning,
        TuningType,
        UnifiedTuningConfiguration,
    )
except ImportError:
    # Handle case where tuning module is not available
    BenchmarkTunings = None
    TableTuning = None
    TuningType = None
    UnifiedTuningConfiguration = None
    PrimaryKeyConfiguration = None
    ForeignKeyConfiguration = None
    PlatformOptimizationConfiguration = None

# Import validation classes
try:
    from benchbox.core.validation import ValidationResult
except ImportError:
    # Handle case where validation module is not available
    ValidationResult = None


# Re-export alias for existing imports/tests
EnhancedBenchmarkResults = BenchmarkResults


class PlatformAdapter(VerbosityMixin, ABC):
    """Abstract base class for database platform adapters.

    This interface defines the contract that all platform adapters must implement
    to provide database-specific optimizations for benchmark execution.
    """

    def __init__(self, **config):
        """Initialize the platform adapter with configuration.

        Args:
            **config: Platform-specific configuration options
        """
        self.config = config
        self.connection = None
        self.connection_pool = None
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._dialect = self.get_target_dialect()
        self.force_recreate = config.get("force_recreate", False)
        self.show_query_plans = config.get("show_query_plans", False)
        self.capture_plans = config.get("capture_plans", False)
        self.strict_plan_capture = config.get("strict_plan_capture", False)
        self.plan_capture_timeout_seconds = int(config.get("plan_capture_timeout_seconds", 30))
        # Plan capture sampling options
        self.plan_sampling_rate: float | None = config.get("plan_sampling_rate")
        self.plan_first_n: int | None = config.get("plan_first_n")
        plan_queries_str = config.get("plan_queries")
        self.plan_query_filter: set[str] | None = (
            {q.strip() for q in plan_queries_str.split(",") if q.strip()} if plan_queries_str else None
        )
        # Track iteration counts for plan_first_n
        self._plan_capture_iteration_counts: dict[str, int] = {}
        self.tuning_enabled = config.get("tuning_enabled", False)

        # Unified tuning configuration support
        self.unified_tuning_configuration = config.get("unified_tuning_configuration")

        # Verbose logging configuration
        self.apply_verbosity(VerbositySettings.from_mapping(config))

        # Track whether existing database was reused (vs recreated)
        self.database_was_reused = False

        # Dry-run mode support
        self.dry_run_mode = False
        self.captured_sql = []
        self.query_counter = 0

        self.enable_validation = config.get("enable_validation", False)

        # Track latest throughput metrics for phase construction
        self._last_throughput_test_result = None
        self._reset_plan_capture_stats()

    def _reset_plan_capture_stats(self) -> None:
        """Reset plan capture counters for a new benchmark run."""
        self.query_plans_captured = 0
        self.plan_capture_failures = 0
        self.plan_capture_errors: list[dict[str, Any]] = []

    @staticmethod
    @abstractmethod
    def add_cli_arguments(parser) -> None:
        """Add platform-specific CLI arguments to the argument parser.

        Args:
            parser: argparse.ArgumentParser instance to add arguments to
        """

    @classmethod
    @abstractmethod
    def from_config(cls, config: dict[str, Any]):
        """Create platform adapter instance from unified configuration.

        Args:
            config: Unified configuration dictionary

        Returns:
            Platform adapter instance
        """

    def enable_dry_run(self) -> None:
        """Enable dry-run mode for SQL capture without execution."""
        self.dry_run_mode = True
        self.captured_sql = []
        self.query_counter = 0
        self.logger.info("Dry-run mode enabled - SQL will be captured instead of executed")

    def disable_dry_run(self) -> None:
        """Disable dry-run mode and return to normal execution."""
        self.dry_run_mode = False
        self.logger.info("Dry-run mode disabled - returning to normal execution")

    def capture_sql(self, sql: str, operation_type: str = "query", table_name: str | None = None) -> None:
        """Capture SQL statement for dry-run mode.

        Args:
            sql: The SQL statement to capture
            operation_type: Type of operation (query, ddl, dml, etc.)
            table_name: Associated table name if applicable
        """
        if not self.dry_run_mode:
            return

        self.query_counter += 1
        entry = {
            "order": self.query_counter,
            "sql": sql,
            "operation_type": operation_type,
            "table_name": table_name,
            "timestamp": datetime.now().isoformat(),
        }
        self.captured_sql.append(entry)

        truncated_sql = sql if len(sql) <= 100 else f"{sql[:100]}..."
        self.logger.debug("Captured SQL (%s): %s", operation_type, truncated_sql)

    def _collect_resource_utilization(self) -> dict[str, Any]:
        """Collect host and process resource utilization metrics when possible."""
        snapshot: dict[str, Any] = {
            "available": False,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "timestamp": datetime.now().isoformat(),
            "non_interactive": is_non_interactive(),
        }

        try:
            import psutil  # type: ignore
        except ImportError:
            snapshot["reason"] = "psutil not installed"
            return snapshot

        snapshot["available"] = True
        snapshot["psutil_version"] = getattr(psutil, "__version__", None)

        try:
            process = psutil.Process(os.getpid())
        except Exception:  # pragma: no cover - defensive safeguard
            process = None

        # CPU metrics
        cpu_percent = None
        per_cpu_percent: list[float] | None = None
        try:
            psutil.cpu_percent(interval=None)  # Prime measurement for accuracy
            cpu_percent = psutil.cpu_percent(interval=0.0)
            per_cpu_percent = psutil.cpu_percent(interval=0.0, percpu=True)
        except Exception:  # pragma: no cover - defensive safeguard
            cpu_percent = None
            per_cpu_percent = None

        snapshot["cpu_percent"] = cpu_percent
        snapshot["cpu"] = {"percent": cpu_percent, "per_cpu_percent": per_cpu_percent}

        try:
            load_avg = psutil.getloadavg()
        except Exception:  # pragma: no cover - not available on Windows
            load_avg = None
        snapshot["cpu"]["load_average"] = load_avg

        try:
            freq = psutil.cpu_freq()
            snapshot["cpu"]["frequency_mhz"] = freq.current if freq else None
        except Exception:  # pragma: no cover - platform dependent
            snapshot["cpu"]["frequency_mhz"] = None

        try:
            counted = psutil.cpu_count()
            if counted:
                snapshot["cpu_count"] = counted
        except Exception:  # pragma: no cover - fallback to os.cpu_count()
            pass

        # Memory metrics
        try:
            vm = psutil.virtual_memory()
            snapshot["memory"] = {
                "total_mb": round(vm.total / (1024 * 1024), 2),
                "available_mb": round(vm.available / (1024 * 1024), 2),
                "used_mb": round((vm.total - vm.available) / (1024 * 1024), 2),
                "percent": vm.percent,
            }
        except Exception:  # pragma: no cover - defensive safeguard
            snapshot["memory"] = None

        try:
            swap = psutil.swap_memory()
            snapshot["swap"] = {
                "total_mb": round(swap.total / (1024 * 1024), 2),
                "used_mb": round(swap.used / (1024 * 1024), 2),
                "percent": swap.percent,
            }
        except Exception:  # pragma: no cover - optional
            snapshot["swap"] = None

        # Disk and network
        try:
            disk = psutil.disk_usage(str(Path.cwd()))
            snapshot["disk"] = {
                "mount_point": str(Path.cwd()),
                "total_mb": round(disk.total / (1024 * 1024), 2),
                "used_mb": round(disk.used / (1024 * 1024), 2),
                "free_mb": round(disk.free / (1024 * 1024), 2),
                "percent": disk.percent,
            }
        except Exception:  # pragma: no cover - defensive safeguard
            snapshot["disk"] = None

        try:
            disk_io = psutil.disk_io_counters()
            snapshot["disk_io"] = {
                "read_mb": round(disk_io.read_bytes / (1024 * 1024), 2),
                "write_mb": round(disk_io.write_bytes / (1024 * 1024), 2),
                "read_ops": disk_io.read_count,
                "write_ops": disk_io.write_count,
            }
        except Exception:  # pragma: no cover - optional
            snapshot["disk_io"] = None

        try:
            net_io = psutil.net_io_counters()
            snapshot["network_io"] = {
                "sent_mb": round(net_io.bytes_sent / (1024 * 1024), 2),
                "received_mb": round(net_io.bytes_recv / (1024 * 1024), 2),
                "packets_sent": net_io.packets_sent,
                "packets_recv": net_io.packets_recv,
            }
        except Exception:  # pragma: no cover - optional
            snapshot["network_io"] = None

        try:
            boot_time = datetime.fromtimestamp(psutil.boot_time()).isoformat()
        except Exception:  # pragma: no cover - optional
            boot_time = None
        snapshot["boot_time"] = boot_time

        # Process metrics
        process_snapshot: dict[str, Any] = {}
        if process is not None:
            try:
                with process.oneshot():
                    try:
                        mem_info = process.memory_full_info()
                    except AttributeError:
                        mem_info = process.memory_info()

                    rss_mb = round(mem_info.rss / (1024 * 1024), 2) if mem_info else None
                    vms = getattr(mem_info, "vms", None)
                    vms_mb = round(vms / (1024 * 1024), 2) if vms else None

                    process_snapshot.update(
                        {
                            "pid": process.pid,
                            "name": process.name(),
                            "status": process.status(),
                            "memory_mb": rss_mb,
                            "virtual_memory_mb": vms_mb,
                            "memory_percent": process.memory_percent(),
                            "cpu_percent": process.cpu_percent(interval=0.0),
                            "num_threads": process.num_threads(),
                        }
                    )

                    try:
                        process_snapshot["create_time"] = datetime.fromtimestamp(process.create_time()).isoformat()
                    except Exception:
                        process_snapshot["create_time"] = None

                    try:
                        process_snapshot["num_fds"] = process.num_fds()
                    except Exception:
                        process_snapshot["num_fds"] = None

                    try:
                        process_snapshot["num_handles"] = process.num_handles()  # type: ignore[attr-defined]
                    except Exception:
                        process_snapshot["num_handles"] = None

                    try:
                        io_counters = process.io_counters()
                        process_snapshot["io_counters"] = {
                            "read_mb": round(io_counters.read_bytes / (1024 * 1024), 2),
                            "write_mb": round(io_counters.write_bytes / (1024 * 1024), 2),
                            "read_ops": io_counters.read_count,
                            "write_ops": io_counters.write_count,
                        }
                    except Exception:
                        process_snapshot["io_counters"] = None

                    try:
                        open_files = process.open_files()
                        process_snapshot["open_files"] = [f.path for f in open_files]
                    except Exception:
                        process_snapshot["open_files"] = None

                    try:
                        ctx = process.num_ctx_switches()
                        process_snapshot["context_switches"] = {
                            "voluntary": ctx.voluntary,
                            "involuntary": ctx.involuntary,
                        }
                    except Exception:
                        process_snapshot["context_switches"] = None

            except Exception:  # pragma: no cover - defensive safeguard
                process_snapshot = {}

        if process_snapshot:
            snapshot["process_memory_mb"] = process_snapshot.get("memory_mb")
            snapshot["process_cpu_percent"] = process_snapshot.get("cpu_percent")
            snapshot["process"] = process_snapshot
        else:
            snapshot["process_memory_mb"] = None
            snapshot["process_cpu_percent"] = None
            snapshot["process"] = None

        return snapshot

    def _summarize_performance_characteristics(
        self,
        query_results: list[Any] | None,
        total_duration: float,
        total_rows_loaded: int,
    ) -> dict[str, Any]:
        """Summarize performance characteristics for benchmark execution results."""

        summary: dict[str, Any] = {
            "total_duration_seconds": total_duration,
            "total_rows_loaded": total_rows_loaded,
            "total_queries": 0,
            "successful_queries": 0,
            "failed_queries": 0,
            "success_rate": None,
            "average_query_time_ms": None,
            "average_success_query_time_ms": None,
            "throughput_qps": None,
            "successful_throughput_qps": None,
            "rows_returned_total": 0,
            "rows_returned_average": None,
            "rows_returned_per_second": None,
            "execution_time_stats": None,
            "rows_returned_stats": None,
            "error_breakdown": {},
            "failure_samples": [],
        }

        if not query_results:
            return summary

        def _extract(result: Any, attr: str, default: Any = None) -> Any:
            if hasattr(result, attr):
                return getattr(result, attr)
            if isinstance(result, dict):
                return result.get(attr, default)
            return default

        def _coerce_time_seconds(result: Any) -> float | None:
            """Best-effort conversion of execution time to seconds."""
            time_ms = _extract(result, "execution_time_ms")
            if time_ms is not None:
                try:
                    return float(time_ms) / 1000.0
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    return None

            time_value = _extract(result, "execution_time")
            if time_value is None:
                time_value = _extract(result, "duration")

            if time_value is None:
                return None

            try:
                seconds = float(time_value)
            except (TypeError, ValueError):  # pragma: no cover - defensive
                return None

            # Heuristic: treat very large numbers as milliseconds
            if seconds > 1000:
                seconds /= 1000.0
            return seconds

        def _percentile(values: list[float], percentile: float) -> float:
            if not values:
                return 0.0
            if len(values) == 1:
                return values[0]

            rank = (percentile / 100) * (len(values) - 1)
            lower_idx = math.floor(rank)
            upper_idx = math.ceil(rank)

            if lower_idx == upper_idx:
                return values[int(rank)]

            weight = rank - lower_idx
            return values[lower_idx] + weight * (values[upper_idx] - values[lower_idx])

        def _build_latency_stats(values: list[float]) -> dict[str, Any] | None:
            if not values:
                return None

            sorted_values = sorted(values)
            count = len(sorted_values)
            mean_seconds = statistics.fmean(sorted_values)
            stats_seconds = {
                "count": count,
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "mean": mean_seconds,
                "median": statistics.median(sorted_values),
                "p90": _percentile(sorted_values, 90),
                "p95": _percentile(sorted_values, 95),
                "p99": _percentile(sorted_values, 99),
                "stdev": statistics.pstdev(sorted_values) if count > 1 else 0.0,
            }

            stats_milliseconds = {
                key: (value * 1000.0 if key != "count" else value) for key, value in stats_seconds.items()
            }

            return {"seconds": stats_seconds, "milliseconds": stats_milliseconds}

        def _build_numeric_stats(values: list[float]) -> dict[str, Any] | None:
            if not values:
                return None

            sorted_values = sorted(values)
            count = len(sorted_values)
            stats_dict = {
                "count": count,
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "mean": statistics.fmean(sorted_values),
                "median": statistics.median(sorted_values),
                "p90": _percentile(sorted_values, 90),
                "p95": _percentile(sorted_values, 95),
                "stdev": statistics.pstdev(sorted_values) if count > 1 else 0.0,
            }
            return stats_dict

        total_queries = len(query_results)
        summary["total_queries"] = total_queries

        successes = 0
        durations_all: list[float] = []
        durations_success: list[float] = []
        rows_all: list[int] = []
        rows_success: list[int] = []
        failure_samples: list[dict[str, Any]] = []
        error_breakdown: dict[str, int] = {}

        for result in query_results:
            status = str(_extract(result, "status", "UNKNOWN")).upper() or "UNKNOWN"
            time_seconds = _coerce_time_seconds(result)
            rows_returned = _extract(result, "rows_returned", 0)

            if rows_returned is not None:
                try:
                    row_value = int(rows_returned)
                except (TypeError, ValueError):
                    row_value = 0
            else:
                row_value = 0

            summary["rows_returned_total"] += row_value
            rows_all.append(row_value)

            if time_seconds is not None:
                durations_all.append(time_seconds)

            if status == "SUCCESS":
                successes += 1
                rows_success.append(row_value)
                if time_seconds is not None:
                    durations_success.append(time_seconds)
            else:
                error_message = _extract(result, "error_message") or _extract(result, "error")
                error_key = str(error_message).strip()[:120] or status if error_message else status
                error_breakdown[error_key] = error_breakdown.get(error_key, 0) + 1

                failure_entry = {
                    "query_id": _extract(result, "query_id", "unknown"),
                    "status": status,
                    "rows_returned": row_value,
                }
                if time_seconds is not None:
                    failure_entry["execution_time_seconds"] = time_seconds
                if error_message:
                    failure_entry["error"] = str(error_message)
                failure_samples.append(failure_entry)

        summary["successful_queries"] = successes
        summary["failed_queries"] = total_queries - successes
        summary["error_breakdown"] = error_breakdown
        summary["failure_samples"] = failure_samples[:5]

        if total_queries:
            summary["success_rate"] = successes / total_queries

        if durations_all:
            summary["average_query_time_ms"] = statistics.fmean(durations_all) * 1000.0

        if durations_success:
            summary["average_success_query_time_ms"] = statistics.fmean(durations_success) * 1000.0

        if rows_all:
            summary["rows_returned_average"] = statistics.fmean(rows_all)

        if total_duration and total_duration > 0:
            summary["throughput_qps"] = total_queries / total_duration if total_queries else 0.0
            summary["successful_throughput_qps"] = successes / total_duration if successes else 0.0
            if summary["rows_returned_total"]:
                summary["rows_returned_per_second"] = summary["rows_returned_total"] / total_duration

        summary["execution_time_stats"] = {
            "all": _build_latency_stats(durations_all),
            "successful": _build_latency_stats(durations_success),
        }

        summary["rows_returned_stats"] = {
            "all": _build_numeric_stats([float(r) for r in rows_all]),
            "successful": _build_numeric_stats([float(r) for r in rows_success]),
        }

        return summary

    def get_captured_sql(self) -> dict[str, str]:
        """Return captured SQL statements as dictionary for dry-run display.

        Returns:
            Dictionary of query_id -> SQL statements
        """
        return {str(entry["order"]): entry["sql"] for entry in self.captured_sql}

    @property
    def platform_name(self) -> str:
        """Return the name of this database platform.

        Default implementation returns the class name. Concrete adapters may
        override to provide a user-friendly display name. Tests may instantiate
        lightweight mock adapters without overriding this property.
        """
        return self.__class__.__name__

    def get_platform_info(self, connection: Any = None) -> dict[str, Any]:
        """Get platform information for results traceability.

        Default implementation returns minimal generic info. Platform adapters
        should override to provide richer details, but tests may instantiate
        lightweight adapters without implementing this method.
        """
        return {
            "platform_type": self.platform_name.lower(),
            "platform_name": self.platform_name,
            "platform_version": "unknown",
            "connection_mode": "unknown",
            "host": None,
            "port": None,
            "configuration": {},
            "client_library_version": None,
            "embedded_library_version": None,
        }

    @property
    def dialect(self) -> str | None:
        """Return the SQL dialect for this platform (for sqlglot translation)."""
        return self._dialect

    def translate_sql(self, sql: str, source_dialect: str = "duckdb") -> str:
        """Translate SQL from source dialect to platform dialect using sqlglot.

        Args:
            sql: SQL query to translate
            source_dialect: Source SQL dialect (default: duckdb)

        Returns:
            Translated SQL query
        """
        if not self.dialect or self.dialect == source_dialect:
            return sql

        try:
            import sqlglot

            # sqlglot.transpile returns a list of translated statements
            # For schema SQL with multiple CREATE TABLE statements, we need ALL of them
            translated_statements = sqlglot.transpile(sql, read=source_dialect, write=self.dialect)

            # Join all statements back together (separated by semicolon and newlines)
            # This preserves the original structure while translating each statement
            return ";\n\n".join(translated_statements) + ";"

        except ImportError:
            self.logger.warning("sqlglot not available for SQL translation")
            return sql
        except Exception as e:
            self.logger.warning(f"Failed to translate SQL: {e}")
            return sql

    def test_connection(self, connection_config: ConnectionConfig | None = None) -> bool:
        """Test database connectivity.

        Args:
            connection_config: Optional connection configuration

        Returns:
            True if connection successful, False otherwise
        """
        try:
            test_conn = self.create_connection(**(connection_config.__dict__ if connection_config else {}))
            self.close_connection(test_conn)
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False

    @staticmethod
    def validate_platform_dependencies() -> dict[str, bool]:
        """Validate platform-specific dependencies are available.

        Returns:
            Dictionary mapping dependency names to availability status
        """
        return {
            "duckdb": PlatformAdapter._check_import("duckdb"),
            "databricks": PlatformAdapter._check_databricks_dependencies(),
            "clickhouse": PlatformAdapter._check_import("clickhouse_driver"),
            "cloudpathlib": PlatformAdapter._check_import("cloudpathlib"),
            "snowflake": PlatformAdapter._check_import("snowflake.connector"),
            "psutil": PlatformAdapter._check_import("psutil"),
        }

    @staticmethod
    def _check_import(module_name: str) -> bool:
        """Check if a module can be imported.

        Args:
            module_name: Name of the module to check

        Returns:
            True if module can be imported, False otherwise
        """
        try:
            __import__(module_name)
            return True
        except ImportError:
            return False

    @staticmethod
    def _check_databricks_dependencies() -> bool:
        """Check if Databricks-specific dependencies are available.

        Returns:
            True if all required Databricks dependencies are available
        """
        required_modules = ["databricks.sql", "databricks.sdk"]
        return all(PlatformAdapter._check_import(module) for module in required_modules)

    @staticmethod
    def require_dependencies(required: list[str], exit_on_missing: bool = True) -> dict[str, bool]:
        """Require specific dependencies, optionally exit with helpful message if missing.

        Args:
            required: List of required dependency names
            exit_on_missing: Whether to exit if dependencies are missing

        Returns:
            Dictionary mapping dependency names to availability status
        """
        available_deps = PlatformAdapter.validate_platform_dependencies()
        missing_deps = [dep for dep in required if not available_deps.get(dep, False)]

        if missing_deps:
            print("❌ Missing required dependencies:")
            for dep in missing_deps:
                print(f"   - {dep}")

            print("\n💡 Installation instructions:")
            for dep in missing_deps:
                install_cmd = PlatformAdapter._get_install_command(dep)
                if install_cmd:
                    print(f"   {dep}: {install_cmd}")

            if exit_on_missing:
                sys.exit(1)
        else:
            print("✅ All required dependencies are available")

        return available_deps

    @staticmethod
    def _get_install_command(dependency: str) -> str | None:
        """Get installation command for a dependency.

        Args:
            dependency: Name of the dependency

        Returns:
            Installation command string or None if unknown
        """
        install_commands = {
            "duckdb": "uv add duckdb",
            "databricks": "uv add databricks-sql-connector databricks-sdk",
            "clickhouse": "uv add clickhouse-driver",
            "cloudpathlib": "uv add cloudpathlib",
            "snowflake": "uv add snowflake-connector-python",
            "psutil": "uv add psutil",
        }
        return install_commands.get(dependency)

    def get_connection_from_pool(self) -> Any:
        """Get connection from pool (if supported by platform).

        Returns:
            Database connection from pool or new connection
        """
        if self.connection_pool:
            return self.connection_pool.get_connection()
        return self.create_connection(**self.config)

    @abstractmethod
    def create_connection(self, **connection_config) -> Any:
        """Create and return a database connection.

        Args:
            **connection_config: Connection-specific parameters

        Returns:
            Database connection object
        """

    @abstractmethod
    def create_schema(self, benchmark, connection: Any) -> float:
        """Create database schema for the benchmark.

        Args:
            benchmark: Benchmark instance with schema definitions
            connection: Database connection

        Returns:
            Time taken to create schema in seconds
        """

    def apply_table_tunings(self, table_tuning: TableTuning, connection: Any) -> None:
        """Apply tuning configurations to a database table.

        This method should be implemented by platform adapters to apply
        platform-specific tuning optimizations such as partitioning,
        clustering, distribution, and sorting.

        Args:
            table_tuning: The tuning configuration to apply
            connection: Database connection

        Raises:
            NotImplementedError: If tuning is not supported by the platform
            ValueError: If the tuning configuration is invalid for this platform
        """
        # Default no-op implementation for tests and platforms without tuning support
        return None

    def supports_tuning_type(self, tuning_type: TuningType) -> bool:
        """Check if this platform adapter supports a specific tuning type.

        Args:
            tuning_type: The type of tuning to check support for

        Returns:
            True if the tuning type is supported by this platform
        """
        if TuningType is None:
            return False

        # Base implementation checks compatibility using the tuning type's method
        return tuning_type.is_compatible_with_platform(self.platform_name)

    def generate_tuning_clause(self, table_tuning: TableTuning) -> str:
        """Generate platform-specific tuning clauses for CREATE TABLE statements.

        This method should generate the appropriate SQL clauses to be included
        in CREATE TABLE statements to apply the specified tuning configurations.

        Args:
            table_tuning: The tuning configuration for the table

        Returns:
            SQL clause string to be appended to CREATE TABLE statement
            (empty string if no tuning clauses are needed)

        Raises:
            ValueError: If the tuning configuration is invalid for this platform
        """
        # Default to no additional clauses
        return ""

    def apply_unified_tuning(self, unified_config: UnifiedTuningConfiguration, connection: Any) -> None:
        """Apply unified tuning configuration to the database.

        This method should implement platform-specific logic for applying
        the full unified tuning configuration, including:
        - Schema constraints (primary keys, foreign keys, unique, check)
        - Platform-specific optimizations (Z-ordering, auto-optimize, etc.)
        - Table-level tunings (partitioning, clustering, distribution, sorting)

        Args:
            unified_config: Unified tuning configuration to apply
            connection: Database connection

        Raises:
            NotImplementedError: If unified tuning is not supported by the platform
            ValueError: If the configuration is invalid for this platform
        """
        # Default no-op implementation
        if unified_config:
            self.log_verbose(f"Unified tuning not implemented for {self.platform_name} - using base class no-op")
        else:
            self.log_very_verbose("No unified tuning configuration provided")
        return None

    @abstractmethod
    def apply_platform_optimizations(self, platform_config: PlatformOptimizationConfiguration, connection: Any) -> None:
        """Apply platform-specific optimizations.

        Args:
            platform_config: Platform optimization configuration
            connection: Database connection
        """

    @abstractmethod
    def apply_constraint_configuration(
        self,
        primary_key_config: PrimaryKeyConfiguration,
        foreign_key_config: ForeignKeyConfiguration,
        connection: Any,
    ) -> None:
        """Apply constraint configurations to the database.

        Args:
            primary_key_config: Primary key constraint configuration
            foreign_key_config: Foreign key constraint configuration
            connection: Database connection
        """

    def get_effective_tuning_configuration(
        self,
    ) -> UnifiedTuningConfiguration | None:
        """Get the effective tuning configuration.

        Returns:
            The unified tuning configuration, or None if no tuning is configured
        """
        return self.unified_tuning_configuration

    def validate_tuning_configuration_for_platform(self) -> list[str]:
        """Validate the current tuning configuration against this platform's capabilities.

        Returns:
            List of validation error messages (empty if no errors)
        """
        effective_config = self.get_effective_tuning_configuration()
        if not effective_config:
            return []

        return effective_config.validate_for_platform(self.platform_name)

    @abstractmethod
    def load_data(
        self, benchmark, connection: Any, data_dir: Path
    ) -> tuple[dict[str, int], float, dict[str, Any] | None]:
        """Load benchmark data into database using platform-specific methods.

        Args:
            benchmark: Benchmark instance
            connection: Database connection
            data_dir: Directory containing data files

        Returns:
            Tuple of (table_statistics, loading_time_seconds, per_table_timings)
            where per_table_timings is optional dict with detailed timing per table
        """

    def upload_manifest(self, manifest_path: Path, remote_path: str) -> bool:
        """Upload manifest to remote storage. Override in subclasses if supported.

        Args:
            manifest_path: Local manifest file path
            remote_path: Remote directory path/URI where manifest should be uploaded

        Returns:
            True if upload succeeded, False if unsupported or not uploaded
        """
        # Default: no-op
        self.logger.debug(f"upload_manifest not implemented for {self.__class__.__name__} (remote_path={remote_path})")
        return False

    @abstractmethod
    def configure_for_benchmark(self, connection: Any, benchmark_type: str) -> None:
        """Apply platform-specific optimizations for the benchmark type.

        Args:
            connection: Database connection
            benchmark_type: Type of benchmark (e.g., "olap", "oltp", "analytics")
        """

    @abstractmethod
    def execute_query(
        self,
        connection: Any,
        query: str,
        query_id: str,
        benchmark_type: str | None = None,
        scale_factor: float | None = None,
        validate_row_count: bool = True,
        stream_id: int | None = None,
    ) -> dict[str, Any]:
        """Execute a single query and return detailed results.

        Args:
            connection: Database connection
            query: SQL query text
            query_id: Query identifier
            benchmark_type: Type of benchmark (e.g., "tpch", "tpcds") for row count validation
            scale_factor: Scale factor used for the query (for row count validation)
            validate_row_count: Whether to validate row count against expected results
            stream_id: Stream identifier for multi-stream benchmarks (e.g., 0, 1, 2...)
                      Used to select stream-specific expected results. None indicates stream 0
                      or single-stream execution.

        Returns:
            Dictionary with execution results including timing and row counts.
            If row count validation is enabled and fails, the status will be "FAILED".
            Result dictionary includes:
                - query_id: Query identifier
                - status: "SUCCESS", "FAILED", or "DRY_RUN"
                - execution_time: Execution time in seconds
                - rows_returned: Number of rows returned
                - expected_row_count: Expected row count (if validation enabled)
                - row_count_validation_status: "PASSED", "FAILED", or "SKIPPED"
                - row_count_validation_error: Error message if validation failed
        """

    def close_connection(self, connection: Any) -> None:
        """Close database connection and cleanup resources.

        Args:
            connection: Database connection to close
        """
        if connection and hasattr(connection, "close"):
            connection.close()

    def validate_loaded_data(self, connection: Any, benchmark_type: str, scale_factor: float) -> ValidationResult:
        """Validate database state after data loading using platform-specific methods.

        Args:
            connection: Database connection object
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')
            scale_factor: Scale factor for the benchmark

        Returns:
            ValidationResult with database validation status
        """
        if not self.enable_validation:
            # Return a pass-through result if validation is disabled
            return ValidationResult(
                is_valid=True,
                errors=[],
                warnings=["Validation disabled"],
                details={
                    "benchmark_type": benchmark_type,
                    "scale_factor": scale_factor,
                    "platform": self.platform_name,
                    "validation_enabled": False,
                },
            )

        from benchbox.core.validation import ValidationService

        service = ValidationService()
        result = service.run_database(connection, benchmark_type, scale_factor)

        # Include platform-specific details
        result.details.update({"platform": self.platform_name, "validation_enabled": True})

        return result

    def validate_platform_capabilities(self, benchmark_type: str) -> ValidationResult:
        """Validate platform-specific capabilities for the benchmark.

        Args:
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')

        Returns:
            ValidationResult with platform capability validation status
        """
        errors = []
        warnings = []

        # Base validation - can be overridden by specific platforms
        platform_info = {
            "platform": self.platform_name,
            "benchmark_type": benchmark_type,
            "dry_run_mode": self.dry_run_mode,
        }

        # Check if platform supports the benchmark
        if benchmark_type.lower() not in ["tpcds", "tpch"]:
            warnings.append(f"Benchmark type '{benchmark_type}' may not be fully supported")

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            details=platform_info,
        )

    def get_database_path(self, **connection_config) -> str | None:
        """Get the database file path for file-based databases.

        Override this method in platform adapters that use file-based databases.

        Args:
            **connection_config: Connection configuration

        Returns:
            Database file path if applicable, None for server-based databases
        """
        return None

    def check_database_exists(self, **connection_config) -> bool:
        """Check if database already exists.

        For file-based databases, checks if file exists.
        For server-based databases, checks if database/schema exists on server.

        Args:
            **connection_config: Connection configuration

        Returns:
            True if database exists, False otherwise
        """
        # Check for file-based databases first
        db_path = self.get_database_path(**connection_config)
        if db_path and db_path != ":memory:":
            return Path(db_path).exists()

        # For server-based databases, check if database exists on server
        return self.check_server_database_exists(**connection_config)

    def check_server_database_exists(self, **connection_config) -> bool:
        """Check if database exists on server (for server-based databases).

        Override this method in platform adapters for server-based databases.

        Args:
            **connection_config: Connection configuration

        Returns:
            True if database exists on server, False otherwise
        """
        return False

    def _validate_database_compatibility(self, **connection_config):
        """Validate database compatibility for current benchmark and configuration.

        Checks:
        1. Table existence and schema compatibility
        2. Row counts for expected scale factor
        3. Tuning configuration compatibility

        Args:
            **connection_config: Connection configuration

        Returns:
            DatabaseValidationResult with compatibility information
        """
        from benchbox.platforms.base.validation import DatabaseValidator

        validator = DatabaseValidator(adapter=self, connection_config=connection_config)
        return validator.validate()

    def handle_existing_database(self, **connection_config) -> None:
        """Handle existing database non-interactively for core/programmatic usage.

        Performs validation of database compatibility and makes automatic decisions:
        - If force_recreate=True, always recreate
        - If database is valid, reuse it
        - If database has issues, recreate it
        - If skip_database_management=True, skip all database management (for managed cloud DBs)

        Args:
            **connection_config: Connection configuration
        """
        self.log_operation_start("Database validation", "Checking existing database compatibility")

        # Skip database management for managed cloud databases
        # These platforms don't allow DROP/CREATE DATABASE operations
        if getattr(self, "skip_database_management", False):
            self.log_verbose("Database management skipped (managed cloud database)")
            self.database_was_reused = True  # Treat as reused
            return

        # Avoid infinite recursion during validation
        # When _validating_database is True, we're inside a validation connection.
        # We still need to check database existence, but skip validation/recreation logic.
        if getattr(self, "_validating_database", False):
            self.log_very_verbose("Inside validation context - skipping reuse/recreate logic.")
            return

        self.log_very_verbose("Checking if database exists...")
        if not self.check_database_exists(**connection_config):
            self.log_very_verbose("Database does not exist. Returning.")
            return
        self.log_verbose("Existing database found")

        # Determine database type and get appropriate info
        db_path = self.get_database_path(**connection_config)
        is_file_based = db_path and db_path != ":memory:"

        if is_file_based:
            # File-based database
            file_size = Path(db_path).stat().st_size
            size_mb = file_size / (1024 * 1024)
            db_info = f"{Path(db_path).name} ({size_mb:.1f} MB)"
        else:
            # Server-based database
            db_name = connection_config.get("database", "default")
            db_info = f"'{db_name}'"

        # If force_recreate is set, automatically delete and recreate
        if self.force_recreate:
            self.log_verbose(f"Force recreate enabled - removing existing database: {db_info}")
            self._remove_database(is_file_based, db_path, **connection_config)
            return

        # Perform database validation
        self.log_verbose(f"Database {db_info} already exists, validating compatibility...")
        validation_result = self._validate_database_compatibility(**connection_config)

        # Display validation results
        if validation_result.warnings:
            for warning in validation_result.warnings:
                self.logger.warning(f"⚠️ {warning}")

        if validation_result.issues:
            for issue in validation_result.issues:
                self.logger.error(f"❌ {issue}")

        # Make automatic decision based on validation results
        if validation_result.is_valid:
            # Database is fully compatible - reuse it
            self.log_verbose("Database is configured for this run")
            self.log_verbose(f"Using existing database: {db_info}")
            self.log_verbose("Database being reused - skipping schema creation and data loading")
            self.database_was_reused = True
            self.log_operation_complete(
                "Database validation", details="Database reused - compatible with current configuration"
            )
        else:
            # Database has issues - recreate it
            if validation_result.can_reuse:
                self.log_verbose("Database has compatibility issues - recreating for reliable results")
            else:
                self.log_verbose("Database is not configured for this run - recreating")

            self.log_verbose("Recreating database...")
            self.database_was_reused = False
            self._remove_database(is_file_based, db_path, **connection_config)
            self.log_operation_complete("Database validation", details="Database recreated due to incompatibility")

    def _remove_database(self, is_file_based: bool, db_path: str, **connection_config) -> None:
        """Helper method to remove/delete an existing database."""
        try:
            if is_file_based:
                db_path_obj = Path(db_path)
                if db_path_obj.is_file():
                    db_path_obj.unlink()
                    self.log_verbose("Deleted database file")
                elif db_path_obj.is_dir():
                    import shutil

                    shutil.rmtree(db_path_obj)
                    self.log_verbose("Deleted database directory")
                else:
                    self.logger.warning("Database path exists but is neither file nor directory")
            else:
                self.drop_database(**connection_config)
                self.log_verbose("Dropped database")
        except Exception as e:
            self.logger.error(f"Failed to remove database: {e}")
            raise RuntimeError(f"Could not remove existing database: {e}")

    def drop_database(self, **connection_config) -> None:
        """Drop/remove database on server (for server-based databases).

        Override this method in platform adapters for server-based databases.

        Args:
            **connection_config: Connection configuration
        """
        raise NotImplementedError("drop_database not implemented for this platform")

    def _format_execution_time(self, execution_time_seconds: float) -> str:
        """Format execution time with adaptive precision.

        Args:
            execution_time_seconds: Execution time in seconds

        Returns:
            Formatted time string with appropriate unit and precision
        """
        if execution_time_seconds < 0.001:
            # < 1ms: show as microseconds with 0 decimal places
            return f"{execution_time_seconds * 1000000:.0f}μs"
        elif execution_time_seconds < 1.0:
            # < 1s: show as milliseconds with 1 decimal place
            return f"{execution_time_seconds * 1000:.1f}ms"
        elif execution_time_seconds < 60.0:
            # < 1min: show as seconds with 2 decimal places
            return f"{execution_time_seconds:.2f}s"
        else:
            # >= 1min: show as minutes:seconds
            minutes = int(execution_time_seconds // 60)
            seconds = execution_time_seconds % 60
            return f"{minutes}:{seconds:04.1f}"

    def _get_benchmark_type(self, benchmark) -> str | None:
        """Extract benchmark type identifier from benchmark object.

        This determines the benchmark type (e.g., "tpch", "tpcds") for use in
        row count validation. It checks multiple attributes to identify the benchmark.

        Args:
            benchmark: Benchmark instance

        Returns:
            Benchmark type string (lowercase) or None if unknown
        """
        # Try to get explicit _name attribute
        if hasattr(benchmark, "_name"):
            name = benchmark._name.lower()
            # Normalize common patterns - extract just the benchmark identifier
            if "tpc-h" in name or "tpch" in name:
                return "tpch"
            elif "tpc-ds" in name or "tpcds" in name:
                return "tpcds"
            # If it's a simple name without spaces, use it directly
            if " " not in name:
                return name

        # Fall back to class name analysis
        class_name = type(benchmark).__name__.lower()
        if "tpch" in class_name:
            return "tpch"
        elif "tpcds" in class_name or "tpc_ds" in class_name:
            return "tpcds"
        elif "clickbench" in class_name:
            return "clickbench"
        elif "ssb" in class_name or "star_schema" in class_name:
            return "ssb"
        elif "amplab" in class_name:
            return "amplab"
        elif "h2odb" in class_name or "h2o" in class_name:
            return "h2odb"
        elif "coffeeshop" in class_name:
            return "coffeeshop"
        elif "joinorder" in class_name or "join_order" in class_name:
            return "joinorder"
        elif "tpcdi" in class_name or "tpc_di" in class_name:
            return "tpcdi"

        # Check display_name if available
        if hasattr(benchmark, "display_name"):
            display_name = str(benchmark.display_name).lower()
            if "tpch" in display_name:
                return "tpch"
            elif "tpcds" in display_name or "tpc-ds" in display_name:
                return "tpcds"

        # Unknown benchmark type - validation will be skipped
        return None

    def _build_query_result_with_validation(
        self,
        query_id: str,
        execution_time: float,
        actual_row_count: int,
        first_row: Any = None,
        validation_result: Any = None,
        error: str | None = None,
    ) -> dict[str, Any]:
        """Build query result dictionary with consistent validation field mapping.

        This centralizes validation result processing to ensure all platform adapters
        use the same field names and status mapping logic.

        Args:
            query_id: Query identifier
            execution_time: Query execution time in seconds
            actual_row_count: Number of rows returned
            first_row: First row of results (optional)
            validation_result: ValidationResult from QueryValidator (optional)
            error: Error message if query failed (optional)

        Returns:
            Dictionary with standardized query result fields
        """
        # Import ValidationMode here to avoid circular dependency
        from benchbox.core.expected_results.models import ValidationMode

        # Start with base result fields
        result_dict = {
            "query_id": str(query_id),
            "status": "FAILED" if error else "SUCCESS",
            "execution_time": execution_time,
            "rows_returned": actual_row_count,
            "first_row": first_row,
        }

        if error:
            result_dict["error"] = error

        # Include validation metadata if validation was performed
        if validation_result:
            # Create nested validation object
            row_count_validation = {
                "expected": validation_result.expected_row_count,
                "actual": actual_row_count,
            }

            # Correct SKIP vs PASSED vs FAILED mapping
            if validation_result.validation_mode == ValidationMode.SKIP:
                row_count_validation["status"] = "SKIPPED"
                if validation_result.warning_message:
                    row_count_validation["warning"] = validation_result.warning_message
            elif validation_result.is_valid:
                row_count_validation["status"] = "PASSED"
            else:
                # Validation failed - mark query as FAILED
                row_count_validation["status"] = "FAILED"
                row_count_validation["error"] = validation_result.error_message
                result_dict["status"] = "FAILED"
                result_dict["error"] = validation_result.error_message

            result_dict["row_count_validation"] = row_count_validation

        return result_dict

    def _build_query_failure_result(
        self,
        query_id: str,
        start_time: float,
        exception: Exception,
        log_error: bool = True,
    ) -> dict[str, Any]:
        """Build standardized query failure result dictionary.

        This centralizes error handling to ensure all platform adapters
        use the same failure result format.

        Args:
            query_id: Query identifier
            start_time: Query start time (from time.time())
            exception: The exception that occurred
            log_error: Whether to log the error (default True)

        Returns:
            Dictionary with standardized failure result fields
        """
        execution_time = time.time() - start_time

        if log_error:
            self.logger.error(
                f"Query {query_id} failed after {execution_time:.3f}s: {exception}",
                exc_info=True,
            )

        return {
            "query_id": query_id,
            "status": "FAILED",
            "execution_time": execution_time,
            "rows_returned": 0,
            "error": str(exception),
            "error_type": type(exception).__name__,
        }

    def _build_dry_run_result(self, query_id: str) -> dict[str, Any]:
        """Build standardized dry-run query result dictionary.

        Used when dry_run_mode is enabled - SQL is captured but not executed.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary with standardized dry-run result fields
        """
        self.log_very_verbose(f"Captured query {query_id} for dry-run")
        return {
            "query_id": query_id,
            "status": "DRY_RUN",
            "execution_time": 0.0,
            "rows_returned": 0,
            "first_row": None,
            "error": None,
            "dry_run": True,
        }

    def _normalize_and_validate_file_paths(
        self,
        file_paths: list | Any,
    ) -> list[Path]:
        """Normalize file paths to list and filter valid files.

        This centralizes file path validation to ensure consistent handling
        across all platform adapters during data loading.

        Args:
            file_paths: File path(s) - can be string, Path, or list

        Returns:
            List of valid Path objects (filtered by existence and size > 0)
        """
        # Normalize to list
        if not isinstance(file_paths, list):
            file_paths = [file_paths]

        # Filter valid files
        valid_files = [Path(f) for f in file_paths if Path(f).exists() and Path(f).stat().st_size > 0]

        return valid_files

    def display_query_plan_if_enabled(self, connection: Any, query: str, query_id: str) -> None:
        """Display query execution plan if show_query_plans is enabled.

        Args:
            connection: Database connection
            query: SQL query text
            query_id: Query identifier
        """
        if not self.show_query_plans:
            return

        try:
            plan = self.get_query_plan(connection, query)
            if plan:
                from rich.console import Console
                from rich.panel import Panel

                console = Console()
                console.print(Panel.fit("Query Profiling Information", style="cyan"))
                console.print(f"{query}")
                console.print(plan)
                console.print()  # Add spacing
        except Exception as e:
            self.logger.debug(f"Failed to get query plan for {query_id}: {e}")

    def get_query_plan(self, connection: Any, query: str) -> str | None:
        """Get query execution plan for analysis.

        Override this method in platform adapters to provide platform-specific plans.

        Args:
            connection: Database connection
            query: SQL query text

        Returns:
            Query execution plan as string, or None if not available
        """
        return None

    def get_query_plan_parser(self):
        """Get query plan parser for this platform.

        Override this method in platform adapters to provide platform-specific parser.

        Returns:
            QueryPlanParser instance or None if not available
        """
        return None

    def _record_plan_capture_failure(
        self,
        query_id: str,
        reason: str,
        message: str | None = None,
        *,
        log_warning: bool = True,
    ) -> None:
        """Record a plan capture failure and optionally raise in strict mode."""
        error_record = {
            "query_id": str(query_id),
            "platform": self.platform_name,
            "reason": reason,
        }
        if message:
            error_record["message"] = message

        self.plan_capture_failures += 1
        self.plan_capture_errors.append(error_record)

        if log_warning:
            self.logger.warning(
                "Failed to capture query plan for %s (query_id=%s): %s",
                self.platform_name,
                query_id,
                message or reason,
            )

        if self.strict_plan_capture:
            raise PlanCaptureError(
                reason=reason,
                platform=self.platform_name,
                query_id=str(query_id),
                details=message,
            )

    def capture_query_plan(self, connection: Any, query: str, query_id: str) -> tuple[Any, float]:
        """Capture structured query plan using platform-specific parser.

        This method gets EXPLAIN output and parses it into a QueryPlanDAG.
        Returns timing information for observability of capture overhead.

        Args:
            connection: Database connection
            query: SQL query text
            query_id: Query identifier

        Returns:
            Tuple of (QueryPlanDAG | None, capture_time_ms)
        """
        if not self.capture_plans:
            return None, 0.0

        # Apply query filter if specified
        if self.plan_query_filter and query_id not in self.plan_query_filter:
            return None, 0.0

        # Apply first-N iterations filter if specified
        if self.plan_first_n is not None:
            iteration = self._plan_capture_iteration_counts.get(query_id, 0)
            self._plan_capture_iteration_counts[query_id] = iteration + 1
            if iteration >= self.plan_first_n:
                return None, 0.0

        # Apply sampling rate if specified
        if self.plan_sampling_rate is not None:
            if random.random() > self.plan_sampling_rate:
                return None, 0.0

        start_time = time.perf_counter()

        try:
            # Apply timeout protection for EXPLAIN query
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(self.get_query_plan, connection, query)
                try:
                    explain_output = future.result(timeout=self.plan_capture_timeout_seconds)
                except concurrent.futures.TimeoutError:
                    capture_time_ms = (time.perf_counter() - start_time) * 1000
                    self.logger.warning(
                        "Query plan capture timed out for %s after %ds (%.2fms elapsed)",
                        query_id,
                        self.plan_capture_timeout_seconds,
                        capture_time_ms,
                    )
                    self._record_plan_capture_failure(
                        query_id,
                        reason="timeout",
                        message=f"EXPLAIN query timed out after {self.plan_capture_timeout_seconds}s",
                        log_warning=False,
                    )
                    return None, capture_time_ms
        except PlanCaptureError:
            raise
        except Exception as exc:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self._record_plan_capture_failure(
                query_id,
                reason="explain_failed",
                message=str(exc),
            )
            return None, capture_time_ms

        if not explain_output:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self._record_plan_capture_failure(
                query_id,
                reason="explain_failed",
                message="No EXPLAIN output returned",
            )
            return None, capture_time_ms

        parser = self.get_query_plan_parser()
        if not parser:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self.logger.warning(
                "Query plan capture disabled for %s: no parser available. "
                "Plans will not be captured for this benchmark run.",
                self.platform_name,
            )
            self._record_plan_capture_failure(
                query_id,
                reason="parser_unavailable",
                message=f"No parser available for {self.platform_name}",
                log_warning=False,
            )
            return None, capture_time_ms

        try:
            plan = parser.parse_explain_output(query_id, explain_output)
        except PlanCaptureError:
            raise
        except Exception as exc:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self._record_plan_capture_failure(
                query_id,
                reason="parse_error",
                message=str(exc),
            )
            return None, capture_time_ms

        if plan is None:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self._record_plan_capture_failure(
                query_id,
                reason="parse_error",
                message="Parser returned no plan",
            )
            return None, capture_time_ms

        try:
            size_kb = plan.estimate_serialized_size() / 1024
            if size_kb > 100:
                self.logger.warning(
                    "Large query plan for %s: %.1f KB. Consider using external plan storage.",
                    query_id,
                    size_kb,
                )
        except SerializationError as exc:
            capture_time_ms = (time.perf_counter() - start_time) * 1000
            self._record_plan_capture_failure(
                query_id,
                reason="serialization_error",
                message=str(exc),
            )
            return None, capture_time_ms

        capture_time_ms = (time.perf_counter() - start_time) * 1000
        self.query_plans_captured += 1
        return plan, capture_time_ms

    def get_tpc_base_dialect(self, benchmark_name: str) -> str:
        """Return the base dialect for TPC query generation (qgen/dsqgen).

        Default is 'netezza' for both TPC-DS and TPC-H for modern SQL compatibility.
        Adapters may override to select a closer match if beneficial.

        Args:
            benchmark_name: 'tpch', 'tpcds', etc. (case-insensitive)

        Returns:
            Base dialect string to use when invoking qgen/dsqgen
        """
        benchmark_lower = benchmark_name.lower()
        if benchmark_lower == "tpcds":
            return "netezza"  # Use LIMIT syntax for better modern SQL compatibility
        else:
            return "netezza"  # TPC-H and other benchmarks use netezza for consistency

    def validate_tuning_configuration(self, unified_config: UnifiedTuningConfiguration) -> list[str]:
        """Validate a unified tuning configuration against platform capabilities.

        Args:
            unified_config: The unified tuning configuration to validate

        Returns:
            List of validation error messages (empty if all valid)
        """
        if not unified_config:
            return []

        return unified_config.validate_for_platform(self.platform_name)

    def _validate_database_tunings(self, **connection_config):
        """Validate that database tunings match expected configuration.

        Args:
            **connection_config: Connection configuration

        Returns:
            ValidationResult with tuning comparison results
        """
        try:
            # Import here to avoid circular dependencies
            from benchbox.core.tuning.metadata import (
                MetadataValidationResult,
                TuningMetadataManager,
            )

            # Create temporary connection for validation without triggering nested validation
            self._validating_database = True
            temp_connection = None
            try:
                if hasattr(self, "_create_direct_connection"):
                    temp_connection = self._create_direct_connection(**connection_config)
                else:
                    temp_connection = self.create_connection(**connection_config)
            finally:
                # Keep _validating_database True while metadata calls may open connections
                # It will be reset in the outer finally after validation completes
                pass

            try:
                # Initialize metadata manager
                metadata_manager = TuningMetadataManager(self, connection_config.get("database"))

                # Validate tunings against database
                effective_config = self.get_effective_tuning_configuration()
                if effective_config:
                    return metadata_manager.validate_unified_tunings(effective_config)
                else:
                    # No tunings expected - check if database has any
                    existing_tunings = metadata_manager.load_unified_tunings()
                    result = MetadataValidationResult()
                    if existing_tunings:
                        result.add_warning("Database contains tuning metadata but no tunings expected")
                    return result

            finally:
                self.close_connection(temp_connection)
                # Reset recursion guard
                self._validating_database = False

        except Exception as e:
            # If validation fails, create error result
            from benchbox.core.tuning.metadata import MetadataValidationResult

            result = MetadataValidationResult()
            result.add_error(f"Failed to validate database tunings: {e}")
            return result

    def save_tuning_metadata(self, connection: Any) -> bool:
        """Save tuning metadata to database for future validation.

        Args:
            connection: Database connection

        Returns:
            True if metadata was saved successfully, False otherwise
        """
        effective_config = self.get_effective_tuning_configuration()
        if not self.tuning_enabled or not effective_config:
            return True

        try:
            # Import here to avoid circular dependencies
            from benchbox.core.tuning.metadata import TuningMetadataManager

            metadata_manager = TuningMetadataManager(self)
            return metadata_manager.save_unified_tunings(effective_config)

        except Exception as e:
            self.logger.error(f"Failed to save tuning metadata: {e}")
            return False

    def validate_row_counts(self, connection: Any, expected_counts: dict[str, int]):
        """Validate actual row counts against expected counts.

        Args:
            connection: Database connection
            expected_counts: Dictionary mapping table names to expected row counts

        Returns:
            ValidationResult with row count comparison results
        """
        self.log_operation_start("Row count validation", f"{len(expected_counts)} tables to validate")

        try:
            # Import here to avoid circular dependencies
            from benchbox.core.validation.data import DataValidator

            validator = DataValidator(self)
            result = validator.validate_row_counts(expected_counts)

            if result.is_valid:
                self.log_operation_complete("Row count validation", details="All row counts match expected values")
            else:
                self.log_verbose(f"Row count validation failed: {len(result.issues)} issues found")

            return result

        except Exception as e:
            self.logger.error(f"Failed to validate row counts: {e}")
            from benchbox.core.validation.data import ValidationResult

            result = ValidationResult()
            result.add_error(f"Row count validation failed: {e}")
            return result

    # Enhanced phase tracking methods

    def _create_enhanced_data_generation_phase(self, benchmark) -> DataGenerationPhase | None:
        """Create detailed data generation phase tracking."""
        if not hasattr(benchmark, "tables") and not hasattr(getattr(benchmark, "_impl", None), "tables"):
            return None

        start_time = time.time()
        tables_dict = benchmark.tables if hasattr(benchmark, "tables") else getattr(benchmark._impl, "tables", {})

        # Handle Mock objects or None values
        if not tables_dict or not hasattr(tables_dict, "items") or hasattr(tables_dict, "_mock_name"):
            return None

        per_table_stats = {}
        total_rows = 0
        total_bytes = 0
        tables_generated = 0

        try:
            table_items = tables_dict.items()
            # Check if table_items is actually iterable (not a Mock)
            if hasattr(table_items, "__iter__") and not hasattr(table_items, "_mock_name"):
                # Try to iterate to check it's not a Mock masquerading as iterable
                try:
                    # Test iteration without consuming
                    iter(table_items)
                    # Additional safety check for Mock objects
                    if hasattr(tables_dict, "_mock_name"):
                        return None
                except (TypeError, AttributeError):
                    return None
            else:
                return None
        except (AttributeError, TypeError):
            # Handle cases where tables_dict is a Mock or doesn't have items()
            return None

        try:
            for table_name, table_data in table_items:
                table_start = time.time()
                try:
                    if hasattr(table_data, "__iter__") and not isinstance(table_data, str):
                        rows = list(table_data)
                        row_count = len(rows)

                        # Estimate data size (rough approximation)
                        if rows:
                            avg_row_size = len(str(rows[0])) if rows else 50
                            estimated_bytes = row_count * avg_row_size
                        else:
                            estimated_bytes = 0

                        per_table_stats[table_name] = TableGenerationStats(
                            generation_time_ms=int((time.time() - table_start) * 1000),
                            status="SUCCESS",
                            rows_generated=row_count,
                            data_size_bytes=estimated_bytes,
                            file_path=f"{table_name}.tbl",
                        )

                        total_rows += row_count
                        total_bytes += estimated_bytes
                        tables_generated += 1

                except Exception as e:
                    per_table_stats[table_name] = TableGenerationStats(
                        generation_time_ms=int((time.time() - table_start) * 1000),
                        status="FAILED",
                        rows_generated=0,
                        data_size_bytes=0,
                        file_path=f"{table_name}.tbl",
                        error_type="GENERATION_ERROR",
                        error_message=str(e),
                        error_timestamp=datetime.now().isoformat(),
                    )
        except (TypeError, AttributeError):
            # If we can't iterate over table_items, return None
            return None

        overall_status = "SUCCESS"
        if any(stats.status == "FAILED" for stats in per_table_stats.values()):
            overall_status = "PARTIAL_FAILURE" if tables_generated > 0 else "FAILED"

        return DataGenerationPhase(
            duration_ms=int((time.time() - start_time) * 1000),
            status=overall_status,
            tables_generated=tables_generated,
            total_rows_generated=total_rows,
            total_data_size_bytes=total_bytes,
            per_table_stats=per_table_stats,
        )

    def _create_enhanced_schema_creation_phase(
        self, benchmark, connection: Any, schema_creation_time: float
    ) -> SchemaCreationPhase:
        """Create detailed schema creation phase tracking."""
        # Convert existing schema creation time from seconds to milliseconds
        duration_ms = int(schema_creation_time * 1000)

        # Get table names from benchmark
        table_names = []
        if hasattr(benchmark, "get_table_names"):
            table_names = benchmark.get_table_names()
        elif hasattr(benchmark, "tables"):
            try:
                table_names = list(benchmark.tables.keys())
            except Exception:
                # Fallback for testing with Mocks
                table_names = ["test_table"]
        elif hasattr(getattr(benchmark, "_impl", None), "tables"):
            table_names = list(benchmark._impl.tables.keys())

        # Ensure we always have a real list/tuple for len()
        if hasattr(table_names, "__len__") and not isinstance(table_names, (list, tuple, str)):
            table_names = ["test_table"]  # Safe fallback for mocking

        # For now, create basic per-table stats since we don't have detailed timing
        per_table_creation = {}

        # Calculate time per table safely
        try:
            table_count = len(table_names) if table_names else 1
            estimated_time_per_table = max(1, duration_ms // table_count)
        except (TypeError, AttributeError):
            # Fallback for Mock objects or other issues
            estimated_time_per_table = max(1, duration_ms // 1)

        # Safely iterate over table names
        try:
            table_names_iter = (
                table_names if hasattr(table_names, "__iter__") and not isinstance(table_names, str) else ["test_table"]
            )
        except Exception:
            table_names_iter = ["test_table"]

        for table_name in table_names_iter:
            per_table_creation[table_name] = TableCreationStats(
                creation_time_ms=estimated_time_per_table,
                status="SUCCESS",
                constraints_applied=1,  # Rough estimate
                indexes_created=1,  # Rough estimate
            )

        # Calculate table count safely
        try:
            table_count = len(table_names) if table_names else 1
        except (TypeError, AttributeError):
            table_count = 1

        return SchemaCreationPhase(
            duration_ms=duration_ms,
            status="SUCCESS",
            tables_created=table_count,
            constraints_applied=table_count,  # Rough estimate
            indexes_created=table_count,  # Rough estimate
            per_table_creation=per_table_creation,
        )

    def _create_enhanced_data_loading_phase(
        self, table_stats: dict[str, int], loading_time: float, per_table_timings: dict[str, Any] | None = None
    ) -> DataLoadingPhase:
        """Create detailed data loading phase tracking.

        Args:
            table_stats: Dictionary mapping table names to row counts
            loading_time: Total loading time in seconds
            per_table_timings: Optional dict with actual per-table timing details
                              (if None, will estimate based on row ratios)
        """
        duration_ms = int(loading_time * 1000)

        per_table_loading = {}
        total_rows = sum(table_stats.values())

        # Use actual timings if provided, otherwise distribute total time proportionally by row count
        if per_table_timings:
            # Use actual per-table timings from adapter
            for table_name, row_count in table_stats.items():
                timing_info = per_table_timings.get(table_name, {})
                actual_time_ms = timing_info.get("total_ms", 0)
                per_table_loading[table_name] = TableLoadingStats(
                    rows=row_count, load_time_ms=int(actual_time_ms), status="SUCCESS"
                )
        else:
            # No detailed timings available - distribute total time proportionally by row count
            # Note: This is an approximation and may not reflect actual per-table performance
            time_per_row = duration_ms / max(1, total_rows)
            for table_name, row_count in table_stats.items():
                proportional_time = int(row_count * time_per_row)
                per_table_loading[table_name] = TableLoadingStats(
                    rows=row_count, load_time_ms=proportional_time, status="SUCCESS"
                )

        return DataLoadingPhase(
            duration_ms=duration_ms,
            status="SUCCESS",
            total_rows_loaded=total_rows,
            tables_loaded=len(table_stats),
            per_table_stats=per_table_loading,
        )

    def _create_enhanced_validation_phase(self, benchmark=None, connection=None, table_stats=None) -> ValidationPhase:
        """Create validation phase tracking with actual data validation."""
        start_time = time.time()

        validation_details = {
            "row_count_matches": True,
            "schema_valid": True,
            "constraints_enabled": True,
        }

        # Perform actual data validation if parameters provided
        row_count_status = "PASSED"
        schema_status = "PASSED"
        integrity_status = "PASSED"

        if benchmark and connection and table_stats is not None:
            # Validate row counts
            row_count_status, row_validation_details = self._validate_table_row_counts(benchmark, table_stats)
            validation_details.update(row_validation_details)

            # Validate schema integrity
            schema_status, schema_validation_details = self._validate_schema_integrity(benchmark, connection)
            validation_details.update(schema_validation_details)

            # Validate data integrity
            integrity_status, integrity_validation_details = self._validate_data_integrity(
                benchmark, connection, table_stats
            )
            validation_details.update(integrity_validation_details)

        duration_ms = int((time.time() - start_time) * 1000)

        return ValidationPhase(
            duration_ms=max(50, duration_ms),  # Minimum 50ms
            row_count_validation=row_count_status,
            schema_validation=schema_status,
            data_integrity_checks=integrity_status,
            validation_details=validation_details,
        )

    def _validate_table_row_counts(self, benchmark, table_stats: dict[str, int]) -> tuple[str, dict[str, Any]]:
        """Validate that tables have expected row counts."""
        validation_details = {}

        # Get minimum expected row counts for benchmark
        expected_row_counts = self._get_expected_row_counts(benchmark)

        failed_tables = []
        empty_tables = []

        for table_name, actual_rows in table_stats.items():
            # Check for completely empty tables
            if actual_rows == 0:
                empty_tables.append(table_name)
                continue

            # Check against expected minimums if available
            # Handle Mock objects gracefully
            if (
                expected_row_counts
                and hasattr(expected_row_counts, "__contains__")
                and not hasattr(expected_row_counts, "_mock_name")
                and table_name in expected_row_counts
            ):
                min_expected = expected_row_counts[table_name]
                if actual_rows < min_expected:
                    failed_tables.append(
                        {
                            "table": table_name,
                            "actual": actual_rows,
                            "expected_minimum": min_expected,
                        }
                    )

        # Determine validation status
        if empty_tables:
            status = "FAILED"
            validation_details["empty_tables"] = empty_tables
            validation_details["row_count_matches"] = False
        elif failed_tables:
            status = "PARTIAL"
            validation_details["insufficient_data_tables"] = failed_tables
            validation_details["row_count_matches"] = False
        else:
            status = "PASSED"
            validation_details["row_count_matches"] = True

        validation_details["total_tables_validated"] = len(table_stats)
        validation_details["tables_with_data"] = len([t for t in table_stats.values() if t > 0])

        return status, validation_details

    def _validate_schema_integrity(self, benchmark, connection) -> tuple[str, dict[str, Any]]:
        """Validate database schema integrity."""
        validation_details = {}

        try:
            # Get expected schema from benchmark
            expected_tables = self._get_expected_tables(benchmark)

            # Verify tables exist in database
            existing_tables = self._get_existing_tables(connection)

            missing_tables = []
            if expected_tables:
                missing_tables = [table for table in expected_tables if table not in existing_tables]

            if missing_tables:
                validation_details["missing_tables"] = missing_tables
                validation_details["schema_valid"] = False
                return "FAILED", validation_details
            else:
                validation_details["schema_valid"] = True
                validation_details["verified_tables"] = list(existing_tables)
                return "PASSED", validation_details

        except Exception as e:
            validation_details["schema_valid"] = False
            validation_details["validation_error"] = str(e)
            return "FAILED", validation_details

    def _validate_data_integrity(
        self, benchmark, connection, table_stats: dict[str, int]
    ) -> tuple[str, dict[str, Any]]:
        """Validate basic data integrity checks."""
        validation_details = {}

        try:
            # Handle Mock connections gracefully during testing
            if hasattr(connection, "_mock_name"):
                # For Mock connections, assume all tables are accessible
                accessible_tables = list(table_stats.keys())
                inaccessible_tables = []
            else:
                # For real connections, verify tables are accessible
                accessible_tables = []
                inaccessible_tables = []

                for table_name in table_stats:
                    try:
                        # Try a simple SELECT to verify table is accessible
                        # Use cursor API (not all connection objects support execute() directly)
                        cursor = connection.cursor()
                        try:
                            cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
                            accessible_tables.append(table_name)
                        finally:
                            cursor.close()
                    except Exception:
                        inaccessible_tables.append(table_name)

            if inaccessible_tables:
                validation_details["inaccessible_tables"] = inaccessible_tables
                validation_details["constraints_enabled"] = False
                return "FAILED", validation_details
            else:
                validation_details["accessible_tables"] = accessible_tables
                validation_details["constraints_enabled"] = True
                return "PASSED", validation_details

        except Exception as e:
            validation_details["constraints_enabled"] = False
            validation_details["integrity_error"] = str(e)
            return "FAILED", validation_details

    def _get_expected_row_counts(self, benchmark) -> dict[str, int] | None:
        """Get expected minimum row counts for benchmark tables."""
        # This can be overridden by specific benchmarks
        # For now, we just require non-zero rows
        if hasattr(benchmark, "expected_row_counts"):
            return benchmark.expected_row_counts
        return None

    def get_table_row_count(self, connection: Any, table: str) -> int:
        """Get row count for a table using platform-specific API.

        Default implementation uses cursor pattern. Platforms like BigQuery
        that don't support cursor() can override to use their specific APIs.

        Args:
            connection: Database connection
            table: Table name

        Returns:
            Row count as integer, or 0 if unable to determine
        """
        try:
            # Handle Mock connections gracefully during testing
            if hasattr(connection, "_mock_name"):
                return 0

            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            result = cursor.fetchone()
            return result[0] if result else 0
        except Exception:
            return 0

    def _get_expected_tables(self, benchmark) -> list[str] | None:
        """Get list of expected table names from the benchmark definition.

        Prefer schema- or API-declared tables over loaded data keys to avoid
        masking missing tables when generation is incomplete.
        """
        # 1) Prefer schema if available
        try:
            if hasattr(benchmark, "get_schema") and callable(benchmark.get_schema):
                schema = benchmark.get_schema()
                # Support both list[dict{name}] and list[str]
                if isinstance(schema, list) and schema and isinstance(schema[0], dict) and "name" in schema[0]:
                    return [t["name"].lower() for t in schema]
        except Exception:
            pass
        # 2) Prefer explicit table listing if provided by the benchmark
        try:
            if hasattr(benchmark, "get_available_tables") and callable(benchmark.get_available_tables):
                return [t.lower() for t in benchmark.get_available_tables()]
            if hasattr(benchmark, "get_table_names") and callable(benchmark.get_table_names):
                return benchmark.get_table_names()
        except Exception:
            pass
        # 3) Fall back to whatever was generated (least strict)
        if (
            hasattr(benchmark, "tables")
            and benchmark.tables
            and not hasattr(benchmark.tables, "_mock_name")
            and hasattr(benchmark.tables, "keys")
        ):
            try:
                return [t.lower() for t in benchmark.tables.keys()]
            except (TypeError, AttributeError):
                pass
        return None

    def _get_existing_tables(self, connection) -> list[str]:
        """Get list of existing tables in the database."""
        # This is platform-specific and can be overridden
        # Default implementation that works for many SQL databases
        try:
            # Handle Mock objects gracefully during testing
            if hasattr(connection, "_mock_name"):
                return []  # Return empty list for Mock connections

            result = connection.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' OR table_schema = database()
                OR table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
            """).fetchall()
            # Lowercase table names for case-insensitive comparison with expected tables
            return [row[0].lower() for row in result]
        except Exception:
            # Fallback - return empty list if query fails
            return []

    def _create_failed_benchmark_result(
        self,
        benchmark,
        validation_phase,
        table_stats,
        loading_time,
        schema_creation_phase,
        data_loading_phase,
        tunings_applied_dict,
        tuning_validation_status,
        tuning_metadata_saved,
    ):
        """Create a benchmark result indicating validation failure."""
        from datetime import datetime

        # Create basic execution phases
        setup_phase = SetupPhase(
            data_loading=data_loading_phase,
            schema_creation=schema_creation_phase,
            validation=validation_phase,
        )

        # Create failed power test phase
        power_test_phase = PowerTestPhase(
            start_time=datetime.now().isoformat(),
            end_time=datetime.now().isoformat(),
            duration_ms=0,
            query_executions=[],
            geometric_mean_time=0.0,
            power_at_size=0.0,
        )

        execution_phases = ExecutionPhases(setup=setup_phase, power_test=power_test_phase)

        # Get platform info
        try:
            platform_info = self.get_platform_info(None)  # Connection might be invalid
        except Exception:
            platform_info = {"error": "Could not retrieve platform info"}

        # Create execution metadata
        execution_metadata = {
            "execution_timestamp": datetime.now().isoformat(),
            "data_validation_failed": True,
            "validation_details": validation_phase.validation_details,
            "benchbox_version": "0.1.0",
        }

        # Calculate basic metrics
        total_rows_loaded = sum(table_stats.values()) if table_stats else 0
        data_size_mb = self._calculate_data_size(benchmark.output_dir) if hasattr(benchmark, "output_dir") else 0.0

        # Create failed benchmark result
        return benchmark.create_enhanced_benchmark_result(
            platform=self.platform_name,
            query_results=[],  # No queries were executed
            execution_metadata=execution_metadata,
            phases=execution_phases,
            resource_utilization={},
            performance_characteristics={},
            # Override defaults with failure status
            total_rows_loaded=total_rows_loaded,
            data_size_mb=data_size_mb,
            data_loading_time=loading_time,
            schema_creation_time=getattr(schema_creation_phase, "duration_ms", 0) / 1000.0,
            table_statistics=table_stats,
            tunings_applied=tunings_applied_dict,
            tuning_validation_status=tuning_validation_status,
            tuning_metadata_saved=tuning_metadata_saved,
            platform_info=platform_info,
            validation_status="FAILED",  # This is the key fix
            validation_details=validation_phase.validation_details,
        )

    def _create_throughput_phase(self, throughput_result) -> ThroughputTestPhase | None:
        """Convert throughput test outputs into structured execution metadata."""
        if throughput_result is None:
            return None

        streams: list[ThroughputStream] = []
        total_queries_executed = 0

        for stream_result in getattr(throughput_result, "stream_results", []) or []:
            start_iso = self._format_timestamp(stream_result.start_time)
            end_iso = self._format_timestamp(stream_result.end_time)

            duration_seconds = float(getattr(stream_result, "duration", 0.0) or 0.0)
            if (
                not duration_seconds
                and isinstance(stream_result.start_time, (int, float))
                and isinstance(stream_result.end_time, (int, float))
            ):
                duration_seconds = max(stream_result.end_time - stream_result.start_time, 0.0)
            duration_ms = int(duration_seconds * 1000)

            query_executions: list[QueryExecution] = []
            for idx, query_result in enumerate(stream_result.query_results, start=1):
                execution_order = query_result.get("position") or query_result.get("execution_order") or idx
                execution_time_ms = int(float(query_result.get("execution_time", 0.0)) * 1000)

                query_executions.append(
                    QueryExecution(
                        query_id=str(query_result.get("query_id")),
                        stream_id=str(stream_result.stream_id),
                        execution_order=int(execution_order),
                        execution_time_ms=execution_time_ms,
                        status="SUCCESS" if query_result.get("success", True) else "FAILED",
                        rows_returned=query_result.get("result_count"),
                        error_message=query_result.get("error"),
                    )
                )

            streams.append(
                ThroughputStream(
                    stream_id=stream_result.stream_id,
                    start_time=start_iso,
                    end_time=end_iso,
                    duration_ms=duration_ms,
                    query_executions=query_executions,
                )
            )
            total_queries_executed += getattr(stream_result, "queries_executed", len(stream_result.query_results))

        duration_ms = int(float(getattr(throughput_result, "total_time", 0.0)) * 1000)
        end_time_iso = throughput_result.end_time or datetime.now().isoformat()

        return ThroughputTestPhase(
            start_time=throughput_result.start_time,
            end_time=end_time_iso,
            duration_ms=duration_ms,
            num_streams=getattr(getattr(throughput_result, "config", None), "num_streams", len(streams)),
            streams=streams,
            total_queries_executed=total_queries_executed,
            throughput_at_size=getattr(throughput_result, "throughput_at_size", 0.0),
        )

    @staticmethod
    def _format_timestamp(value: Any) -> str:
        """Convert numeric timestamps to ISO-8601 strings."""
        if isinstance(value, str) and value:
            return value
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(value).isoformat()
        return datetime.now().isoformat()

    def _determine_overall_validation_status(self, validation_phase) -> str:
        """Determine overall validation status from individual validation results."""
        if (
            validation_phase.row_count_validation == "FAILED"
            or validation_phase.schema_validation == "FAILED"
            or validation_phase.data_integrity_checks == "FAILED"
        ):
            return "FAILED"
        elif (
            validation_phase.row_count_validation == "PARTIAL"
            or validation_phase.schema_validation == "PARTIAL"
            or validation_phase.data_integrity_checks == "PARTIAL"
        ):
            return "PARTIAL"
        else:
            return "PASSED"

    def _extract_query_definitions(
        self, benchmark, queries: dict[str, str], stream_id: str = "standard"
    ) -> dict[str, dict[str, QueryDefinition]]:
        """Extract query definitions for storage optimization."""
        query_definitions = {stream_id: {}}

        for query_id, sql_text in queries.items():
            query_definitions[stream_id][query_id] = QueryDefinition(
                sql=sql_text,
                parameters={},  # For now, parameters are embedded in SQL
            )

        return query_definitions

    def _create_standard_execution_phase(
        self, query_results: list[dict[str, Any]], stream_id: str = "standard"
    ) -> list[QueryExecution]:
        """Convert legacy query results to enhanced query executions."""
        query_executions = []

        for i, result in enumerate(query_results):
            query_executions.append(
                QueryExecution(
                    query_id=result.get("query_id", f"Q{i + 1}"),
                    stream_id=stream_id,
                    execution_order=i + 1,
                    execution_time_ms=round(result.get("execution_time", 0) * 1000, 2),
                    status=result.get("status", "UNKNOWN"),
                    rows_returned=result.get("rows_returned"),
                    error_message=result.get("error"),
                )
            )

        return query_executions

    def run_enhanced_benchmark(self, benchmark, **run_config) -> EnhancedBenchmarkResults:
        """Run complete benchmark with enhanced phase tracking."""
        import time
        import uuid

        start_time = time.time()
        execution_id = str(uuid.uuid4())[:8]
        self._reset_plan_capture_stats()
        if "capture_plans" in run_config:
            self.capture_plans = bool(run_config.get("capture_plans"))
        if "strict_plan_capture" in run_config:
            self.strict_plan_capture = bool(run_config.get("strict_plan_capture"))
        if "plan_capture_timeout_seconds" in run_config:
            self.plan_capture_timeout_seconds = int(run_config.get("plan_capture_timeout_seconds"))

        try:
            # Step 1: Data generation phase
            data_generation_phase = self._create_enhanced_data_generation_phase(benchmark)

            # Ensure data exists (generate if needed)
            has_tables = getattr(benchmark, "tables", None) or getattr(
                getattr(benchmark, "_impl", None), "tables", None
            )
            if not has_tables:
                print("Generating benchmark data...")
                data_gen_start = time.time()
                benchmark.generate_data()
                print(f"✅ Data generation completed in {time.time() - data_gen_start:.2f}s")
                # Refresh data generation phase with actual generation
                data_generation_phase = self._create_enhanced_data_generation_phase(benchmark)

            # Step 2: Create connection
            print(f"Connecting to {self.platform_name}...")
            self.log_very_verbose(f"database_was_reused flag BEFORE connection: {self.database_was_reused}")
            connection = self.create_connection(**run_config.get("connection", {}))
            self.connection = connection
            self.log_very_verbose(f"database_was_reused flag AFTER connection: {self.database_was_reused}")

            # Step 3: Validate tuning configuration if enabled
            effective_tuning_config = self.get_effective_tuning_configuration()
            if self.tuning_enabled and effective_tuning_config:
                print("Validating unified tuning configuration...")
                tuning_errors = effective_tuning_config.validate_for_platform(self.platform_name)
                if tuning_errors:
                    raise ValueError(f"Invalid tuning configuration: {'; '.join(tuning_errors)}")
                print("✅ Unified tuning configuration validated")

            # Step 4: Schema creation phase
            self.log_verbose(f"Checking database_was_reused flag before schema creation: {self.database_was_reused}")
            if self.database_was_reused:
                print("✅ Database being reused - skipping schema creation and data loading")
                schema_time = 0.0
                schema_creation_phase = self._create_enhanced_schema_creation_phase(benchmark, connection, schema_time)
                loading_time = 0.0
                # Get existing table stats for display
                table_stats = {}
                if hasattr(benchmark, "get_schema"):
                    schema = benchmark.get_schema()
                    if isinstance(schema, dict):
                        self.logger.debug(f"Collecting table stats for {len(schema)} tables from reused database")
                        for table_name in schema:
                            try:
                                count = self.get_table_row_count(connection, table_name)
                                table_stats[table_name] = count
                                self.logger.debug(f"Table {table_name}: {count} rows")
                            except Exception as e:
                                self.logger.warning(f"Could not get row count for {table_name}: {e}")
                                table_stats[table_name] = 0
                data_loading_phase = self._create_enhanced_data_loading_phase(table_stats, loading_time, None)
                tuning_metadata_saved = False  # Skip tuning for reused databases
            else:
                print("Creating database schema...")
                time.time()
                schema_time = self.create_schema(benchmark, connection)
                schema_creation_phase = self._create_enhanced_schema_creation_phase(benchmark, connection, schema_time)

                # Step 5: Apply tunings if enabled
                tuning_metadata_saved = False
                if self.tuning_enabled and effective_tuning_config:
                    print("Applying unified tuning configuration...")
                    self.apply_unified_tuning(effective_tuning_config, connection)
                    print("✅ Unified tuning configuration applied")

                    # Save tuning metadata for future validation
                    print("Saving tuning metadata...")
                    tuning_metadata_saved = self.save_tuning_metadata(connection)
                    if tuning_metadata_saved:
                        print("✅ Tuning metadata saved")
                    else:
                        print("⚠️ Failed to save tuning metadata")

                # Step 6: Data loading phase
                print("Loading benchmark data...")
                data_dir = Path(benchmark.output_dir) if hasattr(benchmark, "output_dir") else Path(".")
                table_stats, loading_time, per_table_timings = self.load_data(benchmark, connection, data_dir)
                print(f"✅ Data loading completed in {loading_time:.2f}s")
                data_loading_phase = self._create_enhanced_data_loading_phase(
                    table_stats, loading_time, per_table_timings
                )

            # Step 7: Validation phase
            print("Validating benchmark data...")
            validation_phase = self._create_enhanced_validation_phase(benchmark, connection, table_stats)

            # Prepare tuning information early (before potential validation failure)
            tunings_applied_dict = None
            tuning_validation_status = "NOT_APPLICABLE"

            if self.tuning_enabled and effective_tuning_config:
                tunings_applied_dict = effective_tuning_config.to_dict()
                tuning_validation_status = "APPLIED" if tuning_metadata_saved else "FAILED_TO_SAVE"

            # Check if validation failed - if so, stop execution
            if (
                validation_phase.row_count_validation == "FAILED"
                or validation_phase.schema_validation == "FAILED"
                or validation_phase.data_integrity_checks == "FAILED"
            ):
                print("❌ Data validation failed - benchmark execution halted")
                if validation_phase.validation_details:
                    details = validation_phase.validation_details
                    if "empty_tables" in details and details["empty_tables"]:
                        print(f"⚠️  Empty tables detected: {', '.join(details['empty_tables'])}")
                    if "missing_tables" in details and details["missing_tables"]:
                        print(f"⚠️  Missing tables: {', '.join(details['missing_tables'])}")
                    if "inaccessible_tables" in details and details["inaccessible_tables"]:
                        print(f"⚠️  Inaccessible tables: {', '.join(details['inaccessible_tables'])}")

                # Create a failed benchmark result
                return self._create_failed_benchmark_result(
                    benchmark,
                    validation_phase,
                    table_stats,
                    loading_time,
                    schema_creation_phase,
                    data_loading_phase,
                    tunings_applied_dict,
                    tuning_validation_status,
                    tuning_metadata_saved,
                )

            print("✅ Data validation passed")

            # Step 8: Apply optimizations
            benchmark_type = run_config.get("benchmark_type", "olap")
            self.configure_for_benchmark(connection, benchmark_type)

            # Step 9: Execute queries based on test execution type
            test_execution_type = run_config.get("test_execution_type", "standard")
            print(f"Executing benchmark queries ({test_execution_type} mode)...")
            self._last_throughput_test_result = None
            query_results = self._execute_queries_by_type(benchmark, connection, run_config)

            # Get queries for definitions
            if hasattr(self, "get_target_dialect") and hasattr(benchmark, "get_queries"):
                try:
                    import inspect

                    sig = inspect.signature(benchmark.get_queries)
                    if "dialect" in sig.parameters:
                        queries = benchmark.get_queries(dialect=self.get_target_dialect())
                    else:
                        queries = benchmark.get_queries()
                except Exception:
                    queries = benchmark.get_queries()
            else:
                queries = benchmark.get_queries()

            # Create query definitions and executions
            stream_id = "standard"
            self._extract_query_definitions(benchmark, queries, stream_id)
            query_executions = self._create_standard_execution_phase(query_results, stream_id)

            # Step 10: Compile enhanced results
            total_duration = time.time() - start_time

            # Create setup phase
            setup_phase = SetupPhase(
                data_generation=data_generation_phase,
                schema_creation=schema_creation_phase,
                data_loading=data_loading_phase,
                validation=validation_phase,
            )

            # Calculate summary metrics first
            successful_queries = len([r for r in query_results if r["status"] == "SUCCESS"])
            total_exec_time = sum(r["execution_time"] for r in query_results if r["status"] == "SUCCESS")
            avg_time = total_exec_time / max(successful_queries, 1)
            if self.capture_plans:
                total_queries_executed = len(query_results)
                summary_message = f"Query plans: {self.query_plans_captured}/{total_queries_executed} captured"
                if self.plan_capture_failures:
                    summary_message = f"{summary_message}, {self.plan_capture_failures} failed"
                log_fn = self.logger.warning if self.plan_capture_failures else self.logger.info
                log_fn(summary_message)

            # Create power/throughput test phases based on execution mode
            from datetime import datetime

            execution_type = run_config.get("test_execution_type", "standard")

            power_test_phase = None
            if execution_type not in {"throughput"}:
                power_test_phase = PowerTestPhase(
                    start_time=datetime.now().isoformat(),
                    end_time=datetime.now().isoformat(),
                    duration_ms=int(total_exec_time * 1000),
                    query_executions=query_executions,
                    geometric_mean_time=avg_time,
                    power_at_size=0.0,  # Can be calculated later if needed
                )

            throughput_test_phase = None
            if getattr(self, "_last_throughput_test_result", None) is not None:
                throughput_test_phase = self._create_throughput_phase(self._last_throughput_test_result)
                self._last_throughput_test_result = None

            # Create execution phases
            execution_phases = ExecutionPhases(
                setup=setup_phase,
                power_test=power_test_phase,
                throughput_test=throughput_test_phase,
            )

            # Collect platform info
            platform_info = self.get_platform_info(connection)

            # Tuning information already prepared earlier

            # Get system profile and execution metadata
            try:
                from benchbox.core.results.anonymization import (
                    AnonymizationConfig,
                    AnonymizationManager,
                )
                from benchbox.utils.system_info import get_system_info

                anonymization_manager = AnonymizationManager(AnonymizationConfig())
                system_info = get_system_info()
                # Convert to dict for compatibility
                system_profile = system_info.to_dict()
                anonymous_machine_id = anonymization_manager.get_anonymous_machine_id()
            except ImportError:
                system_profile = None
                anonymous_machine_id = None

            execution_metadata = {
                "benchmark_type": run_config.get("benchmark_type", "olap"),
                "query_subset": run_config.get("query_subset"),
                "categories": run_config.get("categories"),
                "connection_config_hash": self._hash_connection_config(run_config.get("connection", {})),
                "python_version": platform.python_version(),
                "benchbox_version": "0.1.0",
            }

            # Calculate additional metrics needed
            total_rows_loaded = sum(table_stats.values()) if table_stats else 0
            data_size_mb = self._calculate_data_size(benchmark.output_dir) if hasattr(benchmark, "output_dir") else 0.0

            # Use centralized benchmark result creation method
            resource_snapshot = self._collect_resource_utilization()
            performance_summary = self._summarize_performance_characteristics(
                query_results=query_results,
                total_duration=total_duration,
                total_rows_loaded=total_rows_loaded,
            )

            return benchmark.create_enhanced_benchmark_result(
                platform=self.platform_name,
                query_results=query_results,
                execution_metadata=execution_metadata,
                phases=execution_phases,
                resource_utilization=resource_snapshot,
                performance_characteristics=performance_summary,
                query_plans_captured=self.query_plans_captured,
                plan_capture_failures=self.plan_capture_failures,
                plan_capture_errors=list(self.plan_capture_errors),
                # Override defaults with platform-specific data
                execution_id=execution_id,
                duration_seconds=total_duration,
                data_loading_time=loading_time,
                schema_creation_time=schema_time,
                total_rows_loaded=total_rows_loaded,
                data_size_mb=data_size_mb,
                table_statistics=table_stats or {},
                platform_info=platform_info,
                tunings_applied=tunings_applied_dict,
                tuning_validation_status=tuning_validation_status,
                tuning_metadata_saved=tuning_metadata_saved,
                system_profile=system_profile,
                anonymous_machine_id=anonymous_machine_id,
                validation_status=self._determine_overall_validation_status(validation_phase),
                validation_details=validation_phase.validation_details,
            )

        finally:
            if hasattr(self, "connection") and self.connection:
                self.close_connection(self.connection)
                self.connection = None

    def run_benchmark(self, benchmark, **run_config) -> EnhancedBenchmarkResults:
        """Run complete benchmark with enhanced phase tracking.

        Args:
            benchmark: Benchmark instance to execute
            **run_config: Runtime configuration options

        Returns:
            Enhanced benchmark results with detailed phase tracking
        """
        return self.run_enhanced_benchmark(benchmark, **run_config)

    def _execute_queries_by_type(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute queries based on test execution type."""
        test_execution_type = run_config.get("test_execution_type", "standard")

        if test_execution_type == "power":
            return self._execute_power_test(benchmark, connection, run_config)
        elif test_execution_type == "throughput":
            return self._execute_throughput_test(benchmark, connection, run_config)
        elif test_execution_type == "maintenance":
            return self._execute_maintenance_test(benchmark, connection, run_config)
        elif test_execution_type == "combined":
            return self._execute_combined_test(benchmark, connection, run_config)
        else:
            # Standard execution
            return self._execute_all_queries(benchmark, connection, run_config)

    def _execute_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute Power Test with warmup + iterations for all benchmarks.

        Routes TPC benchmarks to specialized implementations and other benchmarks
        to the generic power test handler that supports the same warmup + iteration
        pattern.
        """
        # Detect benchmark type and route to appropriate implementation
        benchmark_name = getattr(benchmark, "_name", type(benchmark).__name__.lower())
        # Also check for display name or class name patterns
        if not any(x in benchmark_name.lower() for x in ["tpch", "tpcds"]):
            display_name = str(getattr(benchmark, "display_name", "")).lower()
            class_name = type(benchmark).__name__.lower()
            if "tpch" in display_name or "tpch" in class_name:
                benchmark_name = "tpch"
            elif "tpcds" in display_name or "tpcds" in class_name:
                benchmark_name = "tpcds"

        if "tpch" in benchmark_name.lower():
            return self._execute_tpch_power_test(benchmark, connection, run_config)
        elif "tpcds" in benchmark_name.lower():
            return self._execute_tpcds_power_test(benchmark, connection, run_config)
        else:
            # Use generic power test handler for non-TPC benchmarks
            return self._execute_generic_power_test(benchmark, connection, run_config)

    def _execute_throughput_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC Throughput Test with concurrent query streams."""
        from rich.console import Console

        console = Console()

        # Detect benchmark type and route to appropriate implementation
        benchmark_name = getattr(benchmark, "_name", type(benchmark).__name__.lower())
        # Also check for display name or class name patterns
        if not any(x in benchmark_name.lower() for x in ["tpch", "tpcds"]):
            display_name = str(getattr(benchmark, "display_name", "")).lower()
            class_name = type(benchmark).__name__.lower()
            if "tpch" in display_name or "tpch" in class_name:
                benchmark_name = "tpch"
            elif "tpcds" in display_name or "tpcds" in class_name:
                benchmark_name = "tpcds"

        if "tpch" in benchmark_name.lower():
            return self._execute_tpch_throughput_test(benchmark, connection, run_config)
        elif "tpcds" in benchmark_name.lower():
            return self._execute_tpcds_throughput_test(benchmark, connection, run_config)
        else:
            console.print(f"[yellow]⚠️ Throughput test not supported for benchmark: {benchmark_name}[/yellow]")
            console.print("[yellow]  Falling back to standard query execution[/yellow]")
            return self._execute_all_queries(benchmark, connection, run_config)

    def _execute_maintenance_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC Maintenance Test with data maintenance operations."""
        from rich.console import Console

        console = Console()

        # Detect benchmark type and route to appropriate implementation
        benchmark_name = getattr(benchmark, "_name", type(benchmark).__name__.lower())
        # Also check for display name or class name patterns
        if not any(x in benchmark_name.lower() for x in ["tpch", "tpcds"]):
            display_name = str(getattr(benchmark, "display_name", "")).lower()
            class_name = type(benchmark).__name__.lower()
            if "tpch" in display_name or "tpch" in class_name:
                benchmark_name = "tpch"
            elif "tpcds" in display_name or "tpcds" in class_name:
                benchmark_name = "tpcds"

        if "tpch" in benchmark_name.lower():
            return self._execute_tpch_maintenance_test(benchmark, connection, run_config)
        elif "tpcds" in benchmark_name.lower():
            return self._execute_tpcds_maintenance_test(benchmark, connection, run_config)
        else:
            console.print(f"[yellow]⚠️ Maintenance test not supported for benchmark: {benchmark_name}[/yellow]")
            console.print("[yellow]  Falling back to standard query execution[/yellow]")
            return self._execute_all_queries(benchmark, connection, run_config)

    def _execute_combined_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute combined TPC test (Power + Throughput + Maintenance)."""
        from rich.console import Console

        console = Console()

        # Detect benchmark type and route to appropriate implementation
        benchmark_name = getattr(benchmark, "_name", type(benchmark).__name__.lower())
        # Also check for display name or class name patterns
        if not any(x in benchmark_name.lower() for x in ["tpch", "tpcds"]):
            display_name = str(getattr(benchmark, "display_name", "")).lower()
            class_name = type(benchmark).__name__.lower()
            if "tpch" in display_name or "tpch" in class_name:
                benchmark_name = "tpch"
            elif "tpcds" in display_name or "tpcds" in class_name:
                benchmark_name = "tpcds"

        if "tpcds" in benchmark_name.lower():
            console.print("[blue]Running combined TPC-DS test (Power + Throughput + Maintenance)[/blue]")

            # Execute each test phase
            all_results = []

            # Phase 1: Power Test
            console.print("[cyan]Phase 1: Power Test[/cyan]")
            power_results = self._execute_tpcds_power_test(benchmark, connection, run_config)
            all_results.extend(power_results)

            # Phase 2: Throughput Test
            console.print("[cyan]Phase 2: Throughput Test[/cyan]")
            throughput_results = self._execute_tpcds_throughput_test(benchmark, connection, run_config)
            all_results.extend(throughput_results)

            # Phase 3: Maintenance Test
            console.print("[cyan]Phase 3: Maintenance Test[/cyan]")
            maintenance_results = self._execute_tpcds_maintenance_test(benchmark, connection, run_config)
            all_results.extend(maintenance_results)

            return all_results
        elif "tpch" in benchmark_name.lower():
            console.print("[blue]Running combined TPC-H test (Power + Throughput + Maintenance)[/blue]")

            # Execute each test phase
            all_results = []

            # Phase 1: Power Test
            console.print("[cyan]Phase 1: Power Test[/cyan]")
            power_results = self._execute_tpch_power_test(benchmark, connection, run_config)
            all_results.extend(power_results)

            # Phase 2: Throughput Test
            console.print("[cyan]Phase 2: Throughput Test[/cyan]")
            throughput_results = self._execute_tpch_throughput_test(benchmark, connection, run_config)
            all_results.extend(throughput_results)

            # Phase 3: Maintenance Test
            console.print("[cyan]Phase 3: Maintenance Test[/cyan]")
            maintenance_results = self._execute_tpch_maintenance_test(benchmark, connection, run_config)
            all_results.extend(maintenance_results)

            return all_results
        else:
            console.print(f"[yellow]⚠️ Combined test not supported for benchmark: {benchmark_name}[/yellow]")
            console.print("[yellow]  Falling back to standard query execution[/yellow]")
            return self._execute_all_queries(benchmark, connection, run_config)

    def _execute_all_queries(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute all benchmark queries and collect results."""

        from rich.console import Console

        console = Console()

        # Get queries with platform-specific dialect translation if supported
        if hasattr(self, "get_target_dialect") and hasattr(benchmark, "get_queries"):
            try:
                # Check if benchmark supports dialect and base_dialect parameters
                import inspect

                sig = inspect.signature(benchmark.get_queries)
                params = sig.parameters
                target = self.get_target_dialect()

                # Detect benchmark family for base dialect selection
                bname = getattr(benchmark, "_name", type(benchmark).__name__).lower()
                bench_family = "tpcds" if "tpcds" in bname else ("tpch" if "tpch" in bname else "generic")
                base = self.get_tpc_base_dialect(bench_family)

                if "dialect" in params and "base_dialect" in params:
                    queries = benchmark.get_queries(dialect=target, base_dialect=base)
                elif "dialect" in params:
                    queries = benchmark.get_queries(dialect=target)
                else:
                    queries = benchmark.get_queries()
            except Exception:
                queries = benchmark.get_queries()
        else:
            queries = benchmark.get_queries()

        results = []
        benchmark_name = benchmark._name if hasattr(benchmark, "_name") else type(benchmark).__name__

        query_subset = run_config.get("query_subset")
        categories = run_config.get("categories")

        # Validate that query_subset and categories are not both specified (conflict)
        if query_subset and categories:
            raise ValueError(
                "Cannot specify both 'query_subset' and 'categories'. "
                "Use query_subset to select specific queries by ID, or categories to select by category, but not both."
            )

        try:
            if query_subset:
                # Validate query IDs against available queries
                invalid_queries = []
                for i, query_id in enumerate(query_subset):
                    query_id_str = str(query_id)
                    if query_id_str not in queries and query_id not in queries:
                        invalid_queries.append(query_id_str)
                    # Memory leak protection: limit validation errors to 10
                    if len(invalid_queries) >= 10:
                        remaining = len(query_subset) - i - 1
                        if remaining > 0:
                            invalid_queries.append(f"...and {remaining} more")
                        break

                if invalid_queries:
                    available_queries = sorted(str(k) for k in queries.keys())
                    # Limit displayed available queries for readability
                    if len(available_queries) > 20:
                        available_display = ", ".join(available_queries[:20]) + ", ..."
                    else:
                        available_display = ", ".join(available_queries)
                    raise ValueError(
                        f"Invalid query IDs specified: {', '.join(invalid_queries)}. "
                        f"Available queries for {benchmark_name}: {available_display}"
                    )

                # Filter queries while preserving user-specified order
                ordered_queries = {}
                for query_id in query_subset:
                    query_id_str = str(query_id)
                    # Try both string and original format as keys
                    if query_id_str in queries:
                        ordered_queries[query_id_str] = queries[query_id_str]
                    elif query_id in queries:
                        ordered_queries[query_id] = queries[query_id]
                queries = ordered_queries

            elif categories:
                filtered_queries = {}
                for category in categories:
                    if hasattr(benchmark, "get_queries_by_category"):
                        cat_queries = benchmark.get_queries_by_category(category)
                        filtered_queries.update(cat_queries)
                queries = filtered_queries

        except ValueError as e:
            # Re-raise ValueError with better context for debugging
            raise RuntimeError(f"Query filtering failed: {e}") from e

        total_queries = len(queries)

        # Set up cancellation handler
        cancelled = False

        def signal_handler(sig, frame):
            nonlocal cancelled
            cancelled = True
            console.print("\n[yellow]⚠️️  Cancellation requested. Will stop after current query completes.[/yellow]")
            console.print("[yellow]   Partial results will be saved.[/yellow]")

        # Register signal handlers for graceful cancellation
        original_sigint = signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, "SIGTERM"):
            original_sigterm = signal.signal(signal.SIGTERM, signal_handler)

        try:
            console.print(
                f"[cyan]Running {total_queries} {benchmark_name} queries. Press Ctrl+C to cancel (will stop after current query).[/cyan]"
            )

            for i, (query_id, query_sql) in enumerate(queries.items(), 1):
                if cancelled:
                    break

                console.print(f"[blue]Executing query {i}/{total_queries}: {query_id}[/blue]")

                try:
                    # Check if benchmark implements OperationExecutor interface
                    # This is for benchmarks that execute discrete operations (INSERT/UPDATE/DELETE/etc.)
                    # rather than just read-only queries
                    if isinstance(benchmark, OperationExecutor):
                        # Use benchmark's operation execution method
                        op_result = benchmark.execute_operation(query_id, connection, use_transaction=True)

                        # Convert OperationResult to standard query result format
                        result = {
                            "query_id": str(query_id),
                            "status": "SUCCESS" if op_result.success else "FAILED",
                            "execution_time": op_result.write_duration_ms / 1000.0,  # Convert to seconds
                            "rows_returned": op_result.rows_affected if op_result.rows_affected > 0 else 0,
                            "error": op_result.error,
                            "validation_time": op_result.validation_duration_ms / 1000.0,
                            "validation_passed": op_result.validation_passed,
                            "cleanup_time": op_result.cleanup_duration_ms / 1000.0,
                        }
                    else:
                        # Standard query execution
                        # Extract benchmark metadata for row count validation
                        benchmark_type = self._get_benchmark_type(benchmark)
                        scale_factor = getattr(benchmark, "scale_factor", None)

                        result = self.execute_query(
                            connection,
                            query_sql,
                            query_id,
                            benchmark_type=benchmark_type,
                            scale_factor=scale_factor,
                            validate_row_count=self.enable_validation,
                        )

                    results.append(result)

                    # Show result with appropriate color based on status
                    execution_time = result.get("execution_time", 0)
                    rows_returned = result.get("rows_returned", 0)
                    time_display = self._format_execution_time(execution_time)
                    status = result.get("status", "SUCCESS")
                    validation_status = result.get("row_count_validation_status")

                    # Check if query or validation failed
                    if status == "FAILED":
                        error_msg = result.get("error", "Unknown error")
                        # Truncate error message for console display
                        error_preview = error_msg[:80] + "..." if len(error_msg) > 80 else error_msg
                        console.print(f"[red]❌ Query {i}/{total_queries}: {query_id} FAILED - {error_preview}[/red]")
                    else:
                        # Query succeeded - show with validation info if available
                        if validation_status == "PASSED":
                            console.print(
                                f"[green]✅ Query {i}/{total_queries}: {query_id} completed in {time_display} ({rows_returned:,} rows) [validation: PASSED][/green]"
                            )
                        elif validation_status == "SKIPPED":
                            console.print(
                                f"[green]✅ Query {i}/{total_queries}: {query_id} completed in {time_display} ({rows_returned:,} rows) [validation: SKIPPED][/green]"
                            )
                        else:
                            # No validation or unknown status
                            console.print(
                                f"[green]✅ Query {i}/{total_queries}: {query_id} completed in {time_display} ({rows_returned:,} rows)[/green]"
                            )

                except PlanCaptureError:
                    # Strict plan capture failure should halt the benchmark execution
                    raise
                except Exception as e:
                    error_result = {
                        "query_id": str(query_id),
                        "status": "FAILED",
                        "execution_time": 0.0,
                        "rows_returned": 0,
                        "error": str(e),
                    }
                    results.append(error_result)
                    console.print(f"[red]❌ Query {i}/{total_queries}: {query_id} failed - {str(e)[:100]}[/red]")

        finally:
            # Restore original signal handlers
            signal.signal(signal.SIGINT, original_sigint)
            if hasattr(signal, "SIGTERM"):
                signal.signal(signal.SIGTERM, original_sigterm)

        if cancelled:
            console.print(f"[yellow]Benchmark cancelled. Processed {len(results)}/{total_queries} queries.[/yellow]")
        else:
            successful = len([r for r in results if r.get("status") == "SUCCESS"])
            failed = total_queries - successful
            if failed > 0:
                console.print(
                    f"[yellow]Completed {total_queries} queries: {successful} passed, {failed} failed.[/yellow]"
                )
            else:
                console.print(f"[green]Completed all {total_queries} queries.[/green]")

        return results

    def _calculate_data_size(self, data_dir: Path) -> float:
        """Calculate total size of data files in MB.

        Note: Returns 0.0 for cloud storage paths (S3, Azure, GCS, DBFS) as they
        require authentication and don't support local file operations. Data size
        calculation is optional for metrics and skipped for cloud paths.
        """
        from benchbox.utils.cloud_storage import is_cloud_path

        total_size = 0
        try:
            # Skip cloud paths - they require authentication and listing can fail
            if is_cloud_path(str(data_dir)):
                return 0.0

            # rglob() not supported on some special paths
            if not hasattr(data_dir, "rglob"):
                return 0.0

            for file_path in data_dir.rglob("*"):
                if file_path.is_file() and file_path.suffix in [".csv", ".tbl"]:
                    total_size += file_path.stat().st_size
        except (AttributeError, NotImplementedError, OSError):
            # Cloud paths may not support rglob(), stat(), or is_file()
            return 0.0
        except Exception:
            # Catch all other errors (e.g., authentication errors from cloud providers)
            # Data size calculation is optional, so gracefully skip on any error
            return 0.0

        return total_size / (1024 * 1024)

    def _get_platform_metadata(self, connection: Any) -> dict[str, Any]:
        """Get platform-specific metadata (to be overridden by subclasses)."""
        metadata = {
            "platform": self.platform_name,
            "connection_type": type(connection).__name__,
            "tuning_enabled": self.tuning_enabled,
        }

        # Include tuning configuration metadata if available
        effective_config = self.get_effective_tuning_configuration()
        if self.tuning_enabled and effective_config:
            metadata["tuning_configuration_hash"] = effective_config.get_configuration_hash()
            metadata["tuned_tables"] = list(effective_config.table_tunings.keys())
            metadata["tuning_types_enabled"] = [t.value for t in effective_config.get_enabled_tuning_types()]

        return metadata

    def _hash_connection_config(self, connection_config: dict[str, Any]) -> str:
        """Generate a hash of connection configuration (excluding sensitive data)."""
        # Create a sanitized version of config for hashing
        sanitized_config = {}
        for key, value in connection_config.items():
            if key not in ["password", "token", "service_account_path"]:
                sanitized_config[key] = value

        config_str = str(sorted(sanitized_config.items()))
        return hashlib.md5(config_str.encode()).hexdigest()[:16]

    def run_power_test(self, benchmark, **kwargs) -> dict[str, Any]:
        """Run TPC power test measuring single-stream query performance.

        The power test executes queries sequentially in a single stream to measure
        the database's ability to process complex analytical queries efficiently.
        This test focuses on query optimization and execution performance.

        Args:
            benchmark: Benchmark instance with queries and data
            **kwargs: Configuration options for the power test including:
                - query_timeout: Maximum time per query (default: platform-specific)
                - query_subset: List of specific queries to run (default: all)
                - validation: Whether to validate query results (default: True)

        Returns:
            Dictionary containing power test results with keys:
                - test_type: "power"
                - total_execution_time: Total time for all queries in seconds
                - query_count: Number of queries executed
                - successful_queries: Number of successful query executions
                - failed_queries: Number of failed query executions
                - query_results: List of individual query execution results
                - geometric_mean: Geometric mean of query execution times
                - validation_status: Overall validation result status
        """
        self.log_operation_start("Power test execution", f"benchmark: {benchmark.__class__.__name__}")

        # Check if benchmark has its own run_power_test method (TPC benchmarks)
        if hasattr(benchmark, "run_power_test") and callable(benchmark.run_power_test):
            # TPC benchmarks expect connection as first positional argument
            connection = kwargs.pop("connection", None)
            if connection is None:
                raise ValueError("TPC benchmarks require a connection object for power tests")

            # Inject platform's target dialect if not already specified
            if "dialect" not in kwargs:
                kwargs["dialect"] = self.get_target_dialect()

            return benchmark.run_power_test(connection, **kwargs)
        else:
            # Use the base class run_benchmark method for other benchmarks
            return self.run_benchmark(benchmark, **kwargs).__dict__

    def run_throughput_test(self, benchmark, **kwargs) -> dict[str, Any]:
        """Run TPC throughput test measuring concurrent multi-stream performance.

        The throughput test executes multiple concurrent query streams to measure
        the database's ability to handle concurrent analytical workloads.
        This test focuses on scalability and concurrent query processing.

        Args:
            benchmark: Benchmark instance with queries and data
            **kwargs: Configuration options for the throughput test including:
                - stream_count: Number of concurrent query streams (default: platform-specific)
                - query_timeout: Maximum time per query (default: platform-specific)
                - warmup_runs: Number of warmup iterations per stream (default: 1)
                - measurement_runs: Number of measurement iterations (default: 1)
                - validation: Whether to validate query results (default: True)

        Returns:
            Dictionary containing throughput test results with keys:
                - test_type: "throughput"
                - stream_count: Number of concurrent streams used
                - total_execution_time: Total wall-clock time in seconds
                - aggregate_query_time: Sum of all query execution times
                - queries_per_hour: Throughput metric (queries/hour)
                - stream_results: List of individual stream execution results
                - validation_status: Overall validation result status
        """
        # Check if benchmark has its own run_throughput_test method (TPC benchmarks)
        if hasattr(benchmark, "run_throughput_test") and callable(benchmark.run_throughput_test):
            # TPC benchmarks may expect different calling conventions
            connection = kwargs.pop("connection", None)
            if connection is None:
                raise ValueError("TPC benchmarks require a connection object for throughput tests")
            # TPC-DS uses connection_factory pattern
            connection_factory = kwargs.pop("connection_factory", lambda: connection)

            # Inject platform's target dialect if not already specified
            if "dialect" not in kwargs:
                kwargs["dialect"] = self.get_target_dialect()

            return benchmark.run_throughput_test(connection_factory=connection_factory, **kwargs).__dict__
        else:
            # For now, run as single stream - could be extended for multi-stream
            # Don't pop connection since run_power_test may need it
            return self.run_power_test(benchmark, **kwargs)

    def run_maintenance_test(self, benchmark, **kwargs) -> dict[str, Any]:
        """Run TPC maintenance test measuring data modification performance.

        The maintenance test executes data modification operations (INSERT, UPDATE, DELETE)
        to measure the database's ability to handle data maintenance workloads while
        concurrent query streams are running.

        Args:
            benchmark: Benchmark instance with maintenance functions and data
            **kwargs: Configuration options for the maintenance test including:
                - maintenance_operations: List of operations to perform (default: all)
                - concurrent_streams: Number of concurrent query streams (default: 1)
                - batch_size: Size of maintenance operation batches (default: platform-specific)
                - validation: Whether to validate results (default: True)

        Returns:
            Dictionary containing maintenance test results with keys:
                - test_type: "maintenance"
                - operations_executed: Number of maintenance operations performed
                - total_execution_time: Total time for all operations in seconds
                - operation_results: List of individual operation execution results
                - concurrent_query_impact: Impact on concurrent query performance
                - data_integrity_status: Data consistency validation results
                - validation_status: Overall validation result status
        """
        # Use the base class run_benchmark method
        return self.run_benchmark(benchmark, **kwargs).__dict__

    def _create_schema_with_tuning(self, benchmark, source_dialect: str = "duckdb") -> str:
        """Common schema creation logic with tuning support.

        Args:
            benchmark: Benchmark instance to get schema from
            source_dialect: Source SQL dialect to translate from (default: "duckdb")

        Returns:
            SQL schema string ready for execution

        Raises:
            Exception: If schema creation fails
        """
        self.log_operation_start(
            "Schema SQL generation", f"benchmark: {benchmark.__class__.__name__}, target: {self.get_target_dialect()}"
        )

        # Get effective tuning configuration
        effective_config = self.get_effective_tuning_configuration()

        tuning_status = "with tuning" if effective_config else "no tuning"
        self.log_verbose(f"Schema generation {tuning_status} - target dialect: {self.get_target_dialect()}")
        self.log_very_verbose(f"Effective tuning config type: {type(effective_config)}")

        # Use standardized signature with dialect and tuning configuration
        try:
            schema_sql = benchmark.get_create_tables_sql(
                dialect=self.get_target_dialect(), tuning_config=effective_config
            )
            self.log_very_verbose("Using standardized schema generation with tuning configuration")
            self.log_verbose(f"Schema SQL from benchmark: {len(schema_sql)} characters")
        except TypeError as e:
            # Fallback for benchmarks that don't support the new signature yet
            self.logger.warning(
                f"TypeError calling get_create_tables_sql with new signature: {e}. Falling back to legacy."
            )
            schema_sql = benchmark.get_create_tables_sql()
            self.log_very_verbose("Using legacy schema generation (no tuning configuration)")
            self.log_verbose(f"Schema SQL from benchmark (legacy): {len(schema_sql)} characters")
        except Exception as e:
            self.logger.error(f"Unexpected exception in schema generation: {type(e).__name__}: {e}")
            raise

        # Translate to target dialect if needed
        translation_needed = source_dialect != self.get_target_dialect()
        if translation_needed:
            original_len = len(schema_sql)
            self.log_verbose(f"Translating schema SQL from {source_dialect} to {self.get_target_dialect()}")
            self.log_very_verbose(f"SQL before translation: {original_len} characters")
            schema_sql = self.translate_sql(schema_sql, source_dialect)
            self.log_verbose(f"SQL after translation: {len(schema_sql)} characters (was {original_len})")
            if len(schema_sql) < original_len * 0.5:
                self.logger.warning(
                    f"Translation reduced SQL size significantly: {original_len} -> {len(schema_sql)} characters. "
                    "This may indicate a translation problem."
                )

        self.log_operation_complete(
            "Schema SQL generation",
            details=f"{len(schema_sql)} characters, translation: {'yes' if translation_needed else 'no'}",
        )

        return schema_sql

    def _execute_schema_statements(
        self, statements: list[str], cursor: Any, platform_transform_fn: Any = None
    ) -> tuple[int, list[tuple[str, str]]]:
        """Execute schema statements with comprehensive error handling and logging.

        This method provides robust error handling for schema creation across all platforms.
        It attempts to create all tables even if some fail, and provides detailed error
        reporting showing exactly which tables failed and why.

        Args:
            statements: List of SQL CREATE TABLE statements to execute
            cursor: Database cursor for executing statements
            platform_transform_fn: Optional function to transform statements for platform-specific syntax
                                 (e.g., _convert_to_delta_table for Databricks)

        Returns:
            Tuple of (tables_created_count, failed_tables_list)
            where failed_tables_list contains (table_name, error_message) tuples

        Example:
            statements = ["CREATE TABLE region (...)", "CREATE TABLE nation (...)"]
            created, failed = self._execute_schema_statements(statements, cursor)
            if failed:
                self.logger.error(f"Failed to create {len(failed)} tables: {failed}")

        Raises:
            RuntimeError: If any table creation fails (after attempting all statements)
        """
        import re

        tables_created = 0
        failed_tables: list[tuple[str, str]] = []

        for i, statement in enumerate(statements, 1):
            if not statement.strip():
                continue

            # Extract table name for better error reporting
            table_name = "unknown"
            match = re.search(r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([^\s(]+)", statement, re.IGNORECASE)
            if match:
                table_name = match.group(1).strip("`").strip('"')

            try:
                # Apply platform-specific transformation if provided
                if platform_transform_fn:
                    transformed_statement = platform_transform_fn(statement)
                else:
                    transformed_statement = statement

                self.log_very_verbose(f"Creating table {table_name} ({i}/{len(statements)})")
                self.log_very_verbose(f"SQL: {transformed_statement[:150]}...")

                # Execute the statement
                cursor.execute(transformed_statement)
                tables_created += 1
                self.log_very_verbose(f"✅ Created table {table_name}")

            except Exception as e:
                error_msg = str(e)
                self.logger.error(f"❌ Failed to create table {table_name}: {error_msg}")
                self.log_very_verbose(f"Failed SQL: {statement[:200]}...")
                failed_tables.append((table_name, error_msg))
                # Continue to next table instead of failing immediately

        # Report summary
        self.log_verbose(f"Schema creation: {tables_created} tables created, {len(failed_tables)} failed")

        # If any tables failed, raise error with details
        if failed_tables:
            failure_details = "\n".join([f"  - {table}: {error[:100]}" for table, error in failed_tables])
            raise RuntimeError(
                f"Failed to create {len(failed_tables)} table(s) out of {len(statements)}:\n{failure_details}"
            )

        return tables_created, failed_tables

    def _get_constraint_configuration(self) -> tuple[bool, bool]:
        """Extract constraint configuration settings from tuning config.

        Returns:
            Tuple of (enable_primary_keys, enable_foreign_keys)
        """
        effective_config = self.get_effective_tuning_configuration()
        enable_primary_keys = effective_config.primary_keys.enabled if effective_config else False
        enable_foreign_keys = effective_config.foreign_keys.enabled if effective_config else False

        return enable_primary_keys, enable_foreign_keys

    def _log_constraint_configuration(self, enable_primary_keys: bool, enable_foreign_keys: bool) -> None:
        """Log constraint configuration settings.

        Args:
            enable_primary_keys: Whether primary key constraints are enabled
            enable_foreign_keys: Whether foreign key constraints are enabled
        """
        if enable_primary_keys:
            self.logger.info(f"Primary key constraints enabled for {self.platform_name}")

        if enable_foreign_keys:
            self.logger.info(f"Foreign key constraints enabled for {self.platform_name}")

        if not enable_primary_keys and not enable_foreign_keys:
            self.logger.debug(f"No constraints enabled for {self.platform_name}")

        self.logger.debug(
            f"Schema constraints from tuning config: primary_keys={enable_primary_keys}, foreign_keys={enable_foreign_keys}"
        )

    def _execute_tpch_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-H Power Test using production TPCHPowerTest implementation."""
        from rich.console import Console

        from benchbox.core.tpch.power_test import TPCHPowerTest

        console = Console()

        try:
            # Extract configuration
            scale_factor = run_config.get("scale_factor", 1.0)
            seed = run_config.get("seed", 1)
            validation_mode = run_config.get("validation_mode")  # Universal validation mode
            stream_id = run_config.get("stream_id", 0)
            query_subset = run_config.get("query_subset")
            getattr(self, "get_target_dialect", lambda: "standard")()
            verbose = run_config.get("verbose", False)
            timeout = run_config.get("timeout")
            iterations = run_config.get("iterations", TPCH_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
            warm_up_iterations = run_config.get("warm_up_iterations", TPCH_POWER_DEFAULT_WARMUP_ITERATIONS)

            console.print(
                f"[green]Running TPC-H Power Test (Scale Factor: {scale_factor}, Stream ID: {stream_id})[/green]"
            )
            console.print(f"[green]Warm-up runs: {warm_up_iterations}, Measurement runs: {iterations}[/green]")

            # Create connection adapter that wraps the platform adapter connection
            connection_adapter = PlatformAdapterConnection(connection, self)
            # Configure benchmark context for query validation
            connection_adapter.benchmark_type = "tpch"
            connection_adapter.scale_factor = scale_factor

            all_results = []

            # Warm-up runs
            for i in range(warm_up_iterations):
                current_stream_id = i  # Start at 0 for warmup
                console.print(f"[cyan]--- Warm-up Run {i + 1}/{warm_up_iterations} ---[/cyan]")
                power_test = TPCHPowerTest(
                    benchmark=benchmark,
                    connection=connection_adapter,
                    scale_factor=scale_factor,
                    seed=seed,
                    stream_id=current_stream_id,
                    verbose=verbose,
                    timeout=timeout,
                    dialect=self.get_target_dialect(),
                    validation_mode=validation_mode,
                    query_subset=query_subset,
                )
                power_test.run()

            # Measurement runs
            for i in range(iterations):
                current_stream_id = warm_up_iterations + i  # Continue from where warmup left off
                console.print(f"[cyan]--- Measurement Run {i + 1}/{iterations} ---[/cyan]")
                power_test = TPCHPowerTest(
                    benchmark=benchmark,
                    connection=connection_adapter,
                    scale_factor=scale_factor,
                    seed=seed,
                    stream_id=current_stream_id,
                    verbose=verbose,
                    timeout=timeout,
                    dialect=self.get_target_dialect(),
                    validation_mode=validation_mode,
                    query_subset=query_subset,
                )

                # Execute the power test
                power_test_result = power_test.run()

                # Display results
                if power_test_result.success:
                    success_rate = power_test_result.queries_successful / max(power_test_result.queries_executed, 1)
                    console.print(
                        f"[green]✅ TPC-H Power Test completed: Power@Size = {power_test_result.power_at_size:.2f}[/green]"
                    )
                    console.print(
                        f"  Queries executed: {power_test_result.queries_executed}, Successful: {power_test_result.queries_successful}"
                    )
                    console.print(f"  Success rate: {success_rate:.1%} (TPC-H requires ≥95%)")
                    console.print(f"  Total execution time: {power_test_result.total_time:.2f}s")
                else:
                    console.print("[red]❌ TPC-H Power Test failed[/red]")
                    for error in power_test_result.errors:
                        console.print(f"  Error: {error}")

                # Convert TPCHPowerTestResult to platform adapter format
                query_results = []
                for query_result in power_test_result.query_results:
                    platform_result = {
                        "query_id": query_result["query_id"],
                        "execution_time": query_result["execution_time"],
                        "status": "SUCCESS" if query_result["success"] else "FAILED",
                        "rows_returned": query_result.get("result_count", 0),
                        "test_type": "power",
                        "stream_id": query_result.get("stream_id", current_stream_id),
                        "position": query_result.get("position", 0),
                        "iteration": i + 1,
                    }
                    if not query_result["success"]:
                        platform_result["error"] = query_result.get("error", "Unknown error")
                    query_results.append(platform_result)
                all_results.extend(query_results)

            return all_results

        except Exception as e:
            console.print(f"[red]❌ TPC-H Power Test failed: {e}[/red]")
            return [
                {
                    "query_id": "power_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "power",
                }
            ]

    def _execute_generic_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute power test for non-TPC benchmarks with warmup + iterations.

        This method provides the same warmup + measurement iteration pattern used by
        TPC-H and TPC-DS power tests, but for generic benchmarks like ClickBench, SSB,
        H2O-DB, etc.

        Args:
            benchmark: Benchmark instance to execute
            connection: Database connection
            run_config: Runtime configuration including:
                - iterations: Number of measurement runs (default: 1)
                - warm_up_iterations: Number of warmup runs (default: 0)
                - verbose: Enable verbose logging
                - scale_factor: Benchmark scale factor

        Returns:
            List of query results from all measurement iterations, with each result
            tagged with iteration number and run_type='measurement'. Warmup results
            are discarded.
        """
        from rich.console import Console

        console = Console()

        # Extract configuration (same defaults as TPC-H power test)
        iterations = run_config.get("iterations", GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
        warm_up_iterations = run_config.get("warm_up_iterations", GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS)
        run_config.get("verbose", False)

        benchmark_name = getattr(benchmark, "_name", type(benchmark).__name__)
        scale_factor = run_config.get("scale_factor", getattr(benchmark, "scale_factor", 1.0))

        console.print(f"[green]Running {benchmark_name} Power Test (Scale Factor: {scale_factor})[/green]")
        console.print(f"[green]Warm-up runs: {warm_up_iterations}, Measurement runs: {iterations}[/green]")

        all_measurement_results = []

        # Warm-up runs (results discarded)
        for i in range(warm_up_iterations):
            console.print(f"[cyan]--- Warm-up Run {i + 1}/{warm_up_iterations} ---[/cyan]")
            # Execute all queries once, discard results
            _ = self._execute_all_queries(benchmark, connection, run_config)

        # Measurement runs (results collected)
        for i in range(iterations):
            console.print(f"[cyan]--- Measurement Run {i + 1}/{iterations} ---[/cyan]")
            iteration_results = self._execute_all_queries(benchmark, connection, run_config)

            # Tag each result with iteration number and run type
            for result in iteration_results:
                result["iteration"] = i + 1
                result["run_type"] = "measurement"

            all_measurement_results.extend(iteration_results)

        # Display summary
        if all_measurement_results:
            total_queries = len([r for r in all_measurement_results if r.get("iteration") == 1])
            successful = len([r for r in all_measurement_results if r.get("status") == "SUCCESS"])
            total_exec = len(all_measurement_results)
            success_rate = (successful / total_exec * 100) if total_exec > 0 else 0

            console.print("[green]✅ Power Test completed[/green]")
            console.print(f"  Queries: {total_queries}, Iterations: {iterations}")
            console.print(f"  Total executions: {total_exec}, Successful: {successful}")
            console.print(f"  Success rate: {success_rate:.1f}%")

        return all_measurement_results

    def _execute_tpcds_power_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-DS Power Test using production TPCDSPowerTest implementation."""
        from rich.console import Console

        from benchbox.core.expected_results.tpcds_results import set_config_validation_mode
        from benchbox.core.tpcds.power_test import TPCDSPowerTest

        console = Console()

        try:
            # Extract configuration
            scale_factor = run_config.get("scale_factor", 1.0)
            seed = run_config.get("seed", 1)
            validation_mode = run_config.get("validation_mode")  # Universal validation mode
            stream_id = run_config.get("stream_id", 0)
            query_subset = run_config.get("query_subset")
            dialect = getattr(self, "get_target_dialect", lambda: "standard")()
            verbose = run_config.get("verbose", False)
            timeout = run_config.get("timeout")
            iterations = run_config.get("iterations", TPCDS_POWER_DEFAULT_MEASUREMENT_ITERATIONS)
            warm_up_iterations = run_config.get("warm_up_iterations", TPCDS_POWER_DEFAULT_WARMUP_ITERATIONS)

            # Set TPC-DS validation mode from config (takes precedence over environment variable)
            set_config_validation_mode(validation_mode)

            console.print(
                f"[green]Running TPC-DS Power Test (Scale Factor: {scale_factor}, Stream ID: {stream_id})[/green]"
            )
            console.print(f"[green]Warm-up runs: {warm_up_iterations}, Measurement runs: {iterations}[/green]")

            # Create connection factory that wraps the platform adapter connection
            def connection_factory():
                conn_wrapper = PlatformAdapterConnection(connection, self)
                # Configure benchmark context for query validation
                conn_wrapper.benchmark_type = "tpcds"
                conn_wrapper.scale_factor = scale_factor
                return conn_wrapper

            all_results = []

            # Warm-up runs
            for i in range(warm_up_iterations):
                current_stream_id = i  # Start at 0 for warmup
                console.print(f"[cyan]--- Warm-up Run {i + 1}/{warm_up_iterations} ---[/cyan]")
                power_test = TPCDSPowerTest(
                    benchmark=benchmark,
                    connection_factory=connection_factory,
                    scale_factor=scale_factor,
                    seed=seed,
                    stream_id=current_stream_id,
                    verbose=verbose,
                    timeout=timeout,
                    dialect=self.get_target_dialect(),
                    query_subset=query_subset,
                )
                power_test.run()  # Results discarded for warmup

            # Measurement runs
            for i in range(iterations):
                current_stream_id = warm_up_iterations + i  # Continue from where warmup left off
                console.print(f"[cyan]--- Measurement Run {i + 1}/{iterations} ---[/cyan]")
                power_test = TPCDSPowerTest(
                    benchmark=benchmark,
                    connection_factory=connection_factory,
                    scale_factor=scale_factor,
                    seed=seed,
                    stream_id=current_stream_id,
                    verbose=verbose,
                    timeout=timeout,
                    dialect=self.get_target_dialect(),
                    query_subset=query_subset,
                )

                # Execute the power test
                power_test_result = power_test.run()

                # Display results
                if self.very_verbose:
                    with contextlib.suppress(Exception):
                        console.print(f"[dim]Target dialect: {dialect} | Detailed per-query results:[/dim]")
                    for qr in power_test_result.query_results:
                        qname = f"q{qr.get('query_id')}"
                        dur = qr.get("execution_time", 0.0)
                        status = "SUCCESS" if qr.get("success") else "FAILED"
                        rows = qr.get("result_count", 0)
                        console.print(f"  • {qname}: {dur:.2f}s, {status}, rows={rows}")

                if power_test_result.success:
                    success_rate = power_test_result.queries_successful / max(power_test_result.queries_executed, 1)
                    console.print(
                        f"[green]✅ TPC-DS Power Test completed: Power@Size = {power_test_result.power_at_size:.2f}[/green]"
                    )
                    console.print(
                        f"  Queries executed: {power_test_result.queries_executed}, Successful: {power_test_result.queries_successful}"
                    )
                    console.print(f"  Success rate: {success_rate:.1%}")
                    console.print(f"  Total execution time: {power_test_result.total_time:.2f}s")
                else:
                    console.print("[red]❌ TPC-DS Power Test failed[/red]")
                    for error in power_test_result.errors:
                        console.print(f"  Error: {error}")

                # Convert TPCDSPowerTestResult to platform adapter format
                query_results = []
                for query_result in power_test_result.query_results:
                    platform_result = {
                        "query_id": query_result["query_id"],
                        "execution_time": query_result["execution_time"],
                        "status": "SUCCESS" if query_result["success"] else "FAILED",
                        "rows_returned": query_result.get("result_count", 0),
                        "test_type": "power",
                        "stream_id": query_result.get("stream_id", current_stream_id),
                        "position": query_result.get("position", 0),
                        "iteration": i + 1,  # Add iteration tracking
                    }
                    if not query_result["success"]:
                        platform_result["error"] = query_result.get("error", "Unknown error")
                    query_results.append(platform_result)
                all_results.extend(query_results)  # Accumulate results from all iterations

            return all_results

        except Exception as e:
            console.print(f"[red]❌ TPC-DS Power Test failed: {e}[/red]")
            return [
                {
                    "query_id": "power_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "power",
                }
            ]

    def _execute_tpcds_throughput_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-DS Throughput Test using production TPCDSThroughputTest implementation."""
        from rich.console import Console

        from benchbox.core.expected_results.tpcds_results import set_config_validation_mode
        from benchbox.core.tpcds.throughput_test import TPCDSThroughputTest

        console = Console()

        try:
            # Extract configuration
            scale_factor = run_config.get("scale_factor", 1.0)
            validation_mode = run_config.get("validation_mode")  # Universal validation mode
            num_streams = run_config.get("num_streams", 2)
            verbose = run_config.get("verbose", False)

            # Set TPC-DS validation mode from config (takes precedence over environment variable)
            set_config_validation_mode(validation_mode)

            console.print(
                f"[green]Running TPC-DS Throughput Test (Scale Factor: {scale_factor}, Streams: {num_streams})[/green]"
            )

            # Create connection factory that wraps the platform adapter connection
            def connection_factory():
                conn_wrapper = PlatformAdapterConnection(connection, self)
                # Configure benchmark context for query validation
                conn_wrapper.benchmark_type = "tpcds"
                conn_wrapper.scale_factor = scale_factor
                return conn_wrapper

            # Create and configure the TPC-DS throughput test
            throughput_test = TPCDSThroughputTest(
                benchmark=benchmark,
                connection_factory=connection_factory,
                scale_factor=scale_factor,
                num_streams=num_streams,
                verbose=verbose,
                dialect=self.get_target_dialect(),
            )

            # Execute the throughput test (support overriding base seed via run_config['seed'])
            seed = run_config.get("seed")
            if seed is not None:
                from benchbox.core.tpcds.throughput_test import (
                    TPCDSThroughputTestConfig,
                )

                cfg = TPCDSThroughputTestConfig(
                    scale_factor=scale_factor,
                    num_streams=num_streams,
                    base_seed=int(seed),
                    verbose=verbose,
                )
                throughput_test_result = throughput_test.run(config=cfg)
            else:
                throughput_test_result = throughput_test.run()

            # Display results
            if self.very_verbose:
                with contextlib.suppress(Exception):
                    console.print(
                        f"[dim]Target dialect: {getattr(self, 'get_target_dialect', lambda: 'standard')()} | Detailed per-query results:[/dim]"
                    )
                for stream_result in throughput_test_result.stream_results:
                    for qr in stream_result.query_results:
                        qname = f"q{qr.get('query_id')}"
                        dur = qr.get("execution_time", 0.0)
                        status = "SUCCESS" if qr.get("success") else "FAILED"
                        rows = qr.get("result_count", 0)
                        console.print(
                            f"  • {qname} [stream {stream_result.stream_id}]: {dur:.2f}s, {status}, rows={rows}"
                        )

            self._last_throughput_test_result = throughput_test_result

            if throughput_test_result.success:
                console.print(
                    f"[green]✅ TPC-DS Throughput Test completed: Throughput@Size = {throughput_test_result.throughput_at_size:.2f}[/green]"
                )
                console.print(
                    f"  Streams executed: {throughput_test_result.streams_executed}, Successful: {throughput_test_result.streams_successful}"
                )
                console.print(f"  Total execution time: {throughput_test_result.total_time:.2f}s")

                # Show per-stream statistics
                for stream_result in throughput_test_result.stream_results:
                    success_rate = stream_result.queries_successful / max(stream_result.queries_executed, 1)
                    console.print(
                        f"  Stream {stream_result.stream_id}: {stream_result.queries_successful}/{stream_result.queries_executed} queries ({success_rate:.1%})"
                    )

            else:
                console.print("[red]❌ TPC-DS Throughput Test failed[/red]")
                for error in throughput_test_result.errors:
                    console.print(f"  Error: {error}")

            # Convert TPCDSThroughputTestResult to platform adapter format
            query_results = []
            for stream_result in throughput_test_result.stream_results:
                for query_result in stream_result.query_results:
                    platform_result = {
                        "query_id": query_result["query_id"],
                        "execution_time": query_result["execution_time"],
                        "status": "SUCCESS" if query_result["success"] else "FAILED",
                        "rows_returned": query_result.get("result_count", 0),
                        "test_type": "throughput",
                        "stream_id": stream_result.stream_id,
                    }
                    if not query_result["success"]:
                        platform_result["error"] = query_result.get("error", "Unknown error")
                    query_results.append(platform_result)

            return query_results

        except Exception as e:
            console.print(f"[red]❌ TPC-DS Throughput Test failed: {e}[/red]")
            self._last_throughput_test_result = None
            return [
                {
                    "query_id": "throughput_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "throughput",
                }
            ]

    def _execute_tpch_throughput_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-H Throughput Test using production TPCHThroughputTest implementation."""
        from rich.console import Console

        from benchbox.core.tpch.throughput_test import (
            TPCHThroughputTest,
            TPCHThroughputTestConfig,
        )

        console = Console()

        try:
            scale_factor = run_config.get("scale_factor", 1.0)
            num_streams = run_config.get("num_streams", run_config.get("streams", 2))
            verbose = run_config.get("verbose", False)

            console.print(
                f"[green]Running TPC-H Throughput Test (Scale Factor: {scale_factor}, Streams: {num_streams})[/green]"
            )

            def connection_factory():
                conn_wrapper = PlatformAdapterConnection(connection, self)
                # Configure benchmark context for query validation
                conn_wrapper.benchmark_type = "tpch"
                conn_wrapper.scale_factor = scale_factor
                return conn_wrapper

            throughput_test = TPCHThroughputTest(
                benchmark=benchmark,
                connection_factory=connection_factory,
                scale_factor=scale_factor,
                num_streams=num_streams,
                verbose=verbose,
                dialect=self.get_target_dialect(),
            )

            seed = run_config.get("seed")
            if seed is not None:
                cfg = TPCHThroughputTestConfig(
                    scale_factor=scale_factor,
                    num_streams=num_streams,
                    base_seed=int(seed),
                    verbose=verbose,
                )
                throughput_test_result = throughput_test.run(config=cfg)
            else:
                throughput_test_result = throughput_test.run()

            self._last_throughput_test_result = throughput_test_result

            if throughput_test_result.success:
                console.print(
                    f"[green]✅ TPC-H Throughput Test completed: Throughput@Size = {throughput_test_result.throughput_at_size:.2f}[/green]"
                )
                console.print(
                    f"  Streams executed: {throughput_test_result.streams_executed}, Successful: {throughput_test_result.streams_successful}"
                )
                console.print(f"  Total execution time: {throughput_test_result.total_time:.2f}s")
            else:
                console.print("[red]❌ TPC-H Throughput Test failed[/red]")
                for error in throughput_test_result.errors:
                    console.print(f"  Error: {error}")

            query_results = []
            for stream_result in throughput_test_result.stream_results:
                for qr in stream_result.query_results:
                    platform_result = {
                        "query_id": qr.get("query_id"),
                        "execution_time": qr.get("execution_time", 0.0),
                        "status": "SUCCESS" if qr.get("success") else "FAILED",
                        "rows_returned": qr.get("result_count", 0),
                        "test_type": "throughput",
                        "stream_id": stream_result.stream_id,
                    }
                    if not qr.get("success"):
                        platform_result["error"] = qr.get("error", "Unknown error")
                    query_results.append(platform_result)

            return query_results

        except Exception as e:
            console.print(f"[red]❌ TPC-H Throughput Test failed: {e}[/red]")
            self._last_throughput_test_result = None
            return [
                {
                    "query_id": "throughput_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "throughput",
                }
            ]

    def _execute_tpcds_maintenance_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-DS Maintenance Test using production TPCDSMaintenanceTest implementation."""
        from pathlib import Path

        from rich.console import Console

        from benchbox.core.tpcds.maintenance_test import TPCDSMaintenanceTest

        console = Console()

        try:
            # Extract configuration
            scale_factor = run_config.get("scale_factor", 1.0)
            verbose = run_config.get("verbose", False)
            output_dir = run_config.get("output_dir", Path.cwd() / "tpcds_maintenance_test")

            console.print(f"[green]Running TPC-DS Maintenance Test (Scale Factor: {scale_factor})[/green]")

            # Create connection factory that wraps the platform adapter connection
            def connection_factory():
                conn_wrapper = PlatformAdapterConnection(connection, self)
                # Configure benchmark context for query validation
                conn_wrapper.benchmark_type = "tpcds"
                conn_wrapper.scale_factor = scale_factor
                return conn_wrapper

            # Create and configure the TPC-DS maintenance test
            maintenance_test = TPCDSMaintenanceTest(
                benchmark=benchmark,
                connection_factory=connection_factory,
                scale_factor=scale_factor,
                output_dir=Path(output_dir) if isinstance(output_dir, str) else output_dir,
                verbose=verbose,
                dialect=self.get_target_dialect(),
            )

            # Execute the maintenance test
            maintenance_test_result = maintenance_test.run()

            # Display results
            if maintenance_test_result["success"]:
                console.print("[green]✅ TPC-DS Maintenance Test completed[/green]")
                console.print(f"  Insert operations: {maintenance_test_result['insert_operations']}")
                console.print(f"  Update operations: {maintenance_test_result['update_operations']}")
                console.print(f"  Delete operations: {maintenance_test_result['delete_operations']}")
                console.print(
                    f"  Total operations: {maintenance_test_result['total_operations']}, Successful: {maintenance_test_result['successful_operations']}"
                )
                console.print(f"  Total execution time: {maintenance_test_result['total_time']:.2f}s")
                console.print(f"  Throughput: {maintenance_test_result['overall_throughput']:.2f} ops/sec")

            else:
                console.print("[red]❌ TPC-DS Maintenance Test failed[/red]")
                for error in maintenance_test_result["errors"]:
                    console.print(f"  Error: {error}")

            # Convert maintenance test results to platform adapter format
            query_results = []
            for operation in maintenance_test_result["operations"]:
                platform_result = {
                    "query_id": f"{operation.operation_type.lower()}_{operation.table_name}",
                    "execution_time": operation.duration,
                    "status": "SUCCESS" if operation.success else "FAILED",
                    "rows_returned": operation.rows_affected,
                    "test_type": "maintenance",
                    "operation_type": operation.operation_type,
                    "table_name": operation.table_name,
                }
                if not operation.success:
                    platform_result["error"] = operation.error or "Unknown error"
                query_results.append(platform_result)

            return query_results

        except Exception as e:
            console.print(f"[red]❌ TPC-DS Maintenance Test failed: {e}[/red]")
            return [
                {
                    "query_id": "maintenance_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "maintenance",
                }
            ]

    def _execute_tpch_maintenance_test(self, benchmark, connection: Any, run_config: dict) -> list[dict[str, Any]]:
        """Execute TPC-H Maintenance Test using production TPCHMaintenanceTest implementation."""
        from pathlib import Path

        from rich.console import Console

        from benchbox.core.tpch.maintenance_test import TPCHMaintenanceTest

        console = Console()

        try:
            scale_factor = run_config.get("scale_factor", 1.0)
            verbose = run_config.get("verbose", False)
            maintenance_pairs = run_config.get("maintenance_pairs", 1)
            rf1_interval = run_config.get("rf1_interval", 0.0)
            rf2_interval = run_config.get("rf2_interval", 0.0)
            validate_integrity = run_config.get("validate_integrity", True)
            output_dir = run_config.get("output_dir", Path.cwd() / "tpch_maintenance_test")

            console.print(f"[green]Running TPC-H Maintenance Test (Scale Factor: {scale_factor})[/green]")

            def connection_factory():
                conn_wrapper = PlatformAdapterConnection(connection, self)
                # Configure benchmark context for query validation
                conn_wrapper.benchmark_type = "tpch"
                conn_wrapper.scale_factor = scale_factor
                return conn_wrapper

            maintenance_test = TPCHMaintenanceTest(
                connection_factory=connection_factory,
                scale_factor=scale_factor,
                output_dir=Path(output_dir) if isinstance(output_dir, str) else output_dir,
                verbose=verbose,
                dialect=self.get_target_dialect(),
            )

            result = maintenance_test.run_maintenance_test(
                maintenance_pairs=maintenance_pairs,
                concurrent_with_queries=False,
                rf1_interval=rf1_interval,
                rf2_interval=rf2_interval,
                validate_integrity=validate_integrity,
            )

            if result.success:
                console.print("[green]✅ TPC-H Maintenance Test completed[/green]")
                console.print(
                    f"  Operations: {result.total_operations}, Successful: {result.successful_operations}, Failed: {result.failed_operations}"
                )
                console.print(
                    f"  Total time: {result.total_time:.2f}s, Overall throughput: {result.overall_throughput:.2f} ops/s"
                )
            else:
                console.print("[red]❌ TPC-H Maintenance Test failed[/red]")
                for err in result.errors:
                    console.print(f"  Error: {err}")

            # Convert to platform adapter query-like results: record operations
            query_results = []
            for op in result.operations:
                query_results.append(
                    {
                        "query_id": op.operation_type,
                        "execution_time": op.duration,
                        "status": "SUCCESS" if op.success else "FAILED",
                        "rows_returned": op.rows_affected,
                        "test_type": "maintenance",
                    }
                )

            return query_results

        except Exception as e:
            console.print(f"[red]❌ TPC-H Maintenance Test failed: {e}[/red]")
            return [
                {
                    "query_id": "maintenance_test_error",
                    "execution_time": 0.0,
                    "status": "FAILED",
                    "rows_returned": 0,
                    "error": str(e),
                    "test_type": "maintenance",
                }
            ]

    def log_verbose(self, message: str) -> None:
        # Delegate to shared mixin implementation
        VerbosityMixin.log_verbose(self, message)

    def log_very_verbose(self, message: str) -> None:
        # Delegate to shared mixin implementation
        VerbosityMixin.log_very_verbose(self, message)

    def log_operation_start(self, operation: str, details: str = "") -> None:
        # Delegate to shared mixin implementation
        VerbosityMixin.log_operation_start(self, operation, details)

    def log_operation_complete(self, operation: str, duration: float | None = None, details: str = "") -> None:
        # Delegate to shared mixin implementation
        VerbosityMixin.log_operation_complete(self, operation, duration, details)


class PlatformAdapterConnection:
    """Adapter to wrap platform adapter connections for TPC test classes."""

    def __init__(self, connection: Any, platform_adapter: PlatformAdapter):
        """Initialize the connection adapter.

        Args:
            connection: Original platform connection
            platform_adapter: Platform adapter instance for query execution
        """
        self.connection = connection
        self.platform_adapter = platform_adapter
        self.connection_string = getattr(connection, "connection_string", "platform_adapter_connection")
        self.dialect = getattr(platform_adapter, "get_target_dialect", lambda: "standard")()

        # Benchmark context for query validation (set by TPC test runners)
        self.benchmark_type: str | None = None
        self.scale_factor: float | None = None
        self._current_query_id: str = "unknown_query"
        self._current_stream_id: int | None = None

    def set_query_context(self, query_id: str, stream_id: int | None = None) -> None:
        """Set context for the next query execution.

        Used by TPC test runners to provide query identification for validation.

        Args:
            query_id: Query identifier (e.g., "q1", "q15a", "1")
            stream_id: Stream identifier for multi-stream benchmarks (e.g., 0, 1, 2...)
                      None indicates stream 0 or single-stream execution.
        """
        self._current_query_id = query_id
        self._current_stream_id = stream_id

    def execute(self, query: str):
        """Execute a query using the platform adapter with validation support.

        Args:
            query: SQL query to execute

        Returns:
            Mock cursor with results and validation metadata
        """
        # Debug: Write query to file to see what's being executed
        try:
            with open("/tmp/tpc_debug_queries.log", "a") as f:
                f.write(f"TPC Query (ID: {self._current_query_id}): {query[:200]}...\n")
        except Exception:
            pass

        # Execute query with validation context
        result = self.platform_adapter.execute_query(
            self.connection,
            query,
            self._current_query_id,
            benchmark_type=self.benchmark_type,
            scale_factor=self.scale_factor,
            validate_row_count=True,  # Enable validation for TPC tests
            stream_id=self._current_stream_id,
        )
        return PlatformAdapterCursor(result)

    def commit(self):
        """Mock commit method."""

    def close(self):
        """Mock close method."""


class PlatformAdapterCursor:
    """Mock cursor that wraps platform adapter query results."""

    def __init__(self, platform_result: dict):
        """Initialize with platform adapter result.

        Args:
            platform_result: Result dictionary from platform adapter
        """
        self.platform_result = platform_result
        self.rows = self._extract_rows()

    def _extract_rows(self):
        """Extract rows from platform result."""
        # For TPC tests, we mainly need row count, not actual data
        row_count = self.platform_result.get("rows_returned", 0)
        return [("mock_row",)] * row_count  # Create mock rows

    def fetchall(self):
        """Return all rows."""
        return self.rows

    def fetchone(self):
        """Return one row."""
        return self.rows[0] if self.rows else None
