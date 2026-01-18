"""Benchmark execution engine using existing proven patterns.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from rich.panel import Panel
from rich.table import Table

from benchbox.cli.display import create_display_manager
from benchbox.cli.execution_pipeline import create_execution_engine
from benchbox.utils.printing import quiet_console

if TYPE_CHECKING:  # pragma: no cover - typing only
    from rich.console import Console

from benchbox.core.results.models import BenchmarkResults

console = quiet_console


ResultType = BenchmarkResults


class BenchmarkExecutor:
    """Benchmark execution engine using proven patterns."""

    def __init__(self, console: Optional["Console"] = None):
        self.console = console or quiet_console
        self.engine = create_execution_engine(self.console)
        self.display = create_display_manager(self.console)
        self.current_execution: Optional[ResultType] = None
        self._benchmark_instance: Optional[Any] = None
        self._stop_requested = False

    def execute_benchmark(self, benchmark_config, database_config, system_profile) -> ResultType:
        """Execute benchmark using pipeline architecture."""

        try:
            # Display execution info using standardized display
            self.display.show_benchmark_config(benchmark_config)
            if database_config:
                self.display.show_database_config(database_config)

            # Execute using new pipeline architecture
            result = self.engine.execute_benchmark(
                benchmark_config=benchmark_config,
                database_config=database_config,
                system_profile=system_profile,
            )

            self._update_cached_benchmark_instance()
            self.current_execution = result

            # Display results using standardized display
            self.display.show_benchmark_result(result)

            return result

        except KeyboardInterrupt:
            self.console.print("\n[yellow]⚠️️  Execution interrupted by user[/yellow]")
            result = self._build_minimal_result(
                benchmark_config=benchmark_config,
                database_config=database_config,
                system_profile=system_profile,
                validation_status="INTERRUPTED",
                validation_details={"reason": "user_interrupt"},
                execution_metadata={"error_type": "keyboard_interrupt"},
            )
            self.current_execution = result
            return result
        except Exception as e:
            from benchbox.cli.exceptions import (
                ErrorContext,
                create_error_handler,
            )

            # Create error context
            context = ErrorContext(
                operation="benchmark_execution",
                stage="execution",
                benchmark_name=benchmark_config.name,
                database_type=database_config.type if database_config else None,
            )

            # Use centralized error handling
            error_handler = create_error_handler(self.console)
            error_handler.handle_error(e, context, show_traceback=False)

            # Return failed result
            result = self._build_minimal_result(
                benchmark_config=benchmark_config,
                database_config=database_config,
                system_profile=system_profile,
                validation_status="FAILED",
                validation_details={"error": str(e)},
                execution_metadata={
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                },
            )
            self.current_execution = result
            return result

    def _display_execution_info(self, benchmark_config, database_config):
        """Display execution configuration."""
        info_table = Table(title="Execution Configuration")
        info_table.add_column("Setting", style="cyan")
        info_table.add_column("Value", style="green")

        info_table.add_row("Benchmark", str(benchmark_config.display_name))
        info_table.add_row("Database", str(database_config.name))
        info_table.add_row("Scale Factor", str(benchmark_config.scale_factor))

        if hasattr(benchmark_config, "queries") and benchmark_config.queries:
            info_table.add_row("Query Subset", f"{len(benchmark_config.queries)} queries")

        if hasattr(benchmark_config, "concurrency") and benchmark_config.concurrency > 1:
            info_table.add_row("Concurrency", f"{benchmark_config.concurrency} streams")

        estimated_time = getattr(benchmark_config, "options", {}).get("estimated_time", "Unknown")
        info_table.add_row("Estimated Time", str(estimated_time))

        console.print(info_table)
        console.print()

    def _display_results_summary(self, result: ResultType):
        """Display execution results summary."""
        console.print("\n" + "=" * 60)
        console.print(Panel.fit("[bold green]Benchmark Execution Complete[/bold green]", style="green"))

        # Summary table
        summary_table = Table(title="Execution Summary")
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="green")

        benchmark_name = getattr(result, "benchmark_name", "Unknown Benchmark")
        execution_id = getattr(result, "execution_id", "Unknown ID")
        duration_seconds = getattr(result, "duration_seconds", 0.0)
        validation_status = getattr(result, "validation_status", "Unknown Status")

        summary_table.add_row("Benchmark", str(benchmark_name))
        summary_table.add_row("Execution ID", str(execution_id))
        summary_table.add_row("Duration", f"{duration_seconds:.2f} seconds")
        summary_table.add_row("Status", str(validation_status))

        summary_metrics = getattr(result, "summary_metrics", {})
        if summary_metrics:
            total_queries = summary_metrics.get("total_queries", 0)
            successful_queries = summary_metrics.get("successful_queries", 0)
            success_rate = summary_metrics.get("success_rate", 0) * 100
            avg_time = summary_metrics.get("avg_time_ms", 0)

            summary_table.add_row("Total Queries", str(int(total_queries)))
            summary_table.add_row("Successful", str(int(successful_queries)))
            summary_table.add_row("Success Rate", f"{success_rate:.1f}%")

            if avg_time > 0:
                summary_table.add_row("Avg Query Time", f"{avg_time:.2f} ms")

        console.print(summary_table)

        # Show some query details if available
        query_results = getattr(result, "query_results", [])
        if query_results and len(query_results) > 1:
            console.print("\n[bold]Query Results:[/bold]")
            perf_table = Table()
            perf_table.add_column("Query", style="cyan")
            perf_table.add_column("Time (ms)", style="yellow", justify="right")
            perf_table.add_column("Status", style="blue")

            for qr in query_results:
                query_id = getattr(qr, "query_id", "Unknown Query")
                execution_time_ms = getattr(qr, "execution_time_ms", 0.0)
                status = getattr(qr, "status", "Unknown Status")

                perf_table.add_row(
                    str(query_id),
                    f"{execution_time_ms:.2f}",
                    "✅" if status == "SUCCESS" else "❌",
                )

            console.print(perf_table)

    def stop_execution(self):
        """Request execution to stop gracefully."""
        self._stop_requested = True

    def _update_cached_benchmark_instance(self) -> None:
        """Refresh cached benchmark instance from the execution engine."""

        getter = getattr(self.engine, "get_benchmark_instance", None)
        if callable(getter):
            self._benchmark_instance = getter()
        elif hasattr(self.engine, "last_context") and self.engine.last_context is not None:
            self._benchmark_instance = getattr(self.engine.last_context, "benchmark_instance", None)

    def _build_minimal_result(
        self,
        *,
        benchmark_config,
        database_config,
        system_profile,
        validation_status: str,
        validation_details: Optional[dict[str, Any]] = None,
        duration_seconds: float = 0.0,
        execution_metadata: Optional[dict[str, Any]] = None,
    ) -> BenchmarkResults:
        """Create a minimal BenchmarkResults instance for CLI error paths."""

        self._update_cached_benchmark_instance()

        platform_name = getattr(database_config, "type", None) or "unknown"
        metadata: dict[str, Any] = {
            "result_type": "minimal",
            "status": validation_status,
            "source": "cli",
            "benchmark_id": getattr(benchmark_config, "name", "unknown"),
        }
        if execution_metadata:
            metadata.update(execution_metadata)

        benchmark_instance = self._benchmark_instance
        if benchmark_instance is not None and hasattr(benchmark_instance, "create_minimal_benchmark_result"):
            result = benchmark_instance.create_minimal_benchmark_result(
                validation_status=validation_status,
                validation_details=validation_details,
                duration_seconds=duration_seconds,
                platform=platform_name,
                system_profile=system_profile,
                execution_metadata=metadata,
            )
            result._benchmark_id_override = metadata["benchmark_id"]
            return result

        benchmark_name = getattr(benchmark_config, "display_name", benchmark_config.name)
        fallback = BenchmarkResults(
            benchmark_name=benchmark_name,
            platform=platform_name,
            scale_factor=getattr(benchmark_config, "scale_factor", 1.0),
            execution_id=uuid.uuid4().hex[:8],
            timestamp=datetime.now(),
            duration_seconds=duration_seconds,
            total_queries=0,
            successful_queries=0,
            failed_queries=0,
            validation_status=validation_status,
            validation_details=validation_details or {},
            execution_metadata=metadata,
            system_profile=system_profile or {},
        )

        fallback.resource_utilization = {}
        fallback._benchmark_id_override = metadata["benchmark_id"]
        return fallback
