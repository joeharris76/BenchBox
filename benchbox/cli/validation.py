"""
CLI presentation layer for BenchBox validation.

This module provides rich console output and user-friendly display of validation
results from the core validation engines. It acts as a presentation layer that
formats and displays validation results without containing validation logic.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from benchbox.core.validation import (
    PlatformValidationResult,
    ValidationResult,
    ValidationService,
    ValidationSummary,
)
from benchbox.utils.printing import quiet_console

logger = logging.getLogger(__name__)


class ValidationDisplay:
    """Rich console display for validation results."""

    def __init__(self, console: Optional[Console] = None):
        """Initialize the validation display.

        Args:
            console: Rich console instance (creates new one if not provided)
        """
        self.console = console or quiet_console

    def display_validation_result(self, result: ValidationResult, title: str = "Validation Result") -> None:
        """Display a single validation result with rich formatting.

        Args:
            result: ValidationResult to display
            title: Title for the display panel
        """
        if not result:
            self.console.print(f"[yellow]Warning: {title} - No validation result available[/yellow]")
            return

        # Status icon and color
        if result.is_valid:
            status_icon = "✅"
            status_color = "green"
            panel_color = "green"
        else:
            status_icon = "❌"
            status_color = "red"
            panel_color = "red"

        # Main content
        content = Text()
        content.append(f"{status_icon} ", style=status_color)
        content.append("PASSED" if result.is_valid else "FAILED", style=f"bold {status_color}")

        # Include errors if present
        if result.errors:
            content.append("\n\nErrors:\n", style="bold red")
            for error in result.errors:
                content.append(f"  • {error}\n", style="red")

        # Include warnings if present
        if result.warnings:
            content.append("\nWarnings:\n", style="bold yellow")
            for warning in result.warnings:
                content.append(f"  • {warning}\n", style="yellow")

        # Include details if available
        if result.details:
            content.append("\nDetails:\n", style="bold blue")
            for key, value in result.details.items():
                if isinstance(value, (dict, list)):
                    content.append(f"  {key}: {str(value)}\n", style="dim blue")
                else:
                    content.append(f"  {key}: {value}\n", style="blue")

        # Display in a panel
        panel = Panel(content, title=title, border_style=panel_color, padding=(1, 2))
        self.console.print(panel)

    def display_validation_summary(self, summary: ValidationSummary, title: str = "Validation Summary") -> None:
        """Display a validation summary with rich formatting.

        Args:
            summary: ValidationSummary to display
            title: Title for the display
        """
        # Summary table
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Category", style="cyan", width=20)
        table.add_column("Count", justify="right", style="magenta", width=10)
        table.add_column("Status", justify="center", width=10)

        # Include summary rows
        table.add_row("Total Validations", str(summary.total_validations), "")
        table.add_row("Passed", str(summary.passed_validations), "[green]✅[/green]")
        table.add_row(
            "Failed",
            str(summary.failed_validations),
            "[red]❌[/red]" if summary.failed_validations > 0 else "[green]✅[/green]",
        )
        table.add_row(
            "Warnings",
            str(summary.warnings_count),
            "[yellow]⚠️[/yellow]" if summary.warnings_count > 0 else "[green]✅[/green]",
        )

        # Overall status
        overall_status = "PASSED" if summary.failed_validations == 0 else "FAILED"
        overall_color = "green" if summary.failed_validations == 0 else "red"

        # Display summary
        self.console.print(
            Panel(
                table,
                title=f"{title} - [bold {overall_color}]{overall_status}[/bold {overall_color}]",
                border_style=overall_color,
                padding=(1, 2),
            )
        )

    def display_preflight_validation(self, result: ValidationResult) -> None:
        """Display preflight validation results."""
        self.display_validation_result(result, "Preflight Validation")

    def display_data_validation(self, result: ValidationResult) -> None:
        """Display data file validation results."""
        self.display_validation_result(result, "Data File Validation")

    def display_database_validation(self, result: ValidationResult) -> None:
        """Display database state validation results."""
        self.display_validation_result(result, "Database State Validation")

    def display_platform_validation(self, result: ValidationResult) -> None:
        """Display platform capability validation results."""
        self.display_validation_result(result, "Platform Capabilities Validation")


class CLIValidationRunner:
    """CLI-specific validation runner that coordinates validation display."""

    def __init__(
        self,
        console: Optional[Console] = None,
        show_details: bool = True,
        service: Optional[ValidationService] = None,
    ):
        """Initialize the CLI validation runner.

        Args:
            console: Rich console instance
            show_details: Whether to show detailed validation results
            service: Optional validation service (useful for testing)
        """
        self.console = console or quiet_console
        self.display = ValidationDisplay(self.console)
        self.show_details = show_details
        self.service = service or ValidationService()

    def run_preflight_validation(
        self,
        benchmark_type: str,
        scale_factor: float,
        output_dir: Path,
        quiet: bool = False,
    ) -> ValidationResult:
        """Run preflight validation with CLI display.

        Args:
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')
            scale_factor: Scale factor for the benchmark
            output_dir: Directory where data will be generated
            quiet: Whether to suppress display output

        Returns:
            ValidationResult with preflight validation status
        """
        if not quiet:
            self.console.print("[blue]Running preflight validation...[/blue]")

        result = self.service.run_preflight(benchmark_type, scale_factor, output_dir)

        if not quiet and self.show_details:
            self.display.display_preflight_validation(result)

        return result

    def run_data_validation(self, manifest_path: Path, quiet: bool = False) -> ValidationResult:
        """Run data file validation with CLI display.

        Args:
            manifest_path: Path to the data generation manifest
            quiet: Whether to suppress display output

        Returns:
            ValidationResult with data validation status
        """
        if not quiet:
            self.console.print("[blue]Validating generated data files...[/blue]")

        result = self.service.run_manifest(manifest_path)

        if not quiet and self.show_details:
            self.display.display_data_validation(result)

        return result

    def run_database_validation(
        self,
        connection: Any,
        benchmark_type: str,
        scale_factor: float,
        quiet: bool = False,
    ) -> ValidationResult:
        """Run database state validation with CLI display.

        Args:
            connection: Database connection object
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')
            scale_factor: Scale factor for the benchmark
            quiet: Whether to suppress display output

        Returns:
            ValidationResult with database validation status
        """
        if not quiet:
            self.console.print("[blue]Validating database state...[/blue]")

        result = self.service.run_database(connection, benchmark_type, scale_factor)

        if not quiet and self.show_details:
            self.display.display_database_validation(result)

        return result

    def run_platform_validation(
        self,
        platform_adapter: Any,
        benchmark_type: str,
        connection: Optional[Any] = None,
        quiet: bool = False,
    ) -> ValidationResult:
        """Run platform-specific validation with CLI display.

        Args:
            platform_adapter: Platform adapter instance
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')
            connection: Optional database connection for health checks
            quiet: Whether to suppress display output

        Returns:
            ValidationResult with platform validation status
        """
        if not quiet:
            self.console.print("[blue]Validating platform capabilities...[/blue]")

        platform_results: PlatformValidationResult = self.service.run_platform(
            platform_adapter,
            benchmark_type,
            connection=connection,
        )

        if not quiet and self.show_details:
            self.display.display_platform_validation(platform_results.capabilities)
            if platform_results.connection_health:
                self.display.display_validation_result(platform_results.connection_health, "Connection Health Check")

        return platform_results.capabilities

    def run_comprehensive_validation(
        self,
        benchmark_type: str,
        scale_factor: float,
        output_dir: Path,
        manifest_path: Optional[Path] = None,
        connection: Optional[Any] = None,
        platform_adapter: Optional[Any] = None,
        quiet: bool = False,
    ) -> list[ValidationResult]:
        """Run comprehensive validation with CLI display and summary.

        Args:
            benchmark_type: Type of benchmark (e.g., 'tpcds', 'tpch')
            scale_factor: Scale factor for the benchmark
            output_dir: Directory where data will be generated
            manifest_path: Optional path to data generation manifest
            connection: Optional database connection
            platform_adapter: Optional platform adapter instance
            quiet: Whether to suppress display output

        Returns:
            List of ValidationResult objects from all validations performed
        """
        if not quiet:
            self.console.print("\n[bold blue]Running Comprehensive Validation[/bold blue]\n")

        results = self.service.run_comprehensive(
            benchmark_type=benchmark_type,
            scale_factor=scale_factor,
            output_dir=output_dir,
            manifest_path=manifest_path,
            connection=connection,
            platform_adapter=platform_adapter,
        )

        if not quiet and self.show_details:
            self.console.print()
            summary = self.service.summarize(results)
            self.display.display_validation_summary(summary, "Comprehensive Validation Summary")

        return results
