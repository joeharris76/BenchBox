"""Dry run display functionality for BenchBox CLI.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from benchbox.core.config import BenchmarkConfig, DatabaseConfig, DryRunResult

# Import DryRunExecutor from core module
from benchbox.core.dryrun import DryRunExecutor as CoreDryRunExecutor
from benchbox.utils.printing import quiet_console

if TYPE_CHECKING:  # pragma: no cover - typing only
    from rich.console import Console

# Module-level console for testing compatibility
# Cast to Console for type checking since QuietConsoleProxy forwards all calls to Console
console: Any = quiet_console


def generate_cli_command(
    platform: str,
    benchmark: str,
    scale: float,
    phases: list[str] | None = None,
    queries: list[str] | None = None,
    tuning: str | None = None,
    seed: int | None = None,
    output: str | None = None,
    convert_format: str | None = None,
    compression: str | None = None,
    mode: str | None = None,
    force: str | None = None,
    official: bool = False,
    capture_plans: bool = False,
    validation: str | None = None,
    verbose: int = 0,
) -> str:
    """Generate equivalent CLI command from interactive wizard configuration.

    Args:
        platform: Platform name (duckdb, snowflake, etc.)
        benchmark: Benchmark name (tpch, tpcds, etc.)
        scale: Scale factor
        phases: List of phases to run
        queries: Query subset (e.g., ["Q1", "Q6"])
        tuning: Tuning mode (tuned, notuning, auto, or YAML path)
        seed: RNG seed for reproducibility
        output: Output directory
        convert_format: Format conversion (parquet, delta, iceberg)
        compression: Compression config (zstd:9, gzip:6, etc.)
        mode: Execution mode (sql, dataframe)
        force: Force mode (all, datagen, upload)
        official: TPC-compliant mode
        capture_plans: Capture query execution plans
        validation: Validation mode (exact, loose, range, disabled, full)
        verbose: Verbosity level (0=off, 1=-v, 2=-vv)

    Returns:
        Complete CLI command string
    """
    parts = ["benchbox run"]

    parts.append(f"--platform {platform}")
    parts.append(f"--benchmark {benchmark}")

    if scale != 0.01:  # Only include if not default
        parts.append(f"--scale {scale}")

    if phases and phases != ["power"]:
        parts.append(f"--phases {','.join(phases)}")

    if queries:
        parts.append(f"--queries {','.join(queries)}")

    if tuning and tuning != "notuning":
        parts.append(f"--tuning {tuning}")

    if seed is not None:
        parts.append(f"--seed {seed}")

    if output:
        parts.append(f"--output {output}")

    if convert_format:
        parts.append(f"--convert {convert_format}")

    if compression:
        parts.append(f"--compression {compression}")

    if mode:
        parts.append(f"--mode {mode}")

    if force:
        parts.append(f"--force {force}")

    if official:
        parts.append("--official")

    if capture_plans:
        parts.append("--capture-plans")

    if validation:
        parts.append(f"--validation {validation}")

    if verbose == 1:
        parts.append("-v")
    elif verbose >= 2:
        parts.append("-vv")

    return " \\\n    ".join(parts)


def display_interactive_preview(
    database_config: DatabaseConfig,
    benchmark_config: BenchmarkConfig,
    phases: list[str],
    output: str | None = None,
    tuning: str | None = None,
    seed: int | None = None,
    force: str | None = None,
    official: bool = False,
    capture_plans: bool = False,
    validation: str | None = None,
    verbose: int = 0,
    console_obj: Console | None = None,
) -> None:
    """Display a preview summary for interactive wizard users.

    Shows configuration summary, resource estimates, and equivalent CLI command
    before proceeding with benchmark execution.

    Args:
        database_config: Database configuration from wizard
        benchmark_config: Benchmark configuration from wizard
        phases: Phases to execute
        output: Output directory (if specified)
        tuning: Tuning mode
        seed: RNG seed
        force: Force mode (all, datagen, upload)
        official: TPC-compliant mode
        capture_plans: Capture query execution plans
        validation: Validation mode
        verbose: Verbosity level (0=off, 1=-v, 2=-vv)
        console_obj: Rich console for output
    """
    display_console = console_obj or console

    display_console.print()
    display_console.print(
        Panel.fit(
            Text("Configuration Preview", style="bold cyan"),
            style="cyan",
        )
    )

    # Configuration summary table
    table = Table(show_header=False, box=None, padding=(0, 2))
    table.add_column("Setting", style="cyan", min_width=18)
    table.add_column("Value", style="white")

    # Platform info
    platform_display = database_config.type.upper()
    if hasattr(database_config, "execution_mode") and database_config.execution_mode:
        platform_display += f" ({database_config.execution_mode} mode)"
    table.add_row("Platform:", platform_display)

    # Benchmark info
    table.add_row("Benchmark:", f"{benchmark_config.display_name} at scale {benchmark_config.scale_factor}")

    # Phases
    table.add_row("Phases:", ", ".join(phases))

    # Queries
    if benchmark_config.queries:
        table.add_row("Queries:", f"{len(benchmark_config.queries)} selected")
    else:
        num_queries = getattr(benchmark_config, "options", {}).get("num_queries", "all")
        table.add_row("Queries:", str(num_queries) if num_queries != "all" else "All")

    # Tuning
    if tuning:
        table.add_row("Tuning:", tuning)

    # Seed
    if seed is not None:
        table.add_row("Seed:", str(seed))

    # Output
    if output:
        table.add_row("Output:", output)

    # Compression
    if benchmark_config.compress_data:
        comp_str = benchmark_config.compression_type or "zstd"
        if benchmark_config.compression_level:
            comp_str += f":{benchmark_config.compression_level}"
        table.add_row("Compression:", comp_str)

    # Estimated resources from benchmark options
    options = getattr(benchmark_config, "options", {})
    time_range = options.get("estimated_time_range")
    if time_range:
        table.add_row("Est. Time:", f"{time_range[0]}-{time_range[1]} minutes")

    display_console.print(table)

    # Generate and show CLI command
    display_console.print()
    display_console.print("[bold]Equivalent CLI command:[/bold]")

    # Extract convert_format from options if present
    convert_format = options.get("convert_format")
    compression_str = None
    if benchmark_config.compress_data and benchmark_config.compression_type:
        compression_str = benchmark_config.compression_type
        if benchmark_config.compression_level:
            compression_str += f":{benchmark_config.compression_level}"

    cli_cmd = generate_cli_command(
        platform=database_config.type,
        benchmark=benchmark_config.name,
        scale=benchmark_config.scale_factor,
        phases=phases if phases != ["power"] else None,
        queries=benchmark_config.queries,
        tuning=tuning,
        seed=seed,
        output=output,
        convert_format=convert_format,
        compression=compression_str if compression_str != "zstd" else None,
        mode=getattr(database_config, "execution_mode", None),
        force=force,
        official=official,
        capture_plans=capture_plans,
        validation=validation,
        verbose=verbose,
    )

    display_console.print(f"[dim]{cli_cmd}[/dim]")
    display_console.print()


class DryRunDisplay:
    """Handles display of dry run results."""

    def __init__(self, console: Console | None = None):
        self.console = console or quiet_console

    def display_dry_run_results(self, result: DryRunResult):
        """Display dry run results to console."""
        self.console.print(
            Panel.fit(
                Text("DRY RUN MODE - No queries will be executed", style="bold yellow"),
                style="yellow",
            )
        )

        self._display_configuration_summary(result)

        self._display_query_preview(result.queries, result)

        # Display schema based on execution mode
        if result.execution_mode == "dataframe" and getattr(result, "dataframe_schema", None):
            self._display_schema_preview(result.dataframe_schema, syntax_lang="python", title="DataFrame Schema")
        elif result.schema_sql:
            self._display_schema_preview(result.schema_sql, syntax_lang="sql", title="Database Schema")

        if result.tuning_config:
            self._display_tuning_config(result.tuning_config)

        # Display DDL preview with tuning clauses
        if result.ddl_preview:
            self._display_ddl_preview(result.ddl_preview)

        # Display post-load statements
        if result.post_load_statements:
            self._display_post_load_statements(result.post_load_statements)

        if result.estimated_resources:
            self._display_resource_estimates(result.estimated_resources)

        if result.warnings:
            self._display_warnings(result.warnings)

    def _display_configuration_summary(self, result: DryRunResult):
        self.console.print("\n[bold]Configuration Summary[/bold]")

        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Category", style="cyan")
        table.add_column("Setting", style="white")
        table.add_column("Value", style="yellow")

        benchmark_config = result.benchmark_config
        table.add_row("Benchmark", "Name", str(benchmark_config.get("name", "N/A")))
        table.add_row("", "Scale Factor", str(benchmark_config.get("scale_factor", "N/A")))
        table.add_row("", "Concurrency", str(benchmark_config.get("concurrency", 1)))

        database_config = result.database_config
        table.add_row("Database", "Type", str(database_config.get("type", "N/A")))
        table.add_row("", "Name", str(database_config.get("name", "N/A")))
        table.add_row("", "Execution Mode", result.execution_mode.upper())

        system_config = result.system_profile
        table.add_row("System", "CPU Cores", str(system_config.get("cpu_cores_logical", "N/A")))
        table.add_row("", "Memory (GB)", str(system_config.get("memory_total_gb", "N/A")))
        table.add_row("", "Platform", str(system_config.get("os_name", "N/A")))

        if result.query_preview:
            test_execution_type = result.query_preview.get("test_execution_type", "standard")
            execution_context = result.query_preview.get("execution_context", "Sequential execution")
        else:
            test_execution_type = "standard"
            execution_context = "Sequential execution"
        table.add_row(
            "Test Execution",
            "Type",
            self._format_test_execution_type(test_execution_type),
        )
        table.add_row("", "Context", execution_context)

        constraint_config = result.constraint_config or {}
        table.add_row(
            "Constraints",
            "Primary Keys",
            str(constraint_config.get("enable_primary_keys", "N/A")),
        )
        table.add_row(
            "",
            "Foreign Keys",
            str(constraint_config.get("enable_foreign_keys", "N/A")),
        )

        self.console.print(table)

    def _display_query_preview(self, queries: dict[str, str], result: DryRunResult | None = None):
        if not queries:
            self.console.print("\n[yellow]No queries available for preview[/yellow]")
            return

        test_execution_type = "standard"
        execution_context = "Sequential execution"
        execution_mode = "sql"
        if result and result.query_preview:
            test_execution_type = result.query_preview.get("test_execution_type", "standard")
            execution_context = result.query_preview.get("execution_context", "Sequential execution")
        if result:
            execution_mode = getattr(result, "execution_mode", "sql")

        # Determine syntax highlighting language based on execution mode
        syntax_lang = "python" if execution_mode == "dataframe" else "sql"

        preview_title = self._get_preview_title(test_execution_type, len(queries))
        if execution_mode == "dataframe":
            preview_title = "DataFrame Query Preview"
        self.console.print(f"\n[bold]{preview_title}[/bold] ([dim]{execution_context}[/dim])")

        # Filter out error entries for DataFrame mode
        display_queries = [(k, v) for k, v in queries.items() if not k.startswith("_")]

        for _i, (query_id, query_content) in enumerate(display_queries[:3]):
            display_title = self._format_query_display_title(query_id, test_execution_type)
            if execution_mode == "dataframe":
                display_title = f"DataFrame Query {query_id}"
            self.console.print(f"\n[cyan]{display_title}:[/cyan]")

            display_content = query_content
            if len(query_content) > 500:
                display_content = query_content[:500] + "\n... [truncated]"

            syntax = Syntax(display_content, syntax_lang, theme="monokai", line_numbers=False)
            panel_title = self._format_panel_title(query_id, test_execution_type)
            if execution_mode == "dataframe":
                panel_title = f"Query {query_id} (Python)"
            self.console.print(Panel(syntax, title=panel_title, border_style="blue"))

        if len(display_queries) > 3:
            remaining = len(display_queries) - 3
            remaining_type = "operations" if test_execution_type == "maintenance" else "queries"
            self.console.print(f"\n[dim]... and {remaining} more {remaining_type}[/dim]")

    def _display_schema_preview(self, schema_content: str, syntax_lang: str = "sql", title: str = "Database Schema"):
        self.console.print("\n[bold]Schema Preview[/bold]")

        display_content = schema_content
        if len(schema_content) > 1000:
            display_content = schema_content[:1000] + "\n... [truncated]"

        syntax = Syntax(display_content, syntax_lang, theme="monokai", line_numbers=False)
        self.console.print(Panel(syntax, title=title, border_style="green"))

    def _display_tuning_config(self, tuning_config: dict[str, Any]):
        self.console.print("\n[bold]Tuning Configuration[/bold]")

        if not tuning_config:
            self.console.print("[dim]No tuning configuration available[/dim]")
            return

        if tuning_config.get("constraints"):
            constraints_table = Table(show_header=True, header_style="bold blue")
            constraints_table.add_column("Constraint Type", style="cyan")
            constraints_table.add_column("Enabled", style="white")
            constraints_table.add_column("Configuration", style="yellow")

            constraints = tuning_config["constraints"]
            if constraints.get("primary_keys"):
                pk_config = constraints["primary_keys"]
                config_str = f"Uniqueness: {pk_config.get('enforce_uniqueness', 'N/A')}, Nullable: {pk_config.get('nullable', 'N/A')}"
                constraints_table.add_row("Primary Keys", str(pk_config.get("enabled", False)), config_str)

            if constraints.get("foreign_keys"):
                fk_config = constraints["foreign_keys"]
                config_str = f"Referential Integrity: {fk_config.get('enforce_referential_integrity', 'N/A')}"
                constraints_table.add_row("Foreign Keys", str(fk_config.get("enabled", False)), config_str)

            self.console.print(constraints_table)

        table_tunings = tuning_config.get("table_tunings")
        if table_tunings:
            self.console.print("\n[bold]Table Organization Tunings[/bold]")

            tuning_table = Table(show_header=True, header_style="bold blue")
            tuning_table.add_column("Table", style="cyan")
            tuning_table.add_column("Tuning Type", style="white")
            tuning_table.add_column("Columns", style="yellow")

            for table_name, table_config in table_tunings.items():
                first_row = True
                for tuning_type in [
                    "partitioning",
                    "sorting",
                    "clustering",
                    "distribution",
                ]:
                    columns = table_config.get(tuning_type)
                    if columns:
                        column_names = [f"{col['name']} ({col['type']})" for col in columns]
                        column_str = ", ".join(column_names)

                        if first_row:
                            tuning_table.add_row(table_name, tuning_type.title(), column_str)
                            first_row = False
                        else:
                            tuning_table.add_row("", tuning_type.title(), column_str)

            self.console.print(tuning_table)

        platform_opts = tuning_config.get("platform_optimizations")
        if platform_opts and any(platform_opts.values()):
            self.console.print("\n[bold]Platform Optimizations[/bold]")

            platform_table = Table(show_header=True, header_style="bold blue")
            platform_table.add_column("Optimization", style="cyan")
            platform_table.add_column("Enabled", style="white")

            for opt_name, opt_value in platform_opts.items():
                if opt_value:
                    platform_table.add_row(opt_name.replace("_", " ").title(), str(opt_value))

            if platform_table.row_count > 0:
                self.console.print(platform_table)

        # Display DataFrame tuning configuration
        df_tuning = tuning_config.get("dataframe_tuning")
        if df_tuning:
            self.console.print("\n[bold]DataFrame Tuning Configuration[/bold]")

            df_table = Table(show_header=True, header_style="bold blue")
            df_table.add_column("Category", style="cyan")
            df_table.add_column("Setting", style="white")
            df_table.add_column("Value", style="yellow")

            # Runtime settings
            if df_tuning.get("parallelism"):
                parallelism = df_tuning["parallelism"]
                first_row = True
                for key, value in parallelism.items():
                    category = "Parallelism" if first_row else ""
                    df_table.add_row(category, key.replace("_", " ").title(), str(value))
                    first_row = False

            if df_tuning.get("memory"):
                memory = df_tuning["memory"]
                first_row = True
                for key, value in memory.items():
                    category = "Memory" if first_row else ""
                    df_table.add_row(category, key.replace("_", " ").title(), str(value))
                    first_row = False

            if df_tuning.get("execution"):
                execution = df_tuning["execution"]
                first_row = True
                for key, value in execution.items():
                    category = "Execution" if first_row else ""
                    df_table.add_row(category, key.replace("_", " ").title(), str(value))
                    first_row = False

            # Write-time physical layout settings
            if df_tuning.get("write"):
                write = df_tuning["write"]
                first_row = True

                if write.get("sort_by"):
                    sort_cols = [f"{col['name']} ({col['order']})" for col in write["sort_by"]]
                    df_table.add_row("Write Layout", "Sort By", ", ".join(sort_cols))
                    first_row = False

                if write.get("partition_by"):
                    part_cols = [f"{col['name']} ({col['strategy']})" for col in write["partition_by"]]
                    category = "Write Layout" if first_row else ""
                    df_table.add_row(category, "Partition By", ", ".join(part_cols))
                    first_row = False

                if write.get("row_group_size"):
                    category = "Write Layout" if first_row else ""
                    df_table.add_row(category, "Row Group Size", f"{write['row_group_size']:,}")
                    first_row = False

                if write.get("repartition_count"):
                    category = "Write Layout" if first_row else ""
                    df_table.add_row(category, "Repartition Count", str(write["repartition_count"]))
                    first_row = False

                if write.get("compression"):
                    category = "Write Layout" if first_row else ""
                    comp_str = write["compression"]
                    if write.get("compression_level"):
                        comp_str += f":{write['compression_level']}"
                    df_table.add_row(category, "Compression", comp_str)
                    first_row = False

                if write.get("dictionary_columns"):
                    category = "Write Layout" if first_row else ""
                    df_table.add_row(category, "Dictionary Columns", ", ".join(write["dictionary_columns"]))

            if df_table.row_count > 0:
                self.console.print(df_table)

        if not tuning_config.get("table_tunings") and not tuning_config.get("constraints") and not df_tuning:
            self.console.print("[dim]No detailed tuning configuration available[/dim]")

    def _display_ddl_preview(self, ddl_preview: dict[str, dict[str, Any]]):
        """Display DDL preview with tuning clauses per table."""
        if not ddl_preview:
            return

        self.console.print("\n[bold]DDL Preview (Tuning Clauses)[/bold]")

        for table_name, table_info in ddl_preview.items():
            self.console.print(f"\n[cyan]Table: {table_name}[/cyan]")

            # Display tuning summary
            tuning_summary = table_info.get("tuning_summary", {})
            if tuning_summary:
                summary_parts = []
                if tuning_summary.get("sort_by"):
                    summary_parts.append(f"Sort: {tuning_summary['sort_by']}")
                if tuning_summary.get("partition_by"):
                    summary_parts.append(f"Partition: {tuning_summary['partition_by']}")
                if tuning_summary.get("cluster_by"):
                    summary_parts.append(f"Cluster: {tuning_summary['cluster_by']}")
                if tuning_summary.get("distribution_style"):
                    summary_parts.append(f"Dist: {tuning_summary['distribution_style']}")
                if tuning_summary.get("distribution_key"):
                    summary_parts.append(f"DistKey: {tuning_summary['distribution_key']}")

                self.console.print(f"  [dim]Tuning: {' | '.join(summary_parts)}[/dim]")

            # Display DDL clauses with syntax highlighting
            ddl_clauses = table_info.get("ddl_clauses")
            if ddl_clauses:
                syntax = Syntax(ddl_clauses, "sql", theme="monokai", line_numbers=False)
                self.console.print(Panel(syntax, border_style="green", padding=(0, 1)))

    def _display_post_load_statements(self, post_load_statements: dict[str, list[str]]):
        """Display post-load statements (VACUUM, ANALYZE, OPTIMIZE, etc.)."""
        if not post_load_statements:
            return

        self.console.print("\n[bold]Post-Load Operations[/bold]")

        all_statements = []
        for table_name, statements in post_load_statements.items():
            for stmt in statements:
                all_statements.append(f"-- {table_name}")
                all_statements.append(stmt)
                all_statements.append("")

        if all_statements:
            sql_content = "\n".join(all_statements)
            syntax = Syntax(sql_content, "sql", theme="monokai", line_numbers=False)
            self.console.print(Panel(syntax, title="Post-Load SQL", border_style="yellow"))

    def _display_resource_estimates(self, estimates: dict[str, Any]):
        self.console.print("\n[bold]Resource Estimates[/bold]")

        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Resource", style="cyan")
        table.add_column("Estimated", style="yellow")
        table.add_column("Available", style="green")

        data_size = estimates.get("estimated_data_size_mb", 0)
        table.add_row("Data Size", f"{data_size:.1f} MB", "N/A")

        memory_usage = estimates.get("estimated_memory_usage_mb", 0)
        memory_available = estimates.get("memory_gb_available", 0) * 1024
        table.add_row("Memory Usage", f"{memory_usage:.0f} MB", f"{memory_available:.0f} MB")

        runtime = estimates.get("estimated_runtime_minutes", 0)
        table.add_row("Runtime", f"{runtime:.1f} minutes", "N/A")

        cpu_cores = estimates.get("cpu_cores_available", 1)
        table.add_row("CPU Cores", "All available", f"{cpu_cores}")

        self.console.print(table)

        if memory_usage > memory_available * 0.9:
            self.console.print("[yellow]⚠️ Warning: Estimated memory usage is close to available memory[/yellow]")

    def _display_warnings(self, warnings: list[str]):
        if not warnings:
            return

        self.console.print(f"\n[bold yellow]Warnings ({len(warnings)})[/bold yellow]")
        for warning in warnings:
            self.console.print(f"[yellow]⚠️[/yellow] {warning}")

    def _get_execution_context(self, benchmark_config: BenchmarkConfig, query_count: int) -> str:
        test_execution_type = getattr(benchmark_config, "test_execution_type", "standard")
        benchmark_name = getattr(benchmark_config, "name", "").lower()

        if test_execution_type == "power":
            if benchmark_name == "tpcds":
                return "TPC-DS PowerTest stream permutation (99 queries in randomized order)"
            elif benchmark_name == "tpch":
                return "TPC-H PowerTest stream 0 permutation (22 queries in a specific, randomized order)"
            else:
                return "Power test execution (stream permutation)"
        elif test_execution_type == "throughput":
            if benchmark_name == "tpcds":
                return f"TPC-DS ThroughputTest (4 concurrent streams, {query_count} queries total)"
            else:
                return "Throughput test execution (concurrent streams)"
        elif test_execution_type == "maintenance":
            if benchmark_name == "tpcds":
                return "TPC-DS MaintenanceTest (data operations: INSERT/UPDATE/DELETE)"
            else:
                return "Maintenance test execution (data operations)"
        else:
            return f"Standard sequential execution ({query_count} queries)"

    def _format_test_execution_type(self, test_execution_type: str) -> str:
        type_formats = {
            "standard": "Standard (Sequential)",
            "power": "PowerTest (Stream Permutation)",
            "throughput": "ThroughputTest (Concurrent Streams)",
            "maintenance": "MaintenanceTest (Data Operations)",
            "combined": "Combined Test (All Phases)",
            "load_only": "Load Only (Data Generation)",
            "data_only": "Data Only (No Database)",
        }
        return type_formats.get(test_execution_type, f"{test_execution_type.title()} Test")

    def _get_preview_title(self, test_execution_type: str, query_count: int) -> str:
        if test_execution_type == "power":
            return "PowerTest Stream Execution Preview"
        elif test_execution_type == "throughput":
            return "ThroughputTest Concurrent Stream Preview"
        elif test_execution_type == "maintenance":
            return "MaintenanceTest Operations Preview"
        else:
            return "Query Preview"

    def _format_query_display_title(self, query_id: str, test_execution_type: str) -> str:
        query_id_str = str(query_id)
        if test_execution_type == "maintenance":
            return f"Operation {query_id_str}"
        elif "Stream_" in query_id_str:
            return query_id_str.replace("_", " ")
        elif "Position_" in query_id_str:
            return query_id_str.replace("_", " ").replace("Position", "Stream Position")
        else:
            return f"Query {query_id_str}"

    def _format_panel_title(self, query_id: str, test_execution_type: str) -> str:
        query_id_str = str(query_id)
        if test_execution_type == "maintenance":
            return f"Maintenance Operation: {query_id_str}"
        elif "Stream_" in query_id_str:
            parts = query_id_str.split("_")
            if len(parts) >= 6:
                stream = parts[1]
                position = parts[3]
                query_num = parts[5]
                return f"Stream {stream} Position {position}: Query {query_num}"
        elif "Position_" in query_id_str:
            parts = query_id_str.split("_")
            if len(parts) >= 4:
                position = parts[1]
                query_num = parts[3]
                return f"Stream Position {position}: Query {query_num}"

        return f"Query {query_id_str}"


class DryRunExecutor(CoreDryRunExecutor):
    """Extended DryRunExecutor with CLI-specific display functionality."""

    def __init__(self, output_dir=None):
        super().__init__(output_dir)
        self.console = quiet_console
        self.display = DryRunDisplay(self.console)

    def display_dry_run_results(self, result: DryRunResult):
        """Display dry run results using the display component."""
        self.display.display_dry_run_results(result)

    def _format_test_execution_type(self, test_execution_type: str) -> str:
        """Format test execution type for display."""
        type_formats = {
            "standard": "Standard (Sequential)",
            "power": "PowerTest (Stream Permutation)",
            "throughput": "ThroughputTest (Concurrent Streams)",
            "maintenance": "MaintenanceTest (Data Operations)",
            "combined": "Combined Test (All Phases)",
            "load_only": "Load Only (Data Generation)",
            "data_only": "Data Only (No Database)",
        }
        return type_formats.get(test_execution_type, f"{test_execution_type.title()} Test")

    def _get_preview_title(self, test_execution_type: str, query_count: int) -> str:
        """Get the preview title for different test types."""
        if test_execution_type == "power":
            return "PowerTest Stream Execution Preview"
        elif test_execution_type == "throughput":
            return "ThroughputTest Concurrent Stream Preview"
        elif test_execution_type == "maintenance":
            return "MaintenanceTest Operations Preview"
        else:
            return "Query Preview"

    def _format_query_display_title(self, query_id: str, test_execution_type: str) -> str:
        """Format query display title based on test type and query ID."""
        query_id_str = str(query_id)
        if test_execution_type == "maintenance":
            return f"Operation {query_id_str}"
        elif "Stream_" in query_id_str:
            return query_id_str.replace("_", " ")
        elif "Position_" in query_id_str:
            return query_id_str.replace("_", " ").replace("Position", "Stream Position")
        else:
            return f"Query {query_id_str}"

    def _format_panel_title(self, query_id: str, test_execution_type: str) -> str:
        """Format panel title for query display."""
        query_id_str = str(query_id)
        if test_execution_type == "maintenance":
            return f"Maintenance Operation: {query_id_str}"
        elif "Stream_" in query_id_str:
            parts = query_id_str.split("_")
            if len(parts) >= 6:
                stream = parts[1]
                position = parts[3]
                query_num = parts[5]
                return f"Stream {stream} Position {position}: Query {query_num}"
        elif "Position_" in query_id_str:
            parts = query_id_str.split("_")
            if len(parts) >= 4:
                position = parts[1]
                query_num = parts[3]
                return f"Stream Position {position}: Query {query_num}"

        return f"Query {query_id_str}"
