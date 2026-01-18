"""DataFrame tuning configuration command implementation.

DEPRECATED: This module is kept for backwards compatibility.
Use `benchbox tuning` commands instead of `benchbox df-tuning`.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path

import click
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from benchbox.cli.shared import console
from benchbox.core.dataframe.tuning import (
    DataFrameTuningConfiguration,
    detect_system_profile,
    format_issues,
    get_profile_summary,
    get_smart_defaults,
    has_errors,
    load_dataframe_tuning,
    save_dataframe_tuning,
    validate_dataframe_tuning,
)


@click.group("df-tuning", hidden=True)
def df_tuning_group():
    """DataFrame tuning configuration commands.

    DEPRECATED: Use `benchbox tuning` commands instead.

    Manage tuning configurations for DataFrame platforms (Polars, Pandas, Dask, etc.).

    Examples:
        benchbox tuning init --platform polars --mode dataframe
        benchbox tuning validate polars_tuning.yaml --platform polars
        benchbox tuning defaults --platform polars
    """
    console.print("[yellow]Warning: 'df-tuning' is deprecated. Use 'benchbox tuning' commands instead.[/yellow]\n")


@df_tuning_group.command("create-sample")
@click.option(
    "--platform",
    type=click.Choice(["polars", "pandas", "dask", "modin", "cudf"], case_sensitive=False),
    required=True,
    help="Target DataFrame platform",
)
@click.option(
    "--profile",
    type=click.Choice(["default", "optimized", "streaming", "memory-constrained", "gpu"]),
    default="default",
    help="Configuration profile (default: default)",
)
@click.option(
    "--output",
    type=str,
    default=None,
    help="Output file path (default: <platform>_<profile>.yaml)",
)
@click.option(
    "--smart-defaults",
    is_flag=True,
    help="Use smart defaults based on detected system profile",
)
def create_sample(platform, profile, output, smart_defaults):
    """Create a sample DataFrame tuning configuration.

    Generates a YAML configuration file with platform-specific tuning options.

    Examples:
        benchbox df-tuning create-sample --platform polars
        benchbox df-tuning create-sample --platform dask --profile memory-constrained
        benchbox df-tuning create-sample --platform polars --smart-defaults
    """
    console.print(
        Panel.fit(
            Text(
                f"Creating DataFrame Tuning Configuration for {platform.title()}",
                style="bold cyan",
            ),
            style="cyan",
        )
    )

    # Determine output path
    if output is None:
        output = f"{platform.lower()}_{profile.replace('-', '_')}.yaml"
    output_path = Path(output)

    try:
        if smart_defaults:
            # Use smart defaults based on detected system profile
            config = get_smart_defaults(platform)
            console.print("[blue]Using smart defaults based on detected system profile[/blue]")

            # Show detected system profile
            sys_profile = detect_system_profile()
            summary = get_profile_summary(sys_profile)
            console.print(f"  CPU cores: {summary['cpu_cores']}")
            console.print(f"  Available memory: {summary['available_memory_gb']:.1f} GB")
            console.print(f"  Memory category: {summary['memory_category']}")
            if summary["has_gpu"]:
                console.print(f"  GPU memory: {summary['gpu_memory_gb']:.1f} GB")
        else:
            # Create profile-based configuration
            config = DataFrameTuningConfiguration()

            if profile == "optimized":
                # Optimized for performance
                config.execution.lazy_evaluation = True
                if platform.lower() == "polars":
                    config.execution.engine_affinity = "in-memory"
                elif platform.lower() == "dask":
                    config.parallelism.worker_count = 4
                    config.parallelism.threads_per_worker = 2
                elif platform.lower() == "cudf":
                    config.gpu.enabled = True
                    config.gpu.pool_type = "pool"

            elif profile == "streaming":
                # Streaming for large datasets
                config.execution.streaming_mode = True
                config.memory.chunk_size = 100_000
                if platform.lower() == "polars":
                    config.execution.engine_affinity = "streaming"

            elif profile == "memory-constrained":
                # Memory-constrained environment
                config.execution.streaming_mode = True
                config.memory.chunk_size = 50_000
                config.memory.spill_to_disk = True
                if platform.lower() == "dask":
                    config.memory.memory_limit = "2GB"

            elif profile == "gpu":
                # GPU-optimized (cuDF)
                if platform.lower() != "cudf":
                    console.print("[yellow]Warning: GPU profile is only applicable to cuDF[/yellow]")
                config.gpu.enabled = True
                config.gpu.pool_type = "pool"
                config.gpu.spill_to_host = True

        # Save configuration
        save_dataframe_tuning(config, output_path)

        console.print("\n[green]Sample DataFrame tuning configuration created[/green]")
        console.print(f"File: [cyan]{output_path}[/cyan]")
        console.print(f"Platform: [yellow]{platform}[/yellow]")
        console.print(f"Profile: [yellow]{profile}[/yellow]")

        # Show enabled settings
        enabled = config.get_enabled_settings()
        if enabled:
            console.print(f"\nEnabled settings ({len(enabled)}):")
            for setting in sorted(enabled, key=lambda x: x.value):
                console.print(f"  - {setting.value}")
        else:
            console.print("\nNo custom settings enabled (using defaults)")

        console.print("\nEdit this file to customize tuning settings for your benchmarks.")
        console.print(f"Use with: [cyan]benchbox run --tuning {output_path}[/cyan]")

    except Exception as e:
        console.print(f"[red]Failed to create sample configuration: {e}[/red]")
        raise click.Abort() from e


@df_tuning_group.command("validate")
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "--platform",
    type=click.Choice(["polars", "pandas", "dask", "modin", "cudf"], case_sensitive=False),
    required=True,
    help="Target DataFrame platform",
)
def validate(config_file, platform):
    """Validate a DataFrame tuning configuration file.

    Checks the configuration for errors and warnings specific to the target platform.

    Examples:
        benchbox df-tuning validate polars_tuning.yaml --platform polars
        benchbox df-tuning validate my_config.yaml --platform dask
    """
    console.print(
        Panel.fit(
            Text("Validating DataFrame Tuning Configuration", style="bold cyan"),
            style="cyan",
        )
    )

    try:
        # Load configuration
        config = load_dataframe_tuning(config_file)
        console.print(f"Loaded: [cyan]{config_file}[/cyan]")

        # Validate for platform
        issues = validate_dataframe_tuning(config, platform)

        if not issues:
            console.print(f"\n[green]Configuration is valid for {platform}[/green]")
        else:
            console.print(f"\n{format_issues(issues)}")

            if has_errors(issues):
                console.print("\n[red]Configuration has errors that must be fixed[/red]")
                raise click.Abort()
            else:
                console.print("\n[yellow]Configuration is valid but has warnings[/yellow]")

        # Show summary
        console.print("\n[bold]Configuration Summary:[/bold]")
        summary = config.get_summary()
        console.print(f"  Enabled settings: {summary['setting_count']}")
        console.print(f"  Has streaming: {summary['has_streaming']}")
        console.print(f"  Has GPU: {summary['has_gpu']}")

    except Exception as e:
        console.print(f"[red]Failed to validate configuration: {e}[/red]")
        raise click.Abort() from e


@df_tuning_group.command("show-defaults")
@click.option(
    "--platform",
    type=click.Choice(["polars", "pandas", "dask", "modin", "cudf"], case_sensitive=False),
    required=True,
    help="Target DataFrame platform",
)
def show_defaults(platform):
    """Show smart defaults for a DataFrame platform based on system profile.

    Analyzes the current system (CPU, memory, GPU) and shows recommended
    tuning settings for the specified platform.

    Examples:
        benchbox df-tuning show-defaults --platform polars
        benchbox df-tuning show-defaults --platform cudf
    """
    console.print(
        Panel.fit(
            Text(f"Smart Defaults for {platform.title()}", style="bold cyan"),
            style="cyan",
        )
    )

    # Detect system profile
    sys_profile = detect_system_profile()
    summary = get_profile_summary(sys_profile)

    # Show system profile
    console.print("\n[bold]Detected System Profile:[/bold]")
    table = Table(show_header=False, box=None)
    table.add_column("Property", style="cyan")
    table.add_column("Value", style="yellow")

    table.add_row("CPU Cores", str(summary["cpu_cores"]))
    table.add_row("Available Memory", f"{summary['available_memory_gb']:.1f} GB")
    table.add_row("Memory Category", summary["memory_category"])
    if summary["has_gpu"]:
        table.add_row("GPU Available", "Yes")
        table.add_row("GPU Memory", f"{summary['gpu_memory_gb']:.1f} GB")
        table.add_row("GPU Count", str(summary["gpu_device_count"]))
    else:
        table.add_row("GPU Available", "No")

    console.print(table)

    # Get smart defaults
    config = get_smart_defaults(platform)

    # Show recommended settings
    console.print(f"\n[bold]Recommended Settings for {platform.title()}:[/bold]")

    settings_table = Table(show_header=True)
    settings_table.add_column("Category", style="cyan")
    settings_table.add_column("Setting", style="white")
    settings_table.add_column("Value", style="yellow")

    # Parallelism
    if config.parallelism.thread_count is not None:
        settings_table.add_row("Parallelism", "thread_count", str(config.parallelism.thread_count))
    if config.parallelism.worker_count is not None:
        settings_table.add_row("Parallelism", "worker_count", str(config.parallelism.worker_count))
    if config.parallelism.threads_per_worker is not None:
        settings_table.add_row("Parallelism", "threads_per_worker", str(config.parallelism.threads_per_worker))

    # Memory
    if config.memory.memory_limit is not None:
        settings_table.add_row("Memory", "memory_limit", config.memory.memory_limit)
    if config.memory.chunk_size is not None:
        settings_table.add_row("Memory", "chunk_size", str(config.memory.chunk_size))
    if config.memory.spill_to_disk:
        settings_table.add_row("Memory", "spill_to_disk", "True")

    # Execution
    if config.execution.streaming_mode:
        settings_table.add_row("Execution", "streaming_mode", "True")
    if config.execution.engine_affinity is not None:
        settings_table.add_row("Execution", "engine_affinity", config.execution.engine_affinity)

    # Data types
    if config.data_types.dtype_backend != "numpy_nullable":
        settings_table.add_row("Data Types", "dtype_backend", config.data_types.dtype_backend)
    if config.data_types.auto_categorize_strings:
        settings_table.add_row("Data Types", "auto_categorize_strings", "True")

    # I/O
    if config.io.memory_map:
        settings_table.add_row("I/O", "memory_map", "True")

    # GPU
    if config.gpu.enabled:
        settings_table.add_row("GPU", "enabled", "True")
        settings_table.add_row("GPU", "device_id", str(config.gpu.device_id))
        settings_table.add_row("GPU", "pool_type", config.gpu.pool_type)
        if config.gpu.spill_to_host:
            settings_table.add_row("GPU", "spill_to_host", "True")

    if settings_table.row_count == 0:
        console.print("  [dim]Using default settings (no custom configuration needed)[/dim]")
    else:
        console.print(settings_table)

    console.print(f"\n[dim]To use these settings: benchbox run --platform {platform}-df --tuning auto[/dim]")


@df_tuning_group.command("list-platforms")
def list_platforms():
    """List available DataFrame platforms and their tuning capabilities.

    Shows which platforms support DataFrame tuning and their key features.
    """
    console.print(
        Panel.fit(
            Text("DataFrame Platforms", style="bold cyan"),
            style="cyan",
        )
    )

    table = Table(show_header=True)
    table.add_column("Platform", style="cyan")
    table.add_column("Family", style="white")
    table.add_column("Key Features", style="yellow")
    table.add_column("GPU Support", style="green")

    platforms = [
        ("polars", "Expression", "Lazy evaluation, streaming, thread control", "No"),
        ("pandas", "Pandas", "dtype_backend, categorical strings", "No"),
        ("dask", "Pandas", "Distributed, worker/thread control, spill to disk", "No"),
        ("modin", "Pandas", "Engine selection (ray/dask), parallelization", "No"),
        ("cudf", "Pandas", "GPU acceleration, memory pools, spill to host", "Yes"),
    ]

    for name, family, features, gpu in platforms:
        table.add_row(name, family, features, gpu)

    console.print(table)

    console.print("\n[dim]Use 'benchbox df-tuning show-defaults --platform <name>' for details[/dim]")


__all__ = ["df_tuning_group"]
