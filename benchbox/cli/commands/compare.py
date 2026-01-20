"""Compare command implementation for benchbox results.

Supports two modes:
1. File comparison: Compare existing result files
2. Run mode: Run benchmarks across platforms then compare

Mode is inferred from arguments:
- If -p/--platform provided → run mode
- If file paths provided → file comparison mode
- If no arguments → interactive wizard

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import json
import statistics
import sys
import warnings
from pathlib import Path
from typing import Any

import click
from rich.prompt import Confirm, Prompt
from rich.table import Table

from benchbox.cli.shared import console
from benchbox.core.results.exporter import ResultExporter
from benchbox.core.results.loader import (
    ResultLoadError,
    UnsupportedSchemaError,
    load_result_file,
)


class ResultFileMetadata:
    """Metadata extracted from a benchmark result file."""

    def __init__(
        self,
        path: Path,
        benchmark: str,
        benchmark_id: str,
        platform: str,
        scale: float,
        timestamp: str,
        execution_id: str,
    ):
        self.path = path
        self.benchmark = benchmark
        self.benchmark_id = benchmark_id
        self.platform = platform
        self.scale = scale
        self.timestamp = timestamp
        self.execution_id = execution_id

    @property
    def formatted_timestamp(self) -> str:
        """Return a human-readable timestamp."""
        try:
            from datetime import datetime

            dt = datetime.fromisoformat(self.timestamp)
            return dt.strftime("%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            return self.timestamp[:16] if self.timestamp else "Unknown"

    @property
    def short_path(self) -> str:
        """Return a shortened path for display."""
        parts = self.path.parts
        if len(parts) > 3:
            return str(Path(*parts[-3:]))
        return str(self.path)


def _discover_result_files_with_metadata(
    search_dirs: list[Path] | None = None,
    max_files: int = 100,
) -> list[ResultFileMetadata]:
    """Discover benchmark result files and extract their metadata.

    Searches for JSON files with valid BenchBox result schema v2.0 (version field)
    and extracts benchmark, platform, scale, and timestamp metadata.

    Args:
        search_dirs: Directories to search. Defaults to benchmark_runs/results, results, .
        max_files: Maximum number of files to scan (for performance)

    Returns:
        List of ResultFileMetadata objects, sorted by timestamp (newest first)
    """
    if search_dirs is None:
        search_dirs = [
            Path("benchmark_runs/results"),
            Path("benchmark_runs"),
            Path("results"),
            Path("."),
        ]

    discovered: list[ResultFileMetadata] = []
    seen_paths: set[Path] = set()

    for search_dir in search_dirs:
        if not search_dir.exists() or not search_dir.is_dir():
            continue

        # Find JSON files, sorted by modification time (newest first)
        # Skip companion files
        json_files = sorted(
            (
                f
                for f in search_dir.glob("**/*.json")
                if not f.name.endswith(".plans.json") and not f.name.endswith(".tuning.json")
            ),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

        for filepath in json_files[:max_files]:
            # Skip if already seen (handles overlapping search directories)
            resolved = filepath.resolve()
            if resolved in seen_paths:
                continue
            seen_paths.add(resolved)

            try:
                with open(filepath, encoding="utf-8") as f:
                    data = json.load(f)

                # Validate it's a BenchBox v2.0 result file
                if data.get("version") != "2.0":
                    continue

                # Also require benchmark section (distinguishes from manifests etc.)
                if "benchmark" not in data:
                    continue

                # Extract metadata (v2.0 format)
                benchmark_section = data.get("benchmark", {})
                run_section = data.get("run", {})
                platform_section = data.get("platform", {})

                metadata = ResultFileMetadata(
                    path=filepath,
                    benchmark=benchmark_section.get("name", "Unknown"),
                    benchmark_id=benchmark_section.get("id", ""),
                    platform=platform_section.get("name", "Unknown"),
                    scale=benchmark_section.get("scale_factor", 1.0),
                    timestamp=run_section.get("timestamp", ""),
                    execution_id=run_section.get("id", ""),
                )
                discovered.append(metadata)

            except (json.JSONDecodeError, OSError, KeyError):
                # Skip files that can't be parsed or have missing fields
                continue

    # Sort by timestamp (newest first)
    def parse_timestamp(meta: ResultFileMetadata) -> str:
        return meta.timestamp or ""

    discovered.sort(key=parse_timestamp, reverse=True)

    return discovered


@click.command("compare")
@click.argument("result_files", nargs=-1, type=click.Path(exists=True))
# Platform options (triggers run mode when provided)
@click.option(
    "-p",
    "--platform",
    "platforms",
    multiple=True,
    help="Platforms to compare (triggers run mode). Repeatable: -p duckdb -p sqlite",
)
# Hidden deprecated --run flag for backwards compatibility
@click.option(
    "--run",
    "run_mode_flag",
    is_flag=True,
    hidden=True,
    help="[DEPRECATED] Run mode is now inferred from -p/--platform",
)
@click.option(
    "--type",
    "platform_type",
    type=click.Choice(["sql", "dataframe", "auto"]),
    default="auto",
    help="Platform type: sql, dataframe, or auto-detect (default: auto)",
)
@click.option(
    "-b",
    "--benchmark",
    default="tpch",
    show_default=True,
    type=click.Choice(["tpch", "tpcds", "ssb", "clickbench"]),
    help="Benchmark to run (run mode)",
)
@click.option(
    "-s",
    "--scale",
    default=0.01,
    show_default=True,
    type=float,
    help="Scale factor (run mode)",
)
@click.option(
    "-q",
    "--queries",
    default=None,
    help="Comma-separated query IDs, e.g., Q1,Q3,Q6 (run mode)",
)
@click.option(
    "--warmup",
    default=1,
    show_default=True,
    type=int,
    help="Warmup iterations (run mode)",
)
@click.option(
    "--iterations",
    default=3,
    show_default=True,
    type=int,
    help="Benchmark iterations (run mode)",
)
@click.option(
    "--data-dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Data directory (run mode, for embedded platforms)",
)
@click.option(
    "--list-platforms",
    is_flag=True,
    help="List available platforms and exit",
)
# Common options
@click.option(
    "--fail-on-regression",
    type=str,
    help='Fail (exit 1) if regression exceeds threshold (e.g., "10%", "0.1", "5%")',
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json", "markdown", "html"], case_sensitive=False),
    default="text",
    help="Output format (default: text)",
)
@click.option(
    "-o",
    "--output",
    "output_file",
    type=click.Path(),
    help="Save output to file or directory",
)
@click.option(
    "--generate-charts",
    is_flag=True,
    help="Generate visualization charts (run mode)",
)
@click.option(
    "--theme",
    type=click.Choice(["light", "dark"]),
    default="light",
    help="Chart theme (with --generate-charts)",
)
@click.option(
    "--show-all-queries",
    is_flag=True,
    help="Show all query comparisons (file mode only)",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Disable interactive prompts (for CI/CD)",
)
# Query plan comparison options (file mode)
@click.option(
    "--include-plans",
    is_flag=True,
    help="Include query plan comparison in file mode (requires captured plans)",
)
@click.option(
    "--plan-threshold",
    type=float,
    default=0.0,
    help="Only show plan changes with similarity below threshold (0.0-1.0)",
)
@click.pass_context
def compare(
    ctx,
    result_files,
    platforms,
    run_mode_flag,
    platform_type,
    benchmark,
    scale,
    queries,
    warmup,
    iterations,
    data_dir,
    list_platforms,
    fail_on_regression,
    output_format,
    output_file,
    generate_charts,
    theme,
    show_all_queries,
    non_interactive,
    include_plans,
    plan_threshold,
):
    """Compare benchmark results, query plans, or run cross-platform benchmarks.

    Mode is automatically detected:

    \b
    RUN MODE (with -p/--platform):
      Run benchmarks across multiple platforms and compare.
      benchbox compare -p duckdb -p sqlite

    \b
    FILE MODE (with file paths):
      Compare existing benchmark result files.
      benchbox compare baseline.json current.json

    \b
    INTERACTIVE MODE (no arguments):
      Launch interactive wizard to guide you through comparison.
      benchbox compare

    Examples:
        # Run SQL platform comparison
        benchbox compare -p duckdb -p sqlite -p clickhouse

        # Run DataFrame platform comparison
        benchbox compare -p polars-df -p pandas-df --scale 0.1

        # Compare two result files
        benchbox compare baseline.json current.json

        # Compare with regression threshold (CI/CD)
        benchbox compare baseline.json current.json --fail-on-regression 10%

        # Compare files with query plan analysis
        benchbox compare baseline.json current.json --include-plans

        # Show only significant plan changes (< 90% similar)
        benchbox compare baseline.json current.json --include-plans --plan-threshold 0.9

        # List available platforms
        benchbox compare --list-platforms

        # Generate charts with results
        benchbox compare -p duckdb -p sqlite -o ./comparison --generate-charts

        # Non-interactive mode (CI/CD)
        benchbox compare --non-interactive -p duckdb -p sqlite
    """
    # Handle --list-platforms
    if list_platforms:
        _list_available_platforms()
        return

    # Show deprecation warning if --run flag was explicitly used
    if run_mode_flag:
        warnings.warn(
            "The --run flag is deprecated and can be omitted. "
            "Run mode is now inferred from -p/--platform options.\n"
            "Use: benchbox compare -p duckdb -p sqlite",
            DeprecationWarning,
            stacklevel=2,
        )
        console.print(
            "[yellow]Note: The --run flag is deprecated. Use: benchbox compare -p duckdb -p sqlite[/yellow]\n"
        )

    # Determine mode from arguments:
    # 1. If platforms specified → run mode
    # 2. If result files specified → file mode
    # 3. Neither → interactive mode (or error if --non-interactive)
    has_platforms = bool(platforms)
    has_files = bool(result_files)

    if has_platforms and has_files:
        console.print("[red]Error: Cannot specify both platforms (-p) and result files[/red]")
        console.print("\nChoose one:")
        console.print("  benchbox compare -p duckdb -p sqlite           # Run and compare platforms")
        console.print("  benchbox compare result1.json result2.json     # Compare existing files")
        sys.exit(1)

    if has_platforms:
        # Run mode: execute benchmarks across platforms
        _run_platform_comparison(
            platforms=list(platforms),
            platform_type=platform_type,
            benchmark=benchmark,
            scale=scale,
            queries=queries,
            warmup=warmup,
            iterations=iterations,
            data_dir=data_dir,
            output_format=output_format,
            output_file=output_file,
            generate_charts=generate_charts,
            theme=theme,
        )
    elif has_files:
        # File mode: compare existing result files
        _run_file_comparison(
            result_files=result_files,
            fail_on_regression=fail_on_regression,
            output_format=output_format,
            output_file=output_file,
            show_all_queries=show_all_queries,
            include_plans=include_plans,
            plan_threshold=plan_threshold,
        )
    else:
        # No arguments: interactive mode or error
        if non_interactive:
            console.print("[red]Error: No platforms or files specified[/red]")
            console.print("\n[bold]Usage:[/bold]")
            console.print("  benchbox compare -p duckdb -p sqlite           # Compare platforms")
            console.print("  benchbox compare result1.json result2.json     # Compare files")
            console.print("\n[dim]Use --list-platforms to see available platforms[/dim]")
            sys.exit(1)

        # Check for TTY
        if not sys.stdin.isatty() or not sys.stdout.isatty():
            console.print("[red]Error: Interactive mode requires a terminal (TTY)[/red]")
            console.print("\n[bold]Provide arguments for non-interactive usage:[/bold]")
            console.print("  benchbox compare -p duckdb -p sqlite           # Compare platforms")
            console.print("  benchbox compare result1.json result2.json     # Compare files")
            console.print("\n[dim]Or use --non-interactive flag with required options[/dim]")
            sys.exit(1)

        # Launch interactive wizard
        _run_interactive_wizard(
            output_format=output_format,
            output_file=output_file,
            generate_charts=generate_charts,
            theme=theme,
            fail_on_regression=fail_on_regression,
            show_all_queries=show_all_queries,
        )


def _run_interactive_wizard(
    output_format: str,
    output_file: str | None,
    generate_charts: bool,
    theme: str,
    fail_on_regression: str | None,
    show_all_queries: bool,
):
    """Interactive wizard for platform comparison."""
    console.print("\n[bold blue]BenchBox Platform Comparison[/bold blue]")
    console.print("Compare benchmark performance across different platforms.\n")

    # Step 1: Choose comparison type
    console.print("[bold]Step 1:[/bold] What would you like to do?\n")
    console.print("  [cyan]1[/cyan]  Compare platforms (run benchmarks)")
    console.print("  [cyan]2[/cyan]  Compare result files\n")

    mode_choice = Prompt.ask(
        "Select option",
        choices=["1", "2"],
        default="1",
    )

    if mode_choice == "1":
        # Run mode: platform comparison
        _interactive_platform_comparison(
            output_format=output_format,
            output_file=output_file,
            generate_charts=generate_charts,
            theme=theme,
        )
    else:
        # File mode: compare result files
        _interactive_file_comparison(
            output_format=output_format,
            output_file=output_file,
            fail_on_regression=fail_on_regression,
            show_all_queries=show_all_queries,
        )


def _interactive_platform_comparison(
    output_format: str,
    output_file: str | None,
    generate_charts: bool,
    theme: str,
):
    """Interactive wizard for platform comparison."""
    from benchbox.platforms import (
        list_available_dataframe_platforms,
        list_available_platforms,
    )

    # Step 2: Select platforms
    console.print("\n[bold]Step 2:[/bold] Select platforms to compare\n")

    # Get available platforms
    sql_platforms = list_available_platforms()
    df_platforms_dict = list_available_dataframe_platforms()
    df_platforms = [name for name, available in df_platforms_dict.items() if available]

    # Display available platforms in a table
    table = Table(title="Available Platforms", show_header=True)
    table.add_column("ID", style="cyan bold", width=4, justify="right")
    table.add_column("Platform", style="green", width=20)
    table.add_column("Type", style="blue", width=12)

    all_platforms = []

    # Add SQL platforms
    for i, platform in enumerate(sorted(sql_platforms), start=1):
        table.add_row(str(i), platform, "SQL")
        all_platforms.append((platform, "sql"))

    # Add DataFrame platforms
    for i, platform in enumerate(sorted(df_platforms), start=len(sql_platforms) + 1):
        table.add_row(str(i), platform, "DataFrame")
        all_platforms.append((platform, "dataframe"))

    console.print(table)

    if not all_platforms:
        console.print("[red]No platforms available![/red]")
        console.print("\nInstall platform dependencies:")
        console.print("  pip install benchbox[duckdb]")
        console.print("  pip install benchbox[dataframe]")
        sys.exit(1)

    # Get platform selection (comma-separated IDs)
    console.print("\n[dim]Enter platform IDs separated by commas (minimum 2)[/dim]")
    default_selection = "1,2" if len(all_platforms) >= 2 else "1"

    while True:
        selection = Prompt.ask(
            "Select platforms",
            default=default_selection,
        )

        try:
            selected_ids = [int(x.strip()) for x in selection.split(",")]
            if len(selected_ids) < 2:
                console.print("[yellow]Please select at least 2 platforms[/yellow]")
                continue

            if any(id < 1 or id > len(all_platforms) for id in selected_ids):
                console.print(f"[yellow]Invalid ID. Choose from 1-{len(all_platforms)}[/yellow]")
                continue

            break
        except ValueError:
            console.print("[yellow]Enter numbers separated by commas (e.g., 1,2,3)[/yellow]")

    selected_platforms = [all_platforms[id - 1][0] for id in selected_ids]
    console.print(f"\n[green]✓ Selected:[/green] {', '.join(selected_platforms)}")

    # Step 3: Select benchmark
    console.print("\n[bold]Step 3:[/bold] Configure benchmark\n")

    benchmark = Prompt.ask(
        "Benchmark",
        choices=["tpch", "tpcds", "ssb", "clickbench"],
        default="tpch",
    )

    scale = float(
        Prompt.ask(
            "Scale factor",
            default="0.01",
        )
    )

    iterations = int(
        Prompt.ask(
            "Iterations",
            default="3",
        )
    )

    # Step 4: Confirm and run
    console.print("\n[bold]Step 4:[/bold] Review configuration\n")
    console.print(f"  Platforms:  {', '.join(selected_platforms)}")
    console.print(f"  Benchmark:  {benchmark}")
    console.print(f"  Scale:      {scale}")
    console.print(f"  Iterations: {iterations}")

    if not Confirm.ask("\nProceed with comparison?", default=True):
        console.print("[yellow]Comparison cancelled[/yellow]")
        return

    # Run comparison
    _run_platform_comparison(
        platforms=selected_platforms,
        platform_type="auto",
        benchmark=benchmark,
        scale=scale,
        queries=None,
        warmup=1,
        iterations=iterations,
        data_dir=None,
        output_format=output_format,
        output_file=output_file,
        generate_charts=generate_charts,
        theme=theme,
    )


def _interactive_file_comparison(
    output_format: str,
    output_file: str | None,
    fail_on_regression: str | None,
    show_all_queries: bool,
):
    """Interactive wizard for file comparison with guided selection."""
    console.print("\n[bold]Step 2:[/bold] How do you want to select files?\n")
    console.print("  [cyan]1[/cyan]  Browse by benchmark/platform/scale (recommended)")
    console.print("  [cyan]2[/cyan]  Enter file paths directly\n")

    selection_mode = Prompt.ask("Select option", choices=["1", "2"], default="1")

    result = _guided_file_selection() if selection_mode == "1" else _direct_file_selection()

    if result is None:
        return

    baseline_path, current_path = result

    console.print(f"\n[green]✓ Baseline:[/green] {baseline_path}")
    console.print(f"[green]✓ Current:[/green]  {current_path}")

    # Ask about plan comparison
    console.print("\n[bold]Step 3:[/bold] Options\n")
    include_plans = Confirm.ask(
        "Include query plan analysis? (requires captured plans)",
        default=False,
    )

    if not Confirm.ask("\nProceed with comparison?", default=True):
        console.print("[yellow]Comparison cancelled[/yellow]")
        return

    # Run comparison
    _run_file_comparison(
        result_files=(str(baseline_path), str(current_path)),
        fail_on_regression=fail_on_regression,
        output_format=output_format,
        output_file=output_file,
        show_all_queries=show_all_queries,
        include_plans=include_plans,
        plan_threshold=0.0,
    )


def _guided_file_selection() -> tuple[Path, Path] | None:
    """Guide user through benchmark/platform/scale selection to find result files.

    Returns:
        Tuple of (baseline_path, current_path) or None if cancelled
    """
    console.print("\n[bold]Discovering result files...[/bold]")
    all_results = _discover_result_files_with_metadata()

    if not all_results:
        console.print("[yellow]No benchmark result files found.[/yellow]")
        console.print("[dim]Run benchmarks first or enter file paths directly.[/dim]")
        return None

    console.print(f"[dim]Found {len(all_results)} result files[/dim]\n")

    # Step 1: Select benchmark
    benchmarks = sorted({r.benchmark for r in all_results})

    if len(benchmarks) == 1:
        selected_benchmark = benchmarks[0]
        console.print(f"[dim]Only one benchmark found: {selected_benchmark}[/dim]")
    else:
        console.print("[bold]Select benchmark:[/bold]")
        for i, bench in enumerate(benchmarks, start=1):
            count = sum(1 for r in all_results if r.benchmark == bench)
            console.print(f"  [cyan]{i}[/cyan]  {bench} ({count} results)")

        while True:
            bench_input = Prompt.ask("\nBenchmark", default="1")
            if bench_input.isdigit():
                idx = int(bench_input) - 1
                if 0 <= idx < len(benchmarks):
                    selected_benchmark = benchmarks[idx]
                    break
            console.print(f"[yellow]Enter 1-{len(benchmarks)}[/yellow]")

    # Filter to selected benchmark
    filtered = [r for r in all_results if r.benchmark == selected_benchmark]

    # Step 2: Select platform
    platforms = sorted({r.platform for r in filtered})

    if len(platforms) == 1:
        selected_platform = platforms[0]
        console.print(f"[dim]Only one platform: {selected_platform}[/dim]")
    else:
        console.print(f"\n[bold]Select platform for {selected_benchmark}:[/bold]")
        for i, plat in enumerate(platforms, start=1):
            count = sum(1 for r in filtered if r.platform == plat)
            console.print(f"  [cyan]{i}[/cyan]  {plat} ({count} results)")

        while True:
            plat_input = Prompt.ask("\nPlatform", default="1")
            if plat_input.isdigit():
                idx = int(plat_input) - 1
                if 0 <= idx < len(platforms):
                    selected_platform = platforms[idx]
                    break
            console.print(f"[yellow]Enter 1-{len(platforms)}[/yellow]")

    # Filter to selected platform
    filtered = [r for r in filtered if r.platform == selected_platform]

    # Step 3: Select scale factor
    scales = sorted({r.scale for r in filtered})

    if len(scales) == 1:
        selected_scale = scales[0]
        console.print(f"[dim]Only one scale factor: {selected_scale}[/dim]")
    else:
        console.print("\n[bold]Select scale factor:[/bold]")
        for i, scale in enumerate(scales, start=1):
            count = sum(1 for r in filtered if r.scale == scale)
            console.print(f"  [cyan]{i}[/cyan]  SF {scale} ({count} results)")

        while True:
            scale_input = Prompt.ask("\nScale factor", default="1")
            if scale_input.isdigit():
                idx = int(scale_input) - 1
                if 0 <= idx < len(scales):
                    selected_scale = scales[idx]
                    break
            console.print(f"[yellow]Enter 1-{len(scales)}[/yellow]")

    # Filter to selected scale and limit to 10 most recent
    filtered = [r for r in filtered if r.scale == selected_scale]
    filtered = filtered[:10]  # Already sorted by timestamp (newest first)

    if len(filtered) < 2:
        console.print(f"\n[yellow]Only {len(filtered)} result(s) found for this combination.[/yellow]")
        console.print("[dim]Need at least 2 results to compare.[/dim]")
        return None

    # Step 4: Display filtered results and let user select
    console.print(f"\n[bold]Select files to compare ({len(filtered)} results):[/bold]\n")

    _display_results_table(filtered)

    # Select baseline
    while True:
        baseline_input = Prompt.ask("\nBaseline file (older/first)", default="2" if len(filtered) >= 2 else "1")
        if baseline_input.isdigit():
            idx = int(baseline_input) - 1
            if 0 <= idx < len(filtered):
                baseline_meta = filtered[idx]
                break
        console.print(f"[yellow]Enter 1-{len(filtered)}[/yellow]")

    # Select current
    while True:
        current_input = Prompt.ask("Current file (newer/second)", default="1")
        if current_input.isdigit():
            idx = int(current_input) - 1
            if 0 <= idx < len(filtered):
                if filtered[idx].path == baseline_meta.path:
                    console.print("[yellow]Cannot compare a file to itself[/yellow]")
                    continue
                current_meta = filtered[idx]
                break
        console.print(f"[yellow]Enter 1-{len(filtered)}[/yellow]")

    return baseline_meta.path, current_meta.path


def _direct_file_selection() -> tuple[Path, Path] | None:
    """Allow user to enter file paths directly.

    Returns:
        Tuple of (baseline_path, current_path) or None if cancelled
    """
    console.print("\n[bold]Enter file paths:[/bold]")
    console.print("[dim]Enter full or relative paths to result JSON files[/dim]\n")

    # Baseline file
    while True:
        baseline_input = Prompt.ask("Baseline file path")
        if not baseline_input:
            console.print("[yellow]Path required[/yellow]")
            continue

        baseline_path = Path(baseline_input)
        if not baseline_path.exists():
            console.print(f"[yellow]File not found: {baseline_path}[/yellow]")
            continue
        break

    # Current file
    while True:
        current_input = Prompt.ask("Current file path")
        if not current_input:
            console.print("[yellow]Path required[/yellow]")
            continue

        current_path = Path(current_input)
        if not current_path.exists():
            console.print(f"[yellow]File not found: {current_path}[/yellow]")
            continue

        if current_path == baseline_path:
            console.print("[yellow]Cannot compare a file to itself[/yellow]")
            continue
        break

    return baseline_path, current_path


def _display_results_table(results: list[ResultFileMetadata]) -> None:
    """Display a rich table of result files with metadata.

    Args:
        results: List of ResultFileMetadata objects to display
    """
    # Color mapping for benchmarks
    benchmark_colors = {
        "TPC-H": "green",
        "TPC-DS": "blue",
        "SSB": "magenta",
        "ClickBench": "yellow",
    }

    table = Table(show_header=True, header_style="bold")
    table.add_column("ID", style="cyan bold", width=4, justify="right")
    table.add_column("Benchmark", width=12)
    table.add_column("Platform", width=14)
    table.add_column("Scale", width=8, justify="right")
    table.add_column("Timestamp", width=18)
    table.add_column("File", style="dim", width=30)

    for i, meta in enumerate(results, start=1):
        # Apply color based on benchmark
        bench_color = benchmark_colors.get(meta.benchmark, "white")
        benchmark_styled = f"[{bench_color}]{meta.benchmark}[/{bench_color}]"

        # Format scale factor
        scale_str = f"SF {meta.scale}"

        table.add_row(
            str(i),
            benchmark_styled,
            meta.platform,
            scale_str,
            meta.formatted_timestamp,
            meta.short_path,
        )

    console.print(table)


def _list_available_platforms():
    """List available SQL and DataFrame platforms."""
    from benchbox.platforms import (
        list_available_dataframe_platforms,
        list_available_platforms,
    )

    console.print("\n[bold]Available Platforms[/bold]\n")

    # SQL Platforms
    console.print("[green]SQL Platforms:[/green]")
    sql_platforms = list_available_platforms()
    for platform in sorted(sql_platforms):
        console.print(f"  {platform}")

    console.print()

    # DataFrame Platforms
    console.print("[cyan]DataFrame Platforms:[/cyan]")
    df_platforms = list_available_dataframe_platforms()

    installed = [name for name, available in df_platforms.items() if available]
    not_installed = [name for name, available in df_platforms.items() if not available]

    if installed:
        console.print("  [green]Installed:[/green]")
        for platform in sorted(installed):
            console.print(f"    {platform}")

    if not_installed:
        console.print("  [dim]Not installed:[/dim]")
        for platform in sorted(not_installed):
            console.print(f"    {platform}")
        console.print("\n  [dim]Install with: pip install benchbox[dataframe-<name>][/dim]")

    console.print()
    console.print("[bold]Usage:[/bold]")
    console.print("  benchbox compare -p duckdb -p sqlite           # SQL comparison")
    console.print("  benchbox compare -p polars-df -p pandas-df     # DataFrame comparison")
    console.print("  benchbox compare                               # Interactive wizard")


def _run_platform_comparison(
    platforms: list[str],
    platform_type: str,
    benchmark: str,
    scale: float,
    queries: str | None,
    warmup: int,
    iterations: int,
    data_dir: Path | None,
    output_format: str,
    output_file: str | None,
    generate_charts: bool,
    theme: str,
):
    """Run benchmarks across platforms and compare."""
    from benchbox.core.comparison import (
        PlatformType,
        UnifiedBenchmarkConfig,
        UnifiedBenchmarkSuite,
        UnifiedComparisonPlotter,
    )

    # Validate platforms
    if not platforms:
        console.print("[red]Error: Specify platforms with -p/--platform[/red]")
        console.print("\n[bold]Examples:[/bold]")
        console.print("  benchbox compare --run -p duckdb -p sqlite")
        console.print("  benchbox compare --run -p polars-df -p pandas-df")
        console.print("\nUse --list-platforms to see available platforms")
        sys.exit(1)

    if len(platforms) < 2:
        console.print("[red]Error: At least 2 platforms required for comparison[/red]")
        sys.exit(1)

    # Parse query IDs
    query_ids = None
    if queries:
        query_ids = [q.strip() for q in queries.split(",")]

    # Map platform type
    type_map = {
        "sql": PlatformType.SQL,
        "dataframe": PlatformType.DATAFRAME,
        "auto": PlatformType.AUTO,
    }
    ptype = type_map[platform_type]

    # Create config
    config = UnifiedBenchmarkConfig(
        platform_type=ptype,
        scale_factor=scale,
        benchmark=benchmark,
        query_ids=query_ids,
        warmup_iterations=warmup,
        benchmark_iterations=iterations,
    )

    suite = UnifiedBenchmarkSuite(config=config)

    # Display header
    console.print("\n[bold]Cross-Platform Benchmark Comparison[/bold]")
    console.print(f"Platforms: {', '.join(platforms)}")
    console.print(f"Benchmark: {benchmark}")
    console.print(f"Scale factor: {scale}")
    console.print(f"Queries: {query_ids or 'all'}")
    console.print(f"Iterations: {iterations}")
    console.print()

    # Run comparison
    console.print("[dim]Running benchmarks...[/dim]")
    try:
        results = suite.run_comparison(platforms=platforms, data_dir=data_dir)
    except ValueError as e:
        console.print(f"[red]Error: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Benchmark failed: {e}[/red]")
        sys.exit(1)

    # Get summary
    summary = suite.get_summary(results)

    # Output results
    if output_format == "text":
        output_text = suite._generate_text_report(results)
        if output_file:
            output_path = Path(output_file)
            if output_path.suffix:
                output_path.write_text(output_text, encoding="utf-8")
            else:
                output_path.mkdir(parents=True, exist_ok=True)
                (output_path / "comparison.txt").write_text(output_text, encoding="utf-8")
            console.print(f"[green]Results saved to {output_file}[/green]")
        else:
            console.print(output_text)

    elif output_format == "json":
        output_path = Path(output_file) if output_file else None
        if output_path:
            if not output_path.suffix:
                output_path.mkdir(parents=True, exist_ok=True)
                output_path = output_path / "comparison.json"
            suite.export_results(results, output_path, format="json")
            console.print(f"[green]Results saved to {output_path}[/green]")
        else:
            data = {
                "config": {
                    "platform_type": config.platform_type.value,
                    "scale_factor": config.scale_factor,
                    "benchmark": config.benchmark,
                    "query_ids": config.query_ids,
                    "iterations": config.benchmark_iterations,
                },
                "results": [r.to_dict() for r in results],
                "summary": summary.to_dict(),
            }
            console.print(json.dumps(data, indent=2))

    elif output_format == "markdown":
        md_content = suite._generate_markdown_report(results)
        if output_file:
            output_path = Path(output_file)
            if not output_path.suffix:
                output_path.mkdir(parents=True, exist_ok=True)
                output_path = output_path / "comparison.md"
            output_path.write_text(md_content, encoding="utf-8")
            console.print(f"[green]Report saved to {output_path}[/green]")
        else:
            console.print(md_content)

    elif output_format == "html":
        # For HTML, generate markdown and convert (or use text for now)
        output_text = suite._generate_text_report(results)
        if output_file:
            output_path = Path(output_file)
            if not output_path.suffix:
                output_path.mkdir(parents=True, exist_ok=True)
                output_path = output_path / "comparison.html"
            output_path.write_text(f"<pre>{output_text}</pre>", encoding="utf-8")
            console.print(f"[green]Report saved to {output_path}[/green]")
        else:
            console.print(output_text)

    # Generate charts if requested
    if generate_charts and output_file:
        try:
            output_dir = Path(output_file)
            if output_dir.suffix:
                output_dir = output_dir.parent
            output_dir.mkdir(parents=True, exist_ok=True)

            plotter = UnifiedComparisonPlotter(results, theme=theme)
            charts_dir = output_dir / "charts"
            exports = plotter.generate_charts(output_dir=charts_dir)
            if exports:
                console.print(f"[green]Charts generated in {charts_dir}[/green]")
            else:
                console.print("[yellow]No charts generated (check visualization dependencies)[/yellow]")
        except Exception as e:
            console.print(f"[yellow]Chart generation failed: {e}[/yellow]")


def _run_file_comparison(
    result_files: tuple[str, ...],
    fail_on_regression: str | None,
    output_format: str,
    output_file: str | None,
    show_all_queries: bool,
    include_plans: bool = False,
    plan_threshold: float = 0.0,
):
    """Compare existing benchmark result files."""
    # Validate inputs
    if len(result_files) < 2:
        console.print("[red]Error: At least 2 result files required for comparison[/red]")
        console.print("\n[bold]Usage:[/bold]")
        console.print("  benchbox compare baseline.json current.json")
        console.print("  benchbox compare file1.json file2.json file3.json")
        console.print("\n[bold]Or use --run mode:[/bold]")
        console.print("  benchbox compare --run -p duckdb -p sqlite")
        sys.exit(1)

    # Parse regression threshold if provided
    regression_threshold = None
    if fail_on_regression:
        regression_threshold = _parse_threshold(fail_on_regression)
        if regression_threshold is None:
            console.print(
                f'[red]Error: Invalid threshold format "{fail_on_regression}". '
                'Use percentage (e.g., "10%") or decimal (e.g., "0.1")[/red]'
            )
            sys.exit(1)

    # For now, support 2-file comparison (baseline vs current)
    if len(result_files) > 2:
        console.print("[yellow]Note: Multi-file comparison not yet supported. Comparing first 2 files only.[/yellow]\n")

    baseline_path = Path(result_files[0])
    current_path = Path(result_files[1])

    # Load result files
    try:
        baseline, baseline_raw = load_result_file(baseline_path)
        current, current_raw = load_result_file(current_path)
    except FileNotFoundError as e:
        console.print(f"[red]Error: Result file not found: {e}[/red]")
        sys.exit(1)
    except UnsupportedSchemaError as e:
        console.print(f"[red]Error: {e}[/red]")
        console.print("[dim]Only schema version 2.0 is supported[/dim]")
        sys.exit(1)
    except ResultLoadError as e:
        console.print(f"[red]Error loading result file: {e}[/red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Unexpected error loading files: {e}[/red]")
        sys.exit(1)

    # Perform comparison using core ResultExporter
    exporter = ResultExporter()
    comparison = exporter.compare_results(baseline_path, current_path)

    # Check for comparison errors
    if "error" in comparison:
        console.print(f"[red]Comparison failed: {comparison['error']}[/red]")
        sys.exit(1)

    # Perform plan comparison if requested
    plan_comparison = None
    if include_plans:
        plan_comparison = _compare_plans(baseline, current, plan_threshold)
        if plan_comparison:
            comparison["plan_comparison"] = plan_comparison
        else:
            console.print("[yellow]Note: No query plans found in result files.[/yellow]")
            console.print("[dim]Plans must be captured with --capture-plans during benchmark runs.[/dim]\n")

    # Format and display output
    if output_format == "text":
        output_text = _format_text_comparison(comparison, baseline, current, show_all_queries)
        if output_file:
            Path(output_file).write_text(output_text, encoding="utf-8")
            console.print(f"[green]Comparison saved to {output_file}[/green]")
        else:
            console.print(output_text)
    elif output_format == "json":
        output_json = json.dumps(comparison, indent=2)
        if output_file:
            Path(output_file).write_text(output_json, encoding="utf-8")
            console.print(f"[green]Comparison saved to {output_file}[/green]")
        else:
            console.print(output_json)
    elif output_format in ("html", "markdown"):
        output_html = _format_html_comparison(comparison, baseline, current)
        if output_file:
            Path(output_file).write_text(output_html, encoding="utf-8")
            console.print(f"[green]Comparison saved to {output_file}[/green]")
        else:
            console.print(output_html)

    # Check for regressions and handle --fail-on-regression
    if regression_threshold is not None:
        has_regression = _check_regression(comparison, regression_threshold)
        if has_regression:
            console.print(
                f"\n[bold red]❌ Performance regression detected (threshold: {regression_threshold * 100:.1f}%)[/bold red]"
            )
            sys.exit(1)
        else:
            console.print(
                f"\n[bold green]✓ No performance regression (threshold: {regression_threshold * 100:.1f}%)[/bold green]"
            )


def _compare_plans(
    baseline: Any,
    current: Any,
    plan_threshold: float = 0.0,
) -> dict[str, Any] | None:
    """Compare query plans between two benchmark results.

    Args:
        baseline: Baseline BenchmarkResults
        current: Current BenchmarkResults
        plan_threshold: Only include plans with similarity below this threshold

    Returns:
        Dictionary with plan comparison data, or None if no plans available
    """
    from benchbox.core.query_plans.comparison import (
        QueryPlanComparator,
        generate_plan_comparison_summary,
    )

    # Generate plan comparison summary (handles all the heavy lifting)
    try:
        summary = generate_plan_comparison_summary(
            baseline,
            current,
            regression_threshold_pct=20.0,  # Default 20% regression threshold
        )
    except Exception as e:
        console.print(f"[yellow]Warning: Plan comparison failed: {e}[/yellow]")
        return None

    # No plans to compare
    if summary.plans_compared == 0:
        return None

    # Build plan comparison result
    plan_comparisons = []
    comparator = QueryPlanComparator()

    # Get detailed per-query plan comparisons
    baseline_map: dict[str, Any] = {}
    current_map: dict[str, Any] = {}

    # Extract all executions
    for phase_results in baseline.phases.values():
        for execution in phase_results.queries:
            if execution.query_id not in baseline_map:
                baseline_map[execution.query_id] = execution

    for phase_results in current.phases.values():
        for execution in phase_results.queries:
            if execution.query_id not in current_map:
                current_map[execution.query_id] = execution

    # Compare common queries
    common_queries = set(baseline_map.keys()) & set(current_map.keys())

    for query_id in sorted(common_queries):
        baseline_exec = baseline_map[query_id]
        current_exec = current_map[query_id]

        baseline_plan = getattr(baseline_exec, "query_plan", None)
        current_plan = getattr(current_exec, "query_plan", None)

        if not baseline_plan or not current_plan:
            continue

        # Compare plans
        comparison = comparator.compare_plans(baseline_plan, current_plan)
        similarity = comparison.similarity.overall_similarity

        # Apply threshold filter (include if similarity < threshold, or if threshold is 0)
        if plan_threshold > 0 and similarity >= plan_threshold:
            continue

        # Get performance change for this query
        baseline_time = getattr(baseline_exec, "execution_time_ms", 0.0) or 0.0
        current_time = getattr(current_exec, "execution_time_ms", 0.0) or 0.0
        perf_change_pct = 0.0
        if baseline_time > 0:
            perf_change_pct = ((current_time - baseline_time) / baseline_time) * 100

        plan_comparisons.append(
            {
                "query_id": query_id,
                "similarity": similarity,
                "plans_identical": comparison.plans_identical,
                "type_mismatches": comparison.similarity.type_mismatches,
                "property_mismatches": comparison.similarity.property_mismatches,
                "structure_mismatches": comparison.similarity.structure_mismatches,
                "summary": comparison.summary,
                "baseline_time_ms": baseline_time,
                "current_time_ms": current_time,
                "perf_change_pct": perf_change_pct,
                "is_regression": (not comparison.plans_identical) and perf_change_pct > 20.0,
            }
        )

    # Identify regressions (plan changed AND perf degraded)
    regressions = [p for p in plan_comparisons if p["is_regression"]]

    return {
        "plans_compared": summary.plans_compared,
        "plans_unchanged": summary.plans_unchanged,
        "plans_changed": summary.plans_changed,
        "regressions_detected": len(regressions),
        "threshold_applied": plan_threshold,
        "query_plans": plan_comparisons,
        "regressions": regressions,
    }


def _parse_threshold(threshold_str: str) -> float | None:
    """Parse regression threshold from string to decimal."""
    threshold_str = threshold_str.strip()

    # Handle percentage format (e.g., "10%", "5.5%")
    if threshold_str.endswith("%"):
        try:
            value = float(threshold_str[:-1])
            return value / 100.0
        except ValueError:
            return None

    # Handle decimal format (e.g., "0.1", "0.05")
    try:
        return float(threshold_str)
    except ValueError:
        return None


def _check_regression(comparison: dict[str, Any], threshold: float) -> bool:
    """Check if any query or overall performance regressed beyond threshold."""
    # Check overall performance changes
    perf_changes = comparison.get("performance_changes", {})
    for _metric_name, metric_data in perf_changes.items():
        if not isinstance(metric_data, dict):
            continue
        change_pct = metric_data.get("change_percent", 0)
        # Positive change = slower = regression
        if change_pct > (threshold * 100):
            return True

    # Check per-query regressions
    query_comparisons = comparison.get("query_comparisons", [])
    for query in query_comparisons:
        change_pct = query.get("change_percent", 0)
        # Positive change = slower = regression
        if change_pct > (threshold * 100):
            return True

    return False


def _format_text_comparison(comparison: dict[str, Any], baseline: Any, current: Any, show_all: bool) -> str:
    """Format comparison as human-readable text."""
    lines = []

    # Header
    lines.append("=" * 80)
    lines.append("BENCHMARK COMPARISON REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Files being compared
    lines.append(f"Baseline: {comparison['baseline_file']}")
    lines.append(f"Current:  {comparison['current_file']}")
    lines.append("")

    # Benchmark metadata
    lines.append(f"Benchmark:  {baseline.benchmark_name}")
    lines.append(f"Platform:   {baseline.platform}")
    lines.append(f"Scale:      {baseline.scale_factor}")
    lines.append("")

    # Overall performance summary
    summary = comparison.get("summary", {})
    if summary:
        lines.append("-" * 80)
        lines.append("OVERALL SUMMARY")
        lines.append("-" * 80)
        lines.append(f"Total queries compared: {summary.get('total_queries_compared', 0)}")
        lines.append(f"  Improved:  {summary.get('improved_queries', 0)}")
        lines.append(f"  Regressed: {summary.get('regressed_queries', 0)}")
        lines.append(f"  Unchanged: {summary.get('unchanged_queries', 0)}")
        lines.append(f"Assessment: {summary.get('overall_assessment', 'unknown').upper()}")
        lines.append("")

    # Performance metrics changes
    perf_changes = comparison.get("performance_changes", {})
    if perf_changes:
        lines.append("-" * 80)
        lines.append("PERFORMANCE METRICS")
        lines.append("-" * 80)

        for metric_name, metric_data in perf_changes.items():
            if not isinstance(metric_data, dict):
                continue

            baseline_val = metric_data.get("baseline", 0)
            current_val = metric_data.get("current", 0)
            change_pct = metric_data.get("change_percent", 0)
            improved = metric_data.get("improved", False)

            # Format metric name nicely
            display_name = metric_name.replace("_", " ").title()

            # Color indicators
            if improved:
                indicator = "🟢"
                change_str = f"-{abs(change_pct):.2f}%"
            elif change_pct > 10:
                indicator = "🔴"
                change_str = f"+{change_pct:.2f}%"
            elif change_pct > 0:
                indicator = "🟡"
                change_str = f"+{change_pct:.2f}%"
            else:
                indicator = "⚪"
                change_str = "0.00%"

            lines.append(f"{indicator} {display_name:25s} {baseline_val:10.3f}s → {current_val:10.3f}s ({change_str})")

        lines.append("")

    # Geometric mean calculation
    query_comparisons = comparison.get("query_comparisons", [])
    if query_comparisons:
        baseline_times = [q["baseline_time_ms"] for q in query_comparisons if q["baseline_time_ms"] > 0]
        current_times = [q["current_time_ms"] for q in query_comparisons if q["current_time_ms"] > 0]

        if baseline_times and current_times:
            baseline_geomean = statistics.geometric_mean(baseline_times)
            current_geomean = statistics.geometric_mean(current_times)
            geomean_change = ((current_geomean - baseline_geomean) / baseline_geomean * 100) if baseline_geomean else 0

            lines.append("-" * 80)
            lines.append("GEOMETRIC MEAN (Query Times)")
            lines.append("-" * 80)

            if geomean_change < 0:
                indicator = "🟢 IMPROVED"
                change_str = f"-{abs(geomean_change):.2f}%"
            elif geomean_change > 10:
                indicator = "🔴 REGRESSED"
                change_str = f"+{geomean_change:.2f}%"
            elif geomean_change > 0:
                indicator = "🟡 SLOWER"
                change_str = f"+{geomean_change:.2f}%"
            else:
                indicator = "⚪ UNCHANGED"
                change_str = "0.00%"

            lines.append(f"Baseline: {baseline_geomean:.3f} ms")
            lines.append(f"Current:  {current_geomean:.3f} ms")
            lines.append(f"Change:   {change_str} {indicator}")
            lines.append("")

    # Per-query comparison
    if query_comparisons:
        lines.append("-" * 80)
        lines.append("PER-QUERY COMPARISON")
        lines.append("-" * 80)
        lines.append(f"{'Query':10s} {'Baseline':>12s} {'Current':>12s} {'Change':>12s}  {'Severity':10s}")
        lines.append("-" * 80)

        # Sort by change percent (largest regressions first)
        sorted_queries = sorted(query_comparisons, key=lambda q: q.get("change_percent", 0), reverse=True)

        for query in sorted_queries:
            query_id = str(query["query_id"])
            baseline_time = query["baseline_time_ms"]
            current_time = query["current_time_ms"]
            change_pct = query["change_percent"]
            improved = query["improved"]

            # Only show if: regression, improvement, or show_all flag
            if not show_all and abs(change_pct) < 1.0:
                continue

            # Determine severity
            if improved:
                severity = "✓ FASTER"
                change_str = f"-{abs(change_pct):.2f}%"
            elif change_pct > 50:
                severity = "🔴 CRITICAL"
                change_str = f"+{change_pct:.2f}%"
            elif change_pct > 25:
                severity = "🔴 MAJOR"
                change_str = f"+{change_pct:.2f}%"
            elif change_pct > 10:
                severity = "🟡 MINOR"
                change_str = f"+{change_pct:.2f}%"
            elif change_pct > 1:
                severity = "⚪ SLIGHT"
                change_str = f"+{change_pct:.2f}%"
            else:
                severity = "⚪ SAME"
                change_str = "0.00%"

            lines.append(
                f"{query_id:10s} {baseline_time:10.2f} ms {current_time:10.2f} ms {change_str:>12s}  {severity}"
            )

        lines.append("")

    # Query plan comparison section (if included)
    plan_comparison = comparison.get("plan_comparison")
    if plan_comparison:
        lines.append("-" * 80)
        lines.append("QUERY PLAN ANALYSIS")
        lines.append("-" * 80)

        plans_compared = plan_comparison.get("plans_compared", 0)
        plans_unchanged = plan_comparison.get("plans_unchanged", 0)
        plans_changed = plan_comparison.get("plans_changed", 0)
        threshold = plan_comparison.get("threshold_applied", 0.0)

        lines.append(f"Plans Compared: {plans_compared}")
        lines.append(f"  Unchanged:    {plans_unchanged}")
        lines.append(f"  Changed:      {plans_changed}")
        if threshold > 0:
            lines.append(f"  (Filtered by similarity < {threshold:.0%})")
        lines.append("")

        # Show plan change details
        query_plans = plan_comparison.get("query_plans", [])
        if query_plans:
            lines.append(
                f"{'Query':10s} {'Similarity':>12s} {'Type Δ':>8s} {'Prop Δ':>8s} {'Perf Δ':>10s}  {'Status':20s}"
            )
            lines.append("-" * 80)

            # Sort by similarity (lowest first = most different)
            sorted_plans = sorted(query_plans, key=lambda p: p.get("similarity", 1.0))

            for plan in sorted_plans:
                query_id = plan.get("query_id", "?")
                similarity = plan.get("similarity", 1.0)
                type_diff = plan.get("type_mismatches", 0)
                prop_diff = plan.get("property_mismatches", 0)
                perf_change = plan.get("perf_change_pct", 0.0)
                is_regression = plan.get("is_regression", False)

                # Determine status
                if plan.get("plans_identical"):
                    status = "✓ Identical"
                elif similarity >= 0.95:
                    status = "≈ Nearly Identical"
                elif similarity >= 0.75:
                    status = "~ Similar"
                elif is_regression:
                    status = "🔴 REGRESSION"
                else:
                    status = "✗ Different"

                perf_str = f"{perf_change:+.1f}%" if perf_change != 0 else "-"
                type_str = str(type_diff) if type_diff > 0 else "-"
                prop_str = str(prop_diff) if prop_diff > 0 else "-"

                lines.append(
                    f"{query_id:10s} {similarity:11.1%} {type_str:>8s} {prop_str:>8s} {perf_str:>10s}  {status:20s}"
                )

            lines.append("")

        # Highlight regressions
        regressions = plan_comparison.get("regressions", [])
        if regressions:
            lines.append(f"⚠️  PLAN-CORRELATED REGRESSIONS: {len(regressions)}")
            lines.append("    (Plan changed AND performance degraded >20%)")
            for reg in regressions:
                query_id = reg.get("query_id", "?")
                perf_change = reg.get("perf_change_pct", 0.0)
                similarity = reg.get("similarity", 0.0)
                lines.append(f"    • {query_id}: plan similarity {similarity:.0%}, perf {perf_change:+.1f}%")
            lines.append("")
        else:
            lines.append("✓ No plan-correlated regressions detected")
            lines.append("")

    lines.append("=" * 80)

    return "\n".join(lines)


def _format_html_comparison(comparison: dict[str, Any], baseline: Any, current: Any) -> str:
    """Format comparison as HTML."""
    html = []
    html.append("<!DOCTYPE html>")
    html.append("<html>")
    html.append("<head>")
    html.append("<title>Benchmark Comparison Report</title>")
    html.append("<style>")
    html.append("body { font-family: Arial, sans-serif; margin: 20px; }")
    html.append("h1, h2 { color: #333; }")
    html.append("table { border-collapse: collapse; width: 100%; margin: 20px 0; }")
    html.append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
    html.append("th { background-color: #f2f2f2; }")
    html.append(".improved { color: green; font-weight: bold; }")
    html.append(".regressed { color: red; font-weight: bold; }")
    html.append(".critical { background-color: #ffcccc; }")
    html.append(".major { background-color: #ffe6cc; }")
    html.append(".minor { background-color: #ffffcc; }")
    html.append("</style>")
    html.append("</head>")
    html.append("<body>")

    html.append("<h1>Benchmark Comparison Report</h1>")
    html.append(f"<p><strong>Baseline:</strong> {comparison['baseline_file']}</p>")
    html.append(f"<p><strong>Current:</strong> {comparison['current_file']}</p>")
    html.append(
        f"<p><strong>Benchmark:</strong> {baseline.benchmark_name} | <strong>Platform:</strong> {baseline.platform} | <strong>Scale:</strong> {baseline.scale_factor}</p>"
    )

    # Summary table
    summary = comparison.get("summary", {})
    if summary:
        html.append("<h2>Summary</h2>")
        html.append("<table>")
        html.append("<tr><th>Metric</th><th>Value</th></tr>")
        html.append(f"<tr><td>Total Queries</td><td>{summary.get('total_queries_compared', 0)}</td></tr>")
        html.append(f"<tr><td>Improved</td><td class='improved'>{summary.get('improved_queries', 0)}</td></tr>")
        html.append(f"<tr><td>Regressed</td><td class='regressed'>{summary.get('regressed_queries', 0)}</td></tr>")
        html.append(f"<tr><td>Unchanged</td><td>{summary.get('unchanged_queries', 0)}</td></tr>")
        html.append("</table>")

    # Query comparison table
    query_comparisons = comparison.get("query_comparisons", [])
    if query_comparisons:
        html.append("<h2>Query Comparison</h2>")
        html.append("<table>")
        html.append(
            "<tr><th>Query</th><th>Baseline (ms)</th><th>Current (ms)</th><th>Change</th><th>Severity</th></tr>"
        )

        sorted_queries = sorted(query_comparisons, key=lambda q: q.get("change_percent", 0), reverse=True)

        for query in sorted_queries:
            query_id = query["query_id"]
            baseline_time = query["baseline_time_ms"]
            current_time = query["current_time_ms"]
            change_pct = query["change_percent"]
            improved = query["improved"]

            # Determine row class
            if improved:
                row_class = ""
                severity = "Improved"
            elif change_pct > 50:
                row_class = ' class="critical"'
                severity = "Critical"
            elif change_pct > 25:
                row_class = ' class="major"'
                severity = "Major"
            elif change_pct > 10:
                row_class = ' class="minor"'
                severity = "Minor"
            else:
                row_class = ""
                severity = "None"

            change_str = f"{change_pct:+.2f}%"

            html.append(f"<tr{row_class}>")
            html.append(f"<td>{query_id}</td>")
            html.append(f"<td>{baseline_time:.2f}</td>")
            html.append(f"<td>{current_time:.2f}</td>")
            html.append(f"<td>{change_str}</td>")
            html.append(f"<td>{severity}</td>")
            html.append("</tr>")

        html.append("</table>")

    # Query plan comparison section (if included)
    plan_comparison = comparison.get("plan_comparison")
    if plan_comparison:
        html.append("<h2>Query Plan Analysis</h2>")

        plans_compared = plan_comparison.get("plans_compared", 0)
        plans_unchanged = plan_comparison.get("plans_unchanged", 0)
        plans_changed = plan_comparison.get("plans_changed", 0)
        regressions_detected = plan_comparison.get("regressions_detected", 0)

        # Summary cards
        html.append("<div style='display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 20px 0;'>")
        html.append(
            f"<div style='background: #f8f9fa; padding: 15px; border-radius: 8px; text-align: center;'><strong>{plans_compared}</strong><br>Plans Compared</div>"
        )
        html.append(
            f"<div style='background: #d4edda; padding: 15px; border-radius: 8px; text-align: center;'><strong>{plans_unchanged}</strong><br>Unchanged</div>"
        )
        html.append(
            f"<div style='background: #fff3cd; padding: 15px; border-radius: 8px; text-align: center;'><strong>{plans_changed}</strong><br>Changed</div>"
        )
        regression_style = "background: #f8d7da;" if regressions_detected > 0 else "background: #d4edda;"
        html.append(
            f"<div style='{regression_style} padding: 15px; border-radius: 8px; text-align: center;'><strong>{regressions_detected}</strong><br>Regressions</div>"
        )
        html.append("</div>")

        # Plan comparison table
        query_plans = plan_comparison.get("query_plans", [])
        if query_plans:
            html.append("<table>")
            html.append(
                "<tr><th>Query</th><th>Similarity</th><th>Type Δ</th><th>Prop Δ</th><th>Perf Δ</th><th>Status</th></tr>"
            )

            sorted_plans = sorted(query_plans, key=lambda p: p.get("similarity", 1.0))

            for plan in sorted_plans:
                query_id = plan.get("query_id", "?")
                similarity = plan.get("similarity", 1.0)
                type_diff = plan.get("type_mismatches", 0)
                prop_diff = plan.get("property_mismatches", 0)
                perf_change = plan.get("perf_change_pct", 0.0)
                is_regression = plan.get("is_regression", False)

                if plan.get("plans_identical"):
                    status = "Identical"
                    status_class = "improved"
                elif similarity >= 0.95:
                    status = "Nearly Identical"
                    status_class = "improved"
                elif similarity >= 0.75:
                    status = "Similar"
                    status_class = ""
                elif is_regression:
                    status = "REGRESSION"
                    status_class = "regressed"
                else:
                    status = "Different"
                    status_class = "minor"

                row_class = ' class="critical"' if is_regression else ""
                html.append(f"<tr{row_class}>")
                html.append(f"<td>{query_id}</td>")
                html.append(f"<td>{similarity:.1%}</td>")
                html.append(f"<td>{type_diff if type_diff > 0 else '-'}</td>")
                html.append(f"<td>{prop_diff if prop_diff > 0 else '-'}</td>")
                html.append(f"<td>{perf_change:+.1f}%</td>")
                html.append(f"<td class='{status_class}'>{status}</td>")
                html.append("</tr>")

            html.append("</table>")

        # Regressions alert
        regressions = plan_comparison.get("regressions", [])
        if regressions:
            html.append(
                "<div style='background: #f8d7da; color: #721c24; padding: 15px; border-radius: 8px; margin-top: 20px;'>"
            )
            html.append(f"<strong>⚠️ Plan-Correlated Regressions: {len(regressions)}</strong>")
            html.append("<p>These queries have both plan changes AND performance degradation >20%:</p>")
            html.append("<ul>")
            for reg in regressions:
                query_id = reg.get("query_id", "?")
                perf_change = reg.get("perf_change_pct", 0.0)
                similarity = reg.get("similarity", 0.0)
                html.append(
                    f"<li><strong>{query_id}</strong>: plan similarity {similarity:.0%}, performance {perf_change:+.1f}%</li>"
                )
            html.append("</ul>")
            html.append("</div>")
        else:
            html.append(
                "<div style='background: #d4edda; color: #155724; padding: 15px; border-radius: 8px; margin-top: 20px;'>"
            )
            html.append("<strong>✓ No plan-correlated regressions detected</strong>")
            html.append("</div>")

    html.append("</body>")
    html.append("</html>")

    return "\n".join(html)


__all__ = ["compare"]
