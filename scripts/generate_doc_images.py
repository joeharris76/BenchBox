#!/usr/bin/env python3
"""Generate documentation images for visualization docs.

This script creates sample benchmark data and generates all chart images
needed for the visualization documentation.

Usage:
    uv run scripts/generate_doc_images.py
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Output directory for images
OUTPUT_DIR = Path("docs/_static/visualization")


def create_sample_results() -> list[dict]:
    """Create synthetic benchmark results for multiple platforms."""
    platforms = [
        ("DuckDB", 1.2, 0.8),  # name, speed_factor, variance_factor
        ("Snowflake", 1.8, 0.6),
        ("BigQuery", 2.1, 0.9),
        ("Redshift", 2.5, 1.1),
    ]

    base_time = datetime.now(timezone.utc)
    results = []

    for i, (platform, speed_factor, variance_factor) in enumerate(platforms):
        # Generate query-level results for TPC-H 22 queries
        query_details = []
        for q in range(1, 23):
            # Varying query times with platform-specific characteristics
            base_query_time = (50 + q * 20) * speed_factor
            variance = base_query_time * 0.1 * variance_factor
            exec_time = base_query_time + (hash(f"{platform}{q}") % 100 - 50) * variance / 50

            query_details.append(
                {
                    "id": f"Q{q}",
                    "query_id": f"Q{q}",
                    "status": "SUCCESS",
                    "sequence": q,
                    "execution_time_ms": round(exec_time, 2),
                    "rows_returned": 100 + q * 10,
                }
            )

        total_time = sum(q["execution_time_ms"] for q in query_details)
        avg_time = total_time / len(query_details)

        result = {
            "schema_version": "1.1",
            "benchmark": {
                "id": "tpc_h_benchmark",
                "name": "TPC-H Benchmark",
                "scale_factor": 10,
            },
            "execution": {
                "id": f"sample_{platform.lower()}_{i}",
                "timestamp": (base_time - timedelta(days=i)).isoformat(),
                "duration_ms": int(total_time),
                "mode": "power",
                "platform": platform,
                "metadata": {
                    "benchmark_type": "olap",
                    "benchbox_version": "0.1.0",
                },
            },
            "configuration": {
                "scale_factor": 10,
            },
            "system": {
                "os": "Linux",
                "arch": "x86_64",
                "cpu_cores": 16,
                "memory_gb": 64,
            },
            "results": {
                "queries": {
                    "total": 22,
                    "successful": 22,
                    "failed": 0,
                    "success_rate": 1.0,
                    "details": query_details,
                },
                "timing": {
                    "total_ms": round(total_time, 2),
                    "avg_ms": round(avg_time, 2),
                },
            },
            "cost_summary": {
                "total_cost": round(0.50 + speed_factor * 0.30 + i * 0.10, 2),
                "currency": "USD",
            },
        }
        results.append(result)

    return results


def create_trend_results() -> list[dict]:
    """Create synthetic results for time-series trend visualization."""
    base_time = datetime.now(timezone.utc)
    results = []

    # Generate 6 months of data for DuckDB showing improvement
    for month in range(6):
        improvement_factor = 1.0 - (month * 0.03)  # 3% improvement per month

        query_details = []
        for q in range(1, 23):
            exec_time = (100 + q * 15) * improvement_factor
            query_details.append(
                {
                    "id": f"Q{q}",
                    "query_id": f"Q{q}",
                    "status": "SUCCESS",
                    "execution_time_ms": round(exec_time, 2),
                }
            )

        total_time = sum(q["execution_time_ms"] for q in query_details)

        result = {
            "schema_version": "1.1",
            "benchmark": {"name": "TPC-H Benchmark", "scale_factor": 10},
            "execution": {
                "id": f"trend_duckdb_{month}",
                "timestamp": (base_time - timedelta(days=30 * (5 - month))).isoformat(),
                "platform": "DuckDB",
            },
            "results": {
                "queries": {"details": query_details},
                "timing": {"total_ms": round(total_time, 2)},
            },
        }
        results.append(result)

    return results


def save_sample_results(results: list[dict], output_dir: Path) -> list[Path]:
    """Save sample results to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = []

    for result in results:
        platform = result["execution"]["platform"].lower()
        exec_id = result["execution"]["id"]
        filename = f"sample_{platform}_{exec_id}.json"
        path = output_dir / filename

        with open(path, "w") as f:
            json.dump(result, f, indent=2)
        paths.append(path)

    return paths


def generate_chart_images(result_paths: list[Path], output_dir: Path) -> None:
    """Generate individual chart type images."""
    from benchbox.core.visualization import ResultPlotter

    output_dir.mkdir(parents=True, exist_ok=True)

    # Load results
    plotter = ResultPlotter.from_sources(result_paths)

    # Generate each chart type individually
    chart_configs = [
        ("performance_bar", "chart-performance-bar"),
        ("distribution_box", "chart-distribution-box"),
        ("query_heatmap", "chart-query-heatmap"),
    ]

    for chart_type, base_name in chart_configs:
        try:
            exports = plotter.generate_all_charts(
                output_dir=output_dir,
                formats=["png"],
                chart_types=[chart_type],
                dpi=150,
            )
            # Rename to standard doc filename
            for ct, paths in exports.items():
                if "png" in paths:
                    src = paths["png"]
                    dst = output_dir / f"{base_name}.png"
                    shutil.move(str(src), str(dst))
                    print(f"Generated: {dst}")
        except Exception as e:
            print(f"Warning: Could not generate {chart_type}: {e}")


def generate_trend_image(trend_paths: list[Path], output_dir: Path) -> None:
    """Generate time-series trend chart."""
    from benchbox.core.visualization import ResultPlotter

    try:
        plotter = ResultPlotter.from_sources(trend_paths)
        exports = plotter.generate_all_charts(
            output_dir=output_dir,
            formats=["png"],
            chart_types=["time_series"],
            dpi=150,
        )
        for ct, paths in exports.items():
            if "png" in paths:
                src = paths["png"]
                dst = output_dir / "chart-time-series.png"
                shutil.move(str(src), str(dst))
                print(f"Generated: {dst}")
    except Exception as e:
        print(f"Warning: Could not generate time_series: {e}")


def generate_cost_scatter_image(result_paths: list[Path], output_dir: Path) -> None:
    """Generate cost-performance scatter plot."""
    from benchbox.core.visualization import ResultPlotter

    try:
        plotter = ResultPlotter.from_sources(result_paths)
        exports = plotter.generate_all_charts(
            output_dir=output_dir,
            formats=["png"],
            chart_types=["cost_scatter"],
            dpi=150,
        )
        for ct, paths in exports.items():
            if "png" in paths:
                src = paths["png"]
                dst = output_dir / "chart-cost-scatter.png"
                shutil.move(str(src), str(dst))
                print(f"Generated: {dst}")
    except Exception as e:
        print(f"Warning: Could not generate cost_scatter: {e}")


def generate_theme_comparison(result_paths: list[Path], output_dir: Path) -> None:
    """Generate light vs dark theme comparison image."""
    try:
        from PIL import Image

        from benchbox.core.visualization import ResultPlotter

        # Generate light theme chart
        plotter_light = ResultPlotter.from_sources(result_paths, theme="light")
        plotter_light.generate_all_charts(
            output_dir=output_dir,
            formats=["png"],
            chart_types=["performance_bar"],
            dpi=100,
        )

        # Find and rename light theme image
        light_images = list(output_dir.glob("*performance*.png"))
        if light_images:
            light_img = light_images[0]
            light_path = output_dir / "theme_light_temp.png"
            shutil.move(str(light_img), str(light_path))

        # Generate dark theme chart
        plotter_dark = ResultPlotter.from_sources(result_paths, theme="dark")
        plotter_dark.generate_all_charts(
            output_dir=output_dir,
            formats=["png"],
            chart_types=["performance_bar"],
            dpi=100,
        )

        # Find dark theme image
        dark_images = list(output_dir.glob("*performance*.png"))
        if dark_images:
            dark_img = dark_images[0]
            dark_path = output_dir / "theme_dark_temp.png"
            shutil.move(str(dark_img), str(dark_path))

        # Combine into side-by-side comparison
        if light_path.exists() and dark_path.exists():
            img_light = Image.open(light_path)
            img_dark = Image.open(dark_path)

            # Create combined image
            total_width = img_light.width + img_dark.width + 20
            max_height = max(img_light.height, img_dark.height)
            combined = Image.new("RGB", (total_width, max_height), (240, 240, 240))

            combined.paste(img_light, (0, 0))
            combined.paste(img_dark, (img_light.width + 20, 0))

            combined.save(output_dir / "theme-light-dark-comparison.png")
            print(f"Generated: {output_dir / 'theme-light-dark-comparison.png'}")

            # Cleanup temp files
            light_path.unlink()
            dark_path.unlink()

    except ImportError:
        print("Warning: PIL not available, skipping theme comparison")
    except Exception as e:
        print(f"Warning: Could not generate theme comparison: {e}")


def generate_color_palette_image(output_dir: Path) -> None:
    """Generate color palette swatch image."""
    try:
        from PIL import Image, ImageDraw, ImageFont

        colors = [
            ("#1b9e77", "Teal"),
            ("#d95f02", "Orange"),
            ("#7570b3", "Purple"),
            ("#e7298a", "Pink"),
            ("#66a61e", "Green"),
            ("#e6ab02", "Yellow"),
            ("#a6761d", "Brown"),
            ("#666666", "Gray"),
        ]

        swatch_width = 100
        swatch_height = 60
        padding = 10
        cols = 4
        rows = 2

        img_width = cols * swatch_width + (cols + 1) * padding
        img_height = rows * swatch_height + (rows + 1) * padding + 30

        img = Image.new("RGB", (img_width, img_height), (255, 255, 255))
        draw = ImageDraw.Draw(img)

        for i, (hex_color, name) in enumerate(colors):
            row = i // cols
            col = i % cols

            x = padding + col * (swatch_width + padding)
            y = padding + row * (swatch_height + padding)

            # Convert hex to RGB
            rgb = tuple(int(hex_color[j : j + 2], 16) for j in (1, 3, 5))

            # Draw swatch
            draw.rectangle([x, y, x + swatch_width, y + swatch_height], fill=rgb)

            # Draw hex code below
            try:
                font = ImageFont.load_default()
            except Exception:
                font = None
            draw.text((x + 5, y + swatch_height + 2), hex_color, fill=(100, 100, 100), font=font)

        # Add title
        draw.text((padding, img_height - 25), "BenchBox Colorblind-Safe Palette", fill=(50, 50, 50))

        img.save(output_dir / "color-palette-swatches.png")
        print(f"Generated: {output_dir / 'color-palette-swatches.png'}")

    except ImportError:
        print("Warning: PIL not available, skipping color palette image")
    except Exception as e:
        print(f"Warning: Could not generate color palette: {e}")


def main():
    """Generate all documentation images."""
    print("=" * 60)
    print("Generating Documentation Images")
    print("=" * 60)

    # Create temp directory for sample results
    temp_dir = Path("_temp_doc_images")
    temp_dir.mkdir(exist_ok=True)

    try:
        # Generate sample results
        print("\n1. Creating sample benchmark results...")
        results = create_sample_results()
        result_paths = save_sample_results(results, temp_dir)
        print(f"   Created {len(result_paths)} sample result files")

        # Generate trend results
        print("\n2. Creating trend results...")
        trend_results = create_trend_results()
        trend_paths = save_sample_results(trend_results, temp_dir / "trends")
        print(f"   Created {len(trend_paths)} trend result files")

        # Create output directory
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # Generate chart images
        print("\n3. Generating chart type images...")
        generate_chart_images(result_paths, OUTPUT_DIR)

        print("\n4. Generating time-series chart...")
        generate_trend_image(trend_paths, OUTPUT_DIR)

        print("\n5. Generating cost-scatter chart...")
        generate_cost_scatter_image(result_paths, OUTPUT_DIR)

        print("\n6. Generating theme comparison...")
        generate_theme_comparison(result_paths, OUTPUT_DIR)

        print("\n7. Generating color palette...")
        generate_color_palette_image(OUTPUT_DIR)

        print("\n" + "=" * 60)
        print("Image generation complete!")
        print(f"Output directory: {OUTPUT_DIR}")
        print("=" * 60)

        # List generated files
        print("\nGenerated files:")
        for f in sorted(OUTPUT_DIR.glob("*.png")):
            print(f"  - {f.name}")

    finally:
        # Cleanup temp directory
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
            print(f"\nCleaned up temp directory: {temp_dir}")


if __name__ == "__main__":
    main()
