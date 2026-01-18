# CLI Reference: `benchbox visualize`

```{tags} intermediate, reference, visualization, cli
```

Generate publication-ready charts directly from benchmark result JSON files.

## Synopsis

```
benchbox visualize [SOURCES...] [OPTIONS]
```

## Description

The `visualize` command transforms BenchBox result JSON files into publication-ready charts. It supports multiple chart types, templates for common workflows, and various export formats.

**Auto-detection:** When no sources are specified, automatically finds the latest result in `benchmark_runs/results/`.

## Arguments

**SOURCES** (optional)
: One or more result JSON files, directories, or glob patterns. If omitted, uses the latest result file.

```bash
benchbox visualize                              # Auto-detect latest
benchbox visualize results.json                 # Single file
benchbox visualize results/*.json               # Glob pattern
benchbox visualize run1.json run2.json          # Multiple files
benchbox visualize benchmark_runs/results/      # Directory (recursive)
```

## Options

### Template Selection

`--template {flagship,head_to_head,trends,cost_optimization,default}`
: Use a predefined chart template. Overrides `--chart-type`.

```bash
benchbox visualize results/*.json --template flagship
```

### Chart Type Selection

`--chart-type {auto,all,performance_bar,distribution_box,query_heatmap,cost_scatter,time_series}`
: Specify chart types to generate. Use comma-separated values for multiple.

```bash
benchbox visualize results/*.json --chart-type performance_bar,distribution_box
```

| Value | Description |
|-------|-------------|
| `auto` | Smart selection based on data (default) |
| `all` | Generate all applicable chart types |
| `performance_bar` | Platform comparison bar chart |
| `distribution_box` | Latency distribution box plot |
| `query_heatmap` | Query × platform variance heatmap |
| `cost_scatter` | Cost-performance scatter with Pareto frontier |
| `time_series` | Performance trend line chart |

### Output Options

`--output PATH`
: Output directory for generated charts. Created if it doesn't exist.

```bash
benchbox visualize results/*.json --output charts/comparison
```
Default: `./charts`

`--format {png,svg,pdf,html}`
: Export format(s). Comma-separated for multiple.

```bash
benchbox visualize results/*.json --format png,svg,html
```
Default: `png,html`

`--dpi INTEGER`
: Resolution for raster exports (72-600).

```bash
benchbox visualize results/*.json --dpi 300
```
Default: `300`

### Appearance

`--theme {light,dark}`
: Color theme for charts.

```bash
benchbox visualize results/*.json --theme dark
```
Default: `light`

### Behavior

`--group-by {platform,benchmark}`
: Generate separate chart sets per group.

```bash
benchbox visualize benchmark_runs/**/*.json --group-by platform
```

`--smart / --no-smart`
: Enable/disable automatic chart type selection based on data characteristics.

```bash
benchbox visualize results/*.json --no-smart --chart-type performance_bar
```
Default: `--smart`

## Examples

### Basic Usage

```bash
# Generate charts from latest result (auto-detected)
benchbox visualize

# Generate from specific file
benchbox visualize benchmark_runs/results/tpch_duckdb_sf1.json

# Generate from multiple files for comparison
benchbox visualize duckdb.json snowflake.json bigquery.json
```

### Using Templates

```bash
# Flagship comparison (4-chart set)
benchbox visualize results/*.json --template flagship --output charts/flagship

# Head-to-head comparison
benchbox visualize platform_a.json platform_b.json --template head_to_head

# Performance trends over time
benchbox visualize runs/2024/*.json runs/2025/*.json --template trends

# Cost optimization analysis
benchbox visualize cloud_results/*.json --template cost_optimization
```

### Format and Quality

```bash
# High-resolution for print
benchbox visualize results/*.json --dpi 300 --format png,pdf

# Vector graphics for scaling
benchbox visualize results/*.json --format svg

# Interactive HTML for web
benchbox visualize results/*.json --format html

# All formats
benchbox visualize results/*.json --format png,svg,pdf,html
```

### Batch Processing

```bash
# Generate chart set per platform
benchbox visualize benchmark_runs/**/*.json --group-by platform --output charts/by-platform

# Generate chart set per benchmark
benchbox visualize benchmark_runs/**/*.json --group-by benchmark --output charts/by-benchmark
```

### Advanced

```bash
# Dark theme for presentations
benchbox visualize results/*.json --theme dark --format png

# Specific chart types only
benchbox visualize results/*.json --no-smart --chart-type performance_bar,cost_scatter

# Low-res for quick preview
benchbox visualize results/*.json --dpi 72 --format png
```

## Output

Charts are saved to the output directory with deterministic names:

```
{output}/
├── {benchmark}_{platforms}_performance.png
├── {benchmark}_{platforms}_performance.html
├── {benchmark}_{platforms}_distribution.png
├── {benchmark}_{platforms}_distribution.html
├── {benchmark}_{platforms}_query-variance.png
├── {benchmark}_{platforms}_query-variance.html
└── ...
```

**Naming convention:** `{benchmark}_{platforms}_{chart-type}.{format}`

Example: `tpch_duckdb-snowflake-bigquery_performance.png`

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Error (missing dependencies, invalid input, etc.) |

## Dependencies

Visualization requires optional dependencies:

```bash
uv add "plotly>=5.24.0" "kaleido>=0.2.1" "pandas>=2.0.0" "pillow>=10.0.0"
```

If dependencies are missing, the command displays installation instructions.

## See Also

- [Chart Types](chart-types.md) - Detailed chart type documentation
- [Templates](templates.md) - Template descriptions and use cases
- [Customization](customization.md) - Themes, colors, and styling options
