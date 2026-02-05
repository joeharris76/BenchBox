# Chart Generation Guide

```{tags} intermediate, guide, visualization
```

This guide walks you through generating publication-ready charts from BenchBox benchmark results. Whether you're creating blog posts, presentations, or reports, this guide covers the end-to-end workflow.

## Prerequisites

### Install Visualization Dependencies

BenchBox visualization requires optional packages. Install them with:

```bash
uv add "plotly>=5.24.0" "kaleido>=0.2.1" "pandas>=2.0.0" "pillow>=10.0.0"
```

**Why these packages?**
- `plotly` - Chart generation engine
- `kaleido` - Headless PNG/SVG/PDF export (no browser required)
- `pandas` - Data manipulation
- `pillow` - Image post-processing

### Verify Installation

```bash
benchbox visualize --help
```

If dependencies are missing, you'll see installation instructions.

---

## Quick Start

### Generate Charts from Latest Result

```bash
benchbox visualize
```

This auto-detects the most recent result in `benchmark_runs/results/` and generates default charts.

*See [Chart Types](chart-types.md) for examples of the generated charts.*

### Generate from Specific Results

```bash
benchbox visualize benchmark_runs/results/tpch_duckdb_sf1.json
```

### Compare Multiple Platforms

```bash
benchbox visualize duckdb.json snowflake.json bigquery.json --output charts/comparison
```

---

## Workflow by Content Type

### Flagship Blog Post (Multi-Platform Comparison)

For comprehensive "State of the Data Warehouse" style posts:

```bash
benchbox visualize results/*.json --template flagship --output charts/flagship
```

**Generates:**
- Performance bar chart (platform comparison)
- Query variance heatmap (per-query analysis)
- Cost-performance scatter (ROI analysis)
- Distribution box plot (consistency analysis)

**Output:**
```
charts/flagship/
├── tpch_duckdb-snowflake-bigquery-redshift_performance.png
├── tpch_duckdb-snowflake-bigquery-redshift_performance.html
├── tpch_duckdb-snowflake-bigquery-redshift_query-variance.png
├── tpch_duckdb-snowflake-bigquery-redshift_query-variance.html
├── tpch_duckdb-snowflake-bigquery-redshift_cost-performance.png
├── tpch_duckdb-snowflake-bigquery-redshift_cost-performance.html
├── tpch_duckdb-snowflake-bigquery-redshift_distribution.png
└── tpch_duckdb-snowflake-bigquery-redshift_distribution.html
```

*See [Chart Types](chart-types.md) for examples of each chart type in the flagship set.*

### Head-to-Head Comparison

For "DuckDB vs Snowflake" style posts:

```bash
benchbox visualize duckdb.json snowflake.json --template head_to_head --output charts/h2h
```

**Generates:**
- Side-by-side performance bar chart
- Distribution comparison box plot
- Query-level heatmap

### Performance Trend Analysis

For tracking performance over time:

```bash
benchbox visualize runs/2024/*.json runs/2025/*.json --template trends --output charts/trends
```

**Generates:**
- Time-series line chart with regression overlay
- Latest snapshot bar chart

### Cost Optimization Report

For ROI and cost analysis:

```bash
benchbox visualize cloud_results/*.json --template cost_optimization --output charts/cost
```

**Generates:**
- Cost-performance scatter with Pareto frontier
- Performance comparison bar chart

---

## Smart Chart Selection

When no template is specified, BenchBox analyzes your data and selects appropriate charts:

| Data Characteristics | Charts Generated |
|---------------------|------------------|
| Single platform | Query breakdown bar + distribution box |
| Multiple platforms | Platform comparison bar + heatmap |
| Cost data present | Adds cost-performance scatter |
| Multiple timestamps | Adds time-series trend |

```bash
# Let BenchBox decide
benchbox visualize results/*.json

# Disable smart selection for specific charts only
benchbox visualize results/*.json --no-smart --chart-type performance_bar
```

---

## Export Formats

### For Blog Posts (PNG)

```bash
benchbox visualize results/*.json --format png --dpi 150
```

Produces optimized images for web embedding.

### For Print (High-DPI PNG or PDF)

```bash
benchbox visualize results/*.json --format png,pdf --dpi 300
```

Produces print-quality images.

### For Presentations (SVG)

```bash
benchbox visualize results/*.json --format svg
```

Produces infinitely scalable vector graphics.

### For Interactive Dashboards (HTML)

```bash
benchbox visualize results/*.json --format html
```

Produces self-contained HTML files with hover, zoom, and pan.

---

## Batch Processing

### Generate Charts per Platform

```bash
benchbox visualize benchmark_runs/**/*.json --group-by platform --output charts/by-platform
```

Creates separate chart sets for each platform found in results.

### Generate Charts per Benchmark

```bash
benchbox visualize benchmark_runs/**/*.json --group-by benchmark --output charts/by-benchmark
```

Creates separate chart sets for each benchmark (TPC-H, TPC-DS, etc.).

---

## Customization

### Dark Theme for Presentations

```bash
benchbox visualize results/*.json --theme dark --format png
```

### Specific Chart Types Only

```bash
benchbox visualize results/*.json --chart-type performance_bar,cost_scatter
```

### Low-Resolution Preview

```bash
benchbox visualize results/*.json --dpi 72 --format png
```

---

## Troubleshooting

### "Missing visualization dependencies"

Install the required packages:
```bash
uv add "plotly>=5.24.0" "kaleido>=0.2.1" "pandas>=2.0.0" "pillow>=10.0.0"
```

### Empty or Missing Charts

Check that your result JSON includes the required fields:
- Performance bar: `results.timing.total_ms` or `avg_ms`
- Distribution box: `results.queries.details[*].execution_time_ms`
- Heatmap: At least 2 platforms with query-level timings
- Cost scatter: `cost_summary.total_cost`

### "No result sources provided"

Either specify files explicitly or ensure `benchmark_runs/results/` contains JSON files:
```bash
benchbox visualize benchmark_runs/results/my_result.json
```

### Kaleido Errors

Update Kaleido to the latest version:
```bash
uv add "kaleido>=0.2.1"
```

### Charts Look Different Than Expected

- Check `--theme` setting (light vs dark)
- Verify `--dpi` is appropriate for your use case
- Ensure you're using the intended `--template`

---

## Best Practices

1. **Use templates** for consistent output across content pieces
2. **Generate multiple formats** (`--format png,html`) for flexibility
3. **Use high DPI** (`--dpi 300`) for anything that might be printed
4. **Name output directories** clearly (`--output charts/2025-q1-comparison`)
5. **Keep source JSON** files for reproducibility
6. **Use `--group-by`** for batch processing large result sets

---

## Next Steps

- [Chart Types](chart-types.md) - Understand each chart type in detail
- [Templates](templates.md) - Learn about predefined templates
- [Customization](customization.md) - Fine-tune colors, themes, and styling
- [CLI Reference](cli-reference.md) - Complete command reference
