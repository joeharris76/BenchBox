<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# DataFrame Cross-Platform Comparison

```{tags} intermediate, guide, dataframe-platform
```

> **DEPRECATED**: The `compare-dataframes` command is deprecated. Use [`benchbox compare --run`](platform-comparison.md) instead.
>
> Migration:
> ```bash
> # OLD
> benchbox compare-dataframes -p polars-df -p pandas-df
>
> # NEW
> benchbox compare --run -p polars-df -p pandas-df
> ```
>
> See the [Platform Comparison Guide](platform-comparison.md) for the unified comparison interface.

---

This guide covers how to compare DataFrame platform performance using the `benchbox compare-dataframes` command. Use this to make data-driven decisions about which DataFrame library to use for your workload.

## Overview

BenchBox provides a comprehensive benchmark suite for comparing:

- **Cross-platform comparisons**: Polars vs Pandas vs DataFusion vs PySpark
- **SQL vs DataFrame API**: Compare SQL execution against DataFrame API on the same data
- **Performance visualization**: Generate charts and reports automatically

## Prerequisites

- BenchBox installed with DataFrame extras: `uv add benchbox --extra dataframe`
- Benchmark data generated (TPC-H)

## Quick Start

### List Available Platforms

```bash
benchbox compare-dataframes --list-platforms
```

Output shows installed and available platforms:

```
DataFrame Platforms

Installed:
  polars-df       (expression, single_node ) [lazy, streaming]
  pandas-df       (pandas    , single_node ) [standard]
  datafusion-df   (expression, single_node ) [lazy]

Not installed:
  modin-df        (pandas    , distributed )
  cudf-df         (pandas    , gpu_accelerated)
  dask-df         (pandas    , distributed )
  pyspark-df      (expression, distributed )

Install extras with: uv add benchbox --extra dataframe-<name>
```

### Compare Two Platforms

```bash
# Compare Polars vs Pandas on TPC-H SF 0.01
benchbox compare-dataframes -p polars-df -p pandas-df --scale 0.01
```

### Compare Against SQL

```bash
# Compare Polars DataFrame API vs DuckDB SQL
benchbox compare-dataframes -p polars-df --vs-sql duckdb --scale 0.01
```

## Command Reference

```bash
benchbox compare-dataframes [OPTIONS]
```

### Platform Selection

| Option | Description |
|--------|-------------|
| `-p, --platforms TEXT` | DataFrame platforms to compare (repeatable) |
| `--vs-sql [duckdb\|sqlite]` | Compare against SQL platform |
| `--list-platforms` | Show available platforms and exit |

### Benchmark Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `-b, --benchmark [tpch]` | `tpch` | Benchmark to run |
| `-s, --scale FLOAT` | `0.01` | Scale factor |
| `-q, --queries TEXT` | all | Comma-separated query IDs (e.g., Q1,Q6,Q10) |
| `--warmup INTEGER` | `1` | Warmup iterations |
| `--iterations INTEGER` | `3` | Benchmark iterations |

### Output Options

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output PATH` | none | Output directory for results |
| `--format [json\|markdown\|text]` | `text` | Output format |
| `--generate-charts` | false | Generate visualization charts |
| `--theme [light\|dark]` | `light` | Chart theme |
| `--data-dir DIRECTORY` | auto | Directory containing benchmark data |

## Usage Examples

### Cross-Platform DataFrame Comparison

Compare multiple DataFrame platforms:

```bash
# Compare Polars, Pandas, and DataFusion
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  -p datafusion-df \
  --scale 0.1 \
  --iterations 5
```

Example output:

```
DataFrame Platform Comparison
Platforms: polars-df, pandas-df, datafusion-df
Scale factor: 0.1
Queries: all
Iterations: 5

Running benchmarks...

============================================================
RESULTS
============================================================

Fastest: polars-df
Slowest: pandas-df

Platform        Geomean (ms)    Total (ms)   Success
------------------------------------------------------------
polars-df            125.34       2847.23       100%
datafusion-df        142.67       3245.89       100%
pandas-df            312.45       7098.34       100%

Query Winners:
  Q1: polars-df
  Q2: datafusion-df
  Q3: polars-df
  ...
============================================================
```

### SQL vs DataFrame Comparison

Compare DataFrame API against SQL for the same queries:

```bash
benchbox compare-dataframes \
  -p polars-df \
  --vs-sql duckdb \
  --scale 0.1
```

Example output:

```
SQL vs DataFrame Comparison
SQL Platform: duckdb
DataFrame Platform: polars-df
Scale factor: 0.1
Queries: all

Running benchmarks...

============================================================
SQL vs DataFrame RESULTS
============================================================

SQL Platform: duckdb
DataFrame Platform: polars-df

DataFrame faster: 15 queries (68.2%)
SQL faster: 7 queries
Average speedup: 1.24x

Query        SQL (ms)   DataFrame (ms)    Speedup
------------------------------------------------------------
Q1              45.23           38.12       1.19x
Q2             123.45           89.34       1.38x
Q3              67.89           72.45       0.94x
...
============================================================
```

### Generate Reports and Charts

Save results with visualization:

```bash
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  --scale 1 \
  --output ./comparison_results \
  --format markdown \
  --generate-charts \
  --theme dark
```

This creates:

```
comparison_results/
├── comparison.md          # Markdown report
└── charts/
    ├── comparison_bar.png
    ├── comparison_bar.html
    ├── platform_distribution.png
    ├── platform_distribution.html
    ├── query_heatmap.png
    └── query_heatmap.html
```

### Specific Query Subset

Run only specific queries for faster iteration:

```bash
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  --queries Q1,Q6,Q10,Q14 \
  --iterations 10
```

### JSON Output for Automation

Export results for programmatic analysis:

```bash
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  --format json \
  --output ./results
```

The JSON output includes:

```json
{
  "config": {
    "scale_factor": 0.01,
    "query_ids": null,
    "iterations": 3
  },
  "results": [
    {
      "platform": "polars-df",
      "query_results": [...],
      "geometric_mean_ms": 125.34,
      "total_time_ms": 2847.23,
      "success_rate": 100.0
    },
    ...
  ],
  "summary": {
    "fastest_platform": "polars-df",
    "slowest_platform": "pandas-df",
    "query_winners": {"Q1": "polars-df", ...}
  }
}
```

## Programmatic API

For custom analysis, use the Python API directly:

```python
from benchbox.core.dataframe import (
    BenchmarkConfig,
    DataFrameBenchmarkSuite,
    SQLVsDataFrameBenchmark,
    run_quick_comparison,
    run_sql_vs_dataframe,
)
```

### Quick Comparison

```python
from benchbox.core.dataframe import run_quick_comparison
from pathlib import Path

# Run quick comparison
results = run_quick_comparison(
    platforms=["polars-df", "pandas-df"],
    data_dir=Path("benchmark_runs/tpch/sf001/data"),
    scale_factor=0.01,
)

for result in results:
    print(f"{result.platform}: {result.geometric_mean_ms:.2f}ms")
```

### Full Suite with Statistics

```python
from benchbox.core.dataframe import (
    BenchmarkConfig,
    DataFrameBenchmarkSuite,
)

# Configure benchmark
config = BenchmarkConfig(
    scale_factor=0.1,
    query_ids=["Q1", "Q6", "Q10"],
    warmup_iterations=2,
    benchmark_iterations=5,
)

# Create suite
suite = DataFrameBenchmarkSuite(config=config)

# Run comparison
results = suite.run_comparison(
    platforms=["polars-df", "pandas-df"],
    data_dir=Path("benchmark_runs/tpch/sf01/data"),
)

# Get summary
summary = suite.get_summary(results)
print(f"Fastest: {summary.fastest_platform}")
print(f"Speedup ratio: {summary.speedup_ratio:.2f}x")
```

### SQL vs DataFrame

```python
from benchbox.core.dataframe import run_sql_vs_dataframe

summary = run_sql_vs_dataframe(
    sql_platform="duckdb",
    df_platform="polars-df",
    data_dir=Path("benchmark_runs/tpch/sf001/data"),
    scale_factor=0.01,
)

print(f"DataFrame faster: {summary.df_faster_count} queries")
print(f"Average speedup: {summary.average_speedup:.2f}x")
```

### Generate Charts Programmatically

```python
from benchbox.core.dataframe import (
    DataFrameBenchmarkSuite,
    DataFrameComparisonPlotter,
)

# Run comparison
suite = DataFrameBenchmarkSuite()
results = suite.run_comparison(["polars-df", "pandas-df"], data_dir)

# Generate charts
plotter = DataFrameComparisonPlotter(results, theme="dark")
exports = plotter.generate_charts(
    output_dir="charts/",
    formats=["png", "html", "svg"],
    dpi=300,
)

for chart_type, paths in exports.items():
    print(f"{chart_type}: {paths}")
```

## Platform Categories

The benchmark suite categorizes platforms by capability:

| Category | Platforms | Use Case |
|----------|-----------|----------|
| **Single Node** | Polars, Pandas, DataFusion | In-memory analysis, medium datasets |
| **Distributed** | PySpark, Dask, Modin | Large datasets, cluster computing |
| **GPU Accelerated** | cuDF | CUDA-enabled GPU acceleration |

### Platform Capabilities

```python
from benchbox.core.dataframe import PLATFORM_CAPABILITIES

for platform, cap in PLATFORM_CAPABILITIES.items():
    features = []
    if cap.supports_lazy:
        features.append("lazy")
    if cap.supports_streaming:
        features.append("streaming")
    if cap.supports_gpu:
        features.append("gpu")
    if cap.supports_distributed:
        features.append("distributed")

    print(f"{platform}: {', '.join(features) or 'standard'}")
```

## Best Practices

### 1. Start Small

Begin with SF 0.01 to verify everything works:

```bash
benchbox compare-dataframes -p polars-df -p pandas-df --scale 0.01
```

### 2. Use Multiple Iterations

Reduce variance with multiple iterations:

```bash
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  --iterations 5 \
  --warmup 2
```

### 3. Match Your Production Scale

Test at scale factors representative of your production data:

| Data Size | Recommended SF |
|-----------|----------------|
| < 1 GB | 0.01 - 0.1 |
| 1-10 GB | 0.1 - 1 |
| 10-100 GB | 1 - 10 |
| > 100 GB | 10+ (distributed platforms) |

### 4. Compare Apples to Apples

When comparing SQL vs DataFrame:
- Use the same underlying data
- Same machine/resources
- Same query semantics

### 5. Document Your Results

Use markdown output for documentation:

```bash
benchbox compare-dataframes \
  -p polars-df \
  -p pandas-df \
  --scale 1 \
  --format markdown \
  --output ./docs/benchmark_results
```

## Interpreting Results

### Geometric Mean

The geometric mean provides a single number summarizing performance across all queries. Lower is better.

### Success Rate

Percentage of queries that completed successfully. 100% expected for production-ready platforms.

### Query Winners

Shows which platform was fastest for each query. Useful for identifying platform strengths:
- Polars often wins aggregation-heavy queries
- DuckDB may win complex joins
- Pandas competitive on small datasets

### Speedup Ratio

For SQL vs DataFrame comparison:
- `> 1.0`: DataFrame faster
- `< 1.0`: SQL faster
- `~1.0`: Approximately equal

## Troubleshooting

### Data Not Found

```
Error: Data directory not found: benchmark_runs/tpch/sf001/data
```

Generate data first:

```bash
benchbox run --platform duckdb --benchmark tpch --scale 0.01
```

### Platform Not Available

```
Warning: Platform modin-df not available, skipping
```

Install the required extra:

```bash
uv add benchbox --extra dataframe-modin
```

### Memory Issues at Large Scale

For large scale factors, consider:
- Using streaming-capable platforms (Polars)
- Using distributed platforms (PySpark, Dask)
- Reducing concurrent platforms compared

## Related Documentation

- [DataFrame Benchmarks](../performance/dataframe-benchmarks.md) - Profiling and optimization
- [DataFrame Migration Guide](dataframe-migration.md) - Adopting DataFrame benchmarking
- [Platform Comparison Matrix](../platforms/comparison-matrix.md) - Full platform comparison
- [Visualization Guide](../visualization/chart-generation-guide.md) - Chart customization
