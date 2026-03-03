---
blogpost: true
date: Feb 15, 2026
author: Joe Harris
tags: benchbox, release, changelog, dataframe, visualization, platforms
---

# BenchBox v0.1.2: release summary

BenchBox v0.1.2 was released on **February 14, 2026**.

This post is a summary of what changed in this version, including a shift to ASCII visualizations.

![ASCII query histogram after a TPC-H run](images/query_histogram.png)

## TL;DR

- Charts are now **terminal-native ASCII** by default; Plotly HTML export is removed.
- DataFrame mode now covers major suites including TPC-H, TPC-DS, SSB, ClickBench, NYC Taxi, and TSBS DevOps, plus more DataFrame engines.
- SQL platform and table-format support expanded significantly.
- Query plan analysis, tuning, and comparison workflows are more integrated.
- Reliability improved across TPC-DS generation, cloud adapters, and non-interactive CLI runs.

## At a glance

| Area            | What changed in v0.1.2                                                      | Why it matters                                          |
| --------------- | --------------------------------------------------------------------------- | ------------------------------------------------------- |
| Visualization   | Plotly removed, ASCII charts added                                          | Works inline in terminal, CI logs, and MCP responses    |
| Execution modes | DataFrame coverage expanded across TPC-H, TPC-DS, SSB, ClickBench, and more | SQL and DataFrame benchmarking are now closer to parity |
| Platforms       | Many new SQL adapters added                                                 | Wider portability for the same benchmark workflow       |
| Table formats   | Delta/Iceberg/Hudi/DuckLake/Vortex support expanded                         | Easier format-aware benchmarking workflows              |
| Analysis        | Query plan capture/comparison improved                                      | Faster root-cause investigation for regressions         |
| Operations      | Type safety, CLI behavior, cloud stability fixes                            | More predictable runs and fewer setup/runtime surprises |

## What changed for typical workflows

### 1. Visualization and reporting

**Before**: chart generation often produced HTML artifacts that were useful in browser-centric workflows.

**Now**: chart output is optimized for text-first workflows.

- Inline ASCII charts are shown in terminal output and tool responses.
- The default experience is no longer dependent on optional visualization extras.
- This is especially useful for MCP/agent use, where inline content is easier to consume than file paths.

### 2. SQL and DataFrame usage

DataFrame mode is no longer a narrow path. v0.1.2 extends DataFrame query implementations across TPC-H, TPC-DS, SSB, ClickBench, NYC Taxi, and TSBS DevOps, plus additional BenchBox suites.

Practical impact:

- Teams can evaluate SQL and DataFrame execution styles within one framework.
- Comparative workflows (same benchmark, different mode/platform) are easier to run consistently.
- The release prioritizes breadth, but validation and consistency checks remain critical as coverage grows.

### 3. Platform and format breadth

v0.1.2 adds significant SQL platform coverage and broadens open table format support.

Practical impact:

- More teams can run BenchBox against their existing stack without custom adapters.
- Format-level benchmarking scenarios are easier to model and compare.
- Cross-platform analysis becomes more useful because platform diversity increased.

## Major additions

### DataFrame mode expansion

v0.1.2 expands DataFrame implementations across the core benchmark groups, including:

- TPC-H
- TPC-DS
- SSB
- ClickBench
- NYC Taxi
- TSBS DevOps
- Other BenchBox benchmark suites

Supported DataFrame engines in the release include Polars, DuckDB DataFrame paths, DataFusion, PySpark, Pandas, Modin, Dask, and cuDF.

### ASCII visualization as default

Key properties of the new default charting path:

- Inline rendering for CLI and MCP responses
- ANSI + Unicode terminal output with fallback behavior
- Focus on benchmark-friendly summaries and comparisons

Trade-off to note:

- Plotly HTML/PNG/SVG workflow is no longer the built-in default path.

### SQL adapter expansion

v0.1.2 adds many platform adapters and improves managed/cloud coverage, including PostgreSQL, Trino/Presto families, Spark variants, Athena, Synapse/Fabric paths, and other warehouse engines documented in the changelog.

Notable additions called out in the v0.1.2 changelog:

- PostgreSQL
- Trino and PrestoDB
- Apache Spark and managed Spark variants
- AWS Athena
- Azure Synapse and Microsoft Fabric
- Firebolt and MotherDuck

### Query-plan and comparison tooling

v0.1.2 improves plan capture and cross-run comparison workflows, including parser coverage and regression detection paths.

Practical impact:

- Easier to tie performance changes to execution plan differences.
- Better support for "run, compare, explain" loops during optimization work.

## Major fixes and stability work

v0.1.2 includes substantial reliability improvements that are easy to miss if you only scan feature headlines.

### TPC-DS generation and load reliability

- Fixes for fractional scale-factor generation edge cases
- Improvements around streaming compression/chunk handling
- Reduced failure modes in data generation and load flows

### Cloud and adapter stability

- Credential refresh and setup-ordering fixes
- Path/key handling fixes for storage-backed workflows
- Adapter-specific correctness and robustness improvements

### CLI and quality improvements

- Non-interactive mode behavior fixes
- `--quiet` propagation and output behavior improvements
- Broad type-safety cleanup across production paths

## Changed behavior to be aware of

- Plotly-based chart output is removed in favor of ASCII-first rendering.
- Some visualization expectations from older runs/docs may no longer match v0.1.2 defaults.
- If you depended on browser-native chart artifacts, plan a separate export/visualization path.

## Quick upgrade checks

After upgrading to v0.1.2:

1. Confirm installed version:

```bash
benchbox --version
```

2. Run a smoke benchmark with non-interactive settings:

```bash
benchbox run --platform duckdb --benchmark tpch --scale 0.01 --phases power --non-interactive
```

3. Validate chart behavior in your environment:

```bash
benchbox visualize benchmark_runs/results/<result_file>.json
```

4. If your team uses DataFrame mode, run one benchmark in SQL mode and one in DataFrame mode to confirm expected parity and runtime characteristics.

## Bottom line

v0.1.2 is a large release focused on practical usability:

- better default output for terminal-centric workflows,
- broader benchmark/platform coverage,
- and stronger reliability for repeated benchmark operations.

For teams already using BenchBox, this version is mainly about **faster feedback loops with less friction**.

## Reference

- Changelog entry: `CHANGELOG.md` (`[0.1.2] - 2026-02-14`)
