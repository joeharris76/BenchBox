# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.3] - 2026-02-23

### Added

- **Driver version pinning** - New `--platform-option driver_version=X.Y.Z` flag to pin any
  platform's Python driver to a specific version. Pair with `driver_auto_install=true` to have
  BenchBox install the requested version automatically via `uv`. All platforms support these
  options; the active driver version is now displayed in the run announcement line.
- **Bulk multi-shard table loading** - New `load_table_bulk()` interface on `FileFormatHandler`
  lets the DataLoader ingest multi-shard tables in a single native call instead of
  row-by-row iteration. DuckDB (CSV, Parquet) and ClickHouse Native handlers are the first
  implementations; benchmark runs with sharded TPC-DS data are measurably faster.
- **Greyscale / no-color ASCII chart fallbacks** - All seven ASCII chart types now have
  fill-pattern and glyph-based differentiation when color is unavailable (e.g. CI logs,
  `NO_COLOR`, piped output). `ASCIIBarChart` uses Unicode fill blocks, `ASCIIComparisonBar`
  uses hatch patterns, and the heatmap cell shading is legible without ANSI colors. A
  standardized `no_color` detection path is shared across all chart renderers, and
  ASCII-only rendering is covered by new unit tests.
- **Five new ASCII chart types** - Percentile ladder, stacked bar, sparkline table, CDF
  (cumulative distribution), rank table, and normalized speedup (log₂-scaled baseline
  comparison). All types are registered in the chart registry, accessible via CLI and MCP,
  and included in the flagship/default chart templates.
- **Post-run summary charts** - Charts are automatically generated and displayed in the
  terminal after every benchmark run and included in MCP `run_benchmark` responses.
- **Three new chart template bundles** - `latency_deep_dive`, `regression_triage`, and
  `executive_summary` template sets added alongside the existing default and flagship bundles.
- `fabric-dw` as a preferred CLI alias for `fabric_dw` platform

### Fixed

- **Driver auto-install version switching** - `sys.modules` and metadata caching could return
  stale package metadata after `driver_auto_install` swapped in a different driver version,
  causing the wrong version to be used for the remainder of the run. The module cache is
  now invalidated on version switch.
- **DataFrame cache path mismatch** - DataFrame mode cached generated data under a different
  directory structure than SQL mode, forcing redundant data generation when switching between
  modes on the same scale factor. Both modes now share a flat directory layout.
- **ClickHouse zstd double-decompression** - `ClickHouseNativeHandler` was applying manual
  zstd decompression on top of the driver's built-in decompression, corrupting data for
  compressed bulk loads.
- **Platform display names** - Corrected display names for Amazon Athena (was "AWS Athena"),
  Google Cloud Dataproc (was "GCP Dataproc"), Microsoft Azure platforms, Databricks (now
  "Databricks SQL"), and `adapter.get_platform_info()` propagated to match.
- CLI warning logged when a platform option's default value is not in the declared choices list
- Ranking normalization crash when all metric values are negative finite numbers
- PySpark SIGINT handler hanging `pytest-xdist` workers in medium-speed test runs
- `--validation-mode` CLI prompt crash when `spec.default` is not a string

### Changed

- **Four platform drivers moved to optional extras** — DuckDB (`benchbox[duckdb]`), Polars
  (`benchbox[polars]`), ClickHouse Connect (`benchbox[clickhouse-connect]`), and psycopg2
  (`benchbox[postgresql]`) are no longer hard dependencies. Users installing BenchBox now get
  a leaner core and can pin each driver independently. `pip install benchbox[all]` restores
  the previous behaviour.
- All user-facing terminal output in the run pipeline now flows through `emit()`, making
  `--quiet` suppression and output capture in tests consistent.
- `BaseQueryCatalogMixin` and `TranslatableQueryMixin` extracted from duplicate query-catalog
  implementations across benchmark classes.

## [0.1.2] - 2026-02-09

### Added

- **DataFrame mode for all benchmarks** - Complete DataFrame query implementations across all
  18 benchmarks including TPC-DS (102 queries), TPC-H (22 queries), SSB (13q), ClickBench (43q),
  NYC Taxi (25q), TSBS DevOps (18q), H2ODB, AMPLab, CoffeeShop, TPC-H Skew, and Data Vault.
  DataFrame platforms now include Polars, DuckDB, DataFusion, PySpark, Pandas, Modin, Dask,
  and cuDF (GPU).
- **ASCII chart visualizations** - Replaced Plotly HTML charts with terminal-native ASCII
  rendering. Seven chart types: performance bar, distribution box, query heatmap, comparison bar,
  diverging bar, summary box, and query latency histogram. Charts include ANSI colors, Unicode
  box-drawing, and best/worst highlighting.
- **14 new SQL platform adapters** - PostgreSQL, Trino, PrestoDB, Apache Spark, AWS Athena,
  Azure Synapse, Microsoft Fabric, Firebolt, MotherDuck, InfluxDB 3.x, TimescaleDB, ClickHouse
  Cloud (first-class), Onehouse Quanton, and managed Spark variants (EMR, Dataproc, Glue,
  Fabric Spark, Synapse Spark, Dataproc Serverless).
- **Open table format support** - Delta Lake, Apache Iceberg, Apache Hudi, DuckLake, and Vortex
  columnar format with format conversion orchestration and manifest v2 for multi-format tracking.
- **Physical tuning DDL generation** - Platform-specific DDL generators for DuckDB, Snowflake,
  Redshift, BigQuery, ClickHouse, Firebolt, PostgreSQL, TimescaleDB, Trino/Presto/Athena, and
  Spark family (Delta, Iceberg, Parquet, Hive) with sort keys, partitioning, clustering, and
  compression support.
- **Query plan capture and comparison** - Plan parsers for DuckDB, PostgreSQL, Redshift,
  DataFusion, and SQLite. Comparison engine with regression detection, fingerprinting, historical
  tracking with flapping detection, and CLI visualization.
- **Interactive CLI wizard** - Guided benchmark configuration with platform selection, tuning
  wizard, scale factor validation, phase/query selection, onboarding system, and persistent
  preferences.
- **TPC-DI benchmark** - Complete implementation across 4 phases: core schema, query suite,
  ETL pipeline, and validation/testing.
- **Cross-platform comparison engine** - `benchbox compare` command with multi-platform
  analysis, SQL vs DataFrame comparison, and unified visualization.
- **Unified tuning configuration** - YAML-based tuning system with per-platform DDL generation,
  write-time physical layout configuration, and dry-run preview support.
- Cloud storage and deployment modes for S3/GCS/ADLS/DBFS with credential setup wizard and
  cost estimation for Snowflake, Redshift, Synapse, Fabric, and Firebolt
- TPC compliance improvements: stream-aware validation, query permutations, warmup/measurement
  iterations, maintenance operations (RF1/RF2), and `--seed` for reproducibility
- Configurable compression (zstd, gzip, none) across all benchmarks and data generation
- New benchmarks: AI/ML Primitives, Metadata Primitives, Write Primitives, Transaction Primitives
- MCP server: `suggest_charts` and `generate_chart` tools, platform/mode parameters
- `--queries` flag for running specific query subsets, `--validation-mode` flag,
  tiered `--help`

### Fixed

- **TPC-DS data generation reliability** - Fixed segfaults with fractional scale factors,
  parallel generation errors, streaming compression, and chunked file handling.
- **Cloud platform stability** - Fixed credential refresh errors, schema creation ordering,
  UC Volume uploads, S3 key handling, and BigQuery/Snowflake/Redshift/Databricks adapter issues.
- **Type safety** - Multi-phase type checking campaign resolving 150+ type errors across
  production code with proper annotations and TYPE_CHECKING imports.
- **SQL dialect translation** - Fixed SQLGlot compatibility for DuckDB, ClickHouse, DataFusion,
  and Netezza dialects; resolved reserved keyword quoting and identifier case sensitivity.
- Security hardening: SQL injection prevention, parameterized queries, path traversal protection
- CLI hanging in non-interactive mode, progress display precision, `--quiet` mode propagation
- TPC compliance: correct stream permutations, maintenance phase SQL execution, Power@Size
  calculation parity between SQL and DataFrame modes

### Changed

- Dropped Plotly HTML charts in favor of ASCII-only rendering
- Lazy-load cloud platform adapters to speed up CLI startup and test suite
- Optimized TPC-DS smoke tests with selective table generation

## [0.1.1] - 2026-01-24

### Fixed

- **Critical: TPC-H/TPC-DS query templates missing from wheel distribution** - BenchBox installed
  from PyPI via wheel could not run TPC-H or TPC-DS benchmarks because query template files were
  stored outside the package tree and excluded from wheels. Templates are now bundled inside
  `benchbox/_binaries/*/templates/` with a resolution utility that checks the bundled location
  first and falls back to `_sources/` for development installs.
- **dsqgen path buffer overflow** - TPC-DS query generation could fail on systems with long temp
  directory paths (e.g., macOS `/var/folders/...`) due to dsqgen's internal 80-char path buffer.
  Fixed by using short symlinks in the temp directory.
- Python 3.10 compatibility for CLI, version utilities, and `tomllib` imports
- Windows CI test failures and cross-platform compatibility issues
- DuckDB version compatibility in tests
- MCP server: XSS prevention and strengthened path traversal checks

### Added

- MCP server: 7 new tools (`get_query_details`, `detect_regressions`, `get_performance_trends`,
  `aggregate_results`, `get_query_plan`, `export_results`, `export_summary`) and 2 prompts
- GitHub Actions PyPI publishing with trusted publishers
- Release automation: `--push`, `--auto-continue`, CI validation integration, bidirectional sync
- Platform adoption tiers (`recommended`, `supported`, `experimental`, `preview`) replacing
  boolean `recommended` field

### Changed

- Minimum Python version explicitly documented as 3.10
- MCP server refactored to use public API instead of CLI internals
- Benchmark metadata centralized into single registry
- Per-platform TPC-DS query template duplicates removed (530 files, ~4MB saved from wheel)
- MANIFEST.in expanded to include TPC patches, EULAs, and compilation infrastructure for
  sdist users who build from source

## [0.1.0] - 2026-01-10 (Initial Release)

> **Alpha Software**: BenchBox is alpha software. APIs may change without notice, features may be incomplete, and production use is not recommended. See [DISCLAIMER.md](DISCLAIMER.md) for full details.

### Overview

BenchBox v0.1.0 is the **initial public release** of the database benchmarking framework. BenchBox makes it simple to run industry-standard benchmarks (TPC-H, TPC-DS) on analytical databases, from embedded engines like DuckDB to cloud data warehouses like Snowflake and Databricks.

### What's Included

**Benchmarks** (18 total):
- **TPC Standards**: TPC-H (22 queries), TPC-DS (99 queries), TPC-DI
- **Academic**: SSB, AMPLab, JoinOrder (IMDB dataset)
- **Industry**: ClickBench, H2ODB, NYC Taxi, TSBS DevOps, CoffeeShop
- **Data Modeling**: TPC-H Data Vault
- **BenchBox Primitives**: Read Primitives, Write Primitives, Transaction Primitives
- **Experimental**: TPC-DS-OBT, TPC-Havoc, TPC-H Skew

**SQL Platforms** (16 total):
- **Embedded**: DuckDB, SQLite, DataFusion
- **Cloud Data Warehouses**: Snowflake, Databricks, BigQuery, Redshift, Azure Synapse
- **Analytical Databases**: ClickHouse, Trino, Presto, Firebolt, InfluxDB
- **General Purpose**: PostgreSQL, Spark, Athena

**DataFrame Platforms** (8 total):
- **Expression Family**: Polars, DataFusion, DuckDB, PySpark
- **Pandas Family**: Pandas, Modin, Dask, cuDF (GPU)

**Core Features**:
- Self-contained data generation (no external tools required)
- Automatic SQL dialect translation between platforms
- CLI with dry-run support, progress bars, and rich output
- Programmatic Python API for integration
- Result export in JSON, CSV, and HTML formats

### Quick Start

```bash
# Install
pip install benchbox

# Run TPC-H on DuckDB
benchbox run --platform duckdb --benchmark tpch --scale 0.01

# Run with DataFrame API
benchbox run --platform polars-df --benchmark tpch --scale 0.01
```

### Links

- **Documentation**: [GitHub Repository](https://github.com/joeharris76/benchbox)
- **Issues**: [Report bugs and request features](https://github.com/joeharris76/benchbox/issues)
- **PyPI**: [pypi.org/project/benchbox](https://pypi.org/project/benchbox/)
