(cli-run)=
# `run` - Run Benchmarks

```{tags} reference, cli, intermediate
```

The primary command for executing benchmarks. Supports interactive mode, direct execution, and various execution phases.

## Basic Syntax

```bash
benchbox run [OPTIONS]
```

## Core Options

**Platform and Benchmark Selection:**
- `--platform TEXT`: Platform type
  - SQL platforms: `duckdb`, `sqlite`, `databricks`, `bigquery`, `snowflake`, `redshift`, `clickhouse`, `datafusion`, `polars`, `trino`, `presto`
  - DataFrame platforms: `polars-df`, `pandas-df`, `pyspark-df`, `datafusion-df` (see [DataFrame Platforms](../../platforms/dataframe.md))
- `--benchmark TEXT`: Benchmark name (tpch, tpcds, tpcdi, ssb, clickbench, h2odb, etc.)
- `--scale FLOAT`: Scale factor for data generation (default: 0.01)

**Execution Control:**
- `--phases TEXT`: Benchmark phases to run (default: "power")
  - Available phases:
    - `generate` - Generate benchmark data files
    - `load` - Load data into database
    - `warmup` - Run warmup queries (optional, prepares caches)
    - `power` - Sequential query execution (Power Test)
    - `throughput` - Concurrent query execution (Throughput Test)
    - `maintenance` - **Data modification operations (DATABASE RELOAD REQUIRED AFTER)** - See [TPC-H Maintenance](../../guides/tpc/tpc-h-official-guide.md#maintenance-test) or [TPC-DS Maintenance](../../guides/tpc/tpc-ds-official-guide.md#3-maintenance-test)
  - Use comma-separated list: `--phases generate,load,power`
  - **Warning:** Maintenance phase permanently modifies data; reload database before running power/throughput again
- `--force [MODE]`: Force regeneration of data (modes: `all`, `datagen`, `upload`, or `datagen,upload`)
- `--non-interactive`: Use defaults for all prompts (useful for automation)

**Output and Paths:**
- `--output TEXT`: Output directory (supports local paths and cloud storage: s3://, gs://, abfss://)
- `--dry-run OUTPUT_DIR`: Preview configuration and queries without execution

**Query Configuration:**
- `--seed INTEGER`: An RNG seed for query parameter generation (power/throughput tests)
- `--queries TEXT`: Run specific queries in specified order (comma-separated IDs: "1,6,17")
  - **Constraints**:
    - Max 100 queries per run
    - Alphanumeric IDs only (letters, numbers, dash, underscore; max 20 chars each)
    - Only works with `power` and `standard` phases (errors if used with ONLY incompatible phases)
    - Duplicates automatically removed
    - Order preserved as specified
  - **TPC Compliance**: Breaks official TPC-H/TPC-DS compliance (overrides stream permutations)
  - **Valid Ranges**: TPC-H (1-22), TPC-DS (1-99), SSB (1-13)
  - **Examples**:
    ```bash
    # Single query for debugging
    benchbox run --platform duckdb --benchmark tpch --queries "Q6"

    # Multiple queries in specific order
    benchbox run --platform postgres --benchmark tpch --queries "Q1,Q6,Q17"

    # With verbose logging for development
    benchbox run --platform duckdb --benchmark tpcds --queries "Q42" --verbose --phases power
    ```

**Tuning and Optimization:**
- `--tuning TEXT`: Tuning mode (default: "notuning")
  - `tuned`: Enable optimizations (constraints for SQL, parallelism for DataFrame)
  - `notuning`: Disable for baseline comparison
  - `auto`: Auto-detect optimal settings based on system profile
  - Path to config file: Custom tuning configuration (YAML)

**Data Compression (Advanced):**
- `--compression TEXT`: Compression configuration (e.g., `zstd`, `zstd:9`, `gzip:6`, `none`)
  - Format: `TYPE` or `TYPE:LEVEL`
  - Types: `zstd` (default), `gzip`, `none`
  - Levels: 1-9 for gzip, 1-22 for zstd

**Validation (Advanced):**
- `--validation TEXT`: Validation mode
  - `exact`: Exact row count validation (default)
  - `loose`: Loose validation (±5% tolerance)
  - `range`: Min/max bounds validation
  - `disabled`: No validation
  - `full`: All validation checks enabled (preflight, postgen, postload, platform check)
  - `preflight`, `postgen`, `postload`, `check-platforms`: Individual validation types

**Platform-Specific Configuration:**
- `--platform-option KEY=VALUE`: Platform-specific option (can be used multiple times)
- Use `benchbox platforms status <name>` to view platform details

**Logging and Output:**
- `--verbose`, `-v`: Enable verbose logging (use `-vv` for very verbose)
- `--quiet`, `-q`: Suppress all output (overrides verbose flags)

**Help System:**
- `-h, --help`: Show common options
- `--help-topic all`: Show all options including advanced
- `--help-topic examples`: Show categorized usage examples

## Usage Examples

### Basic Local Benchmark

```bash
# Simple TPC-H benchmark on DuckDB
benchbox run --platform duckdb --benchmark tpch --scale 0.01

# With default settings
benchbox run --platform duckdb --benchmark tpch
```

### Cloud Platform Examples

```bash
# Databricks with custom output location
benchbox run --platform databricks --benchmark tpch --scale 0.1 \
  --output dbfs:/Volumes/workspace/raw/benchmarks/

# BigQuery with verbose logging
benchbox run --platform bigquery --benchmark tpcds --scale 0.01 \
  --verbose --phases power

# Snowflake with tuning enabled
benchbox run --platform snowflake --benchmark tpch --scale 1 \
  --tuning tuned
```

### DataFrame Platform Examples

```bash
# Polars DataFrame with auto-tuning
benchbox run --platform polars-df --benchmark tpch --scale 1 \
  --tuning auto

# Pandas DataFrame with PyArrow backend
benchbox run --platform pandas-df --benchmark tpch --scale 0.1 \
  --platform-option dtype_backend=pyarrow

# PySpark DataFrame (local mode)
benchbox run --platform pyspark-df --benchmark tpch --scale 1 \
  --platform-option driver_memory=8g

# Compare SQL vs DataFrame on same workload
benchbox run --platform polars --benchmark tpch --scale 0.1     # SQL mode
benchbox run --platform polars-df --benchmark tpch --scale 0.1  # DataFrame mode
```

### Advanced Phase Control

```bash
# Data generation only
benchbox run --benchmark tpch --scale 0.1 --phases generate \
  --output ./tpch-data

# Load data into database
benchbox run --platform duckdb --benchmark tpch --scale 0.1 \
  --phases load

# Full TPC-DS power and throughput test
benchbox run --platform duckdb --benchmark tpcds --scale 0.01 \
  --phases power,throughput --seed 42
```

### Dry Run and Preview

```bash
# Preview configuration without execution
benchbox run --platform duckdb --benchmark tpch --scale 0.1 \
  --dry-run ./preview

# Preview with specific seed for reproducible queries
benchbox run --platform snowflake --benchmark tpcds \
  --phases power --dry-run ./debug --seed 7
```

### Platform-Specific Options

```bash
# ClickHouse with local mode and TLS
benchbox run --platform clickhouse --benchmark tpch \
  --platform-option mode=local \
  --platform-option secure=true

# Show platform details and capabilities
benchbox platforms status clickhouse
```

## Related

- [Configuration](configuration.md) - Configuration files and environment variables
- [Platform Options](configuration.md#platform-specific-options) - Platform-specific settings
- [Tuning Commands](tuning.md) - Generate and manage tuning configurations
