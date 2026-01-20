# Open Table Formats Guide

```{tags} advanced, guide
```

Convert benchmark data to modern columnar formats for improved query performance, storage efficiency, and platform compatibility.

## Overview

BenchBox supports three open table formats for storing benchmark data:

| Format | Type | Key Features | Best For |
|--------|------|--------------|----------|
| **Parquet** | File | Fast, universal support | General analytics, portability |
| **Delta Lake** | Table | ACID, time travel | Production data lakes |
| **Iceberg** | Table | Schema evolution, hidden partitioning | Enterprise data platforms |

## Why Use Columnar Formats?

TPC benchmarks generate data in TBL format (pipe-delimited text files). Converting to columnar formats provides:

### Performance Benefits

- **Faster queries**: Columnar storage enables reading only required columns (improvement varies by query selectivity)
- **Predicate pushdown**: Filter data at the storage layer before loading into memory
- **Partition pruning**: Skip irrelevant data partitions entirely
- **Statistics-based optimization**: Min/max values and row counts per column chunk

### Storage Benefits

- **3-5x compression**: Columnar layout enables efficient encoding and compression
- **Type-aware encoding**: Dictionary encoding for strings, RLE for repeated values
- **Compression algorithms**: Snappy, Gzip, or Zstd for different size/speed tradeoffs

### Platform Compatibility

| Platform | Parquet | Delta Lake | Iceberg |
|----------|---------|------------|---------|
| DuckDB | Native | Extension | - |
| Spark | Native | Native | Native |
| Databricks | Native | Native | Native |
| Snowflake | External | External | Native |
| BigQuery | External | - | External |
| Polars | Native | Native | - |
| DataFusion | Native | - | - |

## Quick Start

### Basic Conversion

```bash
# Generate TPC-H data first
benchbox run --platform duckdb --benchmark tpch --scale 1 --phases generate

# Convert to Parquet
benchbox convert --input ./benchmark_runs/tpch_sf1 --format parquet
```

### Query Converted Data

```python
import duckdb

# Connect and query Parquet files
conn = duckdb.connect()
result = conn.execute("""
    SELECT l_returnflag, SUM(l_extendedprice) as revenue
    FROM read_parquet('./benchmark_runs/tpch_sf1/lineitem.parquet')
    GROUP BY l_returnflag
""").fetchdf()
```

## Format Deep Dive

### Apache Parquet

The most widely supported columnar format. Ideal for analytics workloads and data interchange.

**Structure:**
```
customer.parquet
├── Row Group 0
│   ├── Column: c_custkey (INT64, snappy)
│   ├── Column: c_name (STRING, dictionary)
│   └── Column: c_acctbal (DECIMAL, snappy)
├── Row Group 1
│   └── ...
└── Footer (schema, statistics)
```

**Usage:**
```bash
# Basic conversion
benchbox convert --input ./data --format parquet

# With Zstd compression (best ratio)
benchbox convert --input ./data --format parquet --compression zstd

# Partitioned by date
benchbox convert --input ./data --format parquet --partition l_shipdate
```

**Reading Parquet:**
```python
# DuckDB
conn.execute("SELECT * FROM read_parquet('customer.parquet')")

# PyArrow
import pyarrow.parquet as pq
table = pq.read_table('customer.parquet')

# Polars
import polars as pl
df = pl.read_parquet('customer.parquet')

# Pandas
import pandas as pd
df = pd.read_parquet('customer.parquet')
```

### Delta Lake

Open table format with ACID transactions, built on Parquet files.

**Structure:**
```
customer/
├── _delta_log/
│   ├── 00000000000000000000.json  # Initial commit
│   └── 00000000000000000001.json  # Update commit
├── part-00000-*.parquet
└── part-00001-*.parquet
```

**Features:**
- **ACID Transactions**: Concurrent reads/writes with isolation
- **Time Travel**: Query historical versions of data
- **Schema Evolution**: Add/remove columns without rewriting
- **Unified Batch/Streaming**: Same table for both workloads

**Installation:**
```bash
uv add deltalake
```

**Usage:**
```bash
# Convert to Delta Lake
benchbox convert --input ./data --format delta

# With compression
benchbox convert --input ./data --format delta --compression zstd
```

**Reading Delta Lake:**
```python
# Python deltalake library
from deltalake import DeltaTable
dt = DeltaTable('./customer')
df = dt.to_pandas()

# DuckDB (requires delta extension)
conn.execute("INSTALL delta; LOAD delta;")
conn.execute("SELECT * FROM delta_scan('./customer')")

# Spark
df = spark.read.format("delta").load("./customer")
```

**Time Travel:**
```python
# Query specific version
dt = DeltaTable('./customer', version=0)

# Query by timestamp
dt = DeltaTable('./customer', as_of='2024-01-15T10:00:00')
```

### Apache Iceberg

Modern table format designed for large-scale data lakes with enterprise features.

**Structure:**
```
customer/
├── metadata/
│   ├── v1.metadata.json
│   ├── snap-*.avro  # Snapshot manifests
│   └── *.avro       # Manifest files
└── data/
    └── *.parquet    # Data files
```

**Features:**
- **Hidden Partitioning**: Partition columns not exposed in queries
- **Schema Evolution**: Full schema changes without data rewrite
- **Snapshot Isolation**: Consistent reads during writes
- **Partition Evolution**: Change partitioning without rewriting data

**Installation:**
```bash
uv add pyiceberg
```

**Usage:**
```bash
# Convert to Iceberg
benchbox convert --input ./data --format iceberg

# With partitioning
benchbox convert --input ./data --format iceberg --partition l_shipdate
```

**Reading Iceberg:**
```python
# PyIceberg
from pyiceberg.catalog import load_catalog
catalog = load_catalog("local", **{"type": "sql", "uri": "sqlite:///catalog.db"})
table = catalog.load_table("default.customer")
df = table.scan().to_pandas()

# Spark
df = spark.read.format("iceberg").load("./customer")
```

## Compression Guide

### Compression Algorithms

| Algorithm | Compression | Speed | Use Case |
|-----------|-------------|-------|----------|
| **Snappy** | Moderate | Fast | Default, good balance |
| **Gzip** | Good | Slow | Cold storage, archival |
| **Zstd** | Best | Moderate | Large datasets, cloud storage |
| **None** | None | Fastest | Debugging, already compressed |

### Compression Benchmarks (TPC-H SF=10)

```
Format          | Algorithm | Size (MB) | Compress Time | Query Time
----------------|-----------|-----------|---------------|------------
TBL (raw)       | -         | 10,240    | -             | baseline
Parquet         | snappy    | 3,413     | 45s           | 0.32x
Parquet         | gzip      | 2,560     | 120s          | 0.35x
Parquet         | zstd      | 2,048     | 65s           | 0.33x
Delta Lake      | snappy    | 3,450     | 48s           | 0.34x
```

### Choosing Compression

```bash
# Fast iteration during development
benchbox convert --input ./data --format parquet --compression snappy

# Production with best compression
benchbox convert --input ./data --format parquet --compression zstd

# Cloud storage (minimize transfer costs)
benchbox convert --input ./data --format parquet --compression zstd
```

## Partitioning Guide

### When to Partition

Partitioning is beneficial when:
- Table has >1M rows
- Queries frequently filter on specific columns (dates, regions, status)
- Data has natural partitioning boundaries

### Partition Column Selection

**Good partition columns:**
- Date/time columns (l_shipdate, o_orderdate)
- Categorical columns with 10-1000 distinct values (l_returnflag, c_mktsegment)
- Region/geography columns

**Bad partition columns:**
- High cardinality columns (customer_id, order_id)
- Columns rarely used in filters

### Partitioning Examples

```bash
# Single partition column (date-based)
benchbox convert --input ./data --format parquet --partition l_shipdate
# Creates: lineitem/l_shipdate=1996-01-01/part-*.parquet

# Multiple partition columns (hierarchical)
benchbox convert --input ./data --format parquet \
    --partition l_returnflag --partition l_linestatus
# Creates: lineitem/l_returnflag=N/l_linestatus=O/part-*.parquet
```

### Querying Partitioned Data

```sql
-- DuckDB: Hive partitioning
SELECT * FROM read_parquet('./lineitem/**/*.parquet', hive_partitioning=true)
WHERE l_shipdate = '1996-03-15';  -- Only reads relevant partition

-- Partition pruning filters are pushed down automatically
```

## Platform Integration

### DuckDB

```python
import duckdb

conn = duckdb.connect()

# Parquet
conn.execute("SELECT * FROM read_parquet('./data/*.parquet')")

# Delta Lake
conn.execute("INSTALL delta; LOAD delta;")
conn.execute("SELECT * FROM delta_scan('./data/customer')")

# Partitioned Parquet
conn.execute("""
    SELECT * FROM read_parquet('./lineitem/**/*.parquet', hive_partitioning=true)
    WHERE l_shipdate >= '1996-01-01'
""")
```

### Polars

```python
import polars as pl

# Parquet
df = pl.read_parquet('./data/customer.parquet')

# Delta Lake
df = pl.read_delta('./data/customer')

# Lazy evaluation for large datasets
lf = pl.scan_parquet('./data/*.parquet')
result = lf.filter(pl.col('c_acctbal') > 1000).collect()
```

### PySpark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Parquet
df = spark.read.parquet('./data/customer.parquet')

# Delta Lake
df = spark.read.format("delta").load('./data/customer')

# Iceberg
df = spark.read.format("iceberg").load('./data/customer')
```

### Databricks

```python
# Parquet (auto-detected)
df = spark.read.load('/mnt/data/customer.parquet')

# Delta Lake (default format)
df = spark.read.load('/mnt/data/customer')

# Iceberg
df = spark.read.format("iceberg").load('/mnt/data/customer')
```

## Best Practices

### 1. Choose the Right Format

| Scenario | Recommended Format |
|----------|-------------------|
| One-time analysis | Parquet |
| Shared datasets | Parquet |
| Production data lake | Delta Lake |
| Multi-engine access | Iceberg |
| Schema changes expected | Iceberg |
| Time travel needed | Delta Lake |

### 2. Compression Strategy

```bash
# Development (fast iteration)
--compression snappy

# Production (storage optimization)
--compression zstd

# Cloud storage (minimize costs)
--compression zstd
```

### 3. Validation

```bash
# Always validate for TPC compliance
benchbox convert --input ./data --format parquet --validate

# Skip validation only for exploratory work
benchbox convert --input ./data --format parquet --no-validate
```

### 4. Directory Organization

```
benchmark_runs/
├── tpch_sf1_tbl/           # Raw TBL files
├── tpch_sf1_parquet/       # Parquet conversion
├── tpch_sf10_delta/        # Delta Lake for larger scale
└── tpch_sf100_iceberg/     # Iceberg for production
```

## Troubleshooting

### Conversion Fails with "deltalake not installed"

```bash
uv add deltalake
```

### Conversion Fails with "pyiceberg not installed"

```bash
uv add pyiceberg
```

### Row Count Mismatch

If validation fails with row count mismatch:
1. Check source TBL files for corruption
2. Regenerate data with `--force datagen`
3. If intentional, use `--no-validate`

### Large File Handling

For very large scale factors (SF > 100):
```bash
# Use Zstd for best compression
benchbox convert --input ./data --format parquet --compression zstd

# Skip validation if conversion is slow
benchbox convert --input ./data --format parquet --no-validate
```

## See Also

- [CLI Reference: convert](../reference/cli/convert.md) - Full command options
- [Data Generation](../usage/data-generation.md) - Generating benchmark data
- [DuckDB Platform](../platforms/duckdb.md) - DuckDB integration
- [Performance Optimization](performance-optimization.md) - Query performance tuning
