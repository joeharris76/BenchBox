# Parquet Deep Dive <!-- content-ok: cliche -->

```{tags} guide, formats, parquet
```

Apache Parquet is the de facto standard for analytical data storage. Understanding row groups, column chunks, and compression options helps you run better benchmarks.

## Overview

Apache Parquet has become the universal language of analytical data. Created in 2013 as a collaboration between Twitter and Cloudera, it's now an Apache top-level project with support across virtually every analytics platform.

We made Parquet the default format in BenchBox because it's the one format we can count on everywhere. DuckDB, Spark, Snowflake, BigQuery, Polars, DataFusion: they all read Parquet natively. This universality matters when you're running the same benchmark across platforms.

But using Parquet effectively requires understanding a few core concepts. This guide covers what benchmarkers need to know: row groups, column chunks, compression options, and when (rarely) to tune these settings.

## Why Columnar Matters for Analytics

Before diving into Parquet specifics, let's understand why columnar storage helps analytical workloads.

### Row vs Columnar Storage

| Storage Type | Data Layout | Best For |
|--------------|-------------|----------|
| Row-oriented | All columns for row 1, then all columns for row 2... | OLTP, full-row access |
| Columnar | All values for column 1, then all values for column 2... | OLAP, analytical queries |

Analytical queries typically read few columns but many rows:

```sql
-- Only needs 'revenue' column, but all 6M rows
SELECT SUM(l_extendedprice) FROM lineitem;

-- Only needs 4 columns out of 16
SELECT l_returnflag, l_linestatus, SUM(l_quantity)
FROM lineitem
GROUP BY l_returnflag, l_linestatus;
```

With row storage, the query reads entire rows, including columns it doesn't need. With columnar storage, it reads only the columns requested.

### Analytical Query Patterns

Parquet enables several optimizations for analytical queries:

- **Column pruning**: Read only required columns, skip the rest
- **Predicate pushdown**: Apply filters at the storage layer before loading into memory
- **Better compression**: Similar values group together within columns
- **Vectorized processing**: Process columns in batches using CPU SIMD instructions

## Parquet File Structure

### Row Groups

Parquet files are divided into row groups, chunks of rows stored together. Each row group is independently readable, enabling parallel processing.

```
lineitem.parquet
├── Row Group 0 (rows 0-999,999)
│   ├── Column: l_orderkey (INT64, compressed)
│   ├── Column: l_quantity (DECIMAL, compressed)
│   └── Column: l_extendedprice (DECIMAL, compressed)
├── Row Group 1 (rows 1,000,000-1,999,999)
│   └── ...
└── Footer (schema, statistics, offsets)
```

**Why row groups matter for benchmarks:**

**Parallelism**: Each row group can be processed independently. More row groups enable more parallel tasks, up to a point.

**Memory**: Larger row groups require more memory to process. For memory-constrained environments, smaller row groups help.

**Statistics**: Parquet stores min/max values per column per row group. Queries with range filters can skip row groups entirely.

**BenchBox default**: 1 million rows per row group (the Parquet default). This works well for most benchmark scenarios.

### Column Chunks

Within each row group, data is stored by column. This is where columnar benefits appear.

**Column pruning**: A query selecting 4 columns from a 16-column table reads 25% of the data.

**Better compression**: Similar values group together. A column of dates compresses better than mixed-type rows.

**Vectorized processing**: Modern query engines process columns in batches (vectors), exploiting CPU SIMD instructions.

### Footer and Statistics

Parquet stores statistics in the file footer:

- **Min/max values** per column per row group
- **Null counts**
- **Distinct counts** (optional)
- **Bloom filters** (optional, for string lookups)

**How statistics affect benchmarks:**

```sql
-- Query with range filter
SELECT * FROM lineitem WHERE l_shipdate > '1998-01-01';
```

If a row group's max l_shipdate is '1997-12-31', the query engine skips that entire row group. This predicate pushdown happens automatically based on statistics.

Statistics overhead is minimal (less than 1% of file size) but the query optimization benefits can be substantial.

## Compression in Parquet

> For quick compression settings, see [Format Conversion: Compression Guide](../../advanced/format-conversion.md#compression-guide).

### Algorithm Comparison

Parquet supports several compression algorithms:

| Algorithm | Speed | Ratio | When to use |
|-----------|-------|-------|-------------|
| Snappy | Fastest | Good | Low-latency queries, interactive workloads |
| LZ4 | Fast | Good | Balance of speed and compression |
| Zstd | Medium | Best | Storage efficiency, batch workloads |
| Gzip | Slow | Good | Compatibility with older tools |

**BenchBox recommendation**: Zstd for most benchmarks. It provides the best compression ratio with acceptable decompression speed. At scale factor 10 and above, smaller files mean less I/O, which often outweighs the decompression cost.

### Compression Levels

Zstd supports levels 1-22. Higher levels provide better compression at the cost of slower writes:

| Priority | Recommended | Reason |
|----------|-------------|--------|
| Storage cost | Zstd:9 | Smallest files |
| Query speed | Snappy | Fastest decompression |
| Balanced | Zstd:3 | BenchBox default |
| Debugging | None | No decompression overhead |

For most benchmarks, the default Zstd:3 works well. Higher compression levels (Zstd:9) save storage but add write time. Snappy trades ~20% larger files for faster decompression.

### When to Adjust Compression

```bash
# Default (Zstd level 3)
benchbox run --platform duckdb --benchmark tpch --scale 1

# Higher compression (smaller files, slower write)
benchbox run --platform duckdb --benchmark tpch --compression zstd:9

# Faster compression (larger files, faster write)
benchbox run --platform duckdb --benchmark tpch --compression snappy

# No compression (debugging, baseline)
benchbox run --platform duckdb --benchmark tpch --compression none
```

## Row Group Tuning

### When to Increase Row Group Size

- Running at SF100+ (reduce footer overhead)
- Using cloud storage (fewer S3/GCS GET requests)
- Memory is not constrained

### When to Decrease Row Group Size

- Maximizing parallelism on many-core systems
- Running in memory-constrained environments
- Debugging query execution patterns

BenchBox uses the 1M row default, which works for most scenarios. Custom row group sizes require manual data generation with PyArrow.

## Best Practices for Benchmarking

### Recommended Settings

```bash
# Standard benchmark (reproducible, efficient)
benchbox run --platform duckdb --benchmark tpch --scale 10 --compression zstd:3

# Storage-focused comparison
benchbox run --platform duckdb --benchmark tpch --scale 10 --compression zstd:9

# Performance baseline (measure decompression impact)
benchbox run --platform duckdb --benchmark tpch --scale 10 --compression none
```

### Common Mistakes to Avoid

**1. Using CSV for benchmarks**

CSV parsing adds overhead that skews results. A "fast" platform might just be a better CSV parser, not a better query engine.

**2. Ignoring compression settings**

Different tools use different defaults. DuckDB might generate Snappy, while Spark uses Zstd. Specify compression explicitly for reproducible comparisons.

**3. Comparing across formats**

Parquet vs CSV isn't a fair comparison. If you're evaluating formats, compare Parquet to Parquet with different settings, or to other columnar formats like ORC or Vortex.

**4. Forgetting about statistics**

Queries that filter on columns with good min/max separation benefit from predicate pushdown. If your benchmark doesn't include filtering queries, you're missing a key Parquet optimization.

## See Also

- [Table Format Guides](index.md): Overview of all formats
- [Format Conversion Reference](../../advanced/format-conversion.md): CLI commands for format conversion
- [Compression Guide](../compression.md): Data compression strategies
- [Delta Lake Guide](delta-lake-guide.md): Table format built on Parquet
