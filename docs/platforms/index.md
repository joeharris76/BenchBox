<!-- Copyright 2026 Joe Harris / BenchBox Project. Licensed under the MIT License. -->

# Platform Documentation

```{tags} guide
```

Documentation for database platforms supported by BenchBox.

## Platform Guides

- [Platform Selection Guide](platform-selection-guide.md) - Choose the right platform for your needs
- [Quick Reference](quick-reference.md) - Quick comparison and feature matrix
- [Comparison Matrix](comparison-matrix.md) - Detailed platform comparison
- [**Deployment Modes**](deployment-modes.md) - Platform deployment architecture (local, self-hosted, cloud)

## DataFrame Platforms (Native API)

BenchBox supports benchmarking DataFrame libraries using their native APIs instead of SQL. This enables direct performance comparison between SQL and DataFrame paradigms on identical workloads.

- [**DataFrame Platforms Overview**](dataframe.md) - Architecture, installation, and usage guide

### Available DataFrame Platforms

| Platform | CLI Name | Family | Status | Documentation |
|----------|----------|--------|--------|---------------|
| **Polars** | `polars-df` | Expression | Production-ready | [Polars DataFrame](polars.md) |
| **Pandas** | `pandas-df` | Pandas | Production-ready | [Pandas DataFrame](pandas-dataframe.md) |
| **Modin** | `modin-df` | Pandas | Production-ready | [Modin DataFrame](modin-dataframe.md) |
| **Dask** | `dask-df` | Pandas | Production-ready | [Dask DataFrame](dask-dataframe.md) |
| **cuDF** | `cudf-df` | Pandas | Production-ready | [cuDF DataFrame](cudf.md) |
| **PySpark** | `pyspark-df` | Expression | Production-ready | [PySpark DataFrame](pyspark-dataframe.md) |
| **DataFusion** | `datafusion-df` | Expression | Production-ready | [DataFusion DataFrame](datafusion-dataframe.md) |

```bash
# Quick start with DataFrame platforms
benchbox run --platform polars-df --benchmark tpch --scale 0.1    # Recommended - fast
benchbox run --platform pandas-df --benchmark tpch --scale 0.1    # Familiar API
benchbox run --platform modin-df --benchmark tpch --scale 0.1     # Parallel Pandas
benchbox run --platform dask-df --benchmark tpch --scale 0.1      # Distributed
benchbox run --platform cudf-df --benchmark tpch --scale 0.1      # GPU (Linux only)
benchbox run --platform pyspark-df --benchmark tpch --scale 0.1   # Spark ecosystem
benchbox run --platform datafusion-df --benchmark tpch --scale 0.1

# Compare SQL vs DataFrame on same workload
benchbox run --platform polars --benchmark tpch --scale 0.1       # SQL mode
benchbox run --platform polars-df --benchmark tpch --scale 0.1    # DataFrame mode
```

## SQL Platforms

### Core Local Databases

These platforms are included in the base BenchBox installation with no additional dependencies:

- [**DuckDB**](duckdb.md) - Embedded analytical database (default local platform)
- [**SQLite**](sqlite.md) - Embedded row-store database for lightweight testing

### Local/Embedded Analytics Engines

- [Apache DataFusion](datafusion.md) - Rust-based in-memory query engine
- [ClickHouse Local Mode](clickhouse-local-mode.md) - Running ClickHouse embedded
- [Polars](polars.md) - High-performance DataFrame library with SQL support

### Traditional Relational Databases

- [PostgreSQL](postgresql.md) - Open-source relational database with TimescaleDB support

### Distributed SQL Engines

- [PrestoDB](presto.md) - Distributed SQL query engine (Facebook fork)
- [Trino](trino.md) - Distributed SQL query engine (community fork, formerly PrestoSQL)
- [Apache Spark](spark.md) - Unified analytics engine for large-scale data processing

### Cloud Data Warehouses

- [**Snowflake**](snowflake.md) - Cloud-native data platform (AWS, Azure, GCP)
- [**Databricks**](databricks.md) - Lakehouse platform with Unity Catalog support
- [**Google BigQuery**](bigquery.md) - Serverless data warehouse
- [**Amazon Redshift**](redshift.md) - Columnar data warehouse
- [**MotherDuck**](motherduck.md) - Serverless DuckDB cloud (inherits DuckDB dialect)
- [**Starburst**](starburst.md) - Managed Trino / Starburst Galaxy (inherits Trino dialect)
- [AWS Athena](athena.md) - Serverless query service for S3
- [Firebolt](firebolt.md) - High-performance cloud analytics (Core + Cloud modes)
- [Azure Synapse Analytics](azure-platforms.md) - Microsoft cloud analytics platform
- [Microsoft Fabric](microsoft-fabric.md) - Microsoft unified analytics platform

### GPU-Accelerated Platforms

- [CUDF](cudf.md) - NVIDIA RAPIDS GPU-accelerated DataFrames

## Platform Categories

### By Installation Complexity

**Zero Config** (included in base install):
- DuckDB, SQLite

**Single Extra** (one `pip install` command):
- DataFusion, Polars, PostgreSQL, ClickHouse

**Cloud SDK Required** (authentication setup needed):
- Databricks, BigQuery, Redshift, Snowflake, Athena, Firebolt, Azure Synapse

**Infrastructure Required** (external cluster needed):
- Trino, Presto, Spark, ClickHouse (server mode)

### By Use Case

**Local Development & Testing:**
- DuckDB (recommended), SQLite, DataFusion, Polars

**Production Benchmarking:**
- Databricks, Snowflake, BigQuery, Redshift

**Self-Hosted Analytics:**
- ClickHouse, PostgreSQL, Trino, Presto, Spark

**GPU Workloads:**
- CUDF (NVIDIA GPUs required)

## Quick Start by Platform

### Local Platforms (No Setup Required)

```bash
# DuckDB - Default, included in base install
benchbox run --platform duckdb --benchmark tpch --scale 0.01

# SQLite - Included in base install
benchbox run --platform sqlite --benchmark tpch --scale 0.01
```

### Cloud Platforms (Credentials Required)

```bash
# Databricks - Requires DATABRICKS_TOKEN and DATABRICKS_HOST
benchbox run --platform databricks --benchmark tpch --scale 1.0

# BigQuery - Requires GOOGLE_APPLICATION_CREDENTIALS
benchbox run --platform bigquery --benchmark tpch --scale 1.0

# Snowflake - Requires SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT
benchbox run --platform snowflake --benchmark tpch --scale 1.0
```

## Future Platforms

- [Future Platforms](future-platforms.md) - Planned platform support and roadmap

## Related Documentation

- [Getting Started](../usage/getting-started.md) - Quick start guide
- [Cloud Storage Guide](../guides/cloud-storage.md) - Cloud storage configuration for staging data
- [Platform Configuration](../usage/configuration.md) - Detailed configuration options

```{toctree}
:maxdepth: 1
:caption: Platform Guides
:hidden:

platform-selection-guide
quick-reference
comparison-matrix
deployment-modes
dataframe
duckdb
sqlite
polars
pandas-dataframe
modin-dataframe
dask-dataframe
cudf
pyspark-dataframe
datafusion-dataframe
spark
clickhouse-local-mode
postgresql
presto
trino
snowflake
databricks
bigquery
redshift
motherduck
starburst
athena
athena-spark
firebolt
microsoft-fabric
azure-platforms
datafusion
influxdb
timescaledb
aws-glue
gcp-dataproc
dataproc-serverless
emr-serverless
fabric-spark
synapse-spark
snowpark-connect
future-platforms
```
