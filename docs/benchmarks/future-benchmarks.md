# Potential Future Benchmarks

```{tags} advanced, concept
```

BenchBox already covers the staple analytical suites (TPC-H, TPC-DS, SSB, ClickBench, …), but several roadmap items are tracking additional, real-world scenarios and benchmark variants. The sections below summarize the open work captured in the `benchmark-expansion` worktree so users and contributors can see what is being explored next.

> These initiatives are in research or early implementation. Timelines are not committed, and scope may evolve with community feedback.

## NYC Taxi Analytics Benchmark

Real-world TLC taxi trip data brings multi-dimensional OLAP workloads that complement our synthetic suites.

- **Highlights:** deterministic sampling from 3B+ TLC trip records, reusable downloader, schema + benchmark module under `benchbox/core/nyctaxi/`, platform-optimised loaders for DuckDB, ClickHouse, PostgreSQL.
- **Planned deliverables:**
  - Data pipeline covering resumable downloads, cleaning, compression handling, and scale factors from 0.01 to 100.
  - Query pack of ~25 analytics queries inspired by Todd Schneider, ClickHouse docs, DuckDB demos, and Mark Litwintschik’s studies.
  - Tests validating scale-factor sampling, data quality, and benchmark execution across supported engines.
- **Key considerations:** large dataset handling, deterministic sampling for reproducibility, tuning bundles per platform.

### Geospatial Extension (NYC Taxi)

A companion effort adds spatial analytics on top of the taxi dataset.

- **Motivation:** compare PostGIS, DuckDB Spatial, ClickHouse geo functions, and other engines on realistic geospatial workloads.
- **Scope:** geometry-aware schema extensions, polygon joins with taxi zones, CRS management, platform-specific spatial indexes, and 10–15 spatial queries per engine (SQLGlot cannot translate these automatically, so bespoke implementations are planned).
- **Outputs:** capability detection in adapters, spatial examples/docs, spatial validation tests.

## Open Data Lake Format Benchmarks

Benchmarking directly against Parquet files or open table formats (Delta Lake, Apache Iceberg) reflects how many lakehouse deployments operate today.

- **What’s planned:**
  - Data generators that emit Parquet with configurable compression/partitioning.
  - Benchmarks that run against Delta/Iceberg tables when a platform (Databricks, Snowflake, BigQuery, Redshift, DuckDB, ClickHouse) supports the format natively.
  - CLI options to pick output formats plus validation/metadata management for schema evolution.
- **Considerations:** format capability detection per adapter, format-specific tuning guidance, keeping manifest + validation tooling in sync.

## TPC-H Skew Variant

The TPC-H Skew proposal introduces realistic data distributions to stress query optimisers under uneven workloads.

- **Goals:** implement skew-aware data generation, maintain referential integrity, add skew-focused query variants, and provide comparison tooling versus uniform TPC-H runs.
- **Planned phases:** research distribution parameters (Zipfian, normal, exponential), extend generators/schema, build analysis utilities for skew visualisation, and integrate regression tests that compare skewed vs uniform performance.

## Time-Series Benchmark Suite (TSBS)

Two TSBS flavours—DevOps and IoT—are being evaluated to cover high-ingest, temporal analytics workloads.

- **DevOps scenario:** infrastructure metrics (CPU, memory, disk, network) with ingestion + analytic query packs.
- **IoT scenario:** sensor/device telemetry with configurable sampling rates and retention.
- **Workstreams:** extract schemas/queries from the upstream TSBS project, build data generators with flexible data rates, add top-level helpers (`benchbox/tsbs_devops.py`, `benchbox/tsbs_iot.py`), and craft platform-tuned examples for time-series databases as well as general-purpose engines.

## TPC-DS Fractional Scale Hardening

Small-scale TPC-DS runs can crash the upstream `dsdgen` binaries. This initiative seeks to make the default experience reliable.

- **Deliverables:** validated minimum scale factors, generator-side enforcement in `benchbox/core/tpcds/generator.py`, improved CLI messaging, documentation updates, and regression tests that cover the smallest supported scale.
- **Result:** predictable “quick start” runs even when users stick with fractional scale defaults.

---

If you rely on any of these benchmarks, please open an issue describing your requirements (scale factors, preferred platforms, compliance needs). Community feedback directly influences prioritisation.
