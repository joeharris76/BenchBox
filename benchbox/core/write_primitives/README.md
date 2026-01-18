# Write Primitives Benchmark

## Overview

The Write Primitives benchmark tests **fundamental write operations** for OLAP databases using the TPC-H schema as foundation. This benchmark provides comprehensive testing of insert, update, delete, bulk load, merge/upsert, DDL, and transaction operations.

**Purpose**: Replace the legacy `merge` benchmark with a comprehensive write operation testing suite that measures:
- Write throughput (rows/second)
- Operation latency and overhead
- Data format efficiency (CSV, Parquet, compressed variants)
- Transaction and isolation level performance
- DDL operation costs
- Data validation and consistency

## Design Philosophy

Following the **primitives benchmark pattern**, this benchmark:
- Uses YAML catalog for operation definitions (`catalog/operations.yaml`)
- Reuses TPC-H data via `get_data_source_benchmark() -> "tpch"`
- Pairs every write operation with validation read queries
- Provides single-sentence descriptions for each operation
- Supports platform-specific SQL variants via SQLGlot
- Measures end-to-end write-read cycle performance

## Benchmark Statistics

- **Total Operations**: 113 (fully implemented)
- **Categories**: 7 (INSERT, UPDATE, DELETE, BULK_LOAD, MERGE, DDL, TRANSACTION)
- **Data Formats**: CSV, Parquet (uncompressed, gzip, zstd, snappy, bzip2)
- **Scale Factors**: Flexible (0.01 to 10.0+)
- **Platform Support**: All platforms via dialect translation
- **Status**: ✅ All operations fully implemented and tested

## Operation Categories

### 1. INSERT Operations (12 operations)

Tests various insert patterns from single row to complex joins:
- Single row INSERT
- Batch INSERT (10, 100, 1000 rows)
- INSERT...SELECT (simple, with JOIN, aggregated, from multiple tables)
- INSERT...UNION
- INSERT with default values
- INSERT...ON CONFLICT (UPSERT)
- INSERT...RETURNING

### 2. UPDATE Operations (15 operations)

Tests selective and bulk updates with various predicates:
- Single row by primary key
- Selective (10%, 50%, 100% of rows)
- With subquery, JOIN, aggregate
- Multi-column updates (5+ columns)
- With CASE expression, computed columns
- String manipulation, date arithmetic
- Conditional updates
- UPDATE...RETURNING

### 3. DELETE Operations (12 operations)

Tests deletion patterns from single row to bulk deletes:
- Single row by primary key
- Selective (10%, 25%, 50%, 75% of rows)
- With subquery, JOIN, aggregation
- With NOT EXISTS (anti-join)
- DELETE...RETURNING
- Cascade simulation
- DELETE vs TRUNCATE comparison

### 4. BULK_LOAD Operations (36 operations)

Tests bulk loading from files with various formats and compression:
- CSV loads (12): uncompressed, gzip, zstd, bzip2 × (1K, 100K, 1M rows)
- Parquet loads (12): uncompressed, snappy, gzip, zstd × (1K, 100K, 1M rows)
- Special loads (12): column subset, transformations, error handling, parallel, upsert, append vs replace modes, custom delimiters, NULL handling, custom date formats

### 5. MERGE Operations (18 operations)

Tests MERGE/UPSERT patterns including INSERT, UPDATE, and DELETE:
- Simple UPSERT
- UPSERT with DELETE clause (tri-directional)
- Varying overlap scenarios (10%, 50%, 90%, none, all)
- Multi-column join conditions
- Aggregated source queries
- Conditional UPDATE and INSERT
- Multi-column updates
- Computed values, string operations, date arithmetic
- CTE sources
- MERGE...RETURNING
- Error handling (duplicate sources)

### 6. DDL Operations (12 operations)

Tests schema evolution and table management:
- CREATE TABLE (simple, with constraints, with indexes)
- CREATE TABLE AS SELECT (simple, aggregated)
- ALTER TABLE (ADD COLUMN, DROP COLUMN, RENAME COLUMN)
- CREATE INDEX (on empty table, on existing data)
- DROP INDEX
- CREATE VIEW
- DROP TABLE
- TRUNCATE TABLE (small, large datasets)

### 7. TRANSACTION Operations (8 operations)

Tests transaction control and isolation levels:
- COMMIT (small/10 writes, medium/100 writes, large/1000 writes)
- ROLLBACK (small/3 writes, medium/100 writes)
- Nested SAVEPOINTs with partial rollback
- Isolation levels (READ COMMITTED, SERIALIZABLE)

## Schema Design

### Base Tables (from TPC-H)
- **REGION**, **NATION**, **CUSTOMER**, **SUPPLIER**, **PART**, **PARTSUPP**, **ORDERS**, **LINEITEM**

### Staging Tables
- **orders_stage** - Copy of ORDERS for UPDATE/DELETE testing
- **lineitem_stage** - Copy of LINEITEM for write testing
- **orders_new** - Source for MERGE testing (50% overlap)
- **orders_summary** - Target for aggregated INSERT...SELECT
- **lineitem_enriched** - Target for joined INSERT...SELECT

### Metadata Tables
- **write_ops_log** - Audit log for all write operations
- **batch_metadata** - Tracks batch operations with file info

## Data Generation

The benchmark reuses TPC-H data through `get_data_source_benchmark() -> "tpch"` and generates:
- Staging tables (10% of ORDERS, 5% of LINEITEM)
- Bulk load files in `_project/write_primitives_files/{scale_factor}/`
- CSV files: uncompressed, gzip, zstd (1K, 100K, 1M rows)
- Parquet files: uncompressed, snappy, zstd (1K, 100K, 1M rows)

## Usage Examples

```python
from benchbox.write_primitives import WritePrimitivesBenchmark

# Initialize and generate data
bench = WritePrimitivesBenchmark(scale_factor=1.0)
bench.generate_data()

# Run single operation
result = bench.execute_operation("insert_single_row", connection)

# Run category
results = bench.run_category("insert", connection, iterations=3)

# Run full benchmark
results = bench.run_benchmark(connection, iterations=3)
```

## Performance Metrics

Each operation captures:
- **Write Metrics**: duration, rows affected, throughput
- **Validation Metrics**: validation duration, passed/failed status
- **Combined Metrics**: end-to-end duration and throughput

## File Structure

```
benchbox/core/write_primitives/
├── __init__.py                  # Public exports
├── README.md                    # This file
├── benchmark.py                 # WritePrimitivesBenchmark class
├── generator.py                 # Data and file generation
├── operations.py                # WriteOperationManager
├── schema.py                    # Schema definitions
└── catalog/
    ├── __init__.py
    ├── loader.py                # YAML catalog loader
    └── operations.yaml          # 113 operation definitions
```

## License

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
