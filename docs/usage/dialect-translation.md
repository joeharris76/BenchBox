# SQL Dialect Translation Guide

```{tags} intermediate, guide, sql-platform
```

## Overview

BenchBox provides powerful SQL dialect translation capabilities via SQLGlot, allowing queries to be translated between different database dialects. However, it's crucial to understand the distinction between **dialect translation** and **platform adapters**.

## Key Distinction

### Dialect Translation (Query Translation)

**What it is**: The ability to convert SQL queries from one dialect to another using SQLGlot.

**What it does**:
- Translates SQL syntax between dialects (e.g., `::DATE` in PostgreSQL to `CAST(... AS DATE)` in BigQuery)
- Handles dialect-specific functions and operators
- Converts data type syntax
- Adapts query structure for target database

**Supported dialects** (via SQLGlot):
- postgres, mysql, sqlite, oracle, mssql (SQL Server)
- duckdb, clickhouse, databricks, snowflake, bigquery, redshift
- athena, trino, hive, presto, and many more

**Example**:
```python
from benchbox import TPCH

tpch = TPCH(scale_factor=0.01)

# Translate to PostgreSQL dialect
postgres_query = tpch.translate_query(1, "postgres")

# Translate to MySQL dialect
mysql_query = tpch.translate_query(1, "mysql")

# Both work even though PostgreSQL/MySQL adapters don't exist yet!
```

### Platform Adapters (Database Connectivity)

**What it is**: Complete integration with a specific database platform, including connection management, data loading, query execution, and result collection.

**What it provides**:
- Database connection handling
- Authentication and credential management
- Data loading optimizations
- Query execution with proper error handling
- Performance metrics collection
- Platform-specific tuning

**Currently supported platforms**:
- ✅ DuckDB
- ✅ SQLite
- ✅ ClickHouse
- ✅ Databricks
- ✅ BigQuery
- ✅ Snowflake
- ✅ Redshift

**Planned platforms** (see [Future Platforms](../platforms/future-platforms.md)):
- 🔄 PostgreSQL
- 🔄 MySQL
- 🔄 SQL Server
- 🔄 Oracle
- 🔄 Athena
- 🔄 Trino/Starburst
- 🔄 Apache Spark SQL
- 🔄 Microsoft Fabric
- 🔄 Azure Synapse

## Common Confusion

### ❌ Incorrect Assumption

> "BenchBox can translate queries to PostgreSQL, so I can use `--platform postgres` in the CLI."

**Why this fails**: Dialect translation only converts the SQL syntax. The platform adapter (which handles connections, data loading, etc.) doesn't exist yet.

### ✅ Correct Understanding

> "BenchBox can translate queries to PostgreSQL dialect. I can use these translated queries with my own PostgreSQL connection, but BenchBox doesn't have a built-in PostgreSQL adapter yet."

**How to use it**:
```python
from benchbox import TPCH
import psycopg2  # You provide the connection

tpch = TPCH(scale_factor=0.01)
tpch.generate_data()

# Get PostgreSQL-dialect query
query = tpch.translate_query(1, "postgres")

# Use your own connection
conn = psycopg2.connect("...")
cursor = conn.cursor()
cursor.execute(query)  # You handle execution
results = cursor.fetchall()
```

## Use Cases for Dialect Translation

### 1. Custom Integration

Translate queries for databases where you have your own connection:

```python
from benchbox import TPCH
import mysql.connector

benchmark = TPCH(scale_factor=0.01)
benchmark.generate_data()

# Your MySQL connection
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    database="benchbox"
)

# Get MySQL-translated queries
for query_id in range(1, 23):
    query = benchmark.translate_query(query_id, "mysql")

    # Execute with your connection
    cursor = mysql_conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"Query {query_id}: {len(results)} rows")
```

### 2. Query Portability Testing

Test how queries translate across different dialects:

```python
from benchbox import TPCH

benchmark = TPCH(scale_factor=0.01)

# Compare translations
dialects = ["postgres", "mysql", "oracle", "mssql"]

for dialect in dialects:
    translated = benchmark.translate_query(1, dialect)
    print(f"\n=== {dialect.upper()} ===")
    print(translated[:200] + "...")
```

### 3. SQL Compatibility Analysis

Understand dialect differences before migration:

```python
from benchbox import TPCH

benchmark = TPCH(scale_factor=0.01)

# Analyze compatibility between source and target
source_dialect = "postgres"
target_dialect = "mysql"

differences = []
for query_id in range(1, 23):
    source_query = benchmark.translate_query(query_id, source_dialect)
    target_query = benchmark.translate_query(query_id, target_dialect)

    if source_query != target_query:
        differences.append({
            "query_id": query_id,
            "source_length": len(source_query),
            "target_length": len(target_query)
        })

print(f"Found {len(differences)} queries with dialect differences")
```

## Using Platform Adapters (When Available)

For supported platforms, use the built-in adapters for complete integration:

```python
from benchbox import TPCH
from benchbox.platforms import DuckDBAdapter

benchmark = TPCH(scale_factor=0.01)

# Platform adapter handles everything
adapter = DuckDBAdapter()
results = benchmark.run_with_platform(adapter)

print(f"Completed {results.successful_queries}/{results.total_queries} queries")
```

**Advantages of platform adapters**:
- ✅ Automatic connection management
- ✅ Optimized data loading
- ✅ Built-in error handling
- ✅ Performance metrics collection
- ✅ Platform-specific optimizations
- ✅ Credential management
- ✅ Result validation

## Translation Limitations

### What Dialect Translation Cannot Do

1. **Execute queries**: Translation only converts syntax, doesn't run queries
2. **Load data**: No built-in data loading to the target database
3. **Handle authentication**: You must manage credentials yourself
4. **Optimize for platform**: Generic translation may not be optimal
5. **Handle platform-specific features**: Custom extensions may not translate

### Translation Quality

Not all SQL features translate perfectly:

```python
from benchbox import TPCH

benchmark = TPCH(scale_factor=0.01)

try:
    # Most queries translate successfully
    query = benchmark.translate_query(1, "mysql")
    print("✅ Translation succeeded")
except Exception as e:
    # Some complex queries may fail
    print(f"❌ Translation failed: {e}")
    # Fallback to a more compatible dialect
    query = benchmark.translate_query(1, "postgres")
```

**Best practices**:
- Always test translated queries on the target platform
- Be aware of platform-specific limitations
- Use fallback dialects for complex cases
- Validate results against source database

## When to Use Each Approach

### Use Dialect Translation When:
- ✅ You have your own database connection
- ✅ The platform adapter doesn't exist yet
- ✅ You need custom control over execution
- ✅ You're analyzing SQL portability
- ✅ You're testing query compatibility

### Use Platform Adapters When:
- ✅ The adapter exists for your database
- ✅ You want automated data loading
- ✅ You need comprehensive benchmarking
- ✅ You want optimized performance
- ✅ You need consistent result collection

## API Reference

### Translation Methods

All benchmark classes inherit from `BaseBenchmark` and provide:

```python
def translate_query(
    self,
    query_id: Union[int, str],
    dialect: str
) -> str:
    """Translate query to target SQL dialect.

    Args:
        query_id: Query identifier
        dialect: Target SQL dialect (postgres, mysql, etc.)

    Returns:
        Translated SQL query string

    Raises:
        ValueError: If query_id or dialect is invalid
        ImportError: If sqlglot is not installed
    """
```

### Alternative: get_query with dialect

```python
# Both methods are equivalent
query1 = benchmark.translate_query(1, "mysql")
query2 = benchmark.get_query(1, dialect="mysql")

assert query1 == query2
```

## See Also

- [Future Platforms](../platforms/future-platforms.md) - Roadmap for planned platform adapters
- [API Reference](../reference/api-reference.md) - Complete API documentation
- [Utilities](../reference/python-api/utilities.rst) - Dialect translation utilities
- [Adding New Platforms](../development/adding-new-platforms.md) - Build custom adapters

## Summary

| Capability | Dialect Translation | Platform Adapters |
|-----------|---------------------|-------------------|
| **SQL Syntax Conversion** | ✅ Yes | ✅ Yes (automatic) |
| **Database Connection** | ❌ No (you provide) | ✅ Yes (built-in) |
| **Data Loading** | ❌ No (manual) | ✅ Yes (optimized) |
| **Query Execution** | ❌ No (you handle) | ✅ Yes (automated) |
| **Error Handling** | ❌ No (your responsibility) | ✅ Yes (comprehensive) |
| **Performance Metrics** | ❌ No | ✅ Yes (detailed) |
| **Supported Databases** | 🌐 Many (via SQLGlot) | 🎯 Limited (7 currently) |

**Key Takeaway**: Dialect translation is powerful but limited. For production benchmarking, use platform adapters when available. For unsupported platforms, use dialect translation with your own integration code while waiting for official adapters.
