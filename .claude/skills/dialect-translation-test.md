---
description: Test SQL dialect translation across platforms
---

# Dialect Translation Test

Verify that SQL queries translate correctly across database dialects using sqlglot.

## Instructions

When the user asks to test dialect translation, verify query compatibility, or check SQL across platforms:

1. **Understand the context**:
   - Source query (which dialect it's written in)
   - Target platforms (DuckDB, Snowflake, BigQuery, Databricks, ClickHouse, Redshift, etc.)
   - Specific query or full benchmark

2. **For a single query test**:

   ```python
   import sqlglot

   query = """
   SELECT * FROM table
   WHERE date > '2023-01-01'
   """

   # Test translation to each target dialect
   dialects = ['duckdb', 'snowflake', 'bigquery', 'databricks', 'clickhouse']

   for dialect in dialects:
       try:
           translated = sqlglot.transpile(query, read='duckdb', write=dialect)[0]
           print(f"{dialect}: ✅")
           print(translated)
       except Exception as e:
           print(f"{dialect}: ❌ {e}")
   ```

3. **For benchmark queries**:
   - Load benchmark: `from benchbox.core.tpch import TPCH`
   - Get queries: `queries = tpch.get_queries()`
   - Test translation for each query
   - Note which queries fail per platform

4. **Check for common translation issues**:

   **Type differences:** `BIGINT` vs `INT64` vs `LONG` | `VARCHAR` vs `STRING` vs `TEXT` | `TIMESTAMP` vs `DATETIME` vs `DATETIME64`

   **Function differences:** Date functions (`DATE_ADD`, `DATEADD`, `DATE_SUB`) | String functions (`CONCAT`, `||`, `CONCAT_WS`) | Cast syntax (`::type` vs `CAST(x AS type)`)

   **Syntax differences:** `LIMIT` vs `TOP` vs `FETCH FIRST` | `OFFSET` variations | `WITH` clause (CTE) support | Window function syntax

   **Quote characters:** Identifier quotes (backticks vs double quotes) | String quotes (single vs double) | Escape sequences

5. **Test query execution** (if possible):
   - Create small test table in DuckDB
   - Run original query
   - Run translated queries
   - Verify results match

6. **Report findings**:

   ```markdown
   ## Dialect Translation Report

   Query: [query name/number]
   Source Dialect: [e.g., DuckDB]

   ### Translation Results

   ✅ Snowflake: Translates successfully
   ✅ BigQuery: Translates successfully
   ❌ ClickHouse: Error - unsupported function DATE_ADD
   ✅ Databricks: Translates successfully
   ⚠️  Redshift: Translates but uses deprecated syntax

   ### Issues Found

   1. **ClickHouse - DATE_ADD not supported**
      - Original: `DATE_ADD(date_col, INTERVAL 1 DAY)`
      - Error: Function DATE_ADD not found
      - Fix: Use `date_col + INTERVAL 1 DAY`

   2. **Redshift - Deprecated LIMIT syntax**
      - Translated: `LIMIT 10 OFFSET 5`
      - Warning: Should use FETCH FIRST/OFFSET
      - Impact: Works but non-standard

   ### Recommendations
   1. Add ClickHouse-specific date function handling
   2. Update Redshift translation to use FETCH FIRST
   ```

## Platform-Specific Considerations

| Platform | Key Characteristics |
|----------|-------------------|
| **DuckDB** | PostgreSQL-compatible; good source dialect; supports most modern SQL features |
| **Snowflake** | Case-insensitive by default; specific date/time functions; uppercase function names |
| **BigQuery** | Uses backticks for identifiers; specific date/timestamp functions; StandardSQL vs Legacy SQL |
| **Databricks** | Spark SQL dialect; Delta Lake features; case-sensitive configurations |
| **ClickHouse** | Unique type system; different function names; array/map syntax variations |
| **Redshift** | PostgreSQL-based with limitations; missing certain window functions; specific COPY/UNLOAD syntax |

## Testing with BenchBox

BenchBox has built-in platform adapters that handle dialect translation:

```python
# Check how a benchmark handles dialect
from benchbox.core.tpch import TPCH

tpch = TPCH(scale_factor=1)

# Get query for specific platform
query = tpch.get_query(query_num=1, dialect='snowflake')
print(query)

# Compare across dialects
for dialect in ['duckdb', 'snowflake', 'bigquery']:
    query = tpch.get_query(query_num=1, dialect=dialect)
    print(f"\n{dialect}:")
    print(query)
```

## Common Test Script

Save to `_project/test_dialect_translation.py`:

```python
import sqlglot

def test_query_translation(query: str, source_dialect: str = 'duckdb'):
    """Test query translation across all supported dialects."""
    dialects = {
        'duckdb': 'DuckDB', 'snowflake': 'Snowflake', 'bigquery': 'BigQuery',
        'databricks': 'Databricks', 'clickhouse': 'ClickHouse',
        'redshift': 'Redshift', 'postgres': 'PostgreSQL'
    }

    results = {}
    for dialect_key, dialect_name in dialects.items():
        try:
            translated = sqlglot.transpile(query, read=source_dialect, write=dialect_key)[0]
            results[dialect_name] = {'status': 'success', 'query': translated}
        except Exception as e:
            results[dialect_name] = {'status': 'error', 'error': str(e)}

    return results
```

## Notes

- sqlglot is the translation engine (already in dependencies)
- Not all SQL features translate perfectly
- Some platforms may need manual adjustments
- Test with actual database when possible
- BenchBox platform adapters handle many edge cases automatically
