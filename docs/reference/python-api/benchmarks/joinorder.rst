Join Order Benchmark API
=========================

.. tags:: reference, python-api, custom-benchmark

Complete Python API reference for the Join Order Benchmark (JOB).

Overview
--------

The Join Order Benchmark is designed to test query optimizer join order selection capabilities using a complex schema with 21 interconnected tables based on the Internet Movie Database (IMDB). This benchmark specifically stresses query optimizers by creating join optimization challenges that expose cardinality estimation and cost model weaknesses.

**Key Features**:

- **Complex Schema**: 21 interconnected tables modeling a movie database
- **Join Optimization Focus**: Queries designed to test join order selection
- **13 Core Queries**: Representative join patterns and complexity levels
- **Synthetic Data Generation**: Realistic data preserving join characteristics
- **Multi-Dialect Support**: Compatible with all major SQL databases
- **Scalable**: From development (SF 0.001) to production (SF 1.0+)

**Reference**: Viktor Leis et al. "How Good Are Query Optimizers, Really?" (VLDB 2015)

Quick Start
-----------

Simple example to get started with Join Order Benchmark:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter

    # Create benchmark with small scale for testing
    benchmark = JoinOrder(scale_factor=0.01)

    # Generate data
    data_files = benchmark.generate_data()

    # Run with DuckDB
    adapter = DuckDBAdapter()
    results = adapter.run_benchmark(benchmark)

    print(f"Queries executed: {results.total_queries}")
    print(f"Average time: {results.average_query_time:.3f}s")

API Reference
-------------

JoinOrder Class
~~~~~~~~~~~~~~~

.. autoclass:: benchbox.joinorder.JoinOrder
   :members:
   :inherited-members:

Main interface for the Join Order Benchmark with complete functionality.

**Constructor**:

.. code-block:: python

    JoinOrder(
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        queries_dir: Optional[str] = None,
        **kwargs
    )

**Parameters**:

- **scale_factor** (float): Data size multiplier (1.0 ≈ 1.7GB, default: 1.0)
- **output_dir** (str | Path | None): Directory for generated data files
- **queries_dir** (str | None): Directory with Join Order Benchmark query files (loads full 113-query set if provided)
- **verbose** (bool | int): Enable verbose output
- **parallel** (int): Number of parallel generation threads
- **force_regenerate** (bool): Force data regeneration even if files exist

**Attributes**:

- **scale_factor** (float): Current scale factor
- **output_dir** (Path): Data file output directory
- **queries_dir** (str | None): External queries directory

Data Generation Methods
~~~~~~~~~~~~~~~~~~~~~~~

.. method:: generate_data() -> list[Path]

   Generate Join Order Benchmark dataset with 21 tables.

   **Returns**: list[Path] - Paths to generated CSV files

   **Example**:

   .. code-block:: python

       from benchbox.joinorder import JoinOrder

       benchmark = JoinOrder(scale_factor=0.1)
       data_files = benchmark.generate_data()

       print(f"Generated {len(data_files)} data files")
       for file in data_files:
           size_mb = file.stat().st_size / (1024 * 1024)
           print(f"  {file.name}: {size_mb:.2f} MB")

       # Output:
       # Generated 21 data files
       #   title.csv: 45.2 MB
       #   name.csv: 62.3 MB
       #   cast_info.csv: 580.1 MB
       #   ...

Schema Methods
~~~~~~~~~~~~~~

.. method:: get_create_tables_sql(dialect="standard", tuning_config=None) -> str

   Get SQL to create all Join Order Benchmark tables.

   **Parameters**:

   - **dialect** (str): SQL dialect (sqlite, postgres, mysql, duckdb)
   - **tuning_config** (UnifiedTuningConfiguration | None): Optional tuning configuration

   **Returns**: str - DDL SQL statements

   **Example**:

   .. code-block:: python

       benchmark = JoinOrder(scale_factor=0.01)

       # Get schema for DuckDB
       ddl = benchmark.get_create_tables_sql(dialect="duckdb")

       # Create tables
       import duckdb
       conn = duckdb.connect(":memory:")
       conn.execute(ddl)

       print("Created 21 tables")

.. method:: get_schema() -> list[dict]

   Get detailed schema information for all tables.

   **Returns**: list[dict] - Table schema definitions

   **Example**:

   .. code-block:: python

       schema = benchmark.get_schema()

       for table in schema[:3]:  # Show first 3 tables
           print(f"\nTable: {table['name']}")
           print(f"  Columns: {len(table['columns'])}")
           print(f"  Primary Key: {table.get('primary_key', 'None')}")
           print(f"  Foreign Keys: {len(table.get('foreign_keys', []))}")

.. method:: get_table_names() -> list[str]

   Get list of all table names in the schema.

   **Returns**: list[str] - Table names

   **Example**:

   .. code-block:: python

       tables = benchmark.get_table_names()
       print(f"Total tables: {len(tables)}")
       print(f"Tables: {', '.join(sorted(tables))}")

       # Output:
       # Total tables: 21
       # Tables: aka_name, aka_title, cast_info, char_name, ...

.. method:: get_table_info(table_name) -> dict

   Get detailed information about a specific table.

   **Parameters**:

   - **table_name** (str): Name of the table

   **Returns**: dict - Table information (columns, constraints, row count estimate)

   **Example**:

   .. code-block:: python

       # Get info for cast_info table
       info = benchmark.get_table_info("cast_info")

       print(f"Table: {info['name']}")
       print(f"Columns: {len(info['columns'])}")
       print(f"Estimated rows (SF=0.1): {info['estimated_rows']:,}")
       print(f"Primary Key: {info.get('primary_key')}")

       # Show column details
       for col in info['columns'][:5]:
           print(f"  - {col['name']}: {col['type']}")

.. method:: get_relationship_tables() -> list[str]

   Get list of relationship/junction tables.

   **Returns**: list[str] - Relationship table names

   **Example**:

   .. code-block:: python

       rel_tables = benchmark.get_relationship_tables()
       print(f"Relationship tables: {', '.join(rel_tables)}")

       # Output:
       # Relationship tables: cast_info, movie_companies, movie_keyword, movie_info, ...

.. method:: get_dimension_tables() -> list[str]

   Get list of main dimension tables.

   **Returns**: list[str] - Dimension table names

   **Example**:

   .. code-block:: python

       dim_tables = benchmark.get_dimension_tables()
       print(f"Dimension tables: {', '.join(dim_tables)}")

       # Output:
       # Dimension tables: title, name, company_name, keyword, char_name

.. method:: get_table_row_count(table_name) -> int

   Get expected row count for a table at current scale factor.

   **Parameters**:

   - **table_name** (str): Table name

   **Returns**: int - Expected row count

   **Example**:

   .. code-block:: python

       # Check row counts at SF=0.1
       tables = ["title", "name", "cast_info", "movie_companies"]

       for table in tables:
           count = benchmark.get_table_row_count(table)
           print(f"{table:20s}: {count:>10,} rows")

       # Output:
       # title              :    250,000 rows
       # name               :    400,000 rows
       # cast_info          :  3,500,000 rows
       # movie_companies    :    260,000 rows

Query Methods
~~~~~~~~~~~~~

.. method:: get_query(query_id, *, params=None) -> str

   Get a specific Join Order Benchmark query.

   **Parameters**:

   - **query_id** (str): Query identifier (e.g., "1a", "2b", "3c")
   - **params** (dict | None): Not supported for JoinOrder (raises ValueError if provided)

   **Returns**: str - SQL query text

   **Raises**: ValueError if params provided

   **Example**:

   .. code-block:: python

       # Get query 1a
       query = benchmark.get_query("1a")

       print(f"Query 1a:")
       print(query)

       # Count number of joins
       join_count = query.upper().count("JOIN")
       print(f"\nJoins: {join_count}")

.. method:: get_queries() -> dict[str, str]

   Get all Join Order Benchmark queries.

   **Returns**: dict - Mapping of query IDs to SQL text

   **Example**:

   .. code-block:: python

       queries = benchmark.get_queries()

       print(f"Total queries: {len(queries)}")

       # Show query IDs and join counts
       for query_id, sql in sorted(queries.items()):
           join_count = sql.upper().count("JOIN")
           print(f"  {query_id}: {join_count} joins")

.. method:: get_query_ids() -> list[str]

   Get list of all query IDs.

   **Returns**: list[str] - Query identifiers

   **Example**:

   .. code-block:: python

       query_ids = benchmark.get_query_ids()
       print(f"Available queries: {', '.join(sorted(query_ids))}")

       # Output:
       # Available queries: 1a, 1b, 1c, 1d, 2a, 2b, 2c, 2d, 3a, 3b, 3c, 4a, 4b

.. method:: get_query_count() -> int

   Get total number of queries available.

   **Returns**: int - Query count

   **Example**:

   .. code-block:: python

       count = benchmark.get_query_count()
       print(f"Total queries: {count}")

       # If loaded from directory
       benchmark_full = JoinOrder(
           scale_factor=0.01,
           queries_dir="/path/to/join-order-benchmark/queries"
       )
       full_count = benchmark_full.get_query_count()
       print(f"Full benchmark queries: {full_count}")  # 113 queries

Query Analysis Methods
~~~~~~~~~~~~~~~~~~~~~~

.. method:: get_queries_by_complexity() -> dict[str, list[str]]

   Get queries categorized by complexity level.

   **Returns**: dict - Mapping of complexity levels to query IDs

   **Example**:

   .. code-block:: python

       by_complexity = benchmark.get_queries_by_complexity()

       for complexity, queries in sorted(by_complexity.items()):
           print(f"\n{complexity.capitalize()} queries ({len(queries)}):")
           print(f"  {', '.join(queries)}")

       # Output:
       # Simple queries (4):
       #   1a, 1b, 1c, 1d
       # Medium queries (5):
       #   2a, 2b, 2c, 2d, 3a
       # Complex queries (4):
       #   3b, 3c, 4a, 4b

.. method:: get_queries_by_pattern() -> dict[str, list[str]]

   Get queries categorized by join pattern type.

   **Returns**: dict - Mapping of join patterns to query IDs

   **Example**:

   .. code-block:: python

       by_pattern = benchmark.get_queries_by_pattern()

       for pattern, queries in sorted(by_pattern.items()):
           print(f"\n{pattern.replace('_', ' ').title()} ({len(queries)} queries):")
           for query_id in queries:
               query = benchmark.get_query(query_id)
               tables = query.upper().count("FROM") + query.upper().count("JOIN")
               print(f"  {query_id}: ~{tables} tables")

Utility Methods
~~~~~~~~~~~~~~~

.. method:: validate_query(query_id) -> bool

   Validate that a query is syntactically correct.

   **Parameters**:

   - **query_id** (str): Query identifier

   **Returns**: bool - True if query is valid

   **Example**:

   .. code-block:: python

       for query_id in benchmark.get_query_ids():
           is_valid = benchmark.validate_query(query_id)
           if not is_valid:
               print(f"⚠️ Query {query_id} validation failed")

.. method:: get_estimated_data_size() -> int

   Get estimated data size in bytes at current scale factor.

   **Returns**: int - Estimated total size in bytes

   **Example**:

   .. code-block:: python

       size_bytes = benchmark.get_estimated_data_size()
       size_mb = size_bytes / (1024 * 1024)
       size_gb = size_bytes / (1024 * 1024 * 1024)

       print(f"Estimated data size:")
       print(f"  {size_mb:.1f} MB")
       print(f"  {size_gb:.2f} GB")

.. method:: get_benchmark_info() -> dict

   Get comprehensive benchmark information and metadata.

   **Returns**: dict - Benchmark metadata

   **Example**:

   .. code-block:: python

       info = benchmark.get_benchmark_info()

       print(f"Benchmark: {info['benchmark_name']}")
       print(f"Description: {info['description']}")
       print(f"Scale Factor: {info['scale_factor']}")
       print(f"Total Queries: {info['total_queries']}")
       print(f"Total Tables: {info['total_tables']}")
       print(f"Estimated Size: {info['estimated_size_bytes'] / 1024 / 1024:.1f} MB")
       print(f"\nReference: {info['reference_paper']}")
       print(f"Authors: {info['authors']}")

.. method:: load_queries_from_directory(queries_dir) -> None

   Load queries from original Join Order Benchmark repository.

   **Parameters**:

   - **queries_dir** (str): Path to directory containing .sql query files

   **Example**:

   .. code-block:: python

       benchmark = JoinOrder(scale_factor=0.01)

       # Initially has 13 core queries
       print(f"Core queries: {benchmark.get_query_count()}")

       # Load full 113-query set
       benchmark.load_queries_from_directory("/path/to/join-order-benchmark/queries")

       print(f"After loading: {benchmark.get_query_count()} queries")

       # Output:
       # Core queries: 13
       # After loading: 113 queries

Usage Examples
--------------

Basic Benchmark Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~

Complete workflow from data generation to query execution:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter

    # Initialize benchmark with small scale
    benchmark = JoinOrder(scale_factor=0.01, verbose=True)

    # Generate data
    print("Generating data...")
    data_files = benchmark.generate_data()
    print(f"Generated {len(data_files)} files")

    # Run with DuckDB
    print("\nRunning benchmark...")
    adapter = DuckDBAdapter(memory_limit="2GB")
    results = adapter.run_benchmark(benchmark)

    # Analyze results
    print(f"\nResults:")
    print(f"  Total queries: {results.total_queries}")
    print(f"  Successful: {results.successful_queries}")
    print(f"  Failed: {results.failed_queries}")
    print(f"  Average time: {results.average_query_time:.3f}s")
    print(f"  Total time: {results.total_execution_time:.2f}s")

Query Complexity Analysis
~~~~~~~~~~~~~~~~~~~~~~~~~

Analyze query complexity distribution:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter
    import time

    benchmark = JoinOrder(scale_factor=0.01)
    benchmark.generate_data()

    # Analyze by complexity
    by_complexity = benchmark.get_queries_by_complexity()

    adapter = DuckDBAdapter()
    conn = adapter.create_connection()
    adapter.create_schema(benchmark, conn)
    adapter.load_data(benchmark, conn, benchmark.output_dir)

    # Run queries by complexity level
    complexity_times = {}

    for complexity, query_ids in sorted(by_complexity.items()):
        print(f"\n{complexity.upper()} Queries ({len(query_ids)}):")

        times = []
        for query_id in query_ids:
            query = benchmark.get_query(query_id)

            start = time.time()
            result = conn.execute(query).fetchall()
            elapsed = time.time() - start

            times.append(elapsed)
            print(f"  {query_id}: {elapsed*1000:.1f} ms ({len(result)} rows)")

        avg_time = sum(times) / len(times)
        complexity_times[complexity] = avg_time

    # Summary
    print(f"\n{'='*50}")
    print("Complexity Performance Summary:")
    for complexity, avg_time in sorted(complexity_times.items()):
        print(f"  {complexity:10s}: {avg_time*1000:.1f} ms average")

Join Pattern Analysis
~~~~~~~~~~~~~~~~~~~~~

Analyze different join patterns:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter

    benchmark = JoinOrder(scale_factor=0.01)
    benchmark.generate_data()

    # Get queries by join pattern
    by_pattern = benchmark.get_queries_by_pattern()

    print("Join Pattern Distribution:")
    for pattern, query_ids in sorted(by_pattern.items()):
        print(f"\n{pattern.replace('_', ' ').title()}:")
        print(f"  Queries: {', '.join(query_ids)}")
        print(f"  Count: {len(query_ids)}")

        # Analyze a sample query
        sample_query = benchmark.get_query(query_ids[0])
        join_count = sample_query.upper().count("JOIN")
        from_count = sample_query.upper().count("FROM")

        print(f"  Example ({query_ids[0]}): {join_count} joins, {from_count} FROM clauses")

Optimizer Testing
~~~~~~~~~~~~~~~~~

Test query optimizer with different query variations:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter
    import time

    benchmark = JoinOrder(scale_factor=0.1)
    benchmark.generate_data()

    adapter = DuckDBAdapter()
    conn = adapter.create_connection()
    adapter.create_schema(benchmark, conn)
    adapter.load_data(benchmark, conn, benchmark.output_dir)

    # Test optimizer on complex queries
    complex_queries = benchmark.get_queries_by_complexity()["complex"]

    print("Optimizer Analysis:")
    for query_id in complex_queries:
        query = benchmark.get_query(query_id)

        # Enable query profiling
        conn.execute("PRAGMA enable_profiling='query_tree'")

        # Run query
        start = time.time()
        result = conn.execute(query).fetchall()
        elapsed = time.time() - start

        print(f"\n{query_id}:")
        print(f"  Execution time: {elapsed*1000:.1f} ms")
        print(f"  Rows returned: {len(result)}")
        print(f"  Tables joined: ~{query.upper().count('JOIN')}")

        # Get query plan (DuckDB specific)
        try:
            plan = conn.execute("EXPLAIN " + query).fetchall()
            print(f"  Plan nodes: {len(plan)}")
        except:
            pass

Schema Exploration
~~~~~~~~~~~~~~~~~~

Explore the complex schema structure:

.. code-block:: python

    from benchbox.joinorder import JoinOrder

    benchmark = JoinOrder(scale_factor=0.1)

    # Get all tables categorized
    dim_tables = benchmark.get_dimension_tables()
    rel_tables = benchmark.get_relationship_tables()

    print("Schema Structure:")
    print(f"\nDimension Tables ({len(dim_tables)}):")
    for table in dim_tables:
        info = benchmark.get_table_info(table)
        row_count = benchmark.get_table_row_count(table)
        print(f"  {table:20s}: {row_count:>10,} rows, {len(info['columns'])} columns")

    print(f"\nRelationship Tables ({len(rel_tables)}):")
    for table in rel_tables:
        info = benchmark.get_table_info(table)
        row_count = benchmark.get_table_row_count(table)
        fk_count = len(info.get('foreign_keys', []))
        print(f"  {table:20s}: {row_count:>10,} rows, {fk_count} foreign keys")

    # Estimate total size
    total_size = benchmark.get_estimated_data_size()
    print(f"\nEstimated Total Size: {total_size / 1024 / 1024:.1f} MB")

Custom Query Execution
~~~~~~~~~~~~~~~~~~~~~~

Run specific queries with custom analysis:

.. code-block:: python

    from benchbox.joinorder import JoinOrder
    from benchbox.platforms.duckdb import DuckDBAdapter
    from benchbox.core.results.timing import TimingCollector, TimingAnalyzer

    benchmark = JoinOrder(scale_factor=0.01)
    benchmark.generate_data()

    adapter = DuckDBAdapter()
    conn = adapter.create_connection()
    adapter.create_schema(benchmark, conn)
    adapter.load_data(benchmark, conn, benchmark.output_dir)

    # Collect detailed timing
    collector = TimingCollector(enable_detailed_timing=True)

    # Run subset of queries
    target_queries = ["1a", "2a", "3a", "4a"]

    for query_id in target_queries:
        query = benchmark.get_query(query_id)

        with collector.time_query(query_id, f"Query {query_id}") as timing:
            with collector.time_phase(query_id, "execute"):
                result = conn.execute(query).fetchall()

            collector.record_metric(query_id, "rows_returned", len(result))
            collector.record_metric(query_id, "tables_joined", query.upper().count("JOIN"))

    # Analyze timing
    timings = collector.get_completed_timings()
    analyzer = TimingAnalyzer(timings)

    analysis = analyzer.analyze_query_performance()

    print("\nPerformance Analysis:")
    print(f"  Mean: {analysis['basic_stats']['mean']*1000:.1f} ms")
    print(f"  Median: {analysis['basic_stats']['median']*1000:.1f} ms")
    print(f"  Min: {analysis['basic_stats']['min']*1000:.1f} ms")
    print(f"  Max: {analysis['basic_stats']['max']*1000:.1f} ms")

Best Practices
--------------

1. **Start with Small Scale Factors**

   Begin with small scale factors for development and testing:

   .. code-block:: python

       # Development - very fast
       dev_benchmark = JoinOrder(scale_factor=0.001)  # ~700 KB

       # Testing - reasonable size
       test_benchmark = JoinOrder(scale_factor=0.01)  # ~17 MB

       # Production - realistic workload
       prod_benchmark = JoinOrder(scale_factor=0.1)   # ~170 MB

2. **Analyze Query Complexity**

   Use complexity categorization to understand workload:

   .. code-block:: python

       by_complexity = benchmark.get_queries_by_complexity()

       # Start with simple queries
       for query_id in by_complexity["simple"]:
           # Test basic join performance
           pass

       # Then move to complex queries
       for query_id in by_complexity["complex"]:
           # Test advanced optimizer capabilities
           pass

3. **Use Join Pattern Analysis**

   Focus on specific join patterns for targeted testing:

   .. code-block:: python

       by_pattern = benchmark.get_queries_by_pattern()

       # Test star joins
       star_queries = by_pattern.get("star_join", [])

       # Test chain joins
       chain_queries = by_pattern.get("chain_join", [])

       # Test complex patterns
       complex_queries = by_pattern.get("complex_join", [])

4. **Monitor Optimizer Behavior**

   Enable query profiling to understand optimizer decisions:

   .. code-block:: python

       # DuckDB example
       conn.execute("PRAGMA enable_profiling='query_tree'")

       # Run query
       result = conn.execute(query).fetchall()

       # Analyze plan
       plan = conn.execute("EXPLAIN " + query).fetchall()

5. **Cache Generated Data**

   Reuse generated data across runs:

   .. code-block:: python

       from pathlib import Path

       data_dir = Path("joinorder_cache/sf01")

       if data_dir.exists():
           # Reuse existing data
           benchmark = JoinOrder(scale_factor=0.1, output_dir=data_dir)
       else:
           # Generate once
           benchmark = JoinOrder(scale_factor=0.1, output_dir=data_dir)
           benchmark.generate_data()

Common Issues
-------------

Large Data Size
~~~~~~~~~~~~~~~

**Problem**: Scale factor 1.0 generates ~1.7GB of data

**Solution**: Use appropriate scale factors:

.. code-block:: python

    # For CI/CD - use minimal scale
    ci_benchmark = JoinOrder(scale_factor=0.001)

    # For local testing - use small scale
    test_benchmark = JoinOrder(scale_factor=0.01)

    # For performance testing - use realistic scale
    perf_benchmark = JoinOrder(scale_factor=0.1)

Slow Query Execution
~~~~~~~~~~~~~~~~~~~~

**Problem**: Complex queries take too long

**Solution**: Start with simpler queries or smaller scale:

.. code-block:: python

    # Filter to simple queries only
    simple_queries = benchmark.get_queries_by_complexity()["simple"]

    for query_id in simple_queries:
        query = benchmark.get_query(query_id)
        # Execute fast queries first
        result = conn.execute(query).fetchall()

Memory Issues
~~~~~~~~~~~~~

**Problem**: Large datasets cause out-of-memory errors

**Solution**: Use file-based database or limit memory:

.. code-block:: python

    from benchbox.platforms.duckdb import DuckDBAdapter

    # Use file-based database
    adapter = DuckDBAdapter(
        database_path="joinorder.duckdb",
        memory_limit="2GB"
    )

See Also
--------

- :doc:`/benchmarks/join-order` - Join Order Benchmark guide
- :doc:`tpch` - TPC-H benchmark API
- :doc:`tpcds` - TPC-DS benchmark API
- :doc:`/reference/python-api/base` - Base benchmark interface
- :doc:`/reference/python-api/platforms/duckdb` - DuckDB adapter
- :doc:`/advanced/performance-optimization` - Performance optimization guide

External Resources
~~~~~~~~~~~~~~~~~~

- `Join Order Benchmark Paper <http://www.vldb.org/pvldb/vol9/p204-leis.pdf>`_ - Original VLDB 2015 paper
- `GitHub Repository <https://github.com/gregrahn/join-order-benchmark>`_ - Original benchmark repository
- `IMDB Interfaces <https://www.imdb.com/interfaces/>`_ - IMDB dataset information
