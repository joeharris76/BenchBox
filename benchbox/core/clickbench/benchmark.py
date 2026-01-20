"""ClickBench (ClickHouse Analytics Benchmark) implementation.

Provides ClickBench benchmark implementation that tests analytical database performance using web analytics data.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.clickbench.generator import ClickBenchDataGenerator
from benchbox.core.clickbench.queries import ClickBenchQueryManager
from benchbox.core.clickbench.schema import TABLES, get_create_table_sql

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import UnifiedTuningConfiguration


class ClickBenchBenchmark(BaseBenchmark):
    """ClickBench (ClickHouse Analytics Benchmark) implementation.

    Tests analytical database performance using web analytics data.

    The benchmark consists of:
    - A single flat table with web analytics data (~100M records in original)
    - 43 analytical queries covering various performance patterns
    - Realistic data distributions based on production web analytics

    Attributes:
        scale_factor: Scale factor for the benchmark (1.0 = ~1M records for testing)
        output_dir: Directory to output generated data and results
        query_manager: ClickBench query manager
        data_generator: ClickBench data generator
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **config: Any,
    ):
        """Initialize ClickBench benchmark.

        Args:
            scale_factor: Scale factor for data generation (1.0 = ~1M records for testing)
            output_dir: Directory for generated data files
            **config: Additional configuration options
        """
        # Extract quiet from config to prevent duplicate kwarg error
        config = dict(config)
        quiet = config.pop("quiet", False)

        super().__init__(scale_factor=scale_factor, output_dir=output_dir, quiet=quiet, **config)

        self._name = "ClickBench"
        self._version = "1.0"
        self._description = "ClickBench - Analytics benchmark for DBMS with web analytics workload"

        # Initialize components
        self.query_manager: ClickBenchQueryManager = ClickBenchQueryManager()
        self.data_generator = ClickBenchDataGenerator(scale_factor, self.output_dir, **config)

        # Data files mapping
        self.tables = {}

    def generate_data(self, tables: Optional[list[str]] = None, output_format: str = "csv") -> dict[str, Any]:
        """Generate ClickBench data.

        Args:
            tables: Optional list of tables to generate. If None, generates all.
            output_format: Format for output data (only "csv" supported currently)

        Returns:
            Dictionary mapping table names to file paths

        Raises:
            ValueError: If output_format is not supported
        """
        if output_format != "csv":
            raise ValueError(f"Unsupported output format: {output_format}")

        if tables is None:
            tables = list(TABLES.keys())

        # Validate table names
        invalid_tables = set(tables) - set(TABLES.keys())
        if invalid_tables:
            raise ValueError(f"Invalid table names: {invalid_tables}")

        self.tables = self.data_generator.generate_data(tables)
        return self.tables

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all available ClickBench queries.

        Args:
            dialect: Target SQL dialect for translation (e.g., 'duckdb', 'bigquery', 'snowflake')
                    If None, returns queries in their original format.

        Returns:
            A dictionary mapping query identifiers to their SQL text
        """
        queries = self.query_manager.get_all_queries()

        # Apply dialect translation if requested
        if dialect:
            translated_queries = {}
            for query_id, query in queries.items():
                translated_queries[query_id] = self.translate_query_text(query, dialect)
            return translated_queries

        return queries

    def translate_query_text(self, query: str, dialect: str) -> str:
        """Translate a query string to a specific SQL dialect."""
        try:
            import sqlglot  # type: ignore[import-untyped]

            # ClickBench queries are designed for ClickHouse, use 'clickhouse' as source
            translated = sqlglot.transpile(  # type: ignore[attr-defined]
                query, read="clickhouse", write=dialect.lower()
            )[0]
            return translated
        except ImportError:
            # sqlglot not available, return original query
            return query
        except Exception:
            # Translation failed, return original query
            return query

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get the SQL text for a specific ClickBench query.

        Args:
            query_id: Query identifier (e.g., "Q1", "Q2", etc.)
            params: Optional parameter values (not supported for ClickBench)

        Returns:
            The SQL text of the query

        Raises:
            ValueError: If the query_id is not valid or params are provided
        """
        if params is not None:
            raise ValueError("ClickBench queries are static and don't accept parameters")
        return self.query_manager.get_query(str(query_id))

    def get_all_queries(self) -> dict[str, str]:
        """Get all available ClickBench queries.

        Returns:
            A dictionary mapping query identifiers to their SQL text
        """
        return self.query_manager.get_all_queries()

    def execute_query(
        self,
        query_id: Union[int, str],
        connection: Any,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute a ClickBench query on the given database connection.

        Args:
            query_id: Query identifier (e.g., "Q1", "Q2", etc.)
            connection: Database connection to use for execution
            params: Optional parameters to use in the query

        Returns:
            Query results from the database

        Raises:
            ValueError: If the query_id is not valid
        """
        sql = self.get_query(query_id, params=params)

        # Execute query using connection
        if hasattr(connection, "execute"):
            # Direct database connection
            cursor = connection.execute(sql)
            return cursor.fetchall()
        elif hasattr(connection, "cursor"):
            # Connection with cursor method
            cursor = connection.cursor()
            cursor.execute(sql)
            return cursor.fetchall()
        else:
            raise ValueError("Unsupported connection type")

    def get_schema(self, dialect: str = "standard") -> dict[str, dict]:
        """Get the ClickBench schema definitions.

        Args:
            dialect: SQL dialect to use for data types

        Returns:
            Dictionary mapping table names to their schema definitions
        """
        return TABLES

    def get_create_tables_sql(
        self,
        dialect: str = "standard",
        tuning_config: Optional["UnifiedTuningConfiguration"] = None,
    ) -> str:
        """Get CREATE TABLE SQL for the ClickBench hits table.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            Complete SQL schema creation script
        """
        # Extract constraint settings from tuning configuration
        enable_primary_keys = tuning_config.primary_keys.enabled if tuning_config else False
        enable_foreign_keys = tuning_config.foreign_keys.enabled if tuning_config else False

        return get_create_table_sql(dialect, enable_primary_keys, enable_foreign_keys)

    def load_data_to_database(self, connection: Any, tables: Optional[list[str]] = None) -> None:
        """Load generated data into a database.

        Args:
            connection: Database connection
            tables: Optional list of tables to load. If None, loads all.

        Raises:
            ValueError: If data hasn't been generated yet
        """
        if not self.tables:
            raise ValueError("No data generated. Call generate_data() first.")

        if tables is None:
            tables = list(self.tables.keys())

        # Create tables first
        schema_sql = self.get_create_tables_sql()
        if hasattr(connection, "executescript"):
            connection.executescript(schema_sql)
        else:
            cursor = connection.cursor()
            for statement in schema_sql.split(";"):
                if statement.strip():
                    cursor.execute(statement)

        # Load data from CSV files
        for table_name in tables:
            if table_name not in self.tables:
                continue

            file_path = self.tables[table_name]
            table_schema = TABLES[table_name]

            # Read CSV and insert data
            import csv

            with open(file_path) as f:
                reader = csv.reader(f, delimiter="|")

                # Prepare insert statement
                columns = [col["name"] for col in table_schema["columns"]]
                placeholders = ",".join(["?" for _ in columns])
                insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

                # Insert data
                if hasattr(connection, "executemany"):
                    rows = list(reader)
                    connection.executemany(insert_sql, rows)
                else:
                    cursor = connection.cursor()
                    for row in reader:
                        cursor.execute(insert_sql, row)

        # Commit transaction
        if hasattr(connection, "commit"):
            connection.commit()

    def run_benchmark(
        self, connection: Any, queries: Optional[list[str]] = None, iterations: int = 1
    ) -> dict[str, Any]:
        """Run the complete ClickBench benchmark.

        Args:
            connection: Database connection to use
            queries: Optional list of query IDs to run. If None, runs all.
            iterations: Number of times to run each query

        Returns:
            Dictionary containing benchmark results
        """
        import time

        if queries is None:
            queries = list(self.query_manager.get_all_queries().keys())

        results = {
            "benchmark": "ClickBench",
            "scale_factor": self.scale_factor,
            "iterations": iterations,
            "queries": {},
        }

        for query_id in queries:
            query_results = {
                "query_id": query_id,
                "iterations": [],
                "avg_time": 0,
                "min_time": float("inf"),
                "max_time": 0,
                "sql_text": self.get_query(query_id),  # Add actual SQL text
            }

            for i in range(iterations):
                start_time = time.time()
                try:
                    result = self.execute_query(query_id, connection)
                    end_time = time.time()
                    execution_time = end_time - start_time

                    query_results["iterations"].append(
                        {
                            "iteration": i + 1,
                            "time": execution_time,
                            "rows": len(result) if result else 0,
                            "success": True,
                        }
                    )

                    query_results["min_time"] = min(query_results["min_time"], execution_time)
                    query_results["max_time"] = max(query_results["max_time"], execution_time)

                except Exception as e:
                    query_results["iterations"].append(
                        {
                            "iteration": i + 1,
                            "time": 0,
                            "error": str(e),
                            "success": False,
                        }
                    )

            # Calculate average time for successful iterations
            iterations_list: list[dict[str, Any]] = query_results["iterations"]  # type: ignore[assignment]
            successful_iterations = [iter_result for iter_result in iterations_list if iter_result["success"]]
            if successful_iterations:
                successful_times = [iter_result["time"] for iter_result in successful_iterations]
                successful_rows = [iter_result.get("rows", 0) for iter_result in successful_iterations]
                query_results["avg_time"] = sum(successful_times) / len(successful_times)
                query_results["rows_returned"] = (
                    int(sum(successful_rows) / len(successful_rows)) if successful_rows else 0
                )

            results["queries"][query_id] = query_results

        return results

    def get_query_categories(self) -> dict[str, list[str]]:
        """Get ClickBench queries organized by category.

        Returns:
            Dictionary mapping category names to lists of query IDs
        """
        return self.query_manager.get_query_categories()

    def get_csv_loading_config(self, table_name: str) -> list[str]:
        """Get CSV loading configuration for ClickBench tables.

        ClickBench requires special handling to preserve empty strings rather than
        converting them to NULL values. This is essential because:
        - ClickBench schema defines all fields as NOT NULL
        - ClickBench queries use '<> \'\'' patterns to filter empty strings
        - Official ClickBench specification expects empty strings for missing data

        Args:
            table_name: Name of the table being loaded

        Returns:
            List of CSV loading configuration parameters for DuckDB
        """
        return [
            "delim='|'",  # ClickBench uses pipe delimiter
            "header=false",  # No header row
            "nullstr='__NULL__'",  # Use a marker that won't appear in real data for NULLs
            "ignore_errors=true",  # Continue on parse errors
            "auto_detect=true",  # Auto-detect types
        ]
