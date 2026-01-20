"""AMPLab Big Data Benchmark implementation.

Provides AMPLab Big Data Benchmark implementation that tests big data processing systems using web analytics workloads.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.amplab.generator import AMPLabDataGenerator
from benchbox.core.amplab.queries import AMPLabQueryManager
from benchbox.core.amplab.schema import TABLES, get_all_create_table_sql
from benchbox.core.connection import DatabaseConnection

if TYPE_CHECKING:
    from benchbox.core.tuning.interface import UnifiedTuningConfiguration


class AMPLabBenchmark(BaseBenchmark):
    """AMPLab Big Data Benchmark implementation.

    Tests big data processing systems using web analytics workloads.

    The benchmark consists of:
    - 3 tables: rankings, uservisits, documents
    - Multiple query types: scan, join, and analytics queries
    - Focus on big data processing performance

    Attributes:
        scale_factor: Scale factor for the benchmark (1.0 = standard size)
        output_dir: Directory to output generated data and results
        query_manager: AMPLab query manager
        data_generator: AMPLab data generator
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **config: Any,
    ):
        """Initialize AMPLab benchmark.

        Args:
            scale_factor: Scale factor for data generation (1.0 = standard size)
            output_dir: Directory for generated data files
            **config: Additional configuration options
        """
        # Extract quiet from config to prevent duplicate kwarg error
        config = dict(config)
        quiet = config.pop("quiet", False)

        super().__init__(scale_factor, output_dir=output_dir, quiet=quiet, **config)

        self._name = "AMPLab Big Data Benchmark"
        self._version = "1.0"
        self._description = "AMPLab Big Data Benchmark - Tests big data processing systems with web analytics workloads"

        # Initialize components
        self.query_manager: AMPLabQueryManager = AMPLabQueryManager()
        self.data_generator = AMPLabDataGenerator(
            scale_factor,
            self.output_dir,
            **config,
        )

        # Data files mapping
        self.tables = {}

    def generate_data(self, tables: Optional[list[str]] = None, output_format: str = "csv") -> dict[str, Any]:
        """Generate AMPLab data.

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

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get the SQL text for a specific AMPLab query.

        Args:
            query_id: Query identifier (e.g., "1", "2", "3", etc.)
            params: Optional parameter values to use in the query

        Returns:
            The SQL text of the query with parameters substituted

        Raises:
            ValueError: If the query_id is not valid
        """
        return self.query_manager.get_query(str(query_id), params)

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all available AMPLab queries.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original queries.

        Returns:
            A dictionary mapping query identifiers to their SQL text
        """
        queries = self.query_manager.get_all_queries()

        if dialect:
            # Translate each query to the target dialect
            translated_queries = {}
            for query_id, query_sql in queries.items():
                translated_queries[query_id] = self.translate_query_text(query_sql, dialect)
            return translated_queries

        return queries

    def translate_query_text(self, query_text: str, target_dialect: str) -> str:
        """Translate a query from AMPLab's source dialect to target dialect.

        Args:
            query_text: SQL query text to translate
            target_dialect: Target SQL dialect (e.g., 'duckdb', 'bigquery', 'snowflake')

        Returns:
            Translated SQL query text

        """
        from benchbox.utils.dialect_utils import translate_sql_query

        # AMPLab queries use modern SQL (netezza/postgres) as source dialect
        return translate_sql_query(
            query=query_text,
            target_dialect=target_dialect,
            source_dialect="netezza",
        )

    def get_all_queries(self) -> dict[str, str]:
        """Get all available AMPLab queries.

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
        """Execute an AMPLab query on the given database connection.

        Args:
            query_id: Query identifier (e.g., "1", "2", "3", etc.)
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
        """Get the AMPLab schema definitions.

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
        """Get CREATE TABLE SQL for all AMPLab tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            Complete SQL schema creation script
        """
        # Extract constraint settings from tuning configuration
        enable_primary_keys = tuning_config.primary_keys.enabled if tuning_config else False
        enable_foreign_keys = tuning_config.foreign_keys.enabled if tuning_config else False

        return get_all_create_table_sql(dialect, enable_primary_keys, enable_foreign_keys)

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

                # Insert data in batches for better performance
                batch_size = 10000
                batch = []

                if hasattr(connection, "executemany"):
                    for row in reader:
                        batch.append(row)
                        if len(batch) >= batch_size:
                            connection.executemany(insert_sql, batch)
                            batch = []
                    if batch:
                        connection.executemany(insert_sql, batch)
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
        """Run the complete AMPLab benchmark.

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
            "benchmark": "AMPLab",
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

    def _load_data(self, connection: DatabaseConnection) -> None:
        """Load AMPLab data into the database.

        This method loads the generated AMPLab data files (.csv format) into the database
        using a simple, database-agnostic approach with INSERT statements.

        Args:
            connection: DatabaseConnection wrapper for database operations

        Raises:
            ValueError: If data hasn't been generated yet
            Exception: If data loading fails
        """
        import logging

        logger = logging.getLogger(__name__)

        # Check if data has been generated
        if not self.tables:
            raise ValueError("No data has been generated. Call generate_data() first.")

        logger.info("Loading AMPLab data into database...")

        # Create database schema first
        try:
            schema_sql = self.get_create_tables_sql()
            # Handle databases that don't support multiple statements at once
            if ";" in schema_sql:
                # Split by semicolons and execute each statement separately
                statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]
                for statement in statements:
                    connection.execute(statement)
            else:
                connection.execute(schema_sql)
            connection.commit()
            logger.info("✅ Created AMPLab database schema")
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}")
            raise

        # Load data for each table
        total_rows = 0
        loaded_tables = 0

        # Load tables in dependency order
        table_order = ["rankings", "documents", "uservisits"]

        for table_name in table_order:
            if table_name not in self.tables:
                logger.warning(f"Skipping {table_name} - no data file found")
                continue

            data_file = Path(self.tables[table_name])
            if not data_file.exists():
                logger.warning(f"Skipping {table_name} - data file does not exist: {data_file}")
                continue

            try:
                logger.info(f"Loading data for {table_name.upper()}...")
                rows_loaded = self._load_table_data(connection, table_name, data_file)

                total_rows += rows_loaded
                loaded_tables += 1
                logger.info(f"✅ Loaded {rows_loaded:,} rows into {table_name.upper()}")

            except Exception as e:
                logger.error(f"Failed to load data for {table_name}: {e}")
                raise

        # Commit all changes
        try:
            connection.commit()
            logger.info(f"✅ Successfully loaded {total_rows:,} total rows across {loaded_tables} tables")
        except Exception as e:
            logger.error(f"Failed to commit data loading transaction: {e}")
            raise

    def _load_table_data(self, connection: DatabaseConnection, table_name: str, data_file: Path) -> int:
        """Load data into a database table using simple INSERT statements.

        Args:
            connection: DatabaseConnection wrapper
            table_name: Name of the table to load data into
            data_file: Path to the data file

        Returns:
            Number of rows loaded
        """
        import csv

        # Get table schema to determine column count
        table_schema = TABLES[table_name]
        num_columns = len(table_schema["columns"])

        # Prepare insert statement with parameter placeholders
        placeholders = ", ".join(["?" for _ in range(num_columns)])
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        rows_loaded = 0

        with open(data_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="|")

            for row in reader:
                # Validate row has correct number of columns
                if len(row) != num_columns:
                    continue  # Skip malformed rows

                # Execute individual INSERT
                connection.execute(insert_sql, row)
                rows_loaded += 1

        return rows_loaded

    def get_csv_loading_config(self, table_name: str) -> list[str]:
        """Get CSV loading configuration for AMPLab tables.

        AMPLab uses pipe (|) delimiter for all CSV files.

        Args:
            table_name: Name of the table being loaded

        Returns:
            List of CSV loading configuration parameters
        """
        return ["delim='|'"]
