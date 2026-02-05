"""Write Primitives benchmark - public API wrapper.

Provides clean public API for Write Primitives benchmark functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.operations import OperationExecutor
from benchbox.core.write_primitives.benchmark import WritePrimitivesBenchmark


class WritePrimitives(BaseBenchmark, OperationExecutor):
    """Write Primitives benchmark implementation.

    Tests fundamental write operations (INSERT, UPDATE, DELETE, BULK_LOAD,
    MERGE, DDL, TRANSACTION) using TPC-H schema as foundation.

    The benchmark covers:
    - INSERT operations: single row, batch, INSERT...SELECT
    - UPDATE operations: selective, bulk, with joins/subqueries
    - DELETE operations: selective, bulk, cascading
    - BULK_LOAD operations: CSV, Parquet, various compressions
    - MERGE operations: insert/update/delete combinations
    - DDL operations: CREATE, DROP, TRUNCATE, ALTER
    - TRANSACTION operations: commit, rollback, savepoints
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Write Primitives benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB TPC-H data)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        # Skip base class output_dir handling - let WritePrimitivesBenchmark set TPC-H path
        # We'll set output_dir after implementation is initialized
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation
        # Pass original output_dir (None or user-specified) so implementation can use TPC-H path
        self._impl = WritePrimitivesBenchmark(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Sync output_dir from implementation (which correctly uses TPC-H path)
        self.output_dir = self._impl.output_dir

    def get_data_source_benchmark(self) -> Optional[str]:
        """Get the benchmark that provides data for this benchmark.

        Returns:
            "tpch" to indicate this benchmark reuses TPC-H data
        """
        return self._impl.get_data_source_benchmark()

    @property
    def tables(self) -> dict[str, Path]:
        """Get the mapping of table names to data file paths.

        Returns:
            Dictionary mapping table names to paths of generated data files
        """
        return getattr(self._impl, "tables", {})

    def generate_data(self, tables: Optional[list[str]] = None) -> list[Union[str, Path]]:
        """Generate Write Primitives benchmark data.

        This generates/reuses TPC-H base data. Staging tables are created
        during benchmark setup.

        Args:
            tables: Optional list of table names to generate. If None, generates all.

        Returns:
            List of paths to generated data files
        """
        return self._impl.generate_data(tables)

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all write operation SQL.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original SQL.

        Returns:
            A dictionary mapping operation IDs to write SQL strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_queries_by_category(self, category: str) -> dict[str, str]:
        """Get write operation SQL filtered by category.

        Args:
            category: Operation category (insert, update, delete, ddl, transaction)

        Returns:
            Dictionary mapping operation IDs to write SQL for the category
        """
        return self._impl.get_queries_by_category(category)

    def get_query(self, query_id: Union[int, str], **kwargs) -> str:
        """Get a specific write operation SQL.

        Args:
            query_id: The ID of the operation to retrieve (e.g., 'insert_single_row')
            **kwargs: Additional parameters

        Returns:
            The write SQL string

        Raises:
            ValueError: If the query_id is invalid
        """
        return self._impl.get_query(query_id, **kwargs)

    def get_operation(self, operation_id: str) -> Any:
        """Get a specific write operation.

        Args:
            operation_id: Operation identifier

        Returns:
            WriteOperation object
        """
        return self._impl.get_operation(operation_id)

    def get_all_operations(self) -> dict[str, Any]:
        """Get all write operations.

        Returns:
            Dictionary mapping operation IDs to WriteOperation objects
        """
        return self._impl.get_all_operations()

    def get_operations_by_category(self, category: str) -> dict[str, Any]:
        """Get operations filtered by category.

        Args:
            category: Category name (e.g., 'insert', 'update', 'delete')

        Returns:
            Dictionary mapping operation IDs to WriteOperation objects
        """
        return self._impl.get_operations_by_category(category)

    def get_operation_categories(self) -> list[str]:
        """Get list of available operation categories.

        Returns:
            List of category names
        """
        return self._impl.get_operation_categories()

    def get_schema(self, dialect: str = "standard") -> dict[str, dict]:
        """Get the Write Primitives schema (staging tables).

        Args:
            dialect: SQL dialect to use for data types

        Returns:
            A dictionary mapping table names to their schema definitions
        """
        return self._impl.get_schema(dialect=dialect)

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all required tables.

        Includes both TPC-H base tables and Write Primitives staging tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)

    def get_benchmark_info(self) -> dict[str, Any]:
        """Get information about the benchmark.

        Returns:
            Dictionary containing benchmark metadata
        """
        return self._impl.get_benchmark_info()

    def setup(self, connection: Any, force: bool = False) -> dict[str, Any]:
        """Setup benchmark for execution.

        Creates staging tables and populates them from TPC-H base tables.

        Args:
            connection: Database connection
            force: If True, drop existing staging tables first

        Returns:
            Dictionary with setup status and details

        Raises:
            RuntimeError: If TPC-H tables don't exist or setup fails
        """
        return self._impl.setup(connection, force)

    def load_data(self, connection: Any, **kwargs) -> dict[str, Any]:
        """Load data into database (standard benchmark interface).

        This wraps the setup() method to integrate with platform adapter's
        data loading pipeline.

        Args:
            connection: Database connection
            **kwargs: Additional arguments (e.g., force)

        Returns:
            Dictionary with loading results
        """
        return self._impl.load_data(connection, **kwargs)

    def teardown(self, connection: Any) -> None:
        """Clean up all staging tables.

        Args:
            connection: Database connection
        """
        return self._impl.teardown(connection)

    def reset(self, connection: Any) -> None:
        """Reset staging tables to initial state.

        Truncates and repopulates staging tables.

        Args:
            connection: Database connection
        """
        return self._impl.reset(connection)

    def is_setup(self, connection: Any) -> bool:
        """Check if staging tables are ready.

        Args:
            connection: Database connection

        Returns:
            True if all staging tables exist and have data
        """
        return self._impl.is_setup(connection)

    def execute_operation(self, operation_id: str, connection: Any, use_transaction: bool = True) -> Any:
        """Execute a write operation and validate results.

        Args:
            operation_id: ID of operation to execute
            connection: Database connection
            use_transaction: If True, wrap in transaction and rollback after validation

        Returns:
            OperationResult with execution metrics
        """
        return self._impl.execute_operation(operation_id, connection, use_transaction)

    def run_benchmark(
        self,
        connection: Any,
        operation_ids: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
    ) -> list[Any]:
        """Run write operations benchmark.

        Args:
            connection: Database connection
            operation_ids: Optional list of specific operations to run
            categories: Optional list of categories to run

        Returns:
            List of OperationResult objects
        """
        return self._impl.run_benchmark(connection, operation_ids, categories)


__all__ = ["WritePrimitives", "WritePrimitivesBenchmark"]
