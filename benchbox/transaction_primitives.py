"""Transaction Primitives benchmark - public API wrapper.

Provides clean public API for Transaction Primitives benchmark functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.operations import OperationExecutor
from benchbox.core.transaction_primitives.benchmark import TransactionPrimitivesBenchmark


class TransactionPrimitives(BaseBenchmark, OperationExecutor):
    """Transaction Primitives benchmark implementation.

    Tests database transaction semantics and overhead using TPC-H schema as foundation.

    The benchmark covers:
    - Transaction commit overhead: small (10 rows), medium (100 rows), large (1000 rows)
    - Transaction rollback overhead: small (3 rows), medium (100 rows)
    - Savepoints: nested savepoints with partial rollback
    - Isolation levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE
    - Multi-statement transactions: mixed DML operations within single transaction
    - Advanced features: deferred constraints, transaction-scoped objects, DDL in transactions

    This benchmark requires full ACID transaction support and is designed for:
    PostgreSQL, MySQL, Snowflake, Databricks, Redshift, and similar platforms.

    Platforms with limited ACID support (DuckDB, ClickHouse, BigQuery) may skip
    operations that are not supported.
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Transaction Primitives benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB TPC-H data)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        # Skip base class output_dir handling - let TransactionPrimitivesBenchmark set TPC-H path
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation
        self._impl = TransactionPrimitivesBenchmark(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

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
        """Generate Transaction Primitives benchmark data.

        This generates/reuses TPC-H base data. Staging tables are created
        during benchmark setup.

        Args:
            tables: Optional list of table names to generate. If None, generates all.

        Returns:
            List of paths to generated data files
        """
        return self._impl.generate_data(tables)

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all transaction operation SQL.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original SQL.

        Returns:
            A dictionary mapping operation IDs to transaction SQL strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_queries_by_category(self, category: str) -> dict[str, str]:
        """Get transaction operation SQL filtered by category.

        Args:
            category: Operation category (overhead, isolation, multi_statement, advanced)

        Returns:
            Dictionary mapping operation IDs to transaction SQL for the category
        """
        return self._impl.get_queries_by_category(category)

    def get_query(self, query_id: Union[int, str], **kwargs) -> str:
        """Get a specific transaction operation SQL.

        Args:
            query_id: The ID of the operation to retrieve (e.g., 'transaction_commit_small')
            **kwargs: Additional parameters

        Returns:
            The transaction SQL string

        Raises:
            ValueError: If the query_id is invalid
        """
        return self._impl.get_query(query_id, **kwargs)

    def get_operation(self, operation_id: str) -> Any:
        """Get a specific transaction operation.

        Args:
            operation_id: Operation identifier

        Returns:
            TransactionOperation object
        """
        return self._impl.get_operation(operation_id)

    def get_all_operations(self) -> dict[str, Any]:
        """Get all transaction operations.

        Returns:
            Dictionary mapping operation IDs to TransactionOperation objects
        """
        return self._impl.get_all_operations()

    def get_operations_by_category(self, category: str) -> dict[str, Any]:
        """Get operations filtered by category.

        Args:
            category: Category name (e.g., 'overhead', 'isolation', 'multi_statement')

        Returns:
            Dictionary mapping operation IDs to TransactionOperation objects
        """
        return self._impl.get_operations_by_category(category)

    def get_operation_categories(self) -> list[str]:
        """Get list of available operation categories.

        Returns:
            List of category names
        """
        return self._impl.get_operation_categories()

    def get_schema(self, dialect: str = "standard") -> dict[str, dict]:
        """Get the Transaction Primitives schema (staging tables).

        Args:
            dialect: SQL dialect to use for data types

        Returns:
            A dictionary mapping table names to their schema definitions
        """
        return self._impl.get_schema(dialect=dialect)

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all required tables.

        Includes both TPC-H base tables and Transaction Primitives staging tables.

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

    def execute_operation(self, operation_id: str, connection: Any) -> Any:
        """Execute a transaction operation and validate results.

        Args:
            operation_id: ID of operation to execute
            connection: Database connection

        Returns:
            OperationResult with execution metrics
        """
        return self._impl.execute_operation(operation_id, connection)

    def run_benchmark(
        self,
        connection: Any,
        operation_ids: Optional[list[str]] = None,
        categories: Optional[list[str]] = None,
    ) -> list[Any]:
        """Run transaction operations benchmark.

        Args:
            connection: Database connection
            operation_ids: Optional list of specific operations to run
            categories: Optional list of categories to run

        Returns:
            List of OperationResult objects
        """
        return self._impl.run_benchmark(connection, operation_ids, categories)


__all__ = ["TransactionPrimitives", "TransactionPrimitivesBenchmark"]
