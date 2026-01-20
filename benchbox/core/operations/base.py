"""Base interfaces for operation execution in benchmarks.

This module defines abstract interfaces for benchmarks that execute discrete
operations (like write operations, maintenance operations, etc.) rather than
just read-only queries.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from abc import ABC, abstractmethod
from typing import Any


class OperationExecutor(ABC):
    """Abstract interface for benchmarks that execute discrete operations.

    This interface distinguishes benchmarks that execute individual operations
    (INSERT, UPDATE, DELETE, DDL, etc.) from those that only execute read queries.

    Implementing this interface allows the platform adapter to detect and properly
    handle operation-based benchmarks with custom execution semantics.

    Example implementations:
    - WritePrimitivesBenchmark: Executes write operations with validation/cleanup
    - Future: MergeOperationsBenchmark, BulkLoadBenchmark, etc.

    Note: This is NOT used by TPC-DS/TPC-H maintenance tests, which have their
    own specialized execution paths through test-specific classes.
    """

    @abstractmethod
    def execute_operation(
        self,
        operation_id: str,
        connection: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute a single operation by its identifier.

        Args:
            operation_id: Unique identifier for the operation (e.g., 'insert_single_row')
            connection: Database connection object
            **kwargs: Additional operation-specific parameters

        Returns:
            Operation result object with execution metrics

        Raises:
            ValueError: If operation_id is invalid
            RuntimeError: If operation cannot be executed
        """

    @abstractmethod
    def get_all_operations(self) -> dict[str, Any]:
        """Get all available operations.

        Returns:
            Dictionary mapping operation IDs to operation definitions
        """

    @abstractmethod
    def get_operation_categories(self) -> list[str]:
        """Get list of available operation categories.

        Returns:
            List of category names (e.g., ['insert', 'update', 'delete'])
        """
