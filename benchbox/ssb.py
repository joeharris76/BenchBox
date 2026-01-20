"""Star Schema Benchmark implementation.

Copyright 2026 Joe Harris / BenchBox Project

This implementation is derived from TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.ssb.benchmark import SSBBenchmark


class SSB(BaseBenchmark):
    """Star Schema Benchmark implementation.

    This class provides an implementation of the Star Schema Benchmark, including
    data generation and access to the 13 benchmark queries organized in 4 flights.

    Reference: O'Neil et al. "The Star Schema Benchmark and Augmented Fact Table Indexing"
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize a Star Schema Benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(SSBBenchmark, scale_factor, output_dir, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate Star Schema Benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all Star Schema Benchmark queries.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original queries.

        Returns:
            A dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_query(self, query_id: str, *, params: Optional[dict[str, Any]] = None) -> str:
        """Get a specific Star Schema Benchmark query.

        Args:
            query_id: The ID of the query to retrieve (Q1.1-Q4.3)
            params: Optional parameters to customize the query

        Returns:
            The query string

        Raises:
            ValueError: If the query_id is invalid
        """
        return self._impl.get_query(query_id, params=params)

    def get_schema(self) -> list[dict]:
        """Get the Star Schema Benchmark schema.

        Returns:
            A list of dictionaries describing the tables in the schema
        """
        return self._impl.get_schema()

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all Star Schema Benchmark tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)
