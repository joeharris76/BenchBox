"""AMPLab Big Data Benchmark implementation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.amplab.benchmark import AMPLabBenchmark


class AMPLab(BaseBenchmark):
    """AMPLab Big Data Benchmark implementation.

    Provides AMPLab Big Data Benchmark implementation, including
    data generation and access to scan, join, and analytical queries for web analytics data.

    Reference: AMPLab Big Data Benchmark - https://amplab.cs.berkeley.edu/benchmark/
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        """Initialize AMPLab Big Data Benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(AMPLabBenchmark, scale_factor, output_dir, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate AMPLab Big Data Benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all AMPLab Big Data Benchmark queries.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original queries.

        Returns:
            Dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get specific AMPLab Big Data Benchmark query.

        Args:
            query_id: ID of the query to retrieve (1-5)
            params: Optional parameters to customize the query

        Returns:
            Query string

        Raises:
            ValueError: If query_id is invalid
        """
        return self._impl.get_query(query_id, params=params)

    def get_schema(self) -> list[dict]:
        """Get AMPLab Big Data Benchmark schema.

        Returns:
            List of dictionaries describing the tables in the schema
        """
        return self._impl.get_schema()

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all AMPLab Big Data Benchmark tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)
