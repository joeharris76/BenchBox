"""ClickBench (ClickHouse Analytics Benchmark) implementation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.clickbench.benchmark import ClickBenchBenchmark


class ClickBench(BaseBenchmark):
    """ClickBench (ClickHouse Analytics Benchmark) implementation.

    Provides ClickBench benchmark implementation, including
    data generation and access to the 43 benchmark queries designed for testing
    analytical database performance with web analytics data.

    Official specification: https://github.com/ClickHouse/ClickBench
    Results dashboard: https://benchmark.clickhouse.com/
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        """Initialize ClickBench benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1M records for testing)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(ClickBenchBenchmark, scale_factor, output_dir, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate ClickBench benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        result = self._impl.generate_data()
        return list(result.values()) if isinstance(result, dict) else result

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all ClickBench benchmark queries.

        Args:
            dialect: Target SQL dialect for translation (e.g., 'duckdb', 'bigquery', 'snowflake')
                    If None, returns queries in their original format.

        Returns:
            Dictionary mapping query IDs (Q1-Q43) to query strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get specific ClickBench benchmark query.

        Args:
            query_id: ID of the query to retrieve (Q1-Q43)
            params: Optional parameters to customize the query

        Returns:
            Query string

        Raises:
            ValueError: If query_id is invalid
        """
        return self._impl.get_query(query_id, params=params)

    def get_schema(self) -> list[dict]:
        """Get ClickBench schema.

        Returns:
            List of dictionaries describing the tables in the schema
        """
        schema_dict = self._impl.get_schema()
        return list(schema_dict.values())

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all ClickBench tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)

    def translate_query(self, query_id: str, dialect: str) -> str:
        """Translate a ClickBench query to a different SQL dialect.

        Args:
            query_id: The ID of the query to translate (Q1-Q43)
            dialect: The target SQL dialect (postgres, mysql, bigquery, etc.)

        Returns:
            The translated query string

        Raises:
            ValueError: If the query_id is invalid
            ImportError: If sqlglot is not installed
            ValueError: If the dialect is not supported
        """
        return super().translate_query(query_id, dialect)

    def get_query_categories(self) -> dict[str, list[str]]:
        """Get ClickBench queries organized by category.

        Returns:
            Dictionary mapping category names to lists of query IDs
        """
        return self._impl.get_query_categories()
