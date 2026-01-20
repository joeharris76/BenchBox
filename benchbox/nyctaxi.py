"""NYC Taxi OLAP benchmark implementation.

NYC Taxi & Limousine Commission trip data for OLAP analytics.
Uses real transportation data from NYC TLC trip records.

Example:
    >>> from benchbox import NYCTaxi
    >>> from benchbox.platforms.duckdb import DuckDBAdapter
    >>>
    >>> # Create benchmark (SF=1 = ~30M trips)
    >>> benchmark = NYCTaxi(scale_factor=1.0)
    >>>
    >>> # Download/generate data
    >>> data_files = benchmark.generate_data()
    >>>
    >>> # Run with platform adapter
    >>> adapter = DuckDBAdapter(database=":memory:")
    >>> adapter.load_benchmark(benchmark)
    >>>
    >>> # Execute queries
    >>> for query_id, query in benchmark.get_queries().items():
    ...     result = adapter.execute_query(query)

Scale factors:
- SF=0.01: ~300K trips (~11MB) - Quick testing
- SF=0.1: ~3M trips (~110MB) - Development
- SF=1.0: ~30M trips (~1.1GB) - Standard benchmark
- SF=10: ~300M trips (~11GB) - Large scale
- SF=100: ~3B trips (~111GB) - Full dataset

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.nyctaxi.benchmark import NYCTaxiBenchmark


class NYCTaxi(BaseBenchmark):
    """NYC Taxi OLAP benchmark for transportation analytics.

    Implements NYC TLC trip data analysis:
    - 25 representative OLAP queries
    - Temporal aggregations (hourly, daily, monthly)
    - Geographic analytics (zone-level patterns)
    - Financial analysis (revenue, tips, fares)
    - Multi-dimensional analysis

    Query categories:
    - temporal: Time-based aggregations
    - geographic: Zone-level analytics
    - financial: Revenue and tip analysis
    - characteristics: Trip characteristics
    - rates: Rate code analysis
    - vendor: Vendor comparisons
    - complex: Multi-dimensional queries
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        year: int = 2019,
        months: Optional[list[int]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize NYC Taxi benchmark.

        Args:
            scale_factor: Scale factor (1.0 = ~30M trips)
            output_dir: Directory for data files
            year: Year of data to use (2019-2025)
            months: Specific months to use (1-12), None for all
            **kwargs: Additional options:
                - seed: Random seed for reproducibility
                - verbose: Verbosity level
                - force_regenerate: Force data regeneration

        Raises:
            ValueError: If scale_factor is not positive or year is invalid
            TypeError: If scale_factor is not a number
        """
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(
            NYCTaxiBenchmark,
            scale_factor,
            output_dir,
            year=year,
            months=months,
            **kwargs,
        )

    def generate_data(self) -> list[Union[str, Path]]:
        """Download/generate NYC Taxi benchmark data.

        Downloads real TLC data or generates synthetic data as fallback.
        Data includes taxi trips and zone dimension table.

        Returns:
            List of paths to data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all benchmark queries.

        Args:
            dialect: Target SQL dialect (not used - queries are standard SQL)

        Returns:
            Dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect)

    def get_query(
        self,
        query_id: Union[int, str],
        *,
        params: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        """Get a specific benchmark query.

        Args:
            query_id: Query identifier (e.g., "trips-per-hour")
            params: Optional parameter overrides
            **kwargs: Additional arguments

        Returns:
            Parameterized query string

        Raises:
            ValueError: If query_id is unknown
        """
        return self._impl.get_query(query_id, params=params, **kwargs)

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the NYC Taxi schema.

        Returns:
            Schema dictionary with table definitions including:
            - taxi_zones: Zone dimension table
            - trips: Trip fact table
        """
        return self._impl.get_schema()

    def get_create_tables_sql(
        self,
        dialect: str = "standard",
        tuning_config: Any = None,
    ) -> str:
        """Get SQL to create all benchmark tables.

        Args:
            dialect: SQL dialect (standard, duckdb, clickhouse, postgres)
            tuning_config: Optional tuning configuration

        Returns:
            SQL script for creating tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)

    def get_benchmark_info(self) -> dict[str, Any]:
        """Get benchmark metadata.

        Returns:
            Dictionary with:
            - name: "NYC Taxi OLAP"
            - description: Benchmark description
            - reference: TLC website URL
            - scale_factor: Current scale factor
            - year: Data year
            - num_queries: Number of queries
            - query_categories: List of categories
            - tables: List of table names
        """
        return self._impl.get_benchmark_info()

    @property
    def tables(self) -> dict[str, Path]:
        """Get mapping of table names to data file paths.

        Returns:
            Dictionary mapping table names to file paths
        """
        return getattr(self._impl, "tables", {})

    @property
    def year(self) -> int:
        """Get the year of data being used."""
        return self._impl.year

    def get_query_info(self, query_id: str) -> dict[str, Any]:
        """Get metadata for a specific query.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary with query metadata (name, description, category, etc.)
        """
        return self._impl.get_query_info(query_id)

    def get_queries_by_category(self, category: str) -> list[str]:
        """Get query IDs for a specific category.

        Available categories:
        - temporal: Time-based aggregations
        - geographic: Zone-level analytics
        - financial: Revenue and tip analysis
        - characteristics: Trip characteristics
        - rates: Rate code analysis
        - vendor: Vendor comparisons
        - complex: Multi-dimensional queries
        - point: Single-value lookups
        - baseline: Full table scans

        Args:
            category: Query category name

        Returns:
            List of query IDs in that category
        """
        return self._impl.get_queries_by_category(category)

    def get_download_stats(self) -> dict:
        """Get statistics about the data download.

        Returns:
            Dictionary with:
            - scale_factor: Current scale factor
            - sample_rate: Data sampling rate
            - year: Data year
            - months: Months included
        """
        return self._impl.get_download_stats()
