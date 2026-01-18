"""NYC Taxi OLAP benchmark implementation.

Implements NYC Taxi benchmark using real TLC trip data for OLAP analytics.
Provides authentic multi-dimensional workloads with temporal and
geographic dimensions.

Based on NYC Taxi & Limousine Commission data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from benchbox.core.nyctaxi.downloader import NYCTaxiDataDownloader
from benchbox.core.nyctaxi.queries import NYCTaxiQueryManager
from benchbox.core.nyctaxi.schema import (
    NYC_TAXI_SCHEMA,
    get_create_tables_sql,
)
from benchbox.utils.verbosity import VerbosityMixin, compute_verbosity

if TYPE_CHECKING:
    from benchbox.core.connection import DatabaseConnection


class NYCTaxiBenchmark(VerbosityMixin):
    """NYC Taxi OLAP benchmark implementation.

    Uses real NYC TLC trip data for OLAP analytics workloads:
    - 25 representative OLAP queries
    - Temporal aggregations (hourly, daily, monthly)
    - Geographic analytics (zone-level patterns)
    - Financial analysis (revenue, tips, fares)
    - Multi-dimensional analysis

    Usage:
        >>> from benchbox.core.nyctaxi import NYCTaxiBenchmark
        >>> from benchbox.platforms.duckdb import DuckDBAdapter
        >>>
        >>> # Create benchmark (SF=1 = ~30M trips)
        >>> benchmark = NYCTaxiBenchmark(scale_factor=1.0)
        >>>
        >>> # Download/generate data
        >>> data_files = benchmark.generate_data()
        >>>
        >>> # Get queries
        >>> queries = benchmark.get_queries()
        >>>
        >>> # Run with platform adapter
        >>> adapter = DuckDBAdapter(database=":memory:")
        >>> adapter.load_benchmark(benchmark)
        >>> results = adapter.run_benchmark(benchmark)

    Scale Factors:
        - SF=0.01: ~300K trips (~11MB) - Quick testing
        - SF=0.1: ~3M trips (~110MB) - Development
        - SF=1.0: ~30M trips (~1.1GB) - Standard benchmark
        - SF=10: ~300M trips (~11GB) - Large scale
        - SF=100: ~3B trips (~111GB) - Full dataset

    Attributes:
        scale_factor: Scale factor controlling data size
        year: Year of data to use
        tables: Dictionary of table names to file paths
    """

    # Available years for validation
    AVAILABLE_YEARS = list(range(2019, 2026))

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Union[str, Path] | None = None,
        year: int = 2019,
        months: list[int] | None = None,
        seed: int | None = None,
        verbose: int | bool = 0,
        quiet: bool = False,
        force_regenerate: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize NYC Taxi benchmark.

        Args:
            scale_factor: Scale factor (1.0 = ~30M trips)
            output_dir: Directory for data files
            year: Year of data to use (2019-2025)
            months: Specific months to use (1-12), None for all
            seed: Random seed for reproducibility
            verbose: Verbosity level
            quiet: Suppress output
            force_regenerate: Force data regeneration
            **kwargs: Additional options

        Raises:
            ValueError: If scale_factor is not positive or year is invalid
        """
        if scale_factor <= 0:
            raise ValueError(f"scale_factor must be positive, got {scale_factor}")

        if year not in self.AVAILABLE_YEARS:
            raise ValueError(f"year must be in {self.AVAILABLE_YEARS[0]}-{self.AVAILABLE_YEARS[-1]}, got {year}")

        self.scale_factor = scale_factor
        self.output_dir = (
            Path(output_dir) if output_dir else Path.cwd() / "benchmark_runs" / "datagen" / f"nyctaxi_{scale_factor}"
        )
        self.year = year
        self.months = months
        self.seed = seed
        self.force_regenerate = force_regenerate

        # Benchmark metadata
        self._name = "NYC Taxi OLAP"
        self._version = "1.0"
        self._description = "NYC Taxi & Limousine Commission trip data for OLAP analytics"

        # Initialize verbosity
        verbosity_settings = compute_verbosity(verbose, quiet)
        self.apply_verbosity(verbosity_settings)
        self.logger = logging.getLogger("benchbox.core.nyctaxi.benchmark")

        # Create data downloader
        self.downloader = NYCTaxiDataDownloader(
            scale_factor=scale_factor,
            output_dir=self.output_dir,
            year=year,
            months=months,
            seed=seed,
            verbose=verbose,
            quiet=quiet,
            force_redownload=force_regenerate,
        )

        # Determine date range for queries
        if months:
            start_month = min(months)
            end_month = max(months)
        else:
            start_month = 1
            end_month = 12

        import calendar

        start_date = datetime(year, start_month, 1)
        # Use last day of the end month (28-31 depending on month)
        _, last_day = calendar.monthrange(year, end_month)
        end_date = datetime(year, end_month, last_day)

        # Initialize query manager
        self.query_manager = NYCTaxiQueryManager(
            start_date=start_date,
            end_date=end_date,
            seed=seed,
        )

        # Track generated table files
        self.tables: dict[str, Path] = {}

    def generate_data(self) -> list[Union[str, Path]]:
        """Download/generate NYC Taxi benchmark data.

        Returns:
            List of paths to data files
        """
        self.log_verbose(f"Generating NYC Taxi data (SF={self.scale_factor})")

        self.tables = self.downloader.download()

        if self.verbose_enabled:
            stats = self.downloader.get_download_stats()
            self.logger.info(f"Data ready: year={stats['year']}, sample_rate={stats['sample_rate']:.4f}")

        return list(self.tables.values())

    def get_queries(self, dialect: str | None = None) -> dict[str, str]:
        """Get all benchmark queries.

        Args:
            dialect: Target SQL dialect (not used - queries are standard SQL)

        Returns:
            Dictionary mapping query IDs to query strings
        """
        return self.query_manager.get_queries()

    def get_query(
        self,
        query_id: Union[int, str],
        *,
        params: dict[str, Any] | None = None,
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
        query_key = str(query_id)
        return self.query_manager.get_query(query_key, params)

    def get_schema(self) -> dict[str, dict[str, Any]]:
        """Get the NYC Taxi schema.

        Returns:
            Schema dictionary with table definitions
        """
        return NYC_TAXI_SCHEMA

    def get_create_tables_sql(
        self,
        dialect: str = "standard",
        include_constraints: bool = True,
        time_partitioning: bool = False,
        tuning_config: Any = None,
    ) -> str:
        """Get SQL to create all benchmark tables.

        Args:
            dialect: SQL dialect (standard, duckdb, clickhouse, postgres)
            include_constraints: Include PRIMARY KEY constraints
            time_partitioning: Add time-based partitioning
            tuning_config: Optional tuning configuration (not used)

        Returns:
            SQL script for creating tables
        """
        return get_create_tables_sql(
            dialect=dialect,
            include_constraints=include_constraints,
            time_partitioning=time_partitioning,
        )

    def get_benchmark_info(self) -> dict[str, Any]:
        """Get benchmark metadata.

        Returns:
            Dictionary with benchmark information
        """
        return {
            "name": "NYC Taxi OLAP",
            "description": "NYC Taxi & Limousine Commission trip data for OLAP analytics",
            "reference": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
            "version": "1.0",
            "scale_factor": self.scale_factor,
            "year": self.year,
            "months": self.months or list(range(1, 13)),
            "num_queries": self.query_manager.get_query_count(),
            "query_categories": self.query_manager.get_categories(),
            "tables": ["taxi_zones", "trips"],
            "data_type": "real",  # Uses real TLC data, not synthetic
            "dimensions": {
                "temporal": "pickup/dropoff timestamps",
                "geographic": "TLC taxi zones",
                "financial": "fares, tips, surcharges",
            },
        }

    def get_query_info(self, query_id: str) -> dict[str, Any]:
        """Get metadata for a specific query.

        Args:
            query_id: Query identifier

        Returns:
            Dictionary with query metadata
        """
        return self.query_manager.get_query_info(query_id)

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
        return self.query_manager.get_queries_by_category(category)

    def get_download_stats(self) -> dict:
        """Get statistics about the data download.

        Returns:
            Statistics dictionary
        """
        return self.downloader.get_download_stats()

    def _load_data(self, connection: DatabaseConnection) -> None:
        """Load NYC Taxi data into the database.

        This method loads the generated NYC Taxi data files (.csv format) into the database
        using a simple, database-agnostic approach with INSERT statements.

        Args:
            connection: DatabaseConnection wrapper for database operations

        Raises:
            ValueError: If data hasn't been generated yet
            Exception: If data loading fails
        """
        logger = logging.getLogger(__name__)

        # Check if data has been generated
        if not self.tables:
            raise ValueError("No data has been generated. Call generate_data() first.")

        logger.info("Loading NYC Taxi data into database...")

        # Create database schema first
        try:
            schema_sql = self.get_create_tables_sql()
            # Handle databases that don't support multiple statements at once
            if ";" in schema_sql:
                statements = [stmt.strip() for stmt in schema_sql.split(";") if stmt.strip()]
                for statement in statements:
                    connection.execute(statement)
            else:
                connection.execute(schema_sql)
            connection.commit()
            logger.info("Created NYC Taxi database schema")
        except Exception as e:
            logger.error(f"Failed to create database schema: {e}")
            raise

        # Load data for each table
        total_rows = 0
        loaded_tables = 0

        # Load tables in dependency order (dimension tables first)
        table_order = ["taxi_zones", "trips"]

        for table_name in table_order:
            if table_name not in self.tables:
                logger.warning(f"Skipping {table_name} - no data file found")
                continue

            data_file = Path(self.tables[table_name])
            if not data_file.exists():
                logger.warning(f"Skipping {table_name} - data file does not exist: {data_file}")
                continue

            try:
                logger.info(f"Loading data for {table_name}...")
                rows_loaded = self._load_table_data(connection, table_name, data_file)

                total_rows += rows_loaded
                loaded_tables += 1
                logger.info(f"Loaded {rows_loaded:,} rows into {table_name}")

            except Exception as e:
                logger.error(f"Failed to load data for {table_name}: {e}")
                raise

        # Commit all changes
        try:
            connection.commit()
            logger.info(f"Successfully loaded {total_rows:,} total rows across {loaded_tables} tables")
        except Exception as e:
            logger.error(f"Failed to commit data loading transaction: {e}")
            raise

    def _load_table_data(self, connection: DatabaseConnection, table_name: str, data_file: Path) -> int:
        """Load data into a database table using INSERT statements.

        Args:
            connection: DatabaseConnection wrapper
            table_name: Name of the table to load data into
            data_file: Path to the CSV data file

        Returns:
            Number of rows loaded
        """
        # Get table schema to determine column count
        table_schema = NYC_TAXI_SCHEMA[table_name]
        column_names = list(table_schema["columns"].keys())
        num_columns = len(column_names)

        # Prepare insert statement with parameter placeholders
        placeholders = ", ".join(["?" for _ in range(num_columns)])
        insert_sql = f"INSERT INTO {table_name} VALUES ({placeholders})"

        rows_loaded = 0

        with open(data_file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            # Skip header row
            next(reader, None)

            for row in reader:
                # Validate row has correct number of columns
                if len(row) != num_columns:
                    continue  # Skip malformed rows

                # Execute individual INSERT
                connection.execute(insert_sql, row)
                rows_loaded += 1

        return rows_loaded
