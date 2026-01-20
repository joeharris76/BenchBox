"""TPC-DI (Data Integration) benchmark implementation.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DI (TPC-DI) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DI specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.tpcdi.benchmark import TPCDIBenchmark


class TPCDI(BaseBenchmark):
    """TPC-DI benchmark implementation.

    This class provides an implementation of the TPC-DI benchmark, including
    data generation and access to validation and analytical queries.

    Official specification: http://www.tpc.org/tpcdi
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs,
    ) -> None:
        """Initialize a TPC-DI benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options
        """
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation
        verbose = kwargs.pop("verbose", False)
        self._impl = TPCDIBenchmark(scale_factor=scale_factor, output_dir=output_dir, verbose=verbose, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate TPC-DI benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all TPC-DI benchmark queries.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original queries.

        Returns:
            A dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_query(self, query_id: Union[int, str], *, params: Optional[dict[str, Any]] = None) -> str:
        """Get a specific TPC-DI benchmark query.

        Args:
            query_id: The ID of the query to retrieve
            params: Optional parameters to customize the query

        Returns:
            The query string

        Raises:
            ValueError: If the query_id is invalid
        """
        return self._impl.get_query(query_id, params=params)

    def get_schema(self, dialect: str = "standard") -> dict[str, dict[str, Any]]:
        """Get the TPC-DI schema.

        Args:
            dialect: Target SQL dialect

        Returns:
            A dictionary mapping table names to table definitions
        """
        return self._impl.get_schema(dialect)

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all TPC-DI tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)

    # ETL methods

    def generate_source_data(
        self,
        formats: Optional[list[str]] = None,
        batch_types: Optional[list[str]] = None,
    ) -> dict[str, list[str]]:
        """Generate source data in various formats for ETL processing.

        Args:
            formats: List of data formats to generate (csv, xml, fixed_width, json)
            batch_types: List of batch types to generate (historical, incremental, scd)

        Returns:
            Dictionary mapping formats to lists of generated file paths
        """
        return self._impl.generate_source_data(formats, batch_types)

    def run_etl_pipeline(
        self,
        connection: Any,
        batch_type: str = "historical",
        validate_data: bool = True,
    ) -> dict[str, Any]:
        """Run the complete ETL pipeline for TPC-DI.

        Args:
            connection: Database connection for target warehouse
            batch_type: Type of batch to process (historical, incremental, scd)
            validate_data: Whether to run data validation after ETL

        Returns:
            Dictionary containing ETL execution results and metrics
        """
        return self._impl.run_etl_pipeline(connection, batch_type, validate_data)

    def validate_etl_results(self, connection: Any) -> dict[str, Any]:
        """Validate ETL results using data quality checks.

        Args:
            connection: Database connection to validate against

        Returns:
            Dictionary containing validation results and data quality metrics
        """
        return self._impl.validate_etl_results(connection)

    def get_etl_status(self) -> dict[str, Any]:
        """Get current ETL processing status and metrics.

        Returns:
            Dictionary containing ETL status, metrics, and batch information
        """
        return self._impl.get_etl_status()

    @property
    def etl_mode(self) -> bool:
        """Check if ETL mode is enabled.

        Returns:
            Always True as TPC-DI is now a pure ETL benchmark
        """
        return True

    def load_data_to_database(self, connection: Any, tables: Optional[list[str]] = None) -> None:
        """Load generated data into a database.

        Args:
            connection: Database connection
            tables: Optional list of tables to load. If None, loads all.

        Raises:
            ValueError: If data hasn't been generated yet
        """
        return self._impl.load_data_to_database(connection, tables)

    def run_benchmark(
        self, connection: Any, queries: Optional[list[str]] = None, iterations: int = 1
    ) -> dict[str, Any]:
        """Run the complete TPC-DI benchmark.

        Args:
            connection: Database connection to use
            queries: Optional list of query IDs to run. If None, runs all.
            iterations: Number of times to run each query

        Returns:
            Dictionary containing benchmark results
        """
        return self._impl.run_benchmark(connection, queries, iterations)

    def execute_query(
        self,
        query_id: Union[int, str],
        connection: Any,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Execute a TPC-DI query on the given database connection.

        Args:
            query_id: Query identifier (e.g., "V1", "V2", "A1", etc.)
            connection: Database connection to use for execution
            params: Optional parameters to use in the query

        Returns:
            Query results from the database

        Raises:
            ValueError: If the query_id is not valid
        """
        return self._impl.execute_query(query_id, connection, params)

    # Simplified public API

    def create_schema(self, connection: Any, dialect: str = "duckdb") -> None:
        """Create TPC-DI schema using the schema manager.

        Args:
            connection: Database connection
            dialect: Target SQL dialect
        """
        return self._impl.create_schema(connection, dialect)

    def run_full_benchmark(self, connection: Any, dialect: str = "duckdb") -> dict[str, Any]:
        """Run the complete TPC-DI benchmark with all phases.

        This is the main entry point for running a complete TPC-DI benchmark
        including schema creation, data loading, ETL processing, validation,
        and metrics calculation.

        Args:
            connection: Database connection
            dialect: SQL dialect for the target database

        Returns:
            Complete benchmark results with all metrics
        """
        return self._impl.run_full_benchmark(connection, dialect)

    def run_etl_benchmark(self, connection: Any, dialect: str = "duckdb") -> Any:
        """Run the ETL benchmark pipeline.

        Args:
            connection: Database connection
            dialect: SQL dialect

        Returns:
            ETL execution results
        """
        return self._impl.run_etl_benchmark(connection, dialect)

    def run_data_validation(self, connection: Any) -> Any:
        """Run data quality validation.

        Args:
            connection: Database connection

        Returns:
            Data quality validation results
        """
        return self._impl.run_data_validation(connection)

    def calculate_official_metrics(self, etl_result: Any, validation_result: Any) -> Any:
        """Calculate official TPC-DI metrics.

        Args:
            etl_result: ETL execution results
            validation_result: Data validation results

        Returns:
            Official TPC-DI benchmark metrics
        """
        return self._impl.calculate_official_metrics(etl_result, validation_result)

    def optimize_database(self, connection: Any) -> dict[str, Any]:
        """Optimize database performance for TPC-DI queries.

        Args:
            connection: Database connection

        Returns:
            Optimization results
        """
        return self._impl.optimize_database(connection)

    @property
    def validator(self) -> Any:
        """Get the TPC-DI validator instance.

        Returns:
            TPCDIValidator instance
        """
        return self._impl.validator

    @property
    def schema_manager(self) -> Any:
        """Get the TPC-DI schema manager instance.

        Returns:
            TPCDISchemaManager instance
        """
        return self._impl.schema_manager

    @property
    def metrics_calculator(self) -> Any:
        """Get the TPC-DI metrics calculator instance.

        Returns:
            TPCDIMetrics instance
        """
        return self._impl.metrics_calculator
