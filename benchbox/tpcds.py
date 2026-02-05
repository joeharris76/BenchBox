"""TPC-DS benchmark implementation.

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ DS (TPC-DS) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-DS specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from benchbox.core.tpcds.generator.manager import TPCDSDataGenerator
    from benchbox.core.tpcds.queries import TPCDSQueryManager

from benchbox.base import BaseBenchmark
from benchbox.core.tpcds.benchmark import TPCDSBenchmark


class TPCDS(BaseBenchmark):
    """TPC-DS benchmark implementation.

    Provides TPC-DS benchmark implementation, including data generation and access to the benchmark queries.

    Official specification: http://www.tpc.org/tpcds
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize TPC-DS benchmark instance.

        Args:
            scale_factor: Scale factor for the benchmark (1.0 = ~1GB)
            output_dir: Directory to output generated data files
            **kwargs: Additional implementation-specific options

        Raises:
            ValueError: If scale_factor is not positive
            TypeError: If scale_factor is not a number
        """
        # Validate scale_factor type (positivity already checked in base class)
        self._validate_scale_factor_type(scale_factor)

        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(TPCDSBenchmark, scale_factor, output_dir, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate TPC-DS benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None, base_dialect: Optional[str] = None) -> dict[str, str]:
        """Get all TPC-DS benchmark queries.

        Args:
            dialect: Target SQL dialect for translation (e.g., 'duckdb', 'postgres')

        Returns:
            A dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect=dialect, base_dialect=base_dialect)

    def get_query(
        self,
        query_id: int,
        *,
        params: Optional[dict[str, Any]] = None,
        seed: Optional[int] = None,
        scale_factor: Optional[float] = None,
        dialect: Optional[str] = None,
        **kwargs,
    ) -> str:
        """Get a specific TPC-DS benchmark query.

        Args:
            query_id: The ID of the query to retrieve (1-99)
            params: Optional parameters to customize the query (legacy parameter, mostly ignored)
            seed: Random number generator seed for parameter generation
            scale_factor: Scale factor for parameter calculations
            dialect: Target SQL dialect
            **kwargs: Additional parameters

        Returns:
            The query string

        Raises:
            ValueError: If the query_id is invalid
            TypeError: If query_id is not an integer
        """
        # Validate query_id to match TPC-H patterns
        if not isinstance(query_id, int):
            raise TypeError(f"query_id must be an integer, got {type(query_id).__name__}")
        if not (1 <= query_id <= 99):
            raise ValueError(f"Query ID must be 1-99, got {query_id}")

        # Validate scale_factor if provided
        if scale_factor is not None:
            if not isinstance(scale_factor, (int, float)):
                raise TypeError(f"scale_factor must be a number, got {type(scale_factor).__name__}")
            if scale_factor <= 0:
                raise ValueError(f"scale_factor must be positive, got {scale_factor}")

        # Validate seed if provided
        if seed is not None and not isinstance(seed, int):
            raise TypeError(f"seed must be an integer, got {type(seed).__name__}")

        return self._impl.get_query(
            query_id,
            params=params,
            seed=seed,
            scale_factor=scale_factor,
            dialect=dialect,
            **kwargs,
        )

    @property
    def queries(self) -> "TPCDSQueryManager":
        """Access to the query manager.

        Returns:
            The underlying query manager instance
        """
        return self._impl.query_manager

    @property
    def generator(self) -> "TPCDSDataGenerator":
        """Access to the data generator.

        Returns:
            The underlying data generator instance
        """
        return self._impl.data_generator

    def get_available_tables(self) -> list[str]:
        """Get list of available tables.

        Returns:
            List of table names
        """
        return self._impl.get_available_tables()

    def get_available_queries(self) -> list[int]:
        """Get list of available query IDs.

        Returns:
            List of query IDs (1-99)
        """
        return self._impl.get_available_queries()

    def generate_table_data(self, table_name: str, output_dir: Optional[str] = None) -> str:
        """Generate data for a specific table.

        Args:
            table_name: Name of the table to generate data for
            output_dir: Optional output directory for generated data

        Returns:
            Iterator of data rows for the table
        """
        return self._impl.generate_table_data(table_name, output_dir)

    def get_schema(self) -> list[dict]:
        """Get the TPC-DS schema.

        Returns:
            A list of dictionaries describing the tables in the schema
        """
        return self._impl.get_schema()

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all TPC-DS tables.

        Args:
            dialect: SQL dialect to use (currently ignored, TPC-DS uses standard SQL)
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)

    def generate_streams(
        self,
        num_streams: int = 1,
        rng_seed: Optional[int] = None,
        streams_output_dir: Optional[Union[str, Path]] = None,
    ) -> list[Path]:
        """Generate TPC-DS query streams.

        Args:
            num_streams: Number of concurrent streams to generate
            rng_seed: Random number generator seed for parameter generation
            streams_output_dir: Directory to output stream files

        Returns:
            List of paths to generated stream files
        """
        return self._impl.generate_streams(
            num_streams=num_streams,
            rng_seed=rng_seed,
            streams_output_dir=streams_output_dir,
        )

    def get_stream_info(self, stream_id: int) -> dict[str, Any]:
        """Get information about a specific stream.

        Args:
            stream_id: Stream identifier

        Returns:
            Dictionary containing stream information
        """
        return self._impl.get_stream_info(stream_id)

    def get_all_streams_info(self) -> list[dict[str, Any]]:
        """Get information about all streams.

        Returns:
            List of dictionaries containing stream information
        """
        return self._impl.get_all_streams_info()

    def get_benchmark_info(self) -> dict[str, Any]:
        """Get benchmark information.

        Returns:
            Dictionary with benchmark information including name, scale factor,
            available tables, queries, and C tools info
        """
        return self._impl.get_benchmark_info()
