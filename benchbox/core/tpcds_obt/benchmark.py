"""TPC-DS One Big Table benchmark implementation."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Union

from benchbox.core.base_benchmark import BaseBenchmark
from benchbox.core.tpcds.generator import TPCDSDataGenerator
from benchbox.core.tpcds_obt.etl.transformer import SUPPORTED_CHANNELS, TPCDSOBTTransformer
from benchbox.core.tpcds_obt.queries import TPCDSOBTQueryManager
from benchbox.utils.scale_factor import format_scale_factor

logger = logging.getLogger(__name__)


class TPCDSOBTBenchmark(BaseBenchmark):
    """Transforms TPC-DS data into a single OBT table and executes adapted queries.

    Architecture:
        - Source data: Uses standard TPC-DS data from `benchmark_runs/datagen/tpcds_sf{N}/`
        - Output data: OBT-specific output stored in `benchmark_runs/datagen/tpcds_obt_sf{N}/`

    This ensures TPC-DS base data is generated once and reused across benchmarks,
    while OBT-specific transformations are stored separately.
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Union[str, Path] | None = None,
        tpcds_source_dir: Union[str, Path] | None = None,
        parallel: int = 1,
        force_regenerate: bool = False,
        dimension_mode: str = "full",
        channels: list[str] | None = None,
        output_format: str = "dat",
        **kwargs: Any,
    ) -> None:
        """Initialize TPC-DS OBT benchmark.

        Args:
            scale_factor: Scale factor for the benchmark (minimum 1.0).
            output_dir: Directory for OBT-specific output. Defaults to
                benchmark_runs/datagen/tpcds_obt_sf{N}/.
            tpcds_source_dir: Directory containing TPC-DS source data. Defaults to
                benchmark_runs/datagen/tpcds_sf{N}/. If data doesn't exist, it will
                be generated there.
            parallel: Number of parallel processes for data generation.
            force_regenerate: Force data regeneration even if valid data exists.
            dimension_mode: OBT dimension mode ('full' or 'minimal').
            channels: Sales channels to include ('store', 'web', 'catalog').
            output_format: Output format for OBT data ('dat', 'parquet', etc.).
            **kwargs: Additional arguments including compression options
                (compress_data, compression_type, compression_level).
        """
        super().__init__(scale_factor=scale_factor, **kwargs)

        if scale_factor < 1.0:
            raise ValueError("TPC-DS-OBT requires scale_factor >= 1.0 to align with TPC-DS generation.")

        self._name = "TPC-DS One Big Table Benchmark"
        self._version = "0.1"
        self._description = (
            "TPC-DS benchmark adapted to a single wide One Big Table with sales + returns merged across channels."
        )

        # Determine standard paths using scale factor formatting
        sf_str = format_scale_factor(scale_factor)
        default_base = Path.cwd() / "benchmark_runs" / "datagen"

        # OBT output directory (for transformed OBT table)
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            self.output_dir = default_base / f"tpcds_obt_{sf_str}"

        # TPC-DS source directory (for base TPC-DS data)
        if tpcds_source_dir:
            self.tpcds_source_dir = Path(tpcds_source_dir)
        else:
            self.tpcds_source_dir = default_base / f"tpcds_{sf_str}"

        self.parallel = parallel
        self.force_regenerate = force_regenerate
        self.dimension_mode = dimension_mode
        self.channels = channels or list(SUPPORTED_CHANNELS)
        self.output_format = output_format

        # Store compression options for data generator
        self._compression_kwargs = {
            k: v for k, v in kwargs.items() if k in ("compress_data", "compression_type", "compression_level")
        }

        self._data_generator: TPCDSDataGenerator | None = None
        self._obt_transformer: TPCDSOBTTransformer | None = None
        self._query_manager: TPCDSOBTQueryManager | None = None
        self.tables: dict[str, Path] = {}
        self.manifest: Path | None = None

    @property
    def data_generator(self) -> TPCDSDataGenerator:
        """Lazy-load TPC-DS data generator.

        The generator writes to tpcds_source_dir (standard TPC-DS location),
        not the OBT output_dir.
        """
        if self._data_generator is None:
            self._data_generator = TPCDSDataGenerator(
                scale_factor=self.scale_factor,
                output_dir=self.tpcds_source_dir,  # Generate in TPC-DS source dir
                parallel=self.parallel,
                force_regenerate=self.force_regenerate,
                **self._compression_kwargs,
            )
        return self._data_generator

    @property
    def obt_transformer(self) -> TPCDSOBTTransformer:
        """Lazy-load OBT transformer."""
        if self._obt_transformer is None:
            self._obt_transformer = TPCDSOBTTransformer()
        return self._obt_transformer

    @property
    def query_manager(self) -> TPCDSOBTQueryManager:
        """Lazy-load query manager."""
        if self._query_manager is None:
            self._query_manager = TPCDSOBTQueryManager()
        return self._query_manager

    def generate_data(
        self,
        tables: list[str] | None = None,
        output_format: str = "dat",
    ) -> dict[str, Any]:
        """Generate TPC-DS data and transform it into the single OBT table.

        Data flow:
            1. Generate TPC-DS base data in tpcds_source_dir (e.g., tpcds_sf1/)
            2. Transform source data into OBT format
            3. Write OBT output to output_dir (e.g., tpcds_obt_sf1/)
        """
        if tables is not None:
            logger.warning("TPC-DS-OBT ignores table selection and always emits a single OBT table.")

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

        obt_output_format = output_format or self.output_format

        # Check for existing OBT output
        if not self.force_regenerate and self._existing_obt(obt_output_format):
            logger.info("Reusing existing OBT output at %s", self.tables.get("tpcds_sales_returns_obt"))
            return {"table": self.tables.get("tpcds_sales_returns_obt"), "manifest": self.manifest}

        # Generate TPC-DS base data in the source directory
        logger.info(
            "Generating base TPC-DS data (scale factor %s) in %s...",
            self.scale_factor,
            self.tpcds_source_dir,
        )
        self.data_generator.generate()

        # Transform from source directory to OBT output directory
        logger.info(
            "Transforming TPC-DS data from %s into OBT at %s...",
            self.tpcds_source_dir,
            self.output_dir,
        )
        result = self.obt_transformer.transform(
            tpcds_dir=self.tpcds_source_dir,  # Read from TPC-DS source
            output_dir=self.output_dir,  # Write to OBT output
            mode=self.dimension_mode,
            channels=self.channels,
            output_format=obt_output_format,
            scale_factor=self.scale_factor,
        )

        self.tables["tpcds_sales_returns_obt"] = Path(result["table"])
        self.manifest = Path(result["manifest"])

        return result

    def _existing_obt(self, output_format: str) -> bool:
        """Check for an existing generated OBT table."""
        existing_path = self.output_dir / f"tpcds_sales_returns_obt.{output_format}"
        manifest_path = self.output_dir / "tpcds_sales_returns_obt_manifest.json"
        if existing_path.exists() and manifest_path.exists():
            self.tables["tpcds_sales_returns_obt"] = existing_path
            self.manifest = manifest_path
            return True
        return False

    def get_query(
        self,
        query_id: Union[int, str],
        *,
        params: dict[str, Any] | None = None,
        seed: int | None = None,
        scale_factor: float | None = None,
        dialect: str | None = None,
        **kwargs: Any,
    ) -> str:
        """Get the SQL text for a specific OBT query.

        Args:
            query_id: The ID of the query to retrieve.
            params: Optional parameters for query customization.
            seed: Random seed (accepted for API compatibility, not used by OBT queries).
            scale_factor: Scale factor (accepted for API compatibility, not used by OBT queries).
            dialect: Target SQL dialect (accepted for API compatibility, not used by OBT queries).
            **kwargs: Additional parameters for API compatibility.

        Returns:
            The SQL text of the query.
        """
        # OBT queries don't use seed/scale_factor for parameterization like TPC-DS/TPC-H
        # They use a static query set. We accept these params for API compatibility.
        return self.query_manager.get_query(query_id, params)

    def get_all_queries(self) -> dict[str, str]:
        """Get all available OBT queries."""
        return {str(k): v for k, v in self.query_manager.get_queries().items()}

    def get_queries(self, dialect: str | None = None) -> dict[str, str]:
        """Get all OBT benchmark queries.

        Args:
            dialect: Target SQL dialect (accepted for API compatibility).

        Returns:
            Dictionary mapping query IDs to SQL text.
        """
        # Convert int keys to str for consistency with other benchmarks
        return {str(k): v for k, v in self.query_manager.get_queries().items()}

    def execute_query(
        self,
        query_id: Union[int, str],
        connection: Any,
        params: Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute an OBT query on the given connection."""
        query = self.get_query(query_id)
        cursor = connection.cursor() if hasattr(connection, "cursor") else connection
        cursor.execute(query, params or {})
        return cursor.fetchall()

    def get_schema(self) -> dict[str, Any]:
        """Return schema metadata for the single OBT table."""
        from benchbox.core.tpcds_obt import schema

        table = schema.get_obt_table(self.dimension_mode)
        return {table.name: table}

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Generate DDL for creating the OBT table.

        Args:
            dialect: Target SQL dialect for the DDL.
            tuning_config: Optional tuning configuration (accepted for API compatibility,
                not currently used by OBT benchmark).

        Returns:
            DDL SQL string for creating the OBT table.
        """
        from benchbox.core.tpcds_obt import schema
        from benchbox.utils.dialect_utils import translate_sql_query

        # Note: tuning_config is accepted for API compatibility but OBT uses a fixed schema
        ddl = schema.get_obt_table(self.dimension_mode).get_create_table_sql()
        target = dialect.lower() if dialect else "duckdb"
        if target not in {"duckdb", "postgres", "ansi", "standard"}:
            ddl = translate_sql_query(ddl, target_dialect=target, source_dialect="postgres", identify=True)
        return ddl
