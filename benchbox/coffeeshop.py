"""Public entrypoint for the reference-aligned CoffeeShop benchmark."""

from pathlib import Path
from typing import Any, Optional, Union

from benchbox.base import BaseBenchmark
from benchbox.core.coffeeshop.benchmark import CoffeeShopBenchmark


class CoffeeShop(BaseBenchmark):
    """High-level wrapper for the CoffeeShop benchmark.

    The rewritten benchmark mirrors the public reference generator and now
    exposes a compact star schema consisting of:

    - ``dim_locations``: geographic metadata and regional weights
    - ``dim_products``: canonical product catalog with seasonal availability
    - ``order_lines``: exploded fact table (1-5 lines per order) with temporal,
      regional, and pricing dynamics

    The query suite (``SA*``, ``PR*``, ``TR*``, ``TM*``, ``QC*``) focuses on
    sales analysis, product behaviour, trend analysis, and quality checks for the
    new schema.
    """

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: Optional[Union[str, Path]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialise a CoffeeShop benchmark instance."""
        super().__init__(scale_factor=scale_factor, output_dir=output_dir, **kwargs)

        # Initialize the actual implementation using common pattern
        self._initialize_benchmark_implementation(CoffeeShopBenchmark, scale_factor, output_dir, **kwargs)

    def generate_data(self) -> list[Union[str, Path]]:
        """Generate Coffee Shop benchmark data.

        Returns:
            A list of paths to the generated data files
        """
        return self._impl.generate_data()

    def get_queries(self, dialect: Optional[str] = None) -> dict[str, str]:
        """Get all Coffee Shop benchmark queries.

        Args:
            dialect: Target SQL dialect for query translation. If None, returns original queries.

        Returns:
            A dictionary mapping query IDs to query strings
        """
        return self._impl.get_queries(dialect=dialect)

    def get_query(self, query_id: str, *, params: Optional[dict[str, Any]] = None) -> str:
        """Return a single CoffeeShop analytics query.

        Query identifiers follow the updated naming convention (e.g. ``SA1`` for
        sales analysis, ``PR1`` for product mix, ``TR1`` for trend review).
        """
        return self._impl.get_query(query_id, params=params)

    def get_schema(self) -> list[dict]:
        """Get the Coffee Shop benchmark schema.

        Returns:
            A list of dictionaries describing the tables in the schema
        """
        return self._impl.get_schema()

    def get_create_tables_sql(self, dialect: str = "standard", tuning_config=None) -> str:
        """Get SQL to create all Coffee Shop benchmark tables.

        Args:
            dialect: SQL dialect to use
            tuning_config: Unified tuning configuration for constraint settings

        Returns:
            SQL script for creating all tables
        """
        return self._impl.get_create_tables_sql(dialect=dialect, tuning_config=tuning_config)
