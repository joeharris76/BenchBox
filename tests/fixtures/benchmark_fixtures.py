"""Benchmark-specific fixtures for BenchBox tests.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import contextlib
from collections.abc import Generator
from pathlib import Path
from typing import Any, Optional, Union
from unittest.mock import Mock, patch

import pytest

from benchbox import TPCDS, TPCH

# Benchmark configuration constants
SCALE_FACTORS = {
    "small": 1.0,  # Small scale for testing (adjusted to minimum supported)
    "medium": 1.0,  # Medium scale for testing (adjusted to minimum supported)
    "large": 1.0,  # Full scale for performance testing
}

# Available benchmark classes with their import paths
BENCHMARK_CLASSES = {
    "tpch": ("benchbox", "TPCH"),
    "tpcds": ("benchbox", "TPCDS"),
    "tpcdi": ("benchbox", "TPCDI"),
    "ssb": ("benchbox", "SSB"),
    "amplab": ("benchbox", "AMPLab"),
    "h2odb": ("benchbox", "H2ODB"),
    "clickbench": ("benchbox", "ClickBench"),
    "merge": ("benchbox", "Merge"),
    "read_primitives": ("benchbox", "Primitives"),
    "joinorder": ("benchbox", "JoinOrder"),
}


def _import_benchmark_class(benchmark_name: str) -> Optional[type]:
    """Dynamically import a benchmark class."""
    if benchmark_name not in BENCHMARK_CLASSES:
        return None

    module_name, class_name = BENCHMARK_CLASSES[benchmark_name]
    try:
        module = __import__(module_name, fromlist=[class_name])
        return getattr(module, class_name)
    except (ImportError, AttributeError):
        return None


def _create_mock_benchmark(benchmark_name: str, scale_factor: float, output_dir: Path) -> Mock:
    """Create a standardized mock benchmark instance."""
    mock_benchmark = Mock()
    mock_benchmark.scale_factor = scale_factor
    mock_benchmark.output_dir = output_dir
    mock_benchmark.benchmark_name = benchmark_name
    mock_benchmark.get_queries = Mock(return_value={})
    mock_benchmark.get_query_categories = Mock(return_value=[])
    mock_benchmark.get_schema = Mock(return_value={})
    mock_benchmark.generate_data = Mock(return_value={})
    mock_benchmark.get_queries_by_category = Mock(return_value={})
    mock_benchmark.get_query = Mock(return_value="SELECT 1")
    return mock_benchmark


@pytest.fixture(params=["small", "medium"])
def scale_factor(request) -> float:
    """Parameterized scale factor fixture for different test scenarios.

    Returns:
        float: Scale factor value (small=0.01, medium=0.1)
    """
    return SCALE_FACTORS[request.param]


@pytest.fixture(params=["small"])
def small_scale_factor(request) -> float:
    """Small scale factor fixture for quick tests.

    Returns:
        float: Small scale factor (1.0)
    """
    return SCALE_FACTORS["small"]


@pytest.fixture(params=["medium"])
def medium_scale_factor(request) -> float:
    """Medium scale factor fixture for more thorough tests.

    Returns:
        float: Medium scale factor (1.0)
    """
    return SCALE_FACTORS["medium"]


@pytest.fixture
def benchmark_factory(tmp_path: Path):
    """Factory fixture for creating benchmark instances dynamically.

    This fixture provides a factory function that can create any benchmark
    with any scale factor, reducing the need for individual benchmark fixtures.

    Args:
        tmp_path: Pytest temporary directory fixture

    Returns:
        Callable: Factory function that creates benchmark instances

    Example:
        def test_any_benchmark(benchmark_factory):
            tpch = benchmark_factory('tpch', scale_factor=1.0)
            tpcds = benchmark_factory('tpcds', scale_factor=1.0)
    """

    def _create_benchmark(
        benchmark_name: str, scale_factor: float = 0.01, verbose: bool = False, **kwargs
    ) -> Union[object, Mock]:
        """Create a benchmark instance or mock if not available.

        Args:
            benchmark_name: Name of the benchmark to create
            scale_factor: Scale factor for the benchmark
            verbose: Whether to enable verbose output
            **kwargs: Additional parameters passed to benchmark constructor

        Returns:
            Benchmark instance or Mock if benchmark is not available
        """
        benchmark_class = _import_benchmark_class(benchmark_name)
        output_dir = tmp_path / f"{benchmark_name}_data_{scale_factor}"

        if benchmark_class is None:
            return _create_mock_benchmark(benchmark_name, scale_factor, output_dir)

        try:
            return benchmark_class(
                scale_factor=scale_factor,
                output_dir=output_dir,
                verbose=verbose,
                **kwargs,
            )
        except Exception:
            return _create_mock_benchmark(benchmark_name, scale_factor, output_dir)

    return _create_benchmark


# Backward compatibility fixtures - these use the factory internally
@pytest.fixture
def tpch_benchmark(benchmark_factory) -> Generator[Union[TPCH, Mock], None, None]:
    """Create a TPC-H benchmark instance for testing.

    This fixture creates a TPC-H benchmark with a small scale factor
    and temporary output directory for testing purposes.

    Returns:
        TPCH: TPC-H benchmark instance
    """
    yield benchmark_factory("tpch", scale_factor=1.0)


@pytest.fixture
def tpch_benchmark_medium(
    benchmark_factory,
) -> Generator[Union[TPCH, Mock], None, None]:
    """Create a TPC-H benchmark with medium scale factor for testing.

    This fixture creates a TPC-H benchmark with a medium scale factor
    for tests that need more realistic data volumes.

    Returns:
        TPCH: TPC-H benchmark instance
    """
    yield benchmark_factory("tpch", scale_factor=1.0)


@pytest.fixture
def tpcds_benchmark(benchmark_factory) -> Generator[Union[TPCDS, Mock], None, None]:
    """Create a TPC-DS benchmark instance for testing.

    This fixture creates a TPC-DS benchmark with a small scale factor
    and temporary output directory for testing purposes.

    Returns:
        TPCDS: TPC-DS benchmark instance
    """
    yield benchmark_factory("tpcds", scale_factor=1.0)


@pytest.fixture
def primitives_benchmark(
    benchmark_factory,
) -> Generator[Union[object, Mock], None, None]:
    """Create a Primitives benchmark instance for testing.

    This fixture creates a Primitives benchmark with a small scale factor
    and temporary output directory for testing purposes.

    Returns:
        Primitives benchmark instance (real or mock)
    """
    yield benchmark_factory("read_primitives", scale_factor=1.0)


@pytest.fixture
def mock_benchmark_data_generation():
    """Mock benchmark data generation for faster testing.

    This fixture patches the data generation methods of all available benchmarks
    to return mock data instead of actually generating files.

    Returns:
        Dict: Mock data generation results with patchers for all benchmarks
    """
    # Standard mock files for TPC-H/TPC-DS style benchmarks
    tpch_mock_files = {
        "region": "/tmp/region.csv",
        "nation": "/tmp/nation.csv",
        "customer": "/tmp/customer.csv",
        "supplier": "/tmp/supplier.csv",
        "part": "/tmp/part.csv",
        "partsupp": "/tmp/partsupp.csv",
        "orders": "/tmp/orders.csv",
        "lineitem": "/tmp/lineitem.csv",
    }

    # Mock files for other benchmarks
    generic_mock_files = {
        "table1": "/tmp/table1.csv",
        "table2": "/tmp/table2.csv",
        "table3": "/tmp/table3.csv",
    }

    patches = {}
    mock_results = {"mock_files": {}}

    # Patch available benchmark classes
    for benchmark_name, (_module_name, _class_name) in BENCHMARK_CLASSES.items():
        benchmark_class = _import_benchmark_class(benchmark_name)
        if benchmark_class is not None:
            try:
                # Choose appropriate mock data based on benchmark type
                mock_data = tpch_mock_files if benchmark_name in ["tpch", "tpcds"] else generic_mock_files

                patcher = patch.object(benchmark_class, "generate_data", return_value=mock_data)
                mock_gen = patcher.start()
                patches[f"{benchmark_name}_gen"] = mock_gen
                mock_results["mock_files"][benchmark_name] = mock_data
            except AttributeError:
                # Some benchmarks might not have generate_data method
                pass

    try:
        yield {**patches, **mock_results}
    finally:
        # Clean up all patches
        for benchmark_name in BENCHMARK_CLASSES:
            with contextlib.suppress(Exception):
                patch.stopall()


@pytest.fixture
def benchmark_mock_factory():
    """Factory for creating reusable benchmark mocks with consistent interfaces.

    This fixture provides a factory function that creates standardized mock
    benchmarks with all the common methods and attributes needed for testing.

    Returns:
        Callable: Factory function that creates mock benchmark instances
    """

    def _create_mock(
        benchmark_name: str,
        scale_factor: float = 0.01,
        custom_queries: Optional[dict[str, str]] = None,
        custom_schema: Optional[dict[str, Any]] = None,
        **kwargs,
    ) -> Mock:
        """Create a comprehensive mock benchmark instance.

        Args:
            benchmark_name: Name of the benchmark to mock
            scale_factor: Scale factor for the mock
            custom_queries: Custom queries to return from get_queries()
            custom_schema: Custom schema to return from get_schema()
            **kwargs: Additional attributes to set on the mock

        Returns:
            Mock: Configured mock benchmark instance
        """
        mock_benchmark = Mock()

        # Standard attributes
        mock_benchmark.benchmark_name = benchmark_name
        mock_benchmark.scale_factor = scale_factor
        mock_benchmark.verbose = False
        mock_benchmark.output_dir = Path(f"/tmp/{benchmark_name}_data")

        # Standard methods with sensible defaults
        mock_benchmark.get_queries = Mock(
            return_value=custom_queries or {f"q{i}": f"SELECT {i} as test_query;" for i in range(1, 6)}
        )

        mock_benchmark.get_query_categories = Mock(return_value=["basic", "aggregation", "join", "window", "complex"])

        mock_benchmark.get_schema = Mock(
            return_value=custom_schema
            or {
                "test_table": {
                    "columns": ["id", "name", "value"],
                    "types": ["INTEGER", "VARCHAR(100)", "DECIMAL(10,2)"],
                }
            }
        )

        mock_benchmark.generate_data = Mock(return_value={"test_table": f"/tmp/{benchmark_name}_test_table.csv"})

        mock_benchmark.get_queries_by_category = Mock(
            return_value={
                "basic": ["q1", "q2"],
                "aggregation": ["q3"],
                "join": ["q4"],
                "complex": ["q5"],
            }
        )

        mock_benchmark.get_query = Mock(return_value="SELECT 1 as test;")

        # Set any additional attributes from kwargs
        for key, value in kwargs.items():
            setattr(mock_benchmark, key, value)

        return mock_benchmark

    return _create_mock


@pytest.fixture(params=list(BENCHMARK_CLASSES.keys()))
def any_benchmark(request, benchmark_factory):
    """Parameterized fixture that provides instances of all available benchmarks.

    This fixture runs tests across all benchmark types, useful for testing
    common functionality that should work across all benchmarks.

    Args:
        request: Pytest request object containing the parameter
        benchmark_factory: Factory fixture for creating benchmarks

    Returns:
        Benchmark instance for the requested benchmark type
    """
    benchmark_name = request.param
    return benchmark_factory(benchmark_name, scale_factor=1.0)


@pytest.fixture(params=[("tpch", 1.0), ("tpcds", 1.0), ("tpch", 1.0)])
def benchmark_with_scale(request, benchmark_factory):
    """Parameterized fixture providing benchmarks with different scale factors.

    This fixture is useful for testing scalability and performance characteristics
    across different data volumes and benchmark types.

    Args:
        request: Pytest request object containing the parameter tuple
        benchmark_factory: Factory fixture for creating benchmarks

    Returns:
        Tuple of (benchmark_instance, benchmark_name, scale_factor)
    """
    benchmark_name, scale_factor = request.param
    benchmark = benchmark_factory(benchmark_name, scale_factor=scale_factor)
    return benchmark, benchmark_name, scale_factor


@pytest.fixture
def benchmark_performance_config() -> dict[str, Any]:
    """Provide performance configuration for benchmark testing.

    Returns:
        Dict[str, Any]: Performance configuration parameters
    """
    return {
        "timeout_seconds": 30,
        "memory_limit_mb": 512,
        "max_concurrent_queries": 2,
        "enable_query_cache": False,
        "enable_result_cache": False,
        "collect_metrics": True,
        "profile_queries": False,
    }


@pytest.fixture
def benchmark_test_queries() -> dict[str, str]:
    """Provide sample test queries for benchmark testing.

    Returns:
        Dict[str, str]: Sample queries by category
    """
    return {
        "simple_select": "SELECT * FROM region LIMIT 10;",
        "aggregation": "SELECT COUNT(*) FROM lineitem;",
        "join": """
            SELECT r.r_name, n.n_name
            FROM region r
            JOIN nation n ON r.r_regionkey = n.n_regionkey
            LIMIT 10;
        """,
        "window_function": """
            SELECT
                l_orderkey,
                l_linenumber,
                ROW_NUMBER() OVER (PARTITION BY l_orderkey ORDER BY l_linenumber) as rn
            FROM lineitem
            LIMIT 10;
        """,
        "complex_aggregation": """
            SELECT
                l_returnflag,
                l_linestatus,
                COUNT(*) as count_order,
                SUM(l_quantity) as sum_qty,
                AVG(l_extendedprice) as avg_price
            FROM lineitem
            GROUP BY l_returnflag, l_linestatus
            ORDER BY l_returnflag, l_linestatus;
        """,
    }


@pytest.fixture
def benchmark_error_scenarios() -> dict[str, str]:
    """Provide error scenarios for benchmark testing.

    Returns:
        Dict[str, str]: Error scenarios and expected behaviors
    """
    return {
        "invalid_table": "SELECT * FROM nonexistent_table;",
        "invalid_column": "SELECT nonexistent_column FROM region;",
        "syntax_error": "SELECT * FORM region;",  # Intentional typo
        "division_by_zero": "SELECT 1/0;",
        "timeout_query": """
            WITH RECURSIVE t(n) AS (
                SELECT 1
                UNION ALL
                SELECT n+1 FROM t WHERE n < 1000000
            )
            SELECT COUNT(*) FROM t;
        """,
    }


@pytest.fixture
def benchmark_comparison_data() -> dict[str, Any]:
    """Provide data for benchmark comparison testing.

    Returns:
        Dict[str, Any]: Benchmark comparison data and expected results
    """
    return {
        "expected_row_counts": {
            "region": 5,
            "nation": 25,
            "customer": 150,  # At scale factor 0.01
            "supplier": 10,
            "part": 200,
            "partsupp": 800,
            "orders": 1500,
            "lineitem": 6000,
        },
        "performance_thresholds": {
            "simple_select": 0.1,  # seconds
            "aggregation": 1.0,
            "join": 2.0,
            "window_function": 3.0,
            "complex_aggregation": 5.0,
        },
        "memory_thresholds": {
            "simple_select": 10,  # MB
            "aggregation": 50,
            "join": 100,
            "window_function": 150,
            "complex_aggregation": 200,
        },
    }
