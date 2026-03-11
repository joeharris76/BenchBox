"""Tests for BaseBenchmark functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path

import pytest

from benchbox.base import BaseBenchmark
from benchbox.core.read_primitives.benchmark import ReadPrimitivesBenchmark

pytestmark = pytest.mark.medium


@pytest.mark.unit
class TestBaseBenchmark:
    """Test the base benchmark functionality."""

    @pytest.fixture
    def temp_dir(self) -> Path:
        """Create temporary directory for testing."""
        return Path(tempfile.mkdtemp())

    @pytest.fixture
    def base_benchmark(self, temp_dir: Path) -> ReadPrimitivesBenchmark:
        """Create a real benchmark instance for testing."""
        return ReadPrimitivesBenchmark(scale_factor=0.01, output_dir=temp_dir)

    def test_initialization(self, temp_dir: Path) -> None:
        """Test that the benchmark can be initialized with various parameters."""
        bench1 = ReadPrimitivesBenchmark(scale_factor=0.01, output_dir=temp_dir)
        assert bench1.scale_factor == 0.01

        bench2 = ReadPrimitivesBenchmark(scale_factor=0.01, output_dir=temp_dir)
        assert bench2.scale_factor == 0.01

        # Test basic functionality
        bench3 = ReadPrimitivesBenchmark(scale_factor=0.01, output_dir=temp_dir)
        assert bench3.scale_factor == 0.01
        assert hasattr(bench3, "get_queries")

    def test_abstract_methods(self) -> None:
        """Test that abstract methods must be implemented by subclasses."""
        with pytest.raises(TypeError):
            # Can't instantiate BaseBenchmark directly
            BaseBenchmark()

    def test_translate_query(self, base_benchmark: ReadPrimitivesBenchmark) -> None:
        """Test query translation functionality."""
        # Get a real query from the primitives benchmark
        queries = base_benchmark.get_queries()
        if queries:
            query_id = next(iter(queries.keys()))
            translated = base_benchmark.translate_query(query_id, dialect="postgres")
            assert isinstance(translated, str)
            assert translated.strip()

    def test_real_data_generation(self, base_benchmark: ReadPrimitivesBenchmark) -> None:
        """Test that real data generation works."""
        try:
            data_result = base_benchmark.generate_data()
        except RuntimeError as e:
            if "dbgen binary" in str(e) or "native tools are not bundled" in str(e):
                pytest.skip("TPC-H dbgen binary not available")
            raise

        # ReadPrimitivesBenchmark returns a dict, convert to list for testing
        data_paths = list(data_result.values()) if isinstance(data_result, dict) else data_result

        assert isinstance(data_paths, list)
        assert len(data_paths) > 0

        # Verify at least some files exist
        existing_files = [path for path in data_paths if Path(path).exists()]
        assert len(existing_files) > 0, "At least some data files should be generated"

    def test_real_queries(self, base_benchmark: ReadPrimitivesBenchmark) -> None:
        """Test that real queries are available."""
        queries = base_benchmark.get_queries()
        assert isinstance(queries, dict)
        assert len(queries) > 0, "Should have at least one query"

        # Test getting individual queries
        for query_id in list(queries.keys())[:3]:  # Test first 3 queries
            query = base_benchmark.get_query(query_id)
            assert isinstance(query, str)
            assert query.strip()
            assert "SELECT" in query.upper()  # Should be valid SQL

    def test_benchmark_name_normalizes_benchmark_suffix_variants(self, temp_dir: Path) -> None:
        """Class names ending with Benchmark should normalize to canonical lowercase IDs."""

        class _NamedBenchmarkBase(BaseBenchmark):
            def generate_data(self):
                return []

            def get_queries(self):
                return {}

            def get_query(self, query_id, *, params=None):
                return "SELECT 1"

        clickbench_cls = type("ClickBenchBenchmark", (_NamedBenchmarkBase,), {})
        tpchavoc_cls = type("TPCHavocBenchmark", (_NamedBenchmarkBase,), {})
        generic_cls = type("CustomWorkloadBenchmark", (_NamedBenchmarkBase,), {})

        assert clickbench_cls(scale_factor=0.01, output_dir=temp_dir)._get_benchmark_name() == "clickbench"
        assert tpchavoc_cls(scale_factor=0.01, output_dir=temp_dir)._get_benchmark_name() == "tpchavoc"
        assert generic_cls(scale_factor=0.01, output_dir=temp_dir)._get_benchmark_name() == "customworkload"
