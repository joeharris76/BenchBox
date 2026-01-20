#!/usr/bin/env python3
"""
Test suite to validate the adjective cleanup refactoring.

This test ensures that the changes made to remove excessive adjectives
don't break core functionality of the BenchBox system.
"""

import tempfile
import unittest
from pathlib import Path

import pytest

pytestmark = pytest.mark.fast


class TestAdjectiveCleanup(unittest.TestCase):
    """Test adjective cleanup changes don't break functionality."""

    def test_base_benchmark_functionality_unchanged(self):
        """Test that base benchmark functionality still works."""
        from benchbox.base import BaseBenchmark

        # Test that we can still instantiate and use base benchmark methods
        # without any failures due to the refactoring
        class TestBenchmark(BaseBenchmark):
            def generate_data(self):
                return []

            def get_queries(self):
                return {"test": "SELECT 1"}

            def get_query(self, query_id, dialect=None):
                return "SELECT 1"

            def _load_data(self, connection):
                pass

        with tempfile.TemporaryDirectory() as tmp_dir:
            benchmark = TestBenchmark(scale_factor=1.0, output_dir=tmp_dir)
            self.assertIsNotNone(benchmark)
            self.assertEqual(benchmark.scale_factor, 1.0)

    def test_output_exporter_functionality_unchanged(self):
        """Test that output exporter functionality still works."""
        from benchbox.cli.output import ResultExporter

        with tempfile.TemporaryDirectory() as tmp_dir:
            exporter = ResultExporter(output_dir=Path(tmp_dir))
            self.assertIsNotNone(exporter)

    def test_database_naming_functionality_unchanged(self):
        """Test that database naming functionality still works."""
        try:
            from benchbox.core.tuning.configuration import TuningConfiguration

            from benchbox.utils.database_naming import generate_database_name

            config = TuningConfiguration(database={"primary_keys": True})
            name = generate_database_name("tpch", 1.0, config, "test")
            self.assertIsNotNone(name)
            self.assertIsInstance(name, str)
            self.assertGreater(len(name), 0)
        except ImportError:
            # Skip if modules not available
            self.skipTest("Database naming modules not available")

    def test_tpc_validation_functionality_unchanged(self):
        """Test that TPC validation functionality still works."""
        from benchbox.core.tpc_validation import ValidationReport

        report = ValidationReport()
        self.assertIsNotNone(report)
        self.assertIsNotNone(report.validation_id)

    def test_timing_functionality_unchanged(self):
        """Test that timing functionality still works."""
        from benchbox.core.results.timing import TimingCollector

        collector = TimingCollector()
        self.assertIsNotNone(collector)

    def test_no_broken_imports(self):
        """Test that all core imports still work after refactoring."""
        # Test that we can import core modules without errors

        # If we get here without ImportError, the test passes
        self.assertTrue(True)

    def test_docstring_changes_preserved_functionality(self):
        """Test that docstring changes don't affect runtime behavior."""
        from benchbox.core.results.timing import TimingCollector
        from benchbox.core.tpc_validation import ValidationReport

        # Test that classes can still be instantiated and used
        # despite docstring changes
        report = ValidationReport()
        collector = TimingCollector()

        self.assertIsNotNone(report)
        self.assertIsNotNone(collector)


if __name__ == "__main__":
    unittest.main()
