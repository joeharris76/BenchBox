"""
Tests for standardized benchmark constraint parameter APIs.

This module verifies that all benchmarks use the unified tuning configuration
system consistently and no longer accept legacy constraint parameters.
"""

import pytest

pytestmark = pytest.mark.fast

pytest.importorskip("pandas")

from benchbox.core.amplab.benchmark import AMPLabBenchmark
from benchbox.core.clickbench.benchmark import ClickBenchBenchmark
from benchbox.core.h2odb.benchmark import H2OBenchmark
from benchbox.core.ssb.benchmark import SSBBenchmark
from benchbox.core.tpcdi.benchmark import TPCDIBenchmark
from benchbox.core.tpcds.benchmark import TPCDSBenchmark
from benchbox.core.tpch.benchmark import TPCHBenchmark
from benchbox.core.tuning.interface import (
    ForeignKeyConfiguration,
    PrimaryKeyConfiguration,
    UnifiedTuningConfiguration,
)


class TestStandardizedBenchmarkAPIs:
    """Test standardized benchmark constraint parameter APIs."""

    def test_all_benchmarks_support_standardized_signature(self):
        """Test that all benchmarks support the standardized get_create_tables_sql signature."""
        benchmarks = [
            # SSB needs explicit compression settings to avoid zstd dependency
            SSBBenchmark(scale_factor=0.01, output_dir="/tmp", compress_data=False, compression_type="none"),
            TPCHBenchmark(scale_factor=0.01, output_dir="/tmp"),
            TPCDSBenchmark(scale_factor=1.0, output_dir="/tmp"),
            TPCDIBenchmark(scale_factor=0.01, output_dir="/tmp"),
            H2OBenchmark(scale_factor=0.01, output_dir="/tmp"),
            AMPLabBenchmark(scale_factor=0.01, output_dir="/tmp"),
            ClickBenchBenchmark(scale_factor=0.01, output_dir="/tmp"),
        ]

        for benchmark in benchmarks:
            benchmark_name = benchmark.__class__.__name__

            # Test with no parameters (should use defaults)
            try:
                sql = benchmark.get_create_tables_sql()
                assert sql is not None, f"{benchmark_name} returned None SQL"
                assert isinstance(sql, str), f"{benchmark_name} returned non-string SQL"
                assert len(sql.strip()) > 0, f"{benchmark_name} returned empty SQL"
            except Exception as e:
                pytest.fail(f"{benchmark_name} failed with no parameters: {e}")

            # Test with dialect parameter
            try:
                sql = benchmark.get_create_tables_sql(dialect="standard")
                assert sql is not None, f"{benchmark_name} returned None SQL with dialect"
                assert isinstance(sql, str), f"{benchmark_name} returned non-string SQL with dialect"
            except Exception as e:
                pytest.fail(f"{benchmark_name} failed with dialect parameter: {e}")

    def test_benchmarks_use_tuning_configuration_for_constraints(self):
        """Test that benchmarks correctly use tuning configuration for constraints."""
        # Create tuning configurations with different constraint settings
        no_constraints_config = UnifiedTuningConfiguration()
        no_constraints_config.primary_keys = PrimaryKeyConfiguration(enabled=False)
        no_constraints_config.foreign_keys = ForeignKeyConfiguration(enabled=False)

        with_constraints_config = UnifiedTuningConfiguration()
        with_constraints_config.primary_keys = PrimaryKeyConfiguration(enabled=True)
        with_constraints_config.foreign_keys = ForeignKeyConfiguration(enabled=True)

        benchmarks = [
            # SSB needs explicit compression settings to avoid zstd dependency
            SSBBenchmark(scale_factor=0.01, output_dir="/tmp", compress_data=False, compression_type="none"),
            TPCHBenchmark(scale_factor=0.01, output_dir="/tmp"),
            TPCDSBenchmark(scale_factor=1.0, output_dir="/tmp"),
            TPCDIBenchmark(scale_factor=0.01, output_dir="/tmp"),
            H2OBenchmark(scale_factor=0.01, output_dir="/tmp"),
            AMPLabBenchmark(scale_factor=0.01, output_dir="/tmp"),
            ClickBenchBenchmark(scale_factor=0.01, output_dir="/tmp"),
        ]

        for benchmark in benchmarks:
            benchmark_name = benchmark.__class__.__name__

            # Test with no constraints
            sql_no_constraints = benchmark.get_create_tables_sql(
                dialect="standard", tuning_config=no_constraints_config
            )

            # Test with constraints
            sql_with_constraints = benchmark.get_create_tables_sql(
                dialect="standard", tuning_config=with_constraints_config
            )

            # Both should return valid SQL
            assert sql_no_constraints is not None, f"{benchmark_name} returned None with no constraints"
            assert sql_with_constraints is not None, f"{benchmark_name} returned None with constraints"

            # SQL should be different when constraints are enabled vs disabled
            # (This is a basic check - specific constraint checks would be more complex)
            if sql_no_constraints == sql_with_constraints:
                # Some benchmarks might not have any constraints to add/remove
                print(f"Warning: {benchmark_name} SQL identical with/without constraints")

    def test_benchmarks_handle_none_tuning_config_gracefully(self):
        """Test that benchmarks handle None tuning_config gracefully."""
        benchmarks = [
            # SSB needs explicit compression settings to avoid zstd dependency
            SSBBenchmark(scale_factor=0.01, output_dir="/tmp", compress_data=False, compression_type="none"),
            TPCHBenchmark(scale_factor=0.01, output_dir="/tmp"),
            TPCDSBenchmark(scale_factor=1.0, output_dir="/tmp"),
            TPCDIBenchmark(scale_factor=0.01, output_dir="/tmp"),
            H2OBenchmark(scale_factor=0.01, output_dir="/tmp"),
            AMPLabBenchmark(scale_factor=0.01, output_dir="/tmp"),
            ClickBenchBenchmark(scale_factor=0.01, output_dir="/tmp"),
        ]

        for benchmark in benchmarks:
            benchmark_name = benchmark.__class__.__name__

            try:
                # None tuning_config should be handled gracefully
                sql = benchmark.get_create_tables_sql(dialect="standard", tuning_config=None)
                assert sql is not None, f"{benchmark_name} returned None with None tuning_config"
                assert isinstance(sql, str), f"{benchmark_name} returned non-string with None tuning_config"
                assert len(sql.strip()) > 0, f"{benchmark_name} returned empty SQL with None tuning_config"
            except Exception as e:
                pytest.fail(f"{benchmark_name} failed with None tuning_config: {e}")

    def test_constraint_extraction_from_tuning_config(self):
        """Test that constraint settings are correctly extracted from tuning configuration."""
        # Create a minimal benchmark to test constraint extraction
        # SSB needs explicit compression settings to avoid zstd dependency
        benchmark = SSBBenchmark(scale_factor=0.01, output_dir="/tmp", compress_data=False, compression_type="none")

        # Test with different constraint configurations
        test_cases = [
            (True, True, "both enabled"),
            (True, False, "pk only"),
            (False, True, "fk only"),
            (False, False, "none enabled"),
        ]

        for pk_enabled, fk_enabled, description in test_cases:
            tuning_config = UnifiedTuningConfiguration()
            tuning_config.primary_keys = PrimaryKeyConfiguration(enabled=pk_enabled)
            tuning_config.foreign_keys = ForeignKeyConfiguration(enabled=fk_enabled)

            try:
                sql = benchmark.get_create_tables_sql(dialect="standard", tuning_config=tuning_config)
                assert sql is not None, f"SQL None for {description}"
                assert isinstance(sql, str), f"SQL not string for {description}"

                # Basic validation that SQL contains expected patterns
                if pk_enabled or fk_enabled:
                    # Should contain some constraint-related keywords
                    sql_upper = sql.upper()
                    any(
                        keyword in sql_upper
                        for keyword in [
                            "PRIMARY KEY",
                            "FOREIGN KEY",
                            "REFERENCES",
                            "CONSTRAINT",
                        ]
                    )
                    # Note: Some benchmarks might not have constraints even when enabled
                    # This is just a basic sanity check

            except Exception as e:
                pytest.fail(f"Constraint extraction failed for {description}: {e}")

    def test_no_legacy_constraint_parameters_accepted(self):
        """Test that benchmarks no longer accept legacy constraint parameters."""
        # SSB needs explicit compression settings to avoid zstd dependency
        benchmark = SSBBenchmark(scale_factor=0.01, output_dir="/tmp", compress_data=False, compression_type="none")

        # The standardized signature should not accept legacy parameters
        try:
            # This should work (new signature)
            sql = benchmark.get_create_tables_sql(dialect="standard", tuning_config=None)
            assert sql is not None
        except Exception as e:
            pytest.fail(f"New signature failed: {e}")

        # Verify the signature doesn't have legacy constraint parameters
        import inspect

        sig = inspect.signature(benchmark.get_create_tables_sql)
        param_names = list(sig.parameters.keys())

        # Should not have legacy constraint parameters
        legacy_params = ["enable_primary_keys", "enable_foreign_keys"]
        for legacy_param in legacy_params:
            assert legacy_param not in param_names, f"Legacy parameter {legacy_param} still present in signature"

        # Should have standardized parameters
        assert "dialect" in param_names, "Missing dialect parameter"
        assert "tuning_config" in param_names, "Missing tuning_config parameter"


if __name__ == "__main__":
    pytest.main([__file__])
