"""Tests to ensure connection interface consistency across all benchmarks.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import inspect
from typing import Any
from unittest.mock import Mock

import pytest

# Import all benchmark classes for testing
import benchbox


class TestConnectionInterfaceConsistency:
    """Test that all benchmarks use consistent connection interfaces."""

    def test_tpc_benchmarks_use_connection_objects(self):
        """Test that all TPC benchmarks accept connection objects, not connection strings."""

        # Test TPC-H benchmark methods
        tpch = benchbox.TPCH()

        # Check that run_power_test accepts connection_factory as first parameter
        if hasattr(tpch, "run_power_test"):
            power_test_sig = inspect.signature(tpch.run_power_test)
            params = list(power_test_sig.parameters.values())
            # TPC-H uses connection_factory pattern for backward compatibility
            assert params[0].name in ["connection_factory", "connection"]

        # Check that run_maintenance_test accepts connection_factory as first parameter
        if hasattr(tpch, "run_maintenance_test"):
            maintenance_test_sig = inspect.signature(tpch.run_maintenance_test)
            params = list(maintenance_test_sig.parameters.values())
            assert params[0].name in ["connection_factory", "connection"]

        # Test TPC-DS benchmark methods
        tpcds = benchbox.TPCDS()

        # Check that run_power_test accepts connection as first parameter
        if hasattr(tpcds, "run_power_test"):
            power_test_sig = inspect.signature(tpcds.run_power_test)
            params = list(power_test_sig.parameters.values())
            assert params[0].name == "connection"
            assert params[0].annotation == Any

        # Check that run_throughput_test accepts connection as first parameter
        if hasattr(tpcds, "run_throughput_test"):
            throughput_test_sig = inspect.signature(tpcds.run_throughput_test)
            params = list(throughput_test_sig.parameters.values())
            assert params[0].name == "connection"
            assert params[0].annotation == Any

        # Check that run_maintenance_test accepts connection as first parameter
        if hasattr(tpcds, "run_maintenance_test"):
            maintenance_test_sig = inspect.signature(tpcds.run_maintenance_test)
            params = list(maintenance_test_sig.parameters.values())
            assert params[0].name == "connection"
            assert params[0].annotation == Any

    def test_no_connection_string_parameters_in_tpc_benchmarks(self):
        """Test that TPC benchmarks don't use connection_string parameters anymore."""

        # Test TPC-H benchmark methods
        tpch = benchbox.TPCH()

        methods_to_check = [
            "run_power_test",
            "run_maintenance_test",
        ]  # Only check methods that exist

        for method_name in methods_to_check:
            if hasattr(tpch, method_name):
                method = getattr(tpch, method_name)
                sig = inspect.signature(method)

                # Ensure no parameter is named 'connection_string'
                param_names = [param.name for param in sig.parameters.values()]
                assert "connection_string" not in param_names, f"Found connection_string parameter in {method_name}"

                # Ensure no parameter has type annotation str for connection-related params
                for param in sig.parameters.values():
                    if "connection" in param.name.lower():
                        assert param.annotation != str, (
                            f"Connection parameter {param.name} should not have str annotation in {method_name}"
                        )

        # Test TPC-DS benchmark methods
        tpcds = benchbox.TPCDS()

        methods_to_check_tpcds = [
            "run_power_test",
            "run_throughput_test",
            "run_maintenance_test",
        ]
        for method_name in methods_to_check_tpcds:
            if hasattr(tpcds, method_name):
                method = getattr(tpcds, method_name)
                sig = inspect.signature(method)

                # Ensure no parameter is named 'connection_string'
                param_names = [param.name for param in sig.parameters.values()]
                assert "connection_string" not in param_names, f"Found connection_string parameter in {method_name}"

                # Ensure no parameter has type annotation str for connection-related params
                for param in sig.parameters.values():
                    if "connection" in param.name.lower():
                        assert param.annotation != str, (
                            f"Connection parameter {param.name} should not have str annotation in {method_name}"
                        )

    def test_tpc_benchmarks_accept_connection_objects_functionally(self):
        """Test that TPC benchmarks can actually accept connection objects."""

        # Create a mock connection object
        mock_connection = Mock()
        mock_connection.execute = Mock()
        mock_connection.fetchall = Mock(return_value=[])
        mock_connection.commit = Mock()
        mock_connection.close = Mock()

        # Test TPC-H benchmark can accept connection object
        tpch = benchbox.TPCH()

        # Test that methods can be called with connection objects without type errors
        if hasattr(tpch, "run_power_test"):
            try:
                # These should not raise TypeError about connection types
                # We expect other errors (like missing data), but not type errors
                tpch.run_power_test(mock_connection)
            except Exception as e:
                # Should not be a TypeError about connection_string
                assert "connection_string" not in str(e).lower()

        # Test TPC-DS benchmark can accept connection object
        tpcds = benchbox.TPCDS()

        if hasattr(tpcds, "run_power_test"):
            try:
                tpcds.run_power_test(mock_connection, seed=42)
            except Exception as e:
                # Should not be a TypeError about connection_string
                assert "connection_string" not in str(e).lower()

    def test_non_tpc_benchmarks_consistent_interface(self):
        """Test that non-TPC benchmarks use connection objects consistently."""

        # Test a few non-TPC benchmarks to ensure they use connection objects
        # These benchmarks should have run methods that accept connection objects

        # Test Primitives benchmark
        primitives = benchbox.ReadPrimitives()
        if hasattr(primitives, "run"):
            sig = inspect.signature(primitives.run)
            params = list(sig.parameters.values())
            if len(params) > 0 and "connection" in params[0].name.lower():
                # If it has a connection parameter, it should be Any type, not str
                assert params[0].annotation != str, "Connection parameter should not be str type"

    def test_platform_adapter_delegation_compatibility(self):
        """Test that platform adapters properly delegate to TPC benchmark methods."""

        from benchbox.platforms.base import PlatformAdapter

        # Check that platform adapter methods have proper signatures for TPC delegation
        adapter_methods = [
            "run_power_test",
            "run_throughput_test",
            "run_maintenance_test",
        ]

        for method_name in adapter_methods:
            if hasattr(PlatformAdapter, method_name):
                method = getattr(PlatformAdapter, method_name)
                sig = inspect.signature(method)
                params = list(sig.parameters.values())

                # Should have 'self', 'benchmark', and accept **kwargs
                assert len(params) >= 3, f"{method_name} should have at least self, benchmark, and **kwargs"
                assert params[0].name == "self"
                assert params[1].name == "benchmark"

                # Should have **kwargs to handle connection parameter
                has_kwargs = any(param.kind == param.VAR_KEYWORD for param in params)
                assert has_kwargs, f"{method_name} should accept **kwargs for connection delegation"

    def test_database_connection_wrapper_consistency(self):
        """Test that DatabaseConnection wrapper properly handles connection objects."""

        from benchbox.core.connection import DatabaseConnection

        # Check that DatabaseConnection.__init__ expects connection objects, not strings
        sig = inspect.signature(DatabaseConnection.__init__)
        params = list(sig.parameters.values())

        # Find the connection parameter (should be second after self)
        connection_param = params[1]
        assert connection_param.name == "connection"
        # Should not be annotated as str
        assert connection_param.annotation != str


@pytest.mark.unit
@pytest.mark.fast
class TestConnectionInterfaceRegression:
    """Regression tests to prevent connection interface inconsistencies."""

    def test_no_connection_string_creation_in_tpc_benchmarks(self):
        """Test that TPC benchmarks don't create DatabaseConnection from strings."""

        # This test ensures we don't regress to the old pattern of:
        # _DatabaseConnection(connection_string)

        # Read the source code of TPC benchmark modules
        import inspect

        import benchbox.core.tpcds.benchmark as tpcds_module
        import benchbox.core.tpch.benchmark as tpch_module

        # Check TPC-DS source doesn't contain problematic patterns
        tpcds_source = inspect.getsource(tpcds_module)

        # Should not contain _DatabaseConnection(connection_string) pattern
        assert "_DatabaseConnection(connection_string)" not in tpcds_source
        assert "DatabaseConnection(connection_string)" not in tpcds_source

        # Check TPC-H source doesn't contain problematic patterns
        tpch_source = inspect.getsource(tpch_module)

        # Should not contain _DatabaseConnection(connection_string) pattern
        assert "_DatabaseConnection(connection_string)" not in tpch_source
        assert "DatabaseConnection(connection_string)" not in tpch_source

    def test_tpc_modules_import_without_connection_string_usage(self):
        """Test that TPC-H modules don't use connection_string parameters."""

        # Check TPC-H power test module
        try:
            from benchbox.core.tpch.power_test import TPCHPowerTest

            sig = inspect.signature(TPCHPowerTest.__init__)
            params = list(sig.parameters.values())

            # Should not have connection_string parameter
            param_names = [p.name for p in params]
            assert "connection_string" not in param_names
            assert "connection" in param_names

            # Connection parameter should be typed as Any, not str
            connection_param = next((p for p in params if p.name == "connection"), None)
            if connection_param:
                assert connection_param.annotation != str

        except ImportError:
            # Module might not exist in all test environments
            pass

        # Check TPC-H throughput test module
        try:
            from benchbox.core.tpch.throughput_test import TPCHThroughputTest

            sig = inspect.signature(TPCHThroughputTest.__init__)
            params = list(sig.parameters.values())

            # Should not have connection_string parameter
            param_names = [p.name for p in params]
            assert "connection_string" not in param_names
            assert "connection_factory" in param_names

            # Connection parameter should be typed as Any, not str
            connection_param = next((p for p in params if p.name == "connection_factory"), None)
            if connection_param:
                assert connection_param.annotation != str

        except ImportError:
            # Module might not exist in all test environments
            pass

        # Check TPC-H maintenance test module
        try:
            from benchbox.core.tpch.maintenance_test import MaintenanceTest

            sig = inspect.signature(MaintenanceTest.__init__)
            params = list(sig.parameters.values())

            # Should not have connection_string parameter
            param_names = [p.name for p in params]
            assert "connection_string" not in param_names

            # Connection parameter should be typed as Any, not str
            connection_param = next((p for p in params if p.name == "connection"), None)
            if connection_param:
                assert connection_param.annotation != str

        except ImportError:
            # Module might not exist in all test environments
            pass

    def test_cli_orchestrator_uses_connection_objects(self):
        """Test that CLI orchestrator properly uses connection objects."""

        try:
            from benchbox.cli.orchestrator import BenchmarkOrchestrator

            # The orchestrator should create connection objects and pass them to benchmarks
            # This test ensures the orchestrator doesn't pass connection strings

            BenchmarkOrchestrator()

            # Check if there are any methods that might pass connection_string
            # This is more of a code review test to catch regressions
            import inspect

            orchestrator_source = inspect.getsource(BenchmarkOrchestrator)

            # Should not contain patterns that pass connection strings to benchmarks
            problematic_patterns = [
                "connection_string=",
                "connection=connection_string",
                "_DatabaseConnection(connection_string)",
            ]

            for pattern in problematic_patterns:
                assert pattern not in orchestrator_source, f"Found problematic pattern: {pattern}"

        except ImportError:
            # Module might not exist in all test environments
            pass


if __name__ == "__main__":
    pytest.main(["-v", __file__])
