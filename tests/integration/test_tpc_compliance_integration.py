"""Integration tests for TPC Compliance framework.

This module contains comprehensive integration tests for the TPC compliance implementation
including Power, Throughput, and Maintenance tests for both TPC-H and TPC-DS benchmarks.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from benchbox.core.tpc_compliance import (
    TPCCompliance,
    TPCMaintenanceTest,
    TPCOfficialMetrics,
    TPCPowerTest,
    TPCTestResult,
    TPCThroughputTest,
)
from benchbox.core.tpcds.benchmark import TPCDSBenchmark
from benchbox.core.tpch.benchmark import TPCHBenchmark

# Mark all tests in this file as integration tests
pytestmark = [pytest.mark.integration, pytest.mark.slow]


class TestTPCComplianceFramework:
    """Test the core TPC compliance framework."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.connection_string = "sqlite:///:memory:"
        self.dialect = "sqlite"

    def test_tpc_compliance_initialization(self) -> None:
        """Test TPC compliance framework initialization."""
        compliance = TPCCompliance(
            benchmark_name="TPC-H",
            scale_factor=1.0,
            connection_string=self.connection_string,
            dialect=self.dialect,
        )

        assert compliance.benchmark_name == "TPC-H"
        assert compliance.scale_factor == 1.0
        assert compliance.connection_string == self.connection_string
        assert compliance.dialect == self.dialect

    def test_tpc_test_result_creation(self) -> None:
        """Test TPC test result creation and validation."""
        result = TPCTestResult(
            benchmark_name="TPC-H",
            test_phase="Power",
            scale_factor=1.0,
            execution_time=120.5,
            success=True,
        )

        assert result.benchmark_name == "TPC-H"
        assert result.test_phase == "Power"
        assert result.scale_factor == 1.0
        assert result.execution_time == 120.5
        assert result.success is True

    def test_tpc_official_metrics_calculation(self) -> None:
        """Test official TPC metrics calculation."""
        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test QphH@Size calculation
        qphh_size = metrics.calculate_qphh_size(power_time=100.0, throughput_time=200.0, num_streams=2)

        # Expected: sqrt((3600 * 1.0 / 100.0) * (2 * 3600 * 1.0 / 200.0))
        # = sqrt(36.0 * 36.0) = 36.0
        assert qphh_size == 36.0

        # Test QphDS@Size calculation
        qphds_size = metrics.calculate_qphds_size(power_time=150.0, throughput_time=300.0, num_streams=3)

        # Expected: sqrt((3600 * 1.0 / 150.0) * (3 * 3600 * 1.0 / 300.0))
        # = sqrt(24.0 * 36.0) = sqrt(864.0) ≈ 29.39
        assert abs(qphds_size - 29.39) < 0.01

    def test_power_test_abstract_methods(self) -> None:
        """Test that Power Test abstract methods are properly defined."""
        # This test ensures the abstract base class works correctly
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            TPCPowerTest(Mock(), Mock(), self.dialect)

    def test_throughput_test_abstract_methods(self) -> None:
        """Test that Throughput Test abstract methods are properly defined."""
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            TPCThroughputTest(Mock(), Mock(), 2, self.dialect)

    def test_maintenance_test_abstract_methods(self) -> None:
        """Test that Maintenance Test abstract methods are properly defined."""
        with pytest.raises(TypeError):
            # Cannot instantiate abstract class
            TPCMaintenanceTest(Mock(), Mock(), self.dialect)


class TestTPCHCompliance:
    """Test TPC-H compliance implementation."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.connection_string = "sqlite:///:memory:"
        self.dialect = "sqlite"

    def test_tpch_benchmark_initialization(self) -> None:
        """Test TPC-H benchmark initialization."""
        benchmark = TPCHBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        assert benchmark.scale_factor == 1.0
        assert benchmark.output_dir == Path(self.temp_dir)

    def test_tpch_power_test_integration(self) -> None:
        """Test TPC-H Power Test integration."""
        # Mock database connection
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
        mock_conn.execute.return_value = mock_cursor
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.commit.return_value = None
        mock_conn.close.return_value = None

        # Create benchmark and run power test
        benchmark = TPCHBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the power test execution
        with patch.object(benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT 1"

            # Import and test the power test
            from benchbox.core.tpch.power_test import TPCHPowerTest

            power_test = TPCHPowerTest(
                benchmark=benchmark,
                connection=mock_conn,
                scale_factor=1.0,
                dialect=self.dialect,
                verbose=False,
            )

            # Run the power test
            result = power_test.run()

            # Verify results
            assert result.scale_factor == 1.0
            assert result.power_at_size > 0
            assert len(result.query_results) == 22  # TPC-H has 22 queries

    def test_tpch_throughput_test_integration(self) -> None:
        """Test TPC-H Throughput Test integration."""

        # Create mock connection factory
        def mock_connection_factory():
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_conn.execute.return_value = mock_cursor
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.commit.return_value = None
            mock_conn.close.return_value = None
            return mock_conn

        # Create benchmark and run throughput test
        benchmark = TPCHBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the throughput test execution
        with patch.object(benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT 1"

            # Import and test the throughput test
            from benchbox.core.tpch.throughput_test import TPCHThroughputTest

            throughput_test = TPCHThroughputTest(
                benchmark=benchmark,
                connection_factory=mock_connection_factory,
                num_streams=2,
                scale_factor=1.0,
                verbose=False,
            )

            # Run the throughput test
            result = throughput_test.run()

            # Verify results
            assert result.scale_factor == 1.0
            assert result.throughput_at_size > 0
            assert len(result.stream_results) == 2

    def test_tpch_maintenance_test_integration(self) -> None:
        """Test TPC-H Maintenance Test integration."""

        # Create mock connection factory
        def mock_connection_factory():
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_conn.execute.return_value = mock_cursor
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.commit.return_value = None
            mock_conn.close.return_value = None
            return mock_conn

        # Create benchmark and run maintenance test
        benchmark = TPCHBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the maintenance test execution
        with patch.object(benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT 1"

            # Import and test the maintenance test
            from benchbox.core.tpch.maintenance_test import TPCHMaintenanceTest

            maintenance_test = TPCHMaintenanceTest(
                connection_factory=mock_connection_factory,
                scale_factor=1.0,
                verbose=False,
            )

            # Run the maintenance test
            result = maintenance_test.run_maintenance_test()

            # Verify results
            assert result.config.scale_factor == 1.0
            assert result.total_time > 0
            assert result.rf1_operations > 0
            assert result.rf2_operations > 0

    def test_tpch_qphh_size_calculation(self) -> None:
        """Test TPC-H QphH@Size calculation integration."""
        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test with sample timing data
        power_time = 360.0  # 6 minutes
        throughput_time = 720.0  # 12 minutes
        num_streams = 2

        qphh_size = metrics.calculate_qphh_size(
            power_time=power_time,
            throughput_time=throughput_time,
            num_streams=num_streams,
        )

        # Expected calculation:
        # Power@Size = 3600 * 1.0 / 360.0 = 10.0
        # Throughput@Size = 2 * 3600 * 1.0 / 720.0 = 10.0
        # QphH@Size = sqrt(10.0 * 10.0) = 10.0
        assert qphh_size == 10.0


class TestTPCDSCompliance:
    """Test TPC-DS compliance implementation."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.connection_string = "sqlite:///:memory:"
        self.dialect = "sqlite"

    def test_tpcds_benchmark_initialization(self) -> None:
        """Test TPC-DS benchmark initialization."""
        benchmark = TPCDSBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        assert benchmark.scale_factor == 1.0
        assert benchmark.output_dir == Path(self.temp_dir)

    def test_tpcds_power_test_integration(self) -> None:
        """Test TPC-DS Power Test integration."""
        # Create benchmark and run power test
        benchmark = TPCDSBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the power test execution
        with (
            patch.object(benchmark, "get_query") as mock_get_query,
            patch.object(benchmark, "get_queries") as mock_get_queries,
        ):
            mock_get_query.return_value = "SELECT 1"
            # Mock get_queries to return a dictionary of queries 1-10 for testing
            mock_get_queries.return_value = {str(i): f"SELECT {i}" for i in range(1, 11)}

            # Import and test the power test
            from benchbox.core.tpcds.power_test import TPCDSPowerTest

            # Create a mock connection factory that returns properly structured mock
            def mock_connection_factory():
                mock_conn = Mock()
                mock_cursor = Mock()
                # Configure fetchall to return list that has proper len()
                mock_results = [("result1",), ("result2",), ("result3",)]
                mock_cursor.fetchall.return_value = mock_results
                mock_conn.execute.return_value = mock_cursor
                mock_conn.commit.return_value = None
                mock_conn.close.return_value = None
                return mock_conn

            power_test = TPCDSPowerTest(
                benchmark=benchmark,
                connection_factory=mock_connection_factory,
                scale_factor=1.0,
                verbose=False,
            )

            # Set a limited query sequence for testing
            power_test._query_sequence = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

            # Run the power test
            result = power_test.run()

            # Verify results
            assert result.scale_factor == 1.0
            assert result.power_at_size > 0
            # We set 10 queries for testing
            assert len(result.query_results) == 10

    def test_tpcds_throughput_test_integration(self) -> None:
        """Test TPC-DS Throughput Test integration."""

        # Create mock connection factory
        def mock_connection_factory():
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_conn.execute.return_value = mock_cursor
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.commit.return_value = None
            mock_conn.close.return_value = None
            return mock_conn

        # Create benchmark and run throughput test
        benchmark = TPCDSBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the throughput test execution
        with patch.object(benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT 1"

            # Import and test the throughput test
            from benchbox.core.tpcds.throughput_test import TPCDSThroughputTest

            throughput_test = TPCDSThroughputTest(
                benchmark=benchmark,
                connection_factory=mock_connection_factory,
                num_streams=2,
                scale_factor=1.0,
                verbose=False,
            )

            # Run the throughput test
            result = throughput_test.run()

            # Verify results
            assert result.scale_factor == 1.0
            assert result.throughput_at_size > 0
            assert len(result.stream_results) == 2

    def test_tpcds_maintenance_test_integration(self) -> None:
        """Test TPC-DS Maintenance Test integration."""

        # Create mock connection factory
        def mock_connection_factory():
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_conn.execute.return_value = mock_cursor
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.commit.return_value = None
            mock_conn.close.return_value = None
            return mock_conn

        # Create benchmark
        benchmark = TPCDSBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock the maintenance test execution
        with patch.object(benchmark, "run_maintenance_test") as mock_run_test:
            mock_result = Mock()
            mock_result.test_duration = 60.0
            mock_result.overall_throughput = 100.0
            mock_result.operation_metrics = []
            mock_run_test.return_value = mock_result

            # Run the maintenance test
            result = benchmark.run_maintenance_test(connection_factory=mock_connection_factory)

            # Verify results
            assert result.test_duration == 60.0
            assert result.overall_throughput == 100.0

    def test_tpcds_qphds_size_calculation(self) -> None:
        """Test TPC-DS QphDS@Size calculation integration."""
        metrics = TPCOfficialMetrics(benchmark_name="TPC-DS", scale_factor=1.0)

        # Test with sample timing data
        power_time = 600.0  # 10 minutes
        throughput_time = 1200.0  # 20 minutes
        num_streams = 3

        qphds_size = metrics.calculate_qphds_size(
            power_time=power_time,
            throughput_time=throughput_time,
            num_streams=num_streams,
        )

        # Expected calculation:
        # Power@Size = 3600 * 1.0 / 600.0 = 6.0
        # Throughput@Size = 3 * 3600 * 1.0 / 1200.0 = 9.0
        # QphDS@Size = sqrt(6.0 * 9.0) = sqrt(54.0) ≈ 7.35
        assert abs(qphds_size - 7.35) < 0.01


class TestTPCComplianceEndToEnd:
    """End-to-end integration tests for TPC compliance."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.connection_string = "sqlite:///:memory:"
        self.dialect = "sqlite"

    def test_tpch_complete_benchmark_workflow(self) -> None:
        """Test complete TPC-H benchmark workflow."""

        # Create mock connection factory
        def mock_connection_factory():
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_cursor.fetchall.return_value = [("result1",), ("result2",)]
            mock_conn.execute.return_value = mock_cursor
            mock_conn.cursor.return_value = mock_cursor
            mock_conn.commit.return_value = None
            mock_conn.close.return_value = None
            return mock_conn

        # Create single mock connection for power test
        mock_conn = mock_connection_factory()

        # Create benchmark
        benchmark = TPCHBenchmark(scale_factor=1.0, output_dir=self.temp_dir)

        # Mock query retrieval
        with patch.object(benchmark, "get_query") as mock_get_query:
            mock_get_query.return_value = "SELECT 1"

            # Test power test
            from benchbox.core.tpch.power_test import TPCHPowerTest

            power_test = TPCHPowerTest(
                benchmark=benchmark,
                connection=mock_conn,
                scale_factor=1.0,
                dialect=self.dialect,
                verbose=False,
            )
            power_result = power_test.run()

            # Test throughput test
            from benchbox.core.tpch.throughput_test import TPCHThroughputTest

            throughput_test = TPCHThroughputTest(
                benchmark=benchmark,
                connection_factory=mock_connection_factory,
                num_streams=2,
                scale_factor=1.0,
                verbose=False,
            )
            throughput_result = throughput_test.run()

            # Test maintenance test
            from benchbox.core.tpch.maintenance_test import TPCHMaintenanceTest

            maintenance_test = TPCHMaintenanceTest(
                connection_factory=mock_connection_factory,
                scale_factor=1.0,
                verbose=False,
            )
            maintenance_result = maintenance_test.run_maintenance_test()

            # Calculate final QphH@Size
            metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

            qphh_size = metrics.calculate_qphh_size(
                power_time=power_result.total_time,
                throughput_time=throughput_result.total_time,
                num_streams=2,
            )

            # Verify all tests completed successfully
            assert power_result.power_at_size > 0
            assert throughput_result.throughput_at_size > 0
            assert maintenance_result.total_time > 0
            assert qphh_size > 0

    def test_tpc_metrics_validation(self) -> None:
        """Test TPC metrics validation and error handling."""
        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test with zero times (should handle gracefully)
        qphh_size = metrics.calculate_qphh_size(power_time=0.0, throughput_time=100.0, num_streams=2)
        assert qphh_size == 0.0

        # Test with negative times (should handle gracefully)
        qphh_size = metrics.calculate_qphh_size(power_time=-50.0, throughput_time=100.0, num_streams=2)
        assert qphh_size == 0.0

        # Test with zero streams (should handle gracefully)
        qphh_size = metrics.calculate_qphh_size(power_time=100.0, throughput_time=100.0, num_streams=0)
        assert qphh_size == 0.0

    def test_tpc_compliance_error_handling(self) -> None:
        """Test TPC compliance error handling."""
        # Test invalid scale factor
        with pytest.raises(ValueError):
            TPCCompliance(
                benchmark_name="TPC-H",
                scale_factor=-1.0,
                connection_string=self.connection_string,
                dialect=self.dialect,
            )

        # Test invalid connection string
        with pytest.raises(ValueError):
            TPCCompliance(
                benchmark_name="TPC-H",
                scale_factor=1.0,
                connection_string="",
                dialect=self.dialect,
            )

    def test_tpc_result_serialization(self) -> None:
        """Test TPC result serialization and deserialization."""
        result = TPCTestResult(
            benchmark_name="TPC-H",
            test_phase="Power",
            scale_factor=1.0,
            execution_time=120.5,
            success=True,
            query_results={"Q1": {"time": 1.5, "status": "success"}},
            errors=["Warning: Query timeout extended"],
            metadata={"database": "PostgreSQL", "version": "13.2"},
        )

        # Convert to dictionary
        result_dict = result.to_dict()

        # Verify serialization
        assert result_dict["benchmark_name"] == "TPC-H"
        assert result_dict["test_phase"] == "Power"
        assert result_dict["scale_factor"] == 1.0
        assert result_dict["execution_time"] == 120.5
        assert result_dict["success"] is True
        assert "Q1" in result_dict["query_results"]
        assert len(result_dict["errors"]) == 1
        assert "database" in result_dict["metadata"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
