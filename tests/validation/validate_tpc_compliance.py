#!/usr/bin/env python3
"""TPC Compliance Validation Script

This script validates the TPC compliance implementation against official TPC specifications
for TPC-H and TPC-DS benchmarks. It checks for:
- Correct test phase implementation
- Official metrics calculation
- Specification compliance requirements
- Error handling and validation

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys
import traceback
from pathlib import Path

# Include current directory to path
sys.path.insert(0, str(Path.cwd()))


def test_tpc_compliance_framework():
    """Test the core TPC compliance framework."""
    print("Testing TPC Compliance Framework...")

    try:
        from benchbox.core.tpc_compliance import (
            TPCOfficialMetrics,
        )

        print("✅ TPC compliance framework imported successfully")

        # Test TPCOfficialMetrics
        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test QphH@Size calculation
        qphh_size = metrics.calculate_qphh_size(power_time=100.0, throughput_time=200.0, num_streams=2)

        expected_qphh = 36.0  # sqrt((3600/100) * (2*3600/200))
        if abs(qphh_size - expected_qphh) < 0.01:
            print("✅ QphH@Size calculation is correct")
        else:
            print(f"❌ QphH@Size calculation error: got {qphh_size}, expected {expected_qphh}")
            return False

        # Test QphDS@Size calculation
        qphds_size = metrics.calculate_qphds_size(power_time=150.0, throughput_time=300.0, num_streams=3)

        # Expected: sqrt((3600/150) * (3*3600/300)) = sqrt(24 * 36) = sqrt(864) ≈ 29.39
        expected_qphds = 29.39
        if abs(qphds_size - expected_qphds) < 0.01:
            print("✅ QphDS@Size calculation is correct")
        else:
            print(f"❌ QphDS@Size calculation error: got {qphds_size}, expected {expected_qphds}")
            return False

        return True

    except Exception as e:
        print(f"❌ TPC compliance framework test failed: {e}")
        traceback.print_exc()
        return False


def test_tpch_compliance():
    """Test TPC-H compliance implementation."""
    print("\nTesting TPC-H Compliance...")

    try:
        from benchbox.core.tpch.benchmark import TPCHBenchmark
        from benchbox.core.tpch.maintenance_test import TPCHMaintenanceTest
        from benchbox.core.tpch.power_test import TPCHPowerTest
        from benchbox.core.tpch.throughput_test import TPCHThroughputTest

        print("✅ TPC-H compliance classes imported successfully")

        # Test benchmark initialization
        benchmark = TPCHBenchmark(scale_factor=0.01)

        if benchmark.scale_factor == 0.01:
            print("✅ TPC-H benchmark initialization is correct")
        else:
            print("❌ TPC-H benchmark initialization error")
            return False

        # Test power test initialization
        # Create a mock connection for validation testing
        import duckdb

        connection = duckdb.connect(":memory:")
        power_test = TPCHPowerTest(
            benchmark=benchmark,
            connection=connection,
            scale_factor=0.01,
            dialect="duckdb",
            verbose=False,
        )

        # Check query sequence
        if len(power_test.query_sequence) == 22:
            print("✅ TPC-H Power Test has correct query sequence (22 queries)")
        else:
            print(f"❌ TPC-H Power Test query sequence error: got {len(power_test.query_sequence)}, expected 22")
            return False

        # Test throughput test initialization
        def create_connection():
            return duckdb.connect(":memory:")

        throughput_test = TPCHThroughputTest(
            benchmark=benchmark,
            connection_factory=create_connection,
            num_streams=2,
            scale_factor=0.01,
            verbose=False,
        )

        if throughput_test.config.num_streams == 2:
            print("✅ TPC-H Throughput Test configuration is correct")
        else:
            print("❌ TPC-H Throughput Test configuration error")
            return False

        # Test maintenance test initialization
        maintenance_test = TPCHMaintenanceTest(
            connection_factory=create_connection,
            scale_factor=0.01,
            verbose=False,
        )

        refresh_functions = maintenance_test.get_refresh_functions()
        if "RF1" in refresh_functions and "RF2" in refresh_functions:
            print("✅ TPC-H Maintenance Test has correct refresh functions")
        else:
            print(f"❌ TPC-H Maintenance Test refresh functions error: {refresh_functions}")
            return False

        return True

    except Exception as e:
        print(f"❌ TPC-H compliance test failed: {e}")
        traceback.print_exc()
        return False


def test_tpcds_compliance():
    """Test TPC-DS compliance implementation."""
    print("\nTesting TPC-DS Compliance...")

    try:
        from benchbox.core.tpcds.benchmark import TPCDSBenchmark
        from benchbox.core.tpcds.maintenance_test import MaintenanceTestConfig
        from benchbox.core.tpcds.power_test import TPCDSPowerTest
        from benchbox.core.tpcds.throughput_test import TPCDSThroughputTest

        print("✅ TPC-DS compliance classes imported successfully")

        # Test benchmark initialization
        benchmark = TPCDSBenchmark(scale_factor=0.01)

        if benchmark.scale_factor == 0.01:
            print("✅ TPC-DS benchmark initialization is correct")
        else:
            print("❌ TPC-DS benchmark initialization error")
            return False

        # Test power test initialization
        power_test = TPCDSPowerTest(
            benchmark=benchmark,
            connection_string=":memory:",
            scale_factor=0.01,
            dialect="duckdb",
            verbose=False,
        )

        # Check query sequence (should include 99 queries plus variants)
        if len(power_test.query_sequence) >= 99:
            print(f"✅ TPC-DS Power Test has correct query sequence ({len(power_test.query_sequence)} queries)")
        else:
            print(f"❌ TPC-DS Power Test query sequence error: got {len(power_test.query_sequence)}, expected >= 99")
            return False

        # Test throughput test initialization
        throughput_test = TPCDSThroughputTest(
            benchmark=benchmark,
            connection_string=":memory:",
            num_streams=2,
            scale_factor=0.01,
            dialect="duckdb",
            verbose=False,
        )

        if throughput_test.num_streams == 2:
            print("✅ TPC-DS Throughput Test configuration is correct")
        else:
            print("❌ TPC-DS Throughput Test configuration error")
            return False

        # Test maintenance test configuration
        config = MaintenanceTestConfig(maintenance_operations=2, operation_interval=30.0, scale_factor=0.01)

        if config.maintenance_operations == 2 and config.operation_interval == 30.0:
            print("✅ TPC-DS Maintenance Test configuration is correct")
        else:
            print("❌ TPC-DS Maintenance Test configuration error")
            return False

        return True

    except Exception as e:
        print(f"❌ TPC-DS compliance test failed: {e}")
        traceback.print_exc()
        return False


def test_tpc_specification_compliance():
    """Test compliance with TPC specifications."""
    print("\nTesting TPC Specification Compliance...")

    try:
        from benchbox.core.tpc_compliance import TPCOfficialMetrics

        # Test TPC-H specification compliance
        print("Checking TPC-H specification compliance...")

        tpch_metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test Power@Size formula: 3600 * SF / T_power
        power_size = tpch_metrics.calculate_power_size(execution_time=360.0)
        expected_power = 3600.0 * 1.0 / 360.0  # = 10.0

        if abs(power_size - expected_power) < 0.01:
            print("✅ TPC-H Power@Size formula is correct")
        else:
            print(f"❌ TPC-H Power@Size formula error: got {power_size}, expected {expected_power}")
            return False

        # Test Throughput@Size formula: S * 3600 * SF / T_throughput
        throughput_size = tpch_metrics.calculate_throughput_size(execution_time=720.0, num_streams=2)
        expected_throughput = 2 * 3600.0 * 1.0 / 720.0  # = 10.0

        if abs(throughput_size - expected_throughput) < 0.01:
            print("✅ TPC-H Throughput@Size formula is correct")
        else:
            print(f"❌ TPC-H Throughput@Size formula error: got {throughput_size}, expected {expected_throughput}")
            return False

        # Test QphH@Size formula: sqrt(Power@Size * Throughput@Size)
        qphh_size = tpch_metrics.calculate_qphh_size(power_time=360.0, throughput_time=720.0, num_streams=2)
        expected_qphh = (10.0 * 10.0) ** 0.5  # = 10.0

        if abs(qphh_size - expected_qphh) < 0.01:
            print("✅ TPC-H QphH@Size formula is correct")
        else:
            print(f"❌ TPC-H QphH@Size formula error: got {qphh_size}, expected {expected_qphh}")
            return False

        # Test TPC-DS specification compliance
        print("Checking TPC-DS specification compliance...")

        tpcds_metrics = TPCOfficialMetrics(benchmark_name="TPC-DS", scale_factor=1.0)

        # Test QphDS@Size formula: sqrt(Power@Size * Throughput@Size)
        qphds_size = tpcds_metrics.calculate_qphds_size(power_time=600.0, throughput_time=1200.0, num_streams=3)

        # Expected: sqrt((3600/600) * (3*3600/1200)) = sqrt(6 * 9) = sqrt(54) ≈ 7.35
        expected_qphds = (6.0 * 9.0) ** 0.5

        if abs(qphds_size - expected_qphds) < 0.01:
            print("✅ TPC-DS QphDS@Size formula is correct")
        else:
            print(f"❌ TPC-DS QphDS@Size formula error: got {qphds_size}, expected {expected_qphds}")
            return False

        return True

    except Exception as e:
        print(f"❌ TPC specification compliance test failed: {e}")
        traceback.print_exc()
        return False


def test_error_handling():
    """Test error handling in TPC compliance implementation."""
    print("\nTesting Error Handling...")

    try:
        from benchbox.core.tpc_compliance import TPCOfficialMetrics

        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test zero execution time handling
        qphh_size = metrics.calculate_qphh_size(power_time=0.0, throughput_time=100.0, num_streams=2)

        if qphh_size == 0.0:
            print("✅ Zero execution time handled correctly")
        else:
            print(f"❌ Zero execution time not handled correctly: got {qphh_size}")
            return False

        # Test negative execution time handling
        qphh_size = metrics.calculate_qphh_size(power_time=-50.0, throughput_time=100.0, num_streams=2)

        if qphh_size == 0.0:
            print("✅ Negative execution time handled correctly")
        else:
            print(f"❌ Negative execution time not handled correctly: got {qphh_size}")
            return False

        # Test zero streams handling
        qphh_size = metrics.calculate_qphh_size(power_time=100.0, throughput_time=100.0, num_streams=0)

        if qphh_size == 0.0:
            print("✅ Zero streams handled correctly")
        else:
            print(f"❌ Zero streams not handled correctly: got {qphh_size}")
            return False

        return True

    except Exception as e:
        print(f"❌ Error handling test failed: {e}")
        traceback.print_exc()
        return False


def test_performance_requirements():
    """Test performance requirements for TPC compliance."""
    print("\nTesting Performance Requirements...")

    try:
        import time

        from benchbox.core.tpc_compliance import TPCOfficialMetrics

        metrics = TPCOfficialMetrics(benchmark_name="TPC-H", scale_factor=1.0)

        # Test metrics calculation performance
        start_time = time.time()

        for i in range(1000):
            qphh_size = metrics.calculate_qphh_size(
                power_time=100.0 + i * 0.1,
                throughput_time=200.0 + i * 0.2,
                num_streams=2,
            )
            assert qphh_size > 0

        calculation_time = time.time() - start_time

        # Should complete 1000 calculations in under 1 second
        if calculation_time < 1.0:
            print(f"✅ Metrics calculation performance is acceptable ({calculation_time:.4f}s for 1000 calculations)")
        else:
            print(f"❌ Metrics calculation performance is too slow ({calculation_time:.4f}s for 1000 calculations)")
            return False

        return True

    except Exception as e:
        print(f"❌ Performance requirements test failed: {e}")
        traceback.print_exc()
        return False


def test_integration_with_benchmarks():
    """Test integration with TPC-H and TPC-DS benchmarks."""
    print("\nTesting Integration with Benchmarks...")

    try:
        from benchbox.tpcds import TPCDS
        from benchbox.tpch import TPCH

        # Test TPC-H integration
        tpch = TPCH(scale_factor=0.01)

        # Check that TPC-H benchmark has required methods
        required_methods = [
            "get_query",
            "get_queries",
            "generate_data",
            "get_available_queries",
        ]
        for method in required_methods:
            if hasattr(tpch, method):
                print(f"✅ TPC-H has {method} method")
            else:
                print(f"❌ TPC-H missing {method} method")
                return False

        # Test TPC-DS integration
        tpcds = TPCDS(scale_factor=0.01)

        # Check that TPC-DS benchmark has required methods
        for method in required_methods:
            if hasattr(tpcds, method):
                print(f"✅ TPC-DS has {method} method")
            else:
                print(f"❌ TPC-DS missing {method} method")
                return False

        # Test that TPC-DS has maintenance test methods
        maintenance_methods = [
            "run_maintenance_test",
            "validate_maintenance_data_integrity",
        ]
        for method in maintenance_methods:
            if hasattr(tpcds, method):
                print(f"✅ TPC-DS has {method} method")
            else:
                print(f"❌ TPC-DS missing {method} method")
                return False

        return True

    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        traceback.print_exc()
        return False


def main():
    """Main validation function."""
    print("TPC Compliance Validation")
    print("=" * 50)

    tests = [
        ("TPC Compliance Framework", test_tpc_compliance_framework),
        ("TPC-H Compliance", test_tpch_compliance),
        ("TPC-DS Compliance", test_tpcds_compliance),
        ("TPC Specification Compliance", test_tpc_specification_compliance),
        ("Error Handling", test_error_handling),
        ("Performance Requirements", test_performance_requirements),
        ("Integration with Benchmarks", test_integration_with_benchmarks),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)

        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            print(f"❌ {test_name} FAILED with exception: {e}")

    print(f"\n{'=' * 50}")
    print("TPC COMPLIANCE VALIDATION SUMMARY")
    print(f"{'=' * 50}")
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed / total * 100:.1f}%")

    if passed == total:
        print("✅ All TPC compliance validation tests passed!")
        print("✅ Implementation is compliant with TPC specifications")
        return 0
    else:
        print(f"❌ {total - passed} TPC compliance validation tests failed")
        print("❌ Implementation may not be fully compliant with TPC specifications")
        return 1


if __name__ == "__main__":
    sys.exit(main())
