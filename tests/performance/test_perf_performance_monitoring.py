"""Performance Monitoring Tests.

This module provides tests for performance monitoring capabilities
that track metrics over time and detect performance trends.

Tests include:
- Performance metric collection
- Trend analysis
- Alerting for performance degradation
- Historical performance tracking
- Benchmark comparison
- Resource utilization monitoring

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import statistics
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from benchbox.core.tpcds.benchmark import TPCDSBenchmark
from benchbox.monitoring import PerformanceMonitor, PerformanceTracker


@pytest.mark.slow  # Resource utilization thresholds are flaky depending on system load
@pytest.mark.performance
@pytest.mark.monitoring
class TestPerformanceMonitoring:
    """Test performance monitoring capabilities."""

    @pytest.fixture
    def benchmark_instance(self):
        """Create a benchmark instance for monitoring tests."""
        return TPCDSBenchmark(
            scale_factor=0.01,
            verbose=False,
        )

    @pytest.fixture
    def performance_tracker(self):
        """Create a performance tracker for tests."""
        temp_file = Path(tempfile.mktemp(suffix=".json"))
        tracker = PerformanceTracker(temp_file)
        yield tracker
        # Cleanup
        if temp_file.exists():
            temp_file.unlink()

    @pytest.fixture
    def performance_monitor(self):
        """Create a performance monitor for tests."""
        return PerformanceMonitor()

    def test_metric_collection(self, benchmark_instance, performance_tracker) -> None:
        """Test basic metric collection."""
        # Collect query generation metrics
        query_times = []
        for i in range(10):
            start_time = time.time()
            benchmark_instance.get_query((i % 20) + 1)
            query_time = time.time() - start_time
            query_times.append(query_time)

            performance_tracker.record_metric("query_generation_time", query_time)

        # Verify metrics were recorded
        trend = performance_tracker.get_trend("query_generation_time")
        assert len(trend["recent_values"]) == 10
        assert trend["average"] > 0
        assert trend["min"] >= 0
        assert trend["max"] >= trend["min"]

    def test_trend_analysis(self, performance_tracker) -> None:
        """Test trend analysis functionality."""
        # Simulate improving performance
        base_time = datetime.now(timezone.utc)
        for i in range(20):
            # Simulate gradually improving performance
            value = 1.0 - (i * 0.02)  # Decreasing from 1.0 to 0.6
            timestamp = base_time + timedelta(hours=i)
            performance_tracker.record_metric("improving_metric", value, timestamp)

        trend = performance_tracker.get_trend("improving_metric")
        assert trend["trend"] == "improving"

        # Simulate degrading performance
        for i in range(20):
            # Simulate gradually degrading performance
            value = 1.0 + (i * 0.03)  # Increasing from 1.0 to 1.57
            timestamp = base_time + timedelta(hours=i)
            performance_tracker.record_metric("degrading_metric", value, timestamp)

        trend = performance_tracker.get_trend("degrading_metric")
        assert trend["trend"] == "degrading"

        # Simulate stable performance
        for i in range(20):
            # Simulate stable performance with small variations
            value = 1.0 + (i % 2) * 0.01  # Alternating between 1.0 and 1.01
            timestamp = base_time + timedelta(hours=i)
            performance_tracker.record_metric("stable_metric", value, timestamp)

        trend = performance_tracker.get_trend("stable_metric")
        assert trend["trend"] == "stable"

    def test_anomaly_detection(self, performance_tracker) -> None:
        """Test anomaly detection in performance metrics."""
        # normal performance data
        base_time = datetime.now(timezone.utc)
        normal_values = [1.0, 0.9, 1.1, 1.0, 0.95, 1.05, 1.0, 0.98, 1.02, 1.0]

        for i, value in enumerate(normal_values):
            timestamp = base_time + timedelta(hours=i)
            performance_tracker.record_metric("normal_metric", value, timestamp)

        # Include some anomalies
        anomaly_values = [3.0, 5.0]  # Significantly higher than normal
        for i, value in enumerate(anomaly_values):
            timestamp = base_time + timedelta(hours=len(normal_values) + i)
            performance_tracker.record_metric("normal_metric", value, timestamp)

        # Detect anomalies
        anomalies = performance_tracker.detect_anomalies("normal_metric", threshold_multiplier=1.5)

        # Should detect at least one anomalous value (either 3.0 or 5.0)
        assert len(anomalies) >= 1
        anomaly_values_detected = [a["value"] for a in anomalies]
        # At least one of the anomalous values should be detected
        assert 3.0 in anomaly_values_detected or 5.0 in anomaly_values_detected

    def test_performance_alerting(self, benchmark_instance, performance_tracker) -> None:
        """Test performance alerting system."""
        # Define performance thresholds
        thresholds = {
            "query_generation_time": {"warning": 0.01, "critical": 0.05},
            "memory_usage": {"warning": 50, "critical": 100},
            "error_rate": {"warning": 0.01, "critical": 0.05},
        }

        alerts = []

        def alert_callback(metric_name: str, level: str, value: float, threshold: float) -> None:
            alerts.append(
                {
                    "metric": metric_name,
                    "level": level,
                    "value": value,
                    "threshold": threshold,
                    "timestamp": datetime.now(timezone.utc),
                }
            )

        # Simulate measurements that should trigger alerts
        performance_tracker.record_metric("query_generation_time", 0.02)  # Warning
        performance_tracker.record_metric("query_generation_time", 0.08)  # Critical
        performance_tracker.record_metric("memory_usage", 75)  # Warning
        performance_tracker.record_metric("memory_usage", 150)  # Critical

        # Check for alerts (simplified - in real implementation this would be automatic)
        # Check all recorded values, not just the latest one
        for metric_name, metric_thresholds in thresholds.items():
            trend = performance_tracker.get_trend(metric_name)
            if trend["recent_values"]:
                # Check all values for alerts
                for value in trend["recent_values"]:
                    if value > metric_thresholds["critical"]:
                        alert_callback(
                            metric_name,
                            "critical",
                            value,
                            metric_thresholds["critical"],
                        )
                    elif value > metric_thresholds["warning"]:
                        alert_callback(
                            metric_name,
                            "warning",
                            value,
                            metric_thresholds["warning"],
                        )

        # Verify alerts were generated
        assert len(alerts) >= 2

        # Check for critical alerts
        critical_alerts = [a for a in alerts if a["level"] == "critical"]
        assert len(critical_alerts) >= 1

        # Check for warning alerts
        warning_alerts = [a for a in alerts if a["level"] == "warning"]
        assert len(warning_alerts) >= 1

    def test_historical_performance_tracking(self, benchmark_instance, performance_tracker) -> None:
        """Test historical performance tracking."""
        # Simulate performance data over time
        base_time = datetime.now(timezone.utc) - timedelta(days=30)

        # Generate 30 days of performance data
        for day in range(30):
            day_time = base_time + timedelta(days=day)

            # Generate multiple measurements per day
            for hour in range(0, 24, 4):  # Every 4 hours
                timestamp = day_time + timedelta(hours=hour)

                # Simulate query generation performance
                query_time = 0.01 + (day * 0.0001)  # Gradually degrading
                performance_tracker.record_metric("daily_query_time", query_time, timestamp)

                # Simulate memory usage
                memory_usage = 20 + (day * 0.5)  # Gradually increasing
                performance_tracker.record_metric("daily_memory_usage", memory_usage, timestamp)

        # Analyze trends over different time periods
        weekly_trend = performance_tracker.get_trend("daily_query_time", days=7)
        monthly_trend = performance_tracker.get_trend("daily_query_time", days=30)

        # Monthly trend should show more data points
        assert len(monthly_trend["recent_values"]) > len(weekly_trend["recent_values"])

        # Both should show degrading trend
        assert monthly_trend["trend"] == "degrading"

        # Memory usage should also show degrading trend
        memory_trend = performance_tracker.get_trend("daily_memory_usage", days=30)
        assert memory_trend["trend"] == "degrading"

    def test_benchmark_comparison(self, benchmark_instance, performance_tracker) -> None:
        """Test benchmark comparison functionality."""
        # Run current benchmark
        current_results = {}

        # Query generation benchmark
        query_times = []
        for i in range(10):
            start_time = time.time()
            benchmark_instance.get_query((i % 20) + 1)
            query_time = time.time() - start_time
            query_times.append(query_time)

        current_results["avg_query_time"] = statistics.mean(query_times)
        current_results["max_query_time"] = max(query_times)

        # Parameterized query benchmark
        param_times = []
        for i in range(10):
            start_time = time.time()
            benchmark_instance.get_query((i % 20) + 1)
            param_time = time.time() - start_time
            param_times.append(param_time)

        current_results["avg_param_time"] = statistics.mean(param_times)
        current_results["max_param_time"] = max(param_times)

        # Store current results
        for metric, value in current_results.items():
            performance_tracker.record_metric(f"benchmark_{metric}", value)

        # Compare with historical data (simulate historical baseline)
        # Use realistic baseline values that reflect actual query generation performance
        historical_baseline = {
            "avg_query_time": 0.1,  # Realistic baseline for query generation
            "max_query_time": 0.2,  # Realistic baseline for max query time
            "avg_param_time": 0.1,  # Realistic baseline for parameterized queries
            "max_param_time": 0.2,  # Realistic baseline for max parameterized query time
        }

        comparison_results = {}
        for metric, current_value in current_results.items():
            baseline_value = historical_baseline[metric]
            performance_ratio = current_value / baseline_value

            if performance_ratio > 1.2:
                comparison_results[metric] = "degraded"
            elif performance_ratio < 0.8:
                comparison_results[metric] = "improved"
            else:
                comparison_results[metric] = "stable"

        # Most metrics should be stable or better
        degraded_metrics = [k for k, v in comparison_results.items() if v == "degraded"]
        assert len(degraded_metrics) <= 1, f"Too many degraded metrics: {degraded_metrics}"

    def test_resource_utilization_monitoring(self, benchmark_instance, performance_tracker) -> None:
        """Test resource utilization monitoring."""
        import os

        import psutil

        process = psutil.Process(os.getpid())

        # Monitor resource usage during benchmark
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        process.cpu_percent()

        resource_measurements = []

        # Run benchmark with resource monitoring
        for i in range(20):
            start_time = time.time()

            # Generate query
            benchmark_instance.get_query((i % 30) + 1)

            # Measure resources
            current_memory = process.memory_info().rss / 1024 / 1024  # MB
            current_cpu = process.cpu_percent()
            query_time = time.time() - start_time

            measurement = {
                "iteration": i,
                "memory_mb": current_memory,
                "memory_increase": current_memory - initial_memory,
                "cpu_percent": current_cpu,
                "query_time": query_time,
            }

            resource_measurements.append(measurement)

            # Record metrics
            performance_tracker.record_metric("resource_memory_usage", current_memory)
            performance_tracker.record_metric("resource_cpu_usage", current_cpu)
            performance_tracker.record_metric("resource_query_time", query_time)

        # Analyze resource usage patterns
        memory_increases = [m["memory_increase"] for m in resource_measurements]
        cpu_usages = [m["cpu_percent"] for m in resource_measurements if m["cpu_percent"] > 0]
        query_times = [m["query_time"] for m in resource_measurements]

        # Memory usage should be reasonable
        max_memory_increase = max(memory_increases)
        assert max_memory_increase < 100, f"Memory usage too high: {max_memory_increase:.2f}MB"

        # CPU usage should be reasonable
        # Note: In parallel test environments, CPU usage can be higher due to concurrent test workers
        if cpu_usages:
            avg_cpu = statistics.mean(cpu_usages)
            assert avg_cpu < 100, f"CPU usage too high: {avg_cpu:.2f}%"

        # Query times should be consistent
        if len(query_times) > 1:
            query_time_std = statistics.stdev(query_times)
            query_time_mean = statistics.mean(query_times)
            cv = query_time_std / query_time_mean if query_time_mean > 0 else 0
            assert cv < 2.0, f"Query time too inconsistent: CV={cv:.2f}"

    def test_performance_regression_alerting(self, benchmark_instance, performance_tracker) -> None:
        """Test performance regression alerting."""
        # Establish baseline performance
        baseline_measurements = []
        for i in range(10):
            start_time = time.time()
            benchmark_instance.get_query((i % 10) + 1)
            query_time = time.time() - start_time
            baseline_measurements.append(query_time)

            performance_tracker.record_metric("regression_baseline", query_time)

        baseline_avg = statistics.mean(baseline_measurements)

        # Simulate performance regression
        regression_measurements = []
        for i in range(10):
            # Simulate 50% performance degradation
            degraded_time = baseline_avg * 1.5
            regression_measurements.append(degraded_time)

            performance_tracker.record_metric("regression_current", degraded_time)

        # Check for regression
        baseline_trend = performance_tracker.get_trend("regression_baseline")
        current_trend = performance_tracker.get_trend("regression_current")

        regression_ratio = current_trend["average"] / baseline_trend["average"]

        # Should detect significant regression
        assert regression_ratio > 1.3, f"Failed to detect regression: {regression_ratio:.2f}"

        # Generate regression alert
        regression_alert = {
            "type": "performance_regression",
            "metric": "query_generation_time",
            "baseline_avg": baseline_trend["average"],
            "current_avg": current_trend["average"],
            "regression_ratio": regression_ratio,
            "severity": "high" if regression_ratio > 2.0 else "medium",
        }

        # Verify alert properties
        assert regression_alert["severity"] in ["medium", "high"]
        assert regression_alert["regression_ratio"] > 1.0
        assert regression_alert["current_avg"] > regression_alert["baseline_avg"]

    def test_performance_dashboard_data(self, benchmark_instance, performance_tracker) -> None:
        """Test data preparation for performance dashboard."""
        # Generate comprehensive performance data
        dashboard_data = {
            "summary": {},
            "trends": {},
            "alerts": [],
            "resource_usage": {},
            "historical_data": {},
        }

        # Run benchmarks and collect data
        metrics = ["query_time", "param_time", "memory_usage", "cpu_usage"]

        for metric in metrics:
            # Generate sample data
            values = []
            for i in range(24):  # 24 hours of data
                if metric == "query_time":
                    value = 0.01 + (i * 0.0001)
                elif metric == "param_time":
                    value = 0.02 + (i * 0.0002)
                elif metric == "memory_usage":
                    value = 25 + (i * 0.5)
                else:  # cpu_usage
                    value = 10 + (i * 0.3)

                values.append(value)
                timestamp = datetime.now(timezone.utc) - timedelta(hours=24 - i)
                performance_tracker.record_metric(metric, value, timestamp)

            # Calculate summary statistics
            dashboard_data["summary"][metric] = {
                "current": values[-1],
                "average": statistics.mean(values),
                "min": min(values),
                "max": max(values),
                "std_dev": statistics.stdev(values),
            }

            # trend analysis
            trend = performance_tracker.get_trend(metric, days=1)
            dashboard_data["trends"][metric] = trend

            # Check for alerts
            if metric == "query_time" and values[-1] > 0.02:
                dashboard_data["alerts"].append(
                    {
                        "metric": metric,
                        "level": "warning",
                        "message": f"Query time elevated: {values[-1]:.6f}s",
                    }
                )
            elif metric == "memory_usage" and values[-1] > 35:
                dashboard_data["alerts"].append(
                    {
                        "metric": metric,
                        "level": "warning",
                        "message": f"Memory usage elevated: {values[-1]:.2f}MB",
                    }
                )

        # Verify dashboard data structure
        assert "summary" in dashboard_data
        assert "trends" in dashboard_data
        assert "alerts" in dashboard_data

        # Verify summary data
        for metric in metrics:
            assert metric in dashboard_data["summary"]
            assert "current" in dashboard_data["summary"][metric]
            assert "average" in dashboard_data["summary"][metric]
            assert dashboard_data["summary"][metric]["current"] > 0
            assert dashboard_data["summary"][metric]["average"] > 0

        # Verify trend data
        for metric in metrics:
            assert metric in dashboard_data["trends"]
            assert "trend" in dashboard_data["trends"][metric]
            assert dashboard_data["trends"][metric]["trend"] in [
                "improving",
                "stable",
                "degrading",
                "unknown",
            ]

        # Should have some alerts based on our test data
        assert len(dashboard_data["alerts"]) > 0

        print(
            f"Dashboard data collected: {len(dashboard_data['summary'])} metrics, {len(dashboard_data['alerts'])} alerts"
        )
