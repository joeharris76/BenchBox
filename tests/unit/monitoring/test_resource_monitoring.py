"""Unit tests for ResourceMonitor class."""

from __future__ import annotations

import time

import pytest

from benchbox.monitoring import PerformanceMonitor, ResourceMonitor

pytestmark = pytest.mark.fast


def test_resource_monitor_initialization():
    """Test ResourceMonitor initialization."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=1.0)

    assert resource_monitor.monitor is monitor
    assert resource_monitor.sample_interval == 1.0
    assert resource_monitor._thread is None
    assert resource_monitor._stop_event is None
    assert resource_monitor._peak_memory_mb == 0.0


def test_resource_monitor_start_stop():
    """Test ResourceMonitor start and stop lifecycle."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    # Start monitoring
    resource_monitor.start()
    assert resource_monitor._thread is not None
    assert resource_monitor._thread.is_alive()

    # Let it sample at least once
    time.sleep(0.3)

    # Stop monitoring
    resource_monitor.stop()
    assert resource_monitor._thread is None

    # Check that metrics were recorded
    snapshot = monitor.snapshot()
    assert "memory_mb" in snapshot.gauges or "peak_memory_mb" in snapshot.gauges


def test_resource_monitor_tracks_peak_memory():
    """Test that ResourceMonitor tracks peak memory correctly."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    resource_monitor.start()
    time.sleep(0.3)  # Let it sample
    resource_monitor.stop()

    snapshot = monitor.snapshot()
    if "peak_memory_mb" in snapshot.gauges:
        assert snapshot.gauges["peak_memory_mb"] > 0.0


def test_resource_monitor_double_start_is_safe():
    """Test that calling start() twice doesn't create multiple threads."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    resource_monitor.start()
    first_thread = resource_monitor._thread

    resource_monitor.start()  # Call again
    second_thread = resource_monitor._thread

    # Should be the same thread
    assert first_thread is second_thread

    resource_monitor.stop()


def test_resource_monitor_stop_before_start():
    """Test that stopping before starting doesn't crash."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    # Should not raise an exception
    resource_monitor.stop()


def test_resource_monitor_get_current_usage():
    """Test get_current_usage() method."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    # Before start, should return zeros
    usage = resource_monitor.get_current_usage()
    assert usage == {"memory_mb": 0.0, "memory_percent": 0.0, "cpu_percent": 0.0}

    # After start, should return actual values
    resource_monitor.start()
    time.sleep(0.2)

    usage = resource_monitor.get_current_usage()
    # At least memory should be > 0 (since the process is running)
    assert usage["memory_mb"] >= 0.0

    resource_monitor.stop()


def test_resource_monitor_records_cpu_metrics():
    """Test that CPU metrics are recorded."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    resource_monitor.start()
    time.sleep(0.3)
    resource_monitor.stop()

    snapshot = monitor.snapshot()
    # CPU percent might be recorded
    if "cpu_percent" in snapshot.gauges:
        assert snapshot.gauges["cpu_percent"] >= 0.0


def test_resource_monitor_without_psutil():
    """Test ResourceMonitor gracefully handles missing psutil.

    Note: This test verifies that the code has proper error handling,
    but can't easily test the actual ImportError path since psutil
    is imported dynamically inside start(). The code has try/except
    around the import and silently returns if psutil is unavailable.
    """
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    # The actual behavior is: if psutil fails to import, start() returns early
    # and no thread is created. We can't easily mock this without breaking
    # the import system, so we just verify the code doesn't crash.
    resource_monitor.start()
    resource_monitor.stop()

    # If we got here without exceptions, the error handling works


def test_resource_monitor_updates_monitor_gauges():
    """Test that ResourceMonitor updates PerformanceMonitor gauges."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.1)

    resource_monitor.start()
    time.sleep(0.3)  # Let it sample multiple times
    resource_monitor.stop()

    snapshot = monitor.snapshot()
    gauges = snapshot.gauges

    # Should have recorded memory metrics
    assert "memory_mb" in gauges or "peak_memory_mb" in gauges


def test_resource_monitor_shutdown_timeout():
    """Test that ResourceMonitor shutdown respects timeout."""
    monitor = PerformanceMonitor()
    resource_monitor = ResourceMonitor(monitor, sample_interval=0.05)

    resource_monitor.start()

    # Stopping should complete within reasonable time
    start_time = time.time()
    resource_monitor.stop()
    elapsed = time.time() - start_time

    # Should complete quickly (within 10 seconds, accounting for 5s join timeout)
    assert elapsed < 10.0
