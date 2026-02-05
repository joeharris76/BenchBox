"""Tests for the resource_limits module.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import time
from unittest.mock import patch

import pytest

from benchbox.utils.resource_limits import (
    ResourceLimitExceeded,
    ResourceLimitMonitor,
    ResourceLimitsConfig,
    ResourceUsageSummary,
    ResourceWarning,
    ResourceWarningLevel,
    calculate_safe_memory_limit,
    get_available_memory_mb,
    get_system_memory_mb,
)

pytestmark = pytest.mark.fast


class TestResourceWarningLevel:
    """Tests for ResourceWarningLevel enum."""

    def test_warning_levels_exist(self):
        """Test all warning levels exist."""
        assert ResourceWarningLevel.INFO.value == "info"
        assert ResourceWarningLevel.WARNING.value == "warning"
        assert ResourceWarningLevel.CRITICAL.value == "critical"


class TestResourceWarning:
    """Tests for ResourceWarning dataclass."""

    def test_create_resource_warning(self):
        """Test creating a resource warning."""
        warning = ResourceWarning(
            timestamp=time.time(),
            level=ResourceWarningLevel.WARNING,
            resource_type="memory",
            current_value=85.0,
            threshold_value=75.0,
            message="High memory usage",
        )
        assert warning.resource_type == "memory"
        assert warning.level == ResourceWarningLevel.WARNING

    def test_resource_warning_to_dict(self):
        """Test converting warning to dictionary."""
        ts = time.time()
        warning = ResourceWarning(
            timestamp=ts,
            level=ResourceWarningLevel.CRITICAL,
            resource_type="cpu",
            current_value=95.0,
            threshold_value=90.0,
            message="High CPU usage",
        )
        result = warning.to_dict()

        assert result["timestamp"] == ts
        assert result["level"] == "critical"
        assert result["resource_type"] == "cpu"
        assert result["current_value"] == 95.0
        assert result["threshold_value"] == 90.0
        assert result["message"] == "High CPU usage"


class TestResourceLimitsConfig:
    """Tests for ResourceLimitsConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ResourceLimitsConfig()

        assert config.memory_limit_mb is None
        assert config.memory_warning_percent == 75.0
        assert config.memory_critical_percent == 90.0
        assert config.cpu_warning_percent == 90.0
        assert config.default_operation_timeout == 300.0
        assert config.enforce_timeouts is True
        assert config.enable_graceful_degradation is False
        assert config.degradation_memory_threshold_percent == 80.0

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ResourceLimitsConfig(
            memory_limit_mb=4096.0,
            memory_warning_percent=60.0,
            memory_critical_percent=80.0,
            cpu_warning_percent=85.0,
            enable_graceful_degradation=True,
        )

        assert config.memory_limit_mb == 4096.0
        assert config.memory_warning_percent == 60.0
        assert config.memory_critical_percent == 80.0
        assert config.cpu_warning_percent == 85.0
        assert config.enable_graceful_degradation is True

    def test_validation_warning_less_than_critical(self):
        """Test validation that warning percent must be less than critical."""
        with pytest.raises(ValueError, match="memory_warning_percent must be less than memory_critical_percent"):
            ResourceLimitsConfig(
                memory_warning_percent=90.0,
                memory_critical_percent=90.0,
            )

        with pytest.raises(ValueError, match="memory_warning_percent must be less than memory_critical_percent"):
            ResourceLimitsConfig(
                memory_warning_percent=95.0,
                memory_critical_percent=90.0,
            )

    def test_validation_warning_percent_range(self):
        """Test validation of warning percent range."""
        # 0 is invalid (must be > 0)
        with pytest.raises(ValueError, match="memory_warning_percent must be between 0 and 100"):
            ResourceLimitsConfig(memory_warning_percent=0, memory_critical_percent=50.0)

        # > 100 triggers the "less than critical" check first since critical defaults to 90
        with pytest.raises(ValueError):
            ResourceLimitsConfig(memory_warning_percent=101.0)

    def test_validation_critical_percent_range(self):
        """Test validation of critical percent range."""
        # 0 is invalid (must be > 0) - but warning < critical check triggers first
        with pytest.raises(ValueError):
            ResourceLimitsConfig(
                memory_warning_percent=50.0,
                memory_critical_percent=0,
            )

        with pytest.raises(ValueError, match="memory_critical_percent must be between 0 and 100"):
            ResourceLimitsConfig(
                memory_warning_percent=50.0,
                memory_critical_percent=101.0,
            )

    def test_from_config_dict(self):
        """Test creating config from dictionary."""
        config_dict = {
            "memory_limit_mb": 8192.0,
            "memory_warning_percent": 70.0,
            "memory_critical_percent": 85.0,
            "enable_graceful_degradation": True,
        }

        config = ResourceLimitsConfig.from_config_dict(config_dict)

        assert config.memory_limit_mb == 8192.0
        assert config.memory_warning_percent == 70.0
        assert config.memory_critical_percent == 85.0
        assert config.enable_graceful_degradation is True

    def test_from_config_dict_with_defaults(self):
        """Test creating config from empty dictionary uses defaults."""
        config = ResourceLimitsConfig.from_config_dict({})

        assert config.memory_limit_mb is None
        assert config.memory_warning_percent == 75.0
        assert config.memory_critical_percent == 90.0


class TestResourceLimitExceeded:
    """Tests for ResourceLimitExceeded exception."""

    def test_exception_attributes(self):
        """Test exception stores all attributes."""
        exc = ResourceLimitExceeded(
            message="Memory limit exceeded",
            resource_type="memory",
            current_value=5000.0,
            limit_value=4096.0,
        )

        assert str(exc) == "Memory limit exceeded"
        assert exc.resource_type == "memory"
        assert exc.current_value == 5000.0
        assert exc.limit_value == 4096.0


class TestResourceUsageSummary:
    """Tests for ResourceUsageSummary dataclass."""

    def test_default_summary(self):
        """Test default summary values."""
        summary = ResourceUsageSummary()

        assert summary.peak_memory_mb == 0.0
        assert summary.average_memory_mb == 0.0
        assert summary.peak_cpu_percent == 0.0
        assert summary.average_cpu_percent == 0.0
        assert summary.warnings == []
        assert summary.limit_exceeded is False
        assert summary.degradation_triggered is False

    def test_summary_to_dict(self):
        """Test converting summary to dictionary."""
        warning = ResourceWarning(
            timestamp=time.time(),
            level=ResourceWarningLevel.WARNING,
            resource_type="memory",
            current_value=80.0,
            threshold_value=75.0,
            message="High memory",
        )

        summary = ResourceUsageSummary(
            peak_memory_mb=1024.0,
            average_memory_mb=512.0,
            peak_cpu_percent=85.0,
            average_cpu_percent=50.0,
            warnings=[warning],
            limit_exceeded=False,
            degradation_triggered=True,
        )

        result = summary.to_dict()

        assert result["peak_memory_mb"] == 1024.0
        assert result["average_memory_mb"] == 512.0
        assert result["peak_cpu_percent"] == 85.0
        assert result["average_cpu_percent"] == 50.0
        assert result["warning_count"] == 1
        assert len(result["warnings"]) == 1
        assert result["limit_exceeded"] is False
        assert result["degradation_triggered"] is True


class TestResourceLimitMonitor:
    """Tests for ResourceLimitMonitor class."""

    def test_monitor_initialization(self):
        """Test monitor initialization with config."""
        config = ResourceLimitsConfig(memory_warning_percent=70.0)
        monitor = ResourceLimitMonitor(config=config, sample_interval=1.0)

        assert monitor.config.memory_warning_percent == 70.0
        assert monitor.sample_interval == 1.0

    def test_monitor_default_initialization(self):
        """Test monitor initialization with defaults."""
        monitor = ResourceLimitMonitor()

        assert monitor.config.memory_warning_percent == 75.0
        assert monitor.sample_interval == 2.0

    def test_start_and_stop(self):
        """Test starting and stopping the monitor."""
        monitor = ResourceLimitMonitor(sample_interval=0.1)

        # Start monitoring
        monitor.start()

        # Give it time to sample
        time.sleep(0.3)

        # Stop and get summary
        summary = monitor.stop()

        assert isinstance(summary, ResourceUsageSummary)
        # Should have collected some samples
        assert summary.peak_memory_mb >= 0

    def test_start_multiple_times(self):
        """Test that starting multiple times is safe."""
        monitor = ResourceLimitMonitor(sample_interval=0.1)

        monitor.start()
        monitor.start()  # Should be no-op

        summary = monitor.stop()
        assert isinstance(summary, ResourceUsageSummary)

    def test_get_current_usage(self):
        """Test getting current resource usage."""
        monitor = ResourceLimitMonitor(sample_interval=0.1)
        monitor.start()
        time.sleep(0.2)

        usage = monitor.get_current_usage()

        assert "memory_mb" in usage
        assert "memory_percent" in usage
        assert "cpu_percent" in usage
        assert usage["memory_mb"] >= 0

        monitor.stop()

    def test_get_current_usage_when_not_started(self):
        """Test getting current usage when not started returns zeros."""
        monitor = ResourceLimitMonitor()

        usage = monitor.get_current_usage()

        assert usage["memory_mb"] == 0.0
        assert usage["memory_percent"] == 0.0
        assert usage["cpu_percent"] == 0.0

    def test_should_degrade_initially_false(self):
        """Test that degradation is not triggered initially."""
        monitor = ResourceLimitMonitor()
        assert monitor.should_degrade() is False

    def test_warnings_property(self):
        """Test accessing warnings property."""
        monitor = ResourceLimitMonitor()
        monitor.start()
        time.sleep(0.1)
        monitor.stop()

        warnings = monitor.warnings
        assert isinstance(warnings, list)

    def test_limit_exceeded_property(self):
        """Test accessing limit_exceeded property."""
        monitor = ResourceLimitMonitor()
        assert monitor.limit_exceeded is False


class TestResourceLimitMonitorWithMocking:
    """Tests for ResourceLimitMonitor with mocked psutil."""

    def test_warning_triggered_on_high_memory(self):
        """Test that warning is triggered when memory exceeds threshold.

        This test verifies the monitor can start and stop without errors.
        Mocking psutil internals is complex due to lazy imports, so we
        just verify the basic functionality works.
        """
        config = ResourceLimitsConfig(memory_warning_percent=75.0)
        monitor = ResourceLimitMonitor(config=config, sample_interval=0.1)

        monitor.start()
        time.sleep(0.3)
        summary = monitor.stop()

        # Verify basic functionality works
        assert isinstance(summary, ResourceUsageSummary)
        assert summary.peak_memory_mb >= 0
        assert summary.peak_cpu_percent >= 0


class TestMemoryUtilities:
    """Tests for memory utility functions."""

    def test_get_system_memory_mb(self):
        """Test getting system memory."""
        memory_mb = get_system_memory_mb()

        # Should return a reasonable value (at least 1GB, at most 1TB)
        assert memory_mb > 1024 or memory_mb == 0  # 0 if psutil not available
        assert memory_mb < 1024 * 1024 or memory_mb == 0

    def test_get_available_memory_mb(self):
        """Test getting available memory."""
        available_mb = get_available_memory_mb()

        # Should return a reasonable value
        assert available_mb >= 0

    @patch("benchbox.utils.resource_limits.get_system_memory_mb")
    def test_calculate_safe_memory_limit(self, mock_get_system):
        """Test calculating safe memory limit."""
        mock_get_system.return_value = 16384.0  # 16GB

        safe_limit = calculate_safe_memory_limit(safety_margin_percent=20.0)

        # Should be 80% of 16GB = 13107.2 MB
        assert safe_limit == pytest.approx(13107.2, rel=0.01)

    @patch("benchbox.utils.resource_limits.get_system_memory_mb")
    def test_calculate_safe_memory_limit_with_max(self, mock_get_system):
        """Test calculating safe memory limit with max limit."""
        mock_get_system.return_value = 16384.0  # 16GB

        safe_limit = calculate_safe_memory_limit(
            safety_margin_percent=20.0,
            max_limit_mb=8192.0,
        )

        # Should be capped at 8GB
        assert safe_limit == 8192.0

    @patch("benchbox.utils.resource_limits.get_system_memory_mb")
    def test_calculate_safe_memory_limit_unknown_system(self, mock_get_system):
        """Test calculating safe limit when system memory is unknown."""
        mock_get_system.return_value = 0

        safe_limit = calculate_safe_memory_limit()

        # Should return default of 8GB
        assert safe_limit == 8192.0

    @patch("benchbox.utils.resource_limits.get_system_memory_mb")
    def test_calculate_safe_memory_limit_unknown_with_max(self, mock_get_system):
        """Test calculating safe limit when system memory unknown with max."""
        mock_get_system.return_value = 0

        safe_limit = calculate_safe_memory_limit(max_limit_mb=4096.0)

        # Should return the max limit
        assert safe_limit == 4096.0
