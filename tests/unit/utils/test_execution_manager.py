"""Tests for execution manager safety features (timeouts and resource limits)."""

import time
from unittest.mock import MagicMock

import pytest

from benchbox.utils.execution_manager import PowerRunExecutor
from benchbox.utils.resource_limits import ResourceUsageSummary

pytestmark = pytest.mark.fast


class _SlowPowerTest:
    def __init__(self, sleep_seconds: float):
        self.sleep_seconds = sleep_seconds

    def run(self):
        time.sleep(self.sleep_seconds)
        return MagicMock(power_at_size=0.0, query_results={})


def _short_config_manager():
    """Return a config manager that supplies minimal execution settings."""
    config_values = {
        "execution.power_run.iterations": 1,
        "execution.power_run.warm_up_iterations": 0,
        "execution.power_run.timeout_per_iteration_minutes": 0.001,  # ~0.06s
        "execution.power_run.fail_fast": False,
        "execution.power_run.collect_metrics": False,
    }
    mgr = MagicMock()
    mgr.get.side_effect = lambda key, default=None: config_values.get(key, default)
    return mgr


def test_power_run_iteration_times_out_quickly():
    """Power run iterations should return promptly when exceeding timeout."""
    executor = PowerRunExecutor(config_manager=_short_config_manager())
    iteration_result = executor._execute_single_iteration(_SlowPowerTest(0.2), iteration_id=1, is_warm_up=False)

    assert iteration_result.timed_out is True
    assert iteration_result.success is False
    assert "timed out" in (iteration_result.error or "").lower()
    assert iteration_result.duration < 0.2  # Should not wait for full sleep


def test_resource_limit_flag_aborts_power_run(monkeypatch):
    """Resource limit breaches should stop execution and flag result."""

    class FakeMonitor:
        def __init__(self, *args, **kwargs):
            self.limit_exceeded = True

        def start(self):
            return None

        def stop(self):
            return ResourceUsageSummary(
                peak_memory_mb=0.0,
                average_memory_mb=0.0,
                peak_cpu_percent=0.0,
                average_cpu_percent=0.0,
                warnings=[],
                limit_exceeded=self.limit_exceeded,
                degradation_triggered=False,
            )

        def get_current_usage(self):
            return {"memory_mb": 5000.0}

    monkeypatch.setattr("benchbox.utils.execution_manager.ResourceLimitMonitor", FakeMonitor)

    config_manager = _short_config_manager()
    executor = PowerRunExecutor(config_manager=config_manager)

    result = executor.execute_power_runs(lambda stream_id=None: MagicMock(run=lambda: MagicMock(power_at_size=1.0)))

    assert result.resource_limit_exceeded is True
    assert result.success is False
    assert any("Resource limit exceeded" in err for err in result.errors)
