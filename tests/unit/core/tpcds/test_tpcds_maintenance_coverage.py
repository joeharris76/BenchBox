from __future__ import annotations

from types import SimpleNamespace

import pytest

from benchbox.core.tpcds.maintenance_operations import MaintenanceOperationType
from benchbox.core.tpcds.maintenance_test import (
    TPCDSMaintenanceOperation,
    TPCDSMaintenanceTest,
    TPCDSMaintenanceTestConfig,
)

pytestmark = pytest.mark.fast


class _Cursor:
    def __init__(self, value):
        self._value = value

    def fetchone(self):
        return (self._value,)


class _Connection:
    def __init__(self, values):
        self._values = list(values)
        self.closed = False

    def execute(self, _sql):
        if not self._values:
            return _Cursor(0)
        return _Cursor(self._values.pop(0))

    def close(self):
        self.closed = True


def test_map_to_maintenance_operation_type_covers_insert_update_delete_paths():
    test = TPCDSMaintenanceTest(benchmark=object(), connection_factory=lambda: _Connection([]))

    assert (
        test._map_to_maintenance_operation_type("INSERT", "store_sales") == MaintenanceOperationType.INSERT_STORE_SALES
    )
    assert test._map_to_maintenance_operation_type("UPDATE", "inventory") == MaintenanceOperationType.UPDATE_INVENTORY
    assert (
        test._map_to_maintenance_operation_type("DELETE", "web_returns") == MaintenanceOperationType.DELETE_OLD_RETURNS
    )
    assert test._map_to_maintenance_operation_type("UNKNOWN", "mystery") == MaintenanceOperationType.BULK_LOAD_SALES


def test_execute_maintenance_operation_success_closes_connection():
    conn = _Connection([])
    test = TPCDSMaintenanceTest(benchmark=object(), connection_factory=lambda: conn)
    test.maintenance_ops = SimpleNamespace(
        initialize=lambda *_args, **_kwargs: None,
        execute_operation=lambda *_args, **_kwargs: SimpleNamespace(rows_affected=7, success=True, error_message=None),
    )

    result = test._execute_maintenance_operation("INSERT", "store_sales", 1)

    assert result.success is True
    assert result.rows_affected == 7
    assert conn.closed is True


def test_run_marks_failure_when_success_rate_below_threshold(monkeypatch):
    test = TPCDSMaintenanceTest(benchmark=object(), connection_factory=lambda: _Connection([]))

    outcomes = [
        TPCDSMaintenanceOperation("INSERT", "catalog_sales", 0.0, 0.0, 0.0, 3, True),
        TPCDSMaintenanceOperation("UPDATE", "web_sales", 0.0, 0.0, 0.0, 0, False, "boom"),
        TPCDSMaintenanceOperation("DELETE", "store_sales", 0.0, 0.0, 0.0, 2, True),
    ]

    def _next(*_args, **_kwargs):
        return outcomes.pop(0)

    monkeypatch.setattr(test, "_execute_maintenance_operation", _next)
    monkeypatch.setattr("benchbox.core.tpcds.maintenance_test.time.sleep", lambda *_args, **_kwargs: None)

    config = TPCDSMaintenanceTestConfig(maintenance_operations=3, operation_interval=0.01)
    result = test.run(config)

    assert result["total_operations"] == 3
    assert result["successful_operations"] == 2
    assert result["failed_operations"] == 1
    assert result["success"] is False
    assert any("failed" in err.lower() for err in result["errors"])


def test_validate_data_integrity_detects_orphans():
    conn = _Connection([2, 0, 0, 0])
    test = TPCDSMaintenanceTest(benchmark=object(), connection_factory=lambda: conn)

    assert test.validate_data_integrity() is False
    assert conn.closed is True


def test_get_maintenance_statistics_has_expected_shape():
    test = TPCDSMaintenanceTest(benchmark=object(), connection_factory=lambda: _Connection([]), scale_factor=2.0)
    stats = test.get_maintenance_statistics()

    assert stats["scale_factor"] == 2.0
    assert stats["estimated_daily_inserts"] == 20000
    assert "maintenance_tables" in stats
