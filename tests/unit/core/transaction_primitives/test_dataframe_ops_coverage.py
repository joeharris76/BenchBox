from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchbox.core.dataframe.maintenance_interface import TransactionIsolation
from benchbox.core.transaction_primitives.dataframe_operations import (
    DataFrameTransactionCapabilities,
    DataFrameTransactionOperationsManager,
    TransactionOperationType,
)

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class FakeMaintenanceOps:
    def __init__(self, capabilities):
        self._capabilities = capabilities

    def get_capabilities(self):
        return self._capabilities

    def insert_rows(self, **kwargs):
        return SimpleNamespace(success=True, duration=0.05, rows_affected=3, error_message=None)

    def update_rows(self, **kwargs):
        return SimpleNamespace(success=True, duration=0.01, rows_affected=2, error_message=None)


def test_build_capabilities_from_maintenance(monkeypatch):
    caps = SimpleNamespace(
        supports_transactions=True, supports_time_travel=True, transaction_isolation=TransactionIsolation.SNAPSHOT
    )
    monkeypatch.setattr(
        "benchbox.core.transaction_primitives.dataframe_operations.get_maintenance_operations_for_platform",
        lambda _name: FakeMaintenanceOps(caps),
    )

    manager = DataFrameTransactionOperationsManager("custom-df")

    built = manager.get_capabilities()
    assert built.supports_transactions is True
    assert built.supports_rollback is True
    assert built.supports_time_travel is True


def test_validate_table_format_detects_iceberg(tmp_path):
    table = tmp_path / "iceberg_tbl"
    metadata = table / "metadata"
    metadata.mkdir(parents=True)
    (metadata / "version-hint.text").write_text("1")

    manager = DataFrameTransactionOperationsManager("iceberg")
    is_valid, message = manager.validate_table_format(table)

    assert is_valid is True
    assert message == ""


def test_execute_atomic_insert_success(monkeypatch, tmp_path):
    manager = DataFrameTransactionOperationsManager("delta-lake")
    manager._maintenance_ops = FakeMaintenanceOps(SimpleNamespace())

    table = tmp_path / "delta_table"
    (table / "_delta_log").mkdir(parents=True)

    versions = iter([1, 2])
    monkeypatch.setattr(manager, "get_table_version", lambda _path: next(versions))

    result = manager.execute_atomic_insert(table_path=table, dataframe=object())

    assert result.success is True
    assert result.rows_affected == 3
    assert result.version_before == 1
    assert result.version_after == 2
    assert result.operation_type == TransactionOperationType.ATOMIC_INSERT


def test_execute_atomic_update_fails_without_maintenance_ops(tmp_path):
    manager = DataFrameTransactionOperationsManager("delta-lake")
    manager._maintenance_ops = None

    table = tmp_path / "delta_table"
    (table / "_delta_log").mkdir(parents=True)

    result = manager.execute_atomic_update(table_path=table, condition="id=1", updates={"x": 2})

    assert result.success is False
    assert "Maintenance operations not available" in (result.error_message or "")


def test_execute_time_travel_requires_selector(tmp_path):
    manager = DataFrameTransactionOperationsManager("delta-lake")
    result = manager.execute_time_travel_query(table_path=tmp_path, version=None, timestamp=None)
    assert result.success is False
    assert "Either version or timestamp" in (result.error_message or "")


def test_execute_version_compare_not_implemented_branch(monkeypatch, tmp_path):
    manager = DataFrameTransactionOperationsManager("delta-lake")
    manager._capabilities = DataFrameTransactionCapabilities(
        platform_name="delta-lake",
        supports_transactions=True,
        supports_rollback=True,
        supports_time_travel=True,
        supports_concurrent_writes=True,
        transaction_isolation=TransactionIsolation.SNAPSHOT,
        table_format="other",
    )

    table = tmp_path / "table"
    (table / "_delta_log").mkdir(parents=True)

    result = manager.execute_version_compare(table_path=table, version1=1, version2=2)

    assert result.success is False
    assert "not implemented for table format" in (result.error_message or "")
