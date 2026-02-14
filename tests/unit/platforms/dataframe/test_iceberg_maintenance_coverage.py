from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from benchbox.platforms.dataframe import iceberg_maintenance as im

pytestmark = pytest.mark.fast


@pytest.mark.skipif(not (im.ICEBERG_AVAILABLE and im.PYARROW_AVAILABLE), reason="PyIceberg/PyArrow not installed")
class TestIcebergMaintenanceCoverage:
    def test_normalize_identifier_and_parse_merge_key(self, tmp_path):
        ops = im.IcebergMaintenanceOperations(working_dir=tmp_path)
        assert ops._normalize_table_identifier("default.orders") == "default.orders"
        assert ops._normalize_table_identifier("/tmp/orders") == "default.orders"
        assert ops._parse_merge_key("target.id = source.id") == "id"
        assert ops._parse_merge_key("orderkey") == "orderkey"

    def test_parse_condition_variants(self, tmp_path):
        ops = im.IcebergMaintenanceOperations(working_dir=tmp_path)
        assert isinstance(ops._parse_condition("id = 10"), im.EqualTo)
        assert isinstance(ops._parse_condition("value >= 3.5"), im.GreaterThanOrEqual)
        assert isinstance(ops._parse_condition("name != 'x'"), im.NotEqualTo)
        assert isinstance(ops._parse_condition("unparseable condition"), im.AlwaysTrue)

    def test_do_delete_nonexistent_table_returns_zero(self, tmp_path):
        ops = im.IcebergMaintenanceOperations(working_dir=tmp_path)
        ops._catalog = MagicMock()
        ops._catalog.load_table.side_effect = RuntimeError("missing")
        assert ops._do_delete("default.missing", "id > 1") == 0

    def test_do_update_zero_matches(self, tmp_path):
        ops = im.IcebergMaintenanceOperations(working_dir=tmp_path)

        table = MagicMock()
        table.scan.return_value.to_arrow.return_value = SimpleNamespace(num_rows=0)

        ops._catalog = MagicMock()
        ops._catalog.load_table.return_value = table

        updated = ops._do_update("default.tbl", "id = 1", {"name": "new"})
        assert updated == 0

    def test_get_maintenance_operations_unavailable_branches(self, monkeypatch):
        monkeypatch.setattr(im, "ICEBERG_AVAILABLE", False)
        assert im.get_iceberg_maintenance_operations() is None

        monkeypatch.setattr(im, "ICEBERG_AVAILABLE", True)
        monkeypatch.setattr(im, "PYARROW_AVAILABLE", False)
        assert im.get_iceberg_maintenance_operations() is None
