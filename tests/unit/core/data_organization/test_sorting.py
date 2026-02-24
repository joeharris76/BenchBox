"""Tests for data_organization config and sorting module."""

from __future__ import annotations

import pytest

from benchbox.core.data_organization.config import (
    DataOrganizationConfig,
    SortColumn,
    SortOrder,
)

# ---------------------------------------------------------------------------
# SortOrder
# ---------------------------------------------------------------------------


class TestSortOrder:
    def test_from_string_asc(self):
        assert SortOrder.from_string("asc") == SortOrder.ASC
        assert SortOrder.from_string("ASC") == SortOrder.ASC

    def test_from_string_desc(self):
        assert SortOrder.from_string("desc") == SortOrder.DESC

    def test_from_string_invalid(self):
        with pytest.raises(ValueError, match="Invalid sort order"):
            SortOrder.from_string("random")


# ---------------------------------------------------------------------------
# SortColumn
# ---------------------------------------------------------------------------


class TestSortColumn:
    def test_defaults_to_asc(self):
        sc = SortColumn(name="l_shipdate")
        assert sc.order == SortOrder.ASC

    def test_empty_name_rejected(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            SortColumn(name="")

    def test_roundtrip_dict(self):
        sc = SortColumn(name="o_orderdate", order=SortOrder.DESC)
        d = sc.to_dict()
        assert d == {"name": "o_orderdate", "order": "desc"}
        restored = SortColumn.from_dict(d)
        assert restored == sc

    def test_from_dict_missing_name(self):
        with pytest.raises(ValueError, match="requires 'name'"):
            SortColumn.from_dict({"order": "asc"})

    def test_frozen(self):
        sc = SortColumn(name="x")
        with pytest.raises(AttributeError):
            sc.name = "y"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DataOrganizationConfig
# ---------------------------------------------------------------------------


class TestDataOrganizationConfig:
    def test_defaults(self):
        cfg = DataOrganizationConfig()
        assert cfg.sort_columns == []
        assert cfg.row_group_size == 128 * 1024 * 1024
        assert cfg.compression == "zstd"
        assert not cfg.has_sorting

    def test_has_sorting_global(self):
        cfg = DataOrganizationConfig(sort_columns=[SortColumn(name="x")])
        assert cfg.has_sorting

    def test_has_sorting_table_override(self):
        cfg = DataOrganizationConfig(table_configs={"lineitem": [SortColumn(name="l_shipdate")]})
        assert cfg.has_sorting

    def test_table_override_takes_priority(self):
        global_cols = [SortColumn(name="x")]
        override_cols = [SortColumn(name="y")]
        cfg = DataOrganizationConfig(sort_columns=global_cols, table_configs={"lineitem": override_cols})
        assert cfg.get_sort_columns_for_table("lineitem") == override_cols
        assert cfg.get_sort_columns_for_table("orders") == global_cols

    def test_invalid_row_group_size(self):
        with pytest.raises(ValueError, match="row_group_size must be positive"):
            DataOrganizationConfig(row_group_size=0)

    def test_invalid_compression(self):
        with pytest.raises(ValueError, match="Invalid compression"):
            DataOrganizationConfig(compression="brotli")

    def test_roundtrip_dict(self):
        cfg = DataOrganizationConfig(
            sort_columns=[SortColumn("a"), SortColumn("b", SortOrder.DESC)],
            row_group_size=64 * 1024 * 1024,
            compression="snappy",
            table_configs={"lineitem": [SortColumn("l_shipdate")]},
        )
        d = cfg.to_dict()
        restored = DataOrganizationConfig.from_dict(d)
        assert restored.sort_columns == cfg.sort_columns
        assert restored.row_group_size == cfg.row_group_size
        assert restored.compression == cfg.compression
        assert restored.table_configs == cfg.table_configs

    def test_from_dict_defaults(self):
        cfg = DataOrganizationConfig.from_dict({})
        assert cfg.sort_columns == []
        assert cfg.compression == "zstd"
