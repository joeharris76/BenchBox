from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchbox.utils.format_converters.base import ConversionError, ConversionOptions
from benchbox.utils.format_converters.vortex_converter import VortexConverter

pytestmark = pytest.mark.fast


def test_get_vortex_module_raises_helpful_error(monkeypatch):
    converter = VortexConverter()

    def _raise(*_args, **_kwargs):
        raise ImportError("missing vortex")

    monkeypatch.setattr("builtins.__import__", _raise)

    with pytest.raises(ConversionError, match="requires the 'vortex' package"):
        converter._get_vortex_module()


def test_convert_success_path_writes_output_and_metadata(monkeypatch, tmp_path):
    converter = VortexConverter()

    source_file = tmp_path / "lineitem.tbl"
    source_file.write_text("1|a\n2|b\n")
    schema = {"columns": [{"name": "id", "type": "INTEGER"}]}

    class _CombinedTable:
        num_rows = 2

    def _write(_array, path):
        Path(path).write_bytes(b"vortex-data")

    fake_vortex = SimpleNamespace(
        encoding=SimpleNamespace(array=lambda table: table),
        io=SimpleNamespace(write=_write),
    )

    monkeypatch.setattr(converter, "_get_vortex_module", lambda: fake_vortex)
    monkeypatch.setattr(converter, "validate_source_files", lambda _files: None)
    monkeypatch.setattr(converter, "validate_schema", lambda _schema: True)
    monkeypatch.setattr(converter, "_build_arrow_schema", lambda _schema: None)
    monkeypatch.setattr(converter, "read_tbl_files", lambda *_args, **_kwargs: _CombinedTable())
    monkeypatch.setattr(converter, "calculate_file_size", lambda _files: 100)
    monkeypatch.setattr(converter, "_detect_source_format", lambda _files: "tbl")
    monkeypatch.setattr(converter, "get_current_timestamp", lambda: "2026-02-08T20:45:00Z")

    result = converter.convert(
        source_files=[source_file],
        table_name="lineitem",
        schema=schema,
        options=ConversionOptions(validate_row_count=False),
    )

    assert result.row_count == 2
    assert result.metadata["compression"] == "snappy"
    assert result.output_files[0].name == "lineitem.vortex"
    assert result.output_files[0].exists()


def test_convert_cleans_partial_file_on_write_error(monkeypatch, tmp_path):
    converter = VortexConverter()

    source_file = tmp_path / "orders.tbl"
    source_file.write_text("1|x\n")
    schema = {"columns": [{"name": "id", "type": "INTEGER"}]}

    class _CombinedTable:
        num_rows = 1

    def _write(_array, path):
        Path(path).write_bytes(b"partial")
        raise RuntimeError("write failed")

    fake_vortex = SimpleNamespace(
        encoding=SimpleNamespace(array=lambda table: table),
        io=SimpleNamespace(write=_write),
    )

    monkeypatch.setattr(converter, "_get_vortex_module", lambda: fake_vortex)
    monkeypatch.setattr(converter, "validate_source_files", lambda _files: None)
    monkeypatch.setattr(converter, "validate_schema", lambda _schema: True)
    monkeypatch.setattr(converter, "_build_arrow_schema", lambda _schema: None)
    monkeypatch.setattr(converter, "read_tbl_files", lambda *_args, **_kwargs: _CombinedTable())

    with pytest.raises(ConversionError, match="Failed to write Vortex file"):
        converter.convert([source_file], "orders", schema, options=ConversionOptions(validate_row_count=False))

    assert not (tmp_path / "orders.vortex").exists()
