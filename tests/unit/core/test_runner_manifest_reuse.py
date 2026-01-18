"""Tests for manifest-based data reuse in the core runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from benchbox.core.config import BenchmarkConfig
from benchbox.core.runner.runner import _ensure_data_generated

pytestmark = pytest.mark.fast


@pytest.fixture()
def benchmark_config() -> BenchmarkConfig:
    return BenchmarkConfig(
        name="tpcds",
        display_name="TPC-DS",
        scale_factor=0.01,
        compress_data=True,
        compression_type="zstd",
        compression_level=None,
        options={},
    )


def _write_manifest(tmp_path: Path, *, table_names: list[str]) -> dict:
    tables: dict[str, list[dict[str, int | str]]] = {}
    file_count = 0
    for table in table_names:
        file_path = tmp_path / f"{table}.dat"
        file_path.write_text("1|sample\n")
        size = file_path.stat().st_size
        tables[table] = [
            {
                "path": file_path.name,
                "size_bytes": size,
                "row_count": 1,
            }
        ]
        file_count += 1

    manifest = {
        "benchmark": "tpcds",
        "scale_factor": 0.01,
        "compression": {"enabled": True, "type": "zstd", "level": None},
        "parallel": 1,
        "created_at": "2025-01-01T00:00:00Z",
        "generator_version": "test",
        "tables": tables,
    }

    with (tmp_path / "_datagen_manifest.json").open("w") as fh:
        json.dump(manifest, fh)

    return manifest


def test_manifest_reuse_populates_tables_and_logs(tmp_path: Path, benchmark_config: BenchmarkConfig):
    manifest = _write_manifest(tmp_path, table_names=["customer", "orders"])

    messages: list[str] = []

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

        def log_verbose(self, message: str) -> None:
            messages.append(message)

    dummy = DummyBenchmark()

    result = _ensure_data_generated(dummy, benchmark_config)

    assert result is False, "Expected False when reusing manifest"
    dummy.generate_data.assert_not_called()
    assert isinstance(dummy.tables, dict)
    assert set(dummy.tables.keys()) == {"customer", "orders"}
    for path in dummy.tables.values():
        assert isinstance(path, Path)
        assert path.exists()

    assert messages, "Expected reuse log message"
    summary_msg = messages[0]
    assert "Reusing benchmark data" in summary_msg
    assert "2 tables" in summary_msg
    # The manifest has one file per table
    assert "2 files" in summary_msg
    assert manifest["created_at"] in summary_msg


def test_manifest_reuse_prints_when_no_logger(
    tmp_path: Path, benchmark_config: BenchmarkConfig, capsys: pytest.CaptureFixture[str]
):
    _write_manifest(tmp_path, table_names=["lineitem"])

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

    dummy = DummyBenchmark()

    result = _ensure_data_generated(dummy, benchmark_config)

    assert result is False, "Expected False when reusing manifest"
    dummy.generate_data.assert_not_called()
    out = capsys.readouterr().out
    assert "Reusing benchmark data" in out
    assert "1 table" in out


def test_missing_manifest_triggers_generation(tmp_path: Path, benchmark_config: BenchmarkConfig):
    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

    dummy = DummyBenchmark()

    result = _ensure_data_generated(dummy, benchmark_config)

    assert result is True, "Expected True when generating fresh data"
    dummy.generate_data.assert_called_once()


def test_no_regenerate_with_missing_manifest_raises(tmp_path: Path, benchmark_config: BenchmarkConfig):
    benchmark_config.options = {"no_regenerate": True}

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

    dummy = DummyBenchmark()

    with pytest.raises(RuntimeError):
        _ensure_data_generated(dummy, benchmark_config)

    dummy.generate_data.assert_not_called()


def test_force_regenerate_ignores_manifest(tmp_path: Path, benchmark_config: BenchmarkConfig):
    _write_manifest(tmp_path, table_names=["customer"])
    benchmark_config.options = {"force_regenerate": True}

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

    dummy = DummyBenchmark()

    result = _ensure_data_generated(dummy, benchmark_config)

    assert result is True, "Expected True when force regenerating"
    dummy.generate_data.assert_called_once()


def test_invalid_manifest_regenerates(tmp_path: Path, benchmark_config: BenchmarkConfig):
    manifest = _write_manifest(tmp_path, table_names=["orders"])
    # Corrupt manifest by altering file size expectation
    manifest["tables"]["orders"][0]["size_bytes"] += 100
    with (tmp_path / "_datagen_manifest.json").open("w") as fh:
        json.dump(manifest, fh)

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

        def log_verbose(self, message: str) -> None:
            self.last_message = message

    dummy = DummyBenchmark()

    result = _ensure_data_generated(dummy, benchmark_config)

    assert result is True, "Expected True when regenerating due to invalid manifest"
    dummy.generate_data.assert_called_once()


def test_no_regenerate_with_invalid_manifest_raises(tmp_path: Path, benchmark_config: BenchmarkConfig):
    manifest = _write_manifest(tmp_path, table_names=["orders"])
    manifest["tables"]["orders"][0]["size_bytes"] += 50
    with (tmp_path / "_datagen_manifest.json").open("w") as fh:
        json.dump(manifest, fh)

    benchmark_config.options = {"no_regenerate": True}

    class DummyBenchmark:
        def __init__(self) -> None:
            self.output_dir = tmp_path
            self.tables = None
            self.generate_data = Mock()

    dummy = DummyBenchmark()

    with pytest.raises(RuntimeError):
        _ensure_data_generated(dummy, benchmark_config)

    dummy.generate_data.assert_not_called()
