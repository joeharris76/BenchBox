from __future__ import annotations

import io
import threading
from pathlib import Path
from subprocess import CalledProcessError
from types import SimpleNamespace

import pytest

from benchbox.core.tpcds.generator.streaming import StreamingGenerationMixin

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class _StreamingHarness(StreamingGenerationMixin):
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.dsdgen_exe = output_dir / "dsdgen"
        self.dsdgen_exe.write_text("#!/bin/sh\nexit 0\n")
        self.scale_factor = 1.0
        self.verbose = False
        self.parallel = 3
        self._manifest_lock = threading.Lock()
        self._manifest_entries = {}

    def _copy_distribution_files(self, _output_dir: Path) -> None:
        return None

    def _is_valid_data_file(self, file_path: Path) -> bool:
        return file_path.exists() and file_path.stat().st_size > 0

    def get_compressed_filename(self, expected_filename: str) -> str:
        return expected_filename + ".zst"

    def open_output_file(self, file_path: Path, mode: str = "wt"):
        return open(file_path, mode)

    def should_use_compression(self) -> bool:
        return True

    def compress_existing_file(self, dat_file: Path, remove_original: bool = False) -> Path:
        compressed = dat_file.with_suffix(dat_file.suffix + ".zst")
        compressed.write_bytes(dat_file.read_bytes())
        if remove_original:
            dat_file.unlink(missing_ok=True)
        return compressed


def test_generate_single_table_streaming_handles_uncompressed_target(monkeypatch, tmp_path):
    harness = _StreamingHarness(tmp_path)
    monkeypatch.setattr(harness, "get_compressed_filename", lambda name: name)
    monkeypatch.setattr("benchbox.core.tpcds.generator.streaming.subprocess.run", lambda *a, **k: None)

    dat_file = tmp_path / "customer.dat"
    dat_file.write_text("1|a\n2|b\n")

    harness._generate_single_table_streaming(tmp_path, "customer")

    assert harness._manifest_entries["customer"][0]["row_count"] == 2
    assert dat_file.exists()


def test_generate_single_table_streaming_wraps_calledprocesserror(monkeypatch, tmp_path):
    harness = _StreamingHarness(tmp_path)

    def _raise(*_args, **_kwargs):
        raise CalledProcessError(returncode=9, cmd=["dsdgen"])

    monkeypatch.setattr("benchbox.core.tpcds.generator.streaming.subprocess.run", _raise)

    with pytest.raises(RuntimeError, match="exit code 9"):
        harness._generate_single_table_streaming(tmp_path, "customer")


def test_generate_single_table_chunk_streaming_counts_last_line_without_newline(monkeypatch, tmp_path):
    harness = _StreamingHarness(tmp_path)

    class _Process:
        def __init__(self):
            self.stdout = io.BytesIO(b"1|only_row")
            self.stderr = io.BytesIO(b"")
            self.returncode = 0

        def wait(self):
            return None

    monkeypatch.setattr("benchbox.core.tpcds.generator.streaming.subprocess.Popen", lambda *a, **k: _Process())

    harness._generate_single_table_chunk_streaming(tmp_path, "call_center", 1)

    entry = harness._manifest_entries["call_center"][0]
    assert entry["row_count"] == 1
    assert entry["path"].endswith(".zst")


def test_generate_single_table_chunk_streaming_raises_on_nonzero_exit_with_no_data(monkeypatch, tmp_path):
    harness = _StreamingHarness(tmp_path)

    class _Process:
        def __init__(self):
            self.stdout = io.BytesIO(b"")
            self.stderr = io.BytesIO(b"failure")
            self.returncode = 2

        def wait(self):
            return None

    monkeypatch.setattr("benchbox.core.tpcds.generator.streaming.subprocess.Popen", lambda *a, **k: _Process())

    with pytest.raises(RuntimeError, match="exit code 2"):
        harness._generate_single_table_chunk_streaming(tmp_path, "call_center", 2)


def test_generate_parent_table_chunk_with_children_tracks_manifest(monkeypatch, tmp_path):
    harness = _StreamingHarness(tmp_path)
    monkeypatch.setattr("benchbox.core.tpcds.generator.streaming.subprocess.run", lambda *a, **k: None)

    parent = tmp_path / "catalog_sales_1_3.dat"
    child = tmp_path / "catalog_returns_1_3.dat"
    parent.write_text("1|a\n2|b\n")
    child.write_text("3|c\n")

    harness._generate_parent_table_chunk_with_children(tmp_path, "catalog_sales", 1, ["catalog_returns"])

    assert harness._manifest_entries["catalog_sales"][0]["row_count"] == 2
    assert harness._manifest_entries["catalog_returns"][0]["row_count"] == 1
