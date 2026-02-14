"""Targeted tests for TPCH stale artifact pruning."""

from pathlib import Path
from unittest.mock import patch

import pytest

from benchbox.core.tpch.generator import TPCHDataGenerator

pytestmark = pytest.mark.fast


@patch("benchbox.core.tpch.generator.TPCHDataGenerator._find_or_build_dbgen")
def test_prune_stale_table_artifacts_removes_conflicting_variants_only(mock_find_dbgen, tmp_path: Path):
    mock_find_dbgen.return_value = Path("/tmp/dbgen")
    generator = TPCHDataGenerator(output_dir=tmp_path)

    stale_files = [
        tmp_path / "customer.tbl",
        tmp_path / "customer.tbl.1",
        tmp_path / "customer.tbl.zst",
        tmp_path / "customer.tbl.1.zst",
    ]
    for path in stale_files:
        path.write_text("stale", encoding="utf-8")
    keep_file = tmp_path / "_datagen_manifest.json"
    keep_file.write_text("{}", encoding="utf-8")

    removed = generator._prune_stale_table_artifacts(tmp_path)

    assert "customer.tbl.1" in {p.name for p in removed}
    assert "customer.tbl.1.zst" in {p.name for p in removed}
    assert "customer.tbl.zst" in {p.name for p in removed}
    assert (tmp_path / "customer.tbl").exists()
    assert not (tmp_path / "customer.tbl.1").exists()
    assert not (tmp_path / "customer.tbl.1.zst").exists()
    assert not (tmp_path / "customer.tbl.zst").exists()
    assert keep_file.exists()


@patch("benchbox.core.tpch.generator.TPCHDataGenerator._find_or_build_dbgen")
def test_generate_local_prunes_stale_artifacts_when_regenerating(mock_find_dbgen, tmp_path: Path):
    mock_find_dbgen.return_value = Path("/tmp/dbgen")
    generator = TPCHDataGenerator(output_dir=tmp_path)

    with patch.object(generator.validator, "should_regenerate_data", return_value=(True, None)):
        with patch.object(generator, "_prune_stale_table_artifacts", return_value=[]) as mock_prune:
            with patch.object(generator, "_run_dbgen_native", return_value=None):
                with patch.object(generator, "_finalize_generation", return_value={}):
                    generator._generate_local(tmp_path)

    mock_prune.assert_called_once_with(tmp_path)


@patch("benchbox.core.tpch.generator.TPCHDataGenerator._find_or_build_dbgen")
def test_prune_stale_table_artifacts_removes_compressed_when_uncompressed_mode(mock_find_dbgen, tmp_path: Path):
    mock_find_dbgen.return_value = Path("/tmp/dbgen")
    generator = TPCHDataGenerator(output_dir=tmp_path, compress_data=False, compression_type="none")

    stale_compressed = tmp_path / "customer.tbl.zst"
    stale_compressed.write_text("stale", encoding="utf-8")

    removed = generator._prune_stale_table_artifacts(tmp_path)

    assert "customer.tbl.zst" in {p.name for p in removed}
    assert not stale_compressed.exists()
