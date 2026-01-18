import json
from pathlib import Path

import pytest

from benchbox.utils.data_validation import BenchmarkDataValidator

pytestmark = pytest.mark.fast


def test_validator_builds_manifest_from_scan(tmp_path: Path):
    # Create a minimal data file
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    f = data_dir / "sample.tbl"
    f.write_text("1|a\n2|b\n3|c\n")

    # Use a benchmark with no hard-coded expectations so presence is enough
    v = BenchmarkDataValidator("ssb", scale_factor=0.01)
    res = v.validate_data_directory(data_dir)

    assert res.valid is True
    # Manifest should be written
    mp = data_dir / "_datagen_manifest.json"
    assert mp.exists()
    manifest = json.loads(mp.read_text())
    assert manifest.get("benchmark") == "ssb"
    assert "tables" in manifest and len(manifest["tables"]) >= 1

    # Validate again; should still be valid and not raise
    res2 = v.validate_data_directory(data_dir)
    assert res2.valid is True
