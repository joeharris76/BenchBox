"""
Lightweight DSDGen (TPC-DS) integration tests.

Exercises the precompiled dsdgen binary through the generator wrapper to
ensure it is present and can generate a small table on this platform.
"""

import tempfile
from pathlib import Path

import pytest

from benchbox.core.tpcds.generator import TPCDSDataGenerator


@pytest.mark.validation
def test_dsdgen_generates_small_table_streaming():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)

        gen = TPCDSDataGenerator(scale_factor=1.0, output_dir=out, verbose=False)

        # Generate a single small table using streaming to minimize I/O
        gen._generate_single_table_streaming(out, "call_center")

        # Expect a compressed file to exist (.zst by default)
        compressed = out / gen.get_compressed_filename("call_center.dat")
        assert compressed.exists(), f"Expected file not found: {compressed}"
        assert compressed.stat().st_size > 0
