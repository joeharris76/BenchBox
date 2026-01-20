"""
Lightweight DBGen (TPC-H) integration tests.

Exercises the precompiled dbgen binary via the TPCHDataGenerator to ensure
it runs on the current platform and produces non-empty output for a small table.
"""

import tempfile
from pathlib import Path

import pytest

from benchbox.core.tpch.generator import TPCHDataGenerator


@pytest.mark.validation
def test_dbgen_generates_small_table():
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)

        # Use a small fractional scale factor supported by dbgen for development
        gen = TPCHDataGenerator(scale_factor=0.01, output_dir=out, verbose=False, uncompressed_output=True)

        # Generate a single dimension table (REGION) to keep runtime tiny
        # TPCHDataGenerator implements a native or file-based path; use minimal invocation
        # The API does not expose single-table, so we mimic a minimal run and verify a file
        # We expect REGION.tbl or .dat depending on implementation; uncompressed_output=True disables .zst
        try:
            # Attempt native path if available
            if hasattr(gen, "_run_dbgen_native"):
                gen._run_dbgen_native(out)
            else:
                # Fallback to internal exposed generation path if present
                if hasattr(gen, "_run_file_based_dbgen"):
                    gen._run_file_based_dbgen(out)
                else:
                    # As a last resort, call whatever public generate exists
                    if hasattr(gen, "generate"):
                        gen.generate()
        except Exception:
            # If full generation ran, that's acceptable; continue to assertions
            pass

        # Check that at least REGION was generated (common tpch file naming)
        candidates = [
            out / "region.tbl",
            out / "region.dat",
            out / "REGION.tbl",
            out / "REGION.dat",
        ]
        exists = [p for p in candidates if p.exists() and p.stat().st_size > 0]
        assert exists, "dbgen did not produce expected REGION output"
