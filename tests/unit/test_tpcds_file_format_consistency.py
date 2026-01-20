"""Hermetic tests for TPC-DS file format consistency validation."""

from pathlib import Path

import pytest

from benchbox.core.tpcds.generator import TPCDSDataGenerator

pytestmark = pytest.mark.fast


def _make_generator(compression_enabled: bool, verbose: bool = False) -> TPCDSDataGenerator:
    generator = object.__new__(TPCDSDataGenerator)
    generator.verbose = verbose
    generator.compression_type = "zstd" if compression_enabled else "none"
    generator.should_use_compression = lambda: compression_enabled

    if compression_enabled:
        generator.get_compressor = lambda: type(
            "Compressor", (), {"get_file_extension": staticmethod(lambda: ".dat.zst")}
        )()
    else:
        generator.get_compressor = lambda: type("Compressor", (), {"get_file_extension": staticmethod(lambda: "")})()

    generator._is_valid_data_file = lambda p: p.stat().st_size > 0
    return generator


@pytest.fixture
def temp_dir(tmp_path: Path) -> Path:
    return tmp_path


def test_compressed_streams_disallow_raw_dat_files(temp_dir: Path):
    generator = _make_generator(compression_enabled=True)

    # Populate with valid compressed files
    for name in ("table1.dat.zst", "table2.dat.zst"):
        (temp_dir / name).write_bytes(b"compressed")

    # Should not raise
    generator._validate_file_format_consistency(temp_dir)

    # Introduce raw .dat file to trigger validation error
    (temp_dir / "bad_table.dat").write_text("raw data")
    with pytest.raises(RuntimeError, match="raw .dat files when compression is enabled"):
        generator._validate_file_format_consistency(temp_dir)


def test_uncompressed_streams_only_dat_files(temp_dir: Path):
    generator = _make_generator(compression_enabled=False)

    # Create non-empty .dat files
    for name in ("table1.dat", "table2.dat"):
        (temp_dir / name).write_text("rows")

    generator._validate_file_format_consistency(temp_dir)

    # Validate helper considers non-empty dat files valid
    dat_files = list(temp_dir.glob("*.dat"))
    assert dat_files
