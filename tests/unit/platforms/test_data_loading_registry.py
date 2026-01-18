"""Tests for file format detection in data loading registry.

Covers multi-suffix filenames such as '*.tbl.1.zst' to ensure the
base data extension is correctly identified and appropriate handlers
are returned.
"""

from pathlib import Path

import pytest

from benchbox.platforms.base.data_loading import (
    DeltaFileHandler,
    FileFormatRegistry,
    IcebergFileHandler,
)

pytestmark = pytest.mark.fast


def test_get_base_data_extension_multi_suffix_tbl_zst():
    """Detect .tbl extension in names like 'customer.tbl.1.zst'."""
    p = Path("/tmp/customer.tbl.1.zst")
    base_ext = FileFormatRegistry.get_base_data_extension(p)
    assert base_ext == ".tbl"


def test_get_handler_for_multi_suffix_tbl_zst():
    """Registry should return a handler for '*.tbl.1.zst' files."""
    p = Path("/tmp/orders.tbl.10.zst")
    handler = FileFormatRegistry.get_handler(p)
    assert handler is not None, "Expected a file format handler for .tbl.10.zst"
    # Ensure handler reports the correct delimiter for TPC-H style tables
    assert handler.get_delimiter() == "|"


def test_get_handler_for_delta_lake_directory(tmp_path):
    """Registry should return DeltaFileHandler for Delta Lake directories."""
    # Create a Delta Lake directory structure
    delta_table_dir = tmp_path / "customer"
    delta_table_dir.mkdir()
    delta_log_dir = delta_table_dir / "_delta_log"
    delta_log_dir.mkdir()

    # Get handler for the Delta Lake directory
    handler = FileFormatRegistry.get_handler(delta_table_dir)
    assert handler is not None, "Expected a file format handler for Delta Lake directory"
    assert isinstance(handler, DeltaFileHandler)
    assert handler.get_delimiter() == ""  # Delta Lake is not delimited


def test_get_handler_for_non_delta_directory(tmp_path):
    """Registry should return None for regular directories without _delta_log."""
    # Create a regular directory
    regular_dir = tmp_path / "data"
    regular_dir.mkdir()

    # Get handler for the regular directory
    handler = FileFormatRegistry.get_handler(regular_dir)
    assert handler is None, "Expected None for non-Delta directory"


def test_get_handler_for_parquet_file(tmp_path):
    """Registry should return ParquetFileHandler for .parquet files."""
    # Create a Parquet file path (doesn't need to exist for handler detection)
    parquet_file = tmp_path / "customer.parquet"

    # Get handler for the Parquet file
    handler = FileFormatRegistry.get_handler(parquet_file)
    assert handler is not None, "Expected a file format handler for .parquet file"
    assert handler.get_delimiter() == ""  # Parquet is not delimited


def test_get_handler_for_iceberg_directory(tmp_path):
    """Registry should return IcebergFileHandler for Iceberg directories."""
    # Create an Iceberg directory structure
    iceberg_table_dir = tmp_path / "customer"
    iceberg_table_dir.mkdir()
    metadata_dir = iceberg_table_dir / "metadata"
    metadata_dir.mkdir()

    # Get handler for the Iceberg directory
    handler = FileFormatRegistry.get_handler(iceberg_table_dir)
    assert handler is not None, "Expected a file format handler for Iceberg directory"
    assert isinstance(handler, IcebergFileHandler)
    assert handler.get_delimiter() == ""  # Iceberg is not delimited
