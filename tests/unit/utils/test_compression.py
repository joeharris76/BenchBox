"""Tests for compression utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path

import pytest

from benchbox.utils.compression import (
    ZSTD_AVAILABLE,
    CompressionError,
    CompressionManager,
    GzipCompressor,
    NoCompressor,
    ZstdCompressor,
)

pytestmark = pytest.mark.fast


class TestCompressionManager:
    """Test compression manager functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.manager = CompressionManager()

    def test_get_available_compressors(self):
        """Test getting available compressor types."""
        available = self.manager.get_available_compressors()
        assert "none" in available
        assert "gzip" in available

        if ZSTD_AVAILABLE:
            assert "zstd" in available

    def test_get_compressor_gzip(self):
        """Test getting gzip compressor."""
        compressor = self.manager.get_compressor("gzip")
        assert isinstance(compressor, GzipCompressor)
        assert compressor.level == 6  # Default level

    def test_get_compressor_none(self):
        """Test getting no compression compressor."""
        compressor = self.manager.get_compressor("none")
        assert isinstance(compressor, NoCompressor)

    @pytest.mark.skipif(not ZSTD_AVAILABLE, reason="zstandard not available")
    def test_get_compressor_zstd(self):
        """Test getting zstd compressor."""
        compressor = self.manager.get_compressor("zstd")
        assert isinstance(compressor, ZstdCompressor)
        assert compressor.level == 3  # Default level

    def test_get_compressor_with_level(self):
        """Test getting compressor with custom level."""
        compressor = self.manager.get_compressor("gzip", level=9)
        assert compressor.level == 9

    def test_get_compressor_invalid_type(self):
        """Test getting invalid compressor type."""
        with pytest.raises(CompressionError):
            self.manager.get_compressor("invalid")

    def test_detect_compression(self):
        """Test compression detection from file extension."""
        assert self.manager.detect_compression(Path("test.txt")) == "none"
        assert self.manager.detect_compression(Path("test.txt.gz")) == "gzip"
        assert self.manager.detect_compression(Path("test.txt.zst")) == "zstd"


class TestGzipCompressor:
    """Test gzip compressor functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.compressor = GzipCompressor()

    def test_file_extension(self):
        """Test gzip file extension."""
        assert self.compressor.get_file_extension() == ".gz"

    def test_invalid_level(self):
        """Test invalid compression level."""
        with pytest.raises(ValueError):
            GzipCompressor(level=0)
        with pytest.raises(ValueError):
            GzipCompressor(level=10)

    def test_compress_decompress_file(self):
        """Test file compression and decompression."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test file
            test_data = "Hello, World!\n" * 1000  # Some repetitive data
            input_file = temp_path / "test.txt"
            input_file.write_text(test_data)

            # Compress file
            compressed_file = self.compressor.compress_file(input_file)
            assert compressed_file.exists()
            assert str(compressed_file).endswith(".gz")
            assert compressed_file.stat().st_size < input_file.stat().st_size

            # Decompress file
            decompressed_file = self.compressor.decompress_file(compressed_file)
            assert decompressed_file.exists()
            assert decompressed_file.read_text() == test_data

    def test_open_for_write_read(self):
        """Test compressed file I/O."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            test_data = "Line 1\nLine 2\nLine 3\n"
            compressed_file = temp_path / "test.txt.gz"

            # Write compressed data
            with self.compressor.open_for_write(compressed_file, "wt") as f:
                f.write(test_data)

            assert compressed_file.exists()

            # Read compressed data
            with self.compressor.open_for_read(compressed_file, "rt") as f:
                read_data = f.read()

            assert read_data == test_data


class TestNoCompressor:
    """Test no compression functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.compressor = NoCompressor()

    def test_file_extension(self):
        """Test no compression file extension."""
        assert self.compressor.get_file_extension() == ""

    def test_compress_file(self):
        """Test file 'compression' (copying)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test file
            test_data = "Hello, World!"
            input_file = temp_path / "test.txt"
            input_file.write_text(test_data)

            # 'Compress' file (should just copy)
            output_file = temp_path / "compressed.txt"
            result_file = self.compressor.compress_file(input_file, output_file)

            assert result_file == output_file
            assert result_file.exists()
            assert result_file.read_text() == test_data


@pytest.mark.skipif(not ZSTD_AVAILABLE, reason="zstandard not available")
class TestZstdCompressor:
    """Test zstd compressor functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.compressor = ZstdCompressor()

    def test_file_extension(self):
        """Test zstd file extension."""
        assert self.compressor.get_file_extension() == ".zst"

    def test_invalid_level(self):
        """Test invalid compression level."""
        with pytest.raises(ValueError):
            ZstdCompressor(level=0)
        with pytest.raises(ValueError):
            ZstdCompressor(level=23)

    def test_compress_decompress_file(self):
        """Test file compression and decompression."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test file
            test_data = "Hello, World!\n" * 1000  # Some repetitive data
            input_file = temp_path / "test.txt"
            input_file.write_text(test_data)

            # Compress file
            compressed_file = self.compressor.compress_file(input_file)
            assert compressed_file.exists()
            assert str(compressed_file).endswith(".zst")
            assert compressed_file.stat().st_size < input_file.stat().st_size

            # Decompress file
            decompressed_file = self.compressor.decompress_file(compressed_file)
            assert decompressed_file.exists()
            assert decompressed_file.read_text() == test_data
