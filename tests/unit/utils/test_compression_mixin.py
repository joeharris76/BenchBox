"""Tests for compression mixin functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
from pathlib import Path

import pytest

from benchbox.utils.compression_mixin import CompressionMixin

pytestmark = pytest.mark.fast


class MockDataGenerator(CompressionMixin):
    """Mock data generator for testing compression mixin."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.output_dir = Path(kwargs.get("output_dir", Path.cwd()))


class TestCompressionMixin:
    """Test compression mixin functionality."""

    def test_init_defaults(self):
        """Test initialization with default values (compression disabled by default)."""
        generator = MockDataGenerator()
        assert not generator.compress_data  # Default is False
        assert generator.compression_type == "none"  # Default is "none"
        assert generator.compression_level is None

    @pytest.mark.requires_zstd
    def test_init_compression_enabled(self):
        """Test initialization with compression enabled auto-defaults to zstd."""
        generator = MockDataGenerator(compress_data=True)
        assert generator.compress_data
        assert generator.compression_type == "zstd"  # Auto-default when enabled

    def test_init_custom_compression(self):
        """Test initialization with custom compression settings."""
        generator = MockDataGenerator(compress_data=True, compression_type="gzip", compression_level=9)
        assert generator.compress_data
        assert generator.compression_type == "gzip"
        assert generator.compression_level == 9

    def test_init_uncompressed_output(self):
        """Test initialization with explicit uncompressed output."""
        generator = MockDataGenerator(uncompressed_output=True)
        assert not generator.compress_data
        assert generator.compression_type == "none"
        assert generator.compression_level is None

    def test_init_uncompressed_output_overrides_compression(self):
        """Test that uncompressed_output overrides other compression settings."""
        generator = MockDataGenerator(
            uncompressed_output=True,
            compress_data=True,
            compression_type="zstd",
            compression_level=5,
        )
        assert not generator.compress_data
        assert generator.compression_type == "none"
        assert generator.compression_level is None

    def test_init_invalid_compression_type(self):
        """Test initialization with invalid compression type."""
        with pytest.raises(ValueError):
            MockDataGenerator(compression_type="invalid")

    def test_get_compressed_filename(self):
        """Test getting compressed filename."""
        # Default is no compression, so should not add extension
        generator = MockDataGenerator()
        assert generator.get_compressed_filename("test.txt") == "test.txt"

        # Uncompressed output should not add extension
        generator = MockDataGenerator(uncompressed_output=True)
        assert generator.get_compressed_filename("test.txt") == "test.txt"

        generator = MockDataGenerator(compress_data=True, compression_type="gzip")
        assert generator.get_compressed_filename("test.txt") == "test.txt.gz"

    @pytest.mark.requires_zstd
    def test_should_use_compression(self):
        """Test compression check method."""
        # Default is no compression
        generator = MockDataGenerator()
        assert not generator.should_use_compression()

        # Uncompressed output should not use compression
        generator = MockDataGenerator(uncompressed_output=True)
        assert not generator.should_use_compression()

        # With compression enabled (auto-defaults to zstd)
        generator = MockDataGenerator(compress_data=True)
        assert generator.should_use_compression()

    def test_should_use_compression_with_gzip(self):
        """Test compression check method with explicit gzip."""
        # With compression enabled using gzip
        generator = MockDataGenerator(compress_data=True, compression_type="gzip")
        assert generator.should_use_compression()

    def test_open_output_file_no_compression(self):
        """Test opening output file without compression."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generator = MockDataGenerator(uncompressed_output=True, output_dir=temp_dir)
            file_path = Path(temp_dir) / "test.txt"

            with generator.open_output_file(file_path, "w") as f:
                f.write("Hello, World!")

            assert file_path.exists()
            assert file_path.read_text() == "Hello, World!"

    def test_open_output_file_with_compression(self):
        """Test opening output file with compression."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generator = MockDataGenerator(compress_data=True, compression_type="gzip", output_dir=temp_dir)
            file_path = Path(temp_dir) / "test.txt"

            with generator.open_output_file(file_path, "wt") as f:
                f.write("Hello, World!")

            # Should create compressed file
            compressed_path = Path(temp_dir) / "test.txt.gz"
            assert compressed_path.exists()

            # Verify content by reading compressed file
            import gzip

            with gzip.open(compressed_path, "rt") as f:
                content = f.read()
                assert content == "Hello, World!"

    def test_compress_existing_file(self):
        """Test compressing an existing file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generator = MockDataGenerator(compress_data=True, compression_type="gzip", output_dir=temp_dir)

            # Create test file
            test_file = Path(temp_dir) / "test.txt"
            test_file.write_text("Hello, World!")

            # Compress it
            compressed_file = generator.compress_existing_file(test_file)
            assert compressed_file.exists()
            assert str(compressed_file).endswith(".gz")

    def test_get_compression_report_no_compression(self):
        """Test compression report when compression is disabled."""
        generator = MockDataGenerator(uncompressed_output=True)
        files = {"test": Path("test.txt")}
        report = generator.get_compression_report(files)
        assert report == {}

    def test_get_compression_report_with_compression(self):
        """Test compression report with compression enabled."""
        with tempfile.TemporaryDirectory() as temp_dir:
            generator = MockDataGenerator(compress_data=True, compression_type="gzip", output_dir=temp_dir)

            # Create original and compressed files
            original_file = Path(temp_dir) / "test.txt"
            compressed_file = Path(temp_dir) / "test.txt.gz"

            test_data = "Hello, World!\n" * 100
            original_file.write_text(test_data)

            # Compress the file
            compressor = generator.get_compressor()
            compressor.compress_file(original_file, compressed_file)

            files = {"test": compressed_file}
            report = generator.get_compression_report(files)

            assert "test" in report
            assert "compression_ratio" in report["test"]
            assert report["test"]["compression_ratio"] > 1.0  # Should be compressed
