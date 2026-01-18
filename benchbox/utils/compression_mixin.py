"""Compression mixin for data generators.

This module provides a mixin class that adds compression capabilities
to data generators throughout BenchBox.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from pathlib import Path
from typing import Union

from .compression import CompressionError, CompressionManager


class CompressionMixin:
    """Mixin that adds compression capabilities to data generators."""

    def __init__(self, *args, **kwargs):
        """Initialize compression mixin.

        Expects compression-related kwargs:
            compression_type: Type of compression ('none', 'gzip', 'zstd')
            compression_level: Compression level (algorithm-specific)
            compress_data: Whether to enable compression (default: True)
            uncompressed_output: Whether to force uncompressed output (default: False)
        """
        # Check for explicit opt-out first
        uncompressed_output = kwargs.pop("uncompressed_output", False)

        if uncompressed_output:
            # Force no compression when explicitly requested
            self.compression_type = "none"
            self.compression_level = None
            self.compress_data = False
        else:
            # Extract compression parameters with new defaults
            self.compression_type = kwargs.pop("compression_type", "none")
            self.compression_level = kwargs.pop("compression_level", None)
            self.compress_data = kwargs.pop("compress_data", False)

        # Initialize compression manager
        self.compression_manager = CompressionManager()

        # If compression is enabled but no type specified, use zstd as default
        if self.compress_data and self.compression_type == "none":
            self.compression_type = "zstd"

        # Validate compression settings
        self._validate_compression_settings()

        # Call parent constructor only if there are remaining kwargs
        # This handles the case where this mixin is used with classes that don't expect these arguments
        if args or kwargs:
            try:
                super().__init__(*args, **kwargs)
            except TypeError:
                # If parent class doesn't accept these arguments, just ignore
                pass

    def _validate_compression_settings(self):
        """Validate compression settings."""
        if self.compression_type not in self.compression_manager.get_available_compressors():
            available = self.compression_manager.get_available_compressors()
            raise ValueError(f"Unsupported compression type '{self.compression_type}'. Available: {available}")

    def get_compressor(self):
        """Get the configured compressor instance."""
        return self.compression_manager.get_compressor(
            compression_type=self.compression_type, level=self.compression_level
        )

    def get_compressed_filename(self, filename: str) -> str:
        """Get the compressed version of a filename.

        Args:
            filename: Original filename

        Returns:
            Filename with compression extension if compression is enabled
        """
        if not self.compress_data or self.compression_type == "none":
            return filename

        compressor = self.get_compressor()
        return filename + compressor.get_file_extension()

    def open_output_file(self, path: Union[str, Path], mode: str = "wt"):
        """Open an output file with optional compression.

        Args:
            path: File path
            mode: File mode

        Returns:
            File-like object (compressed or uncompressed)
        """
        path = Path(path)

        if not self.compress_data or self.compression_type == "none":
            return open(path, mode)

        # Add compression extension if not already present
        compressor = self.get_compressor()
        if not str(path).endswith(compressor.get_file_extension()):
            path = path.with_suffix(path.suffix + compressor.get_file_extension())

        return compressor.open_for_write(path, mode)

    def compress_existing_file(self, file_path: Path, remove_original: bool = False) -> Path:
        """Compress an existing file.

        Args:
            file_path: Path to file to compress
            remove_original: Whether to remove the original file after compression

        Returns:
            Path to compressed file
        """
        if not self.compress_data or self.compression_type == "none":
            return file_path

        compressor = self.get_compressor()
        compressed_path = compressor.compress_file(file_path)

        if remove_original and compressed_path != file_path:
            try:
                file_path.unlink()
            except OSError:
                pass  # Ignore errors when removing original

        return compressed_path

    def get_compression_report(self, files: dict[str, Path]) -> dict[str, dict]:
        """Generate a compression report for generated files.

        Args:
            files: Dictionary mapping table names to file paths

        Returns:
            Dictionary with compression statistics
        """
        if not self.compress_data or self.compression_type == "none":
            return {}

        report = {}
        total_original = 0
        total_compressed = 0

        for table_name, file_path in files.items():
            # Try to find the original file if compression was applied
            original_path = file_path
            compressor = self.get_compressor()
            extension = compressor.get_file_extension()

            if str(file_path).endswith(extension):
                # This is the compressed file, look for original
                original_path = Path(str(file_path)[: -len(extension)])
                if not original_path.exists():
                    # Original doesn't exist, skip this file
                    continue

            try:
                info = self.compression_manager.get_compression_info(original_path, file_path)
                report[table_name] = info
                total_original += info["original_size"]
                total_compressed += info["compressed_size"]
            except CompressionError:
                # Skip files that can't be analyzed
                continue

        # Add overall statistics
        if total_original > 0:
            report["total"] = {
                "original_size": total_original,
                "compressed_size": total_compressed,
                "compression_ratio": total_original / total_compressed if total_compressed > 0 else float("inf"),
                "space_savings_percent": ((total_original - total_compressed) / total_original * 100),
            }

        return report

    def print_compression_report(self, files: dict[str, Path], verbose: bool = False):
        """Print a compression report.

        Args:
            files: Dictionary mapping table names to file paths
            verbose: Whether to show detailed per-file statistics
        """
        if not self.compress_data or self.compression_type == "none":
            return

        report = self.get_compression_report(files)
        if not report:
            return

        print(f"\nCompression Report ({self.compression_type})")
        print("=" * 50)

        if verbose and len(report) > 1:
            for table_name, info in report.items():
                if table_name == "total":
                    continue

                original_mb = info["original_size"] / (1024 * 1024)
                compressed_mb = info["compressed_size"] / (1024 * 1024)

                print(f"{table_name}:")
                print(f"  Original: {original_mb:.2f} MB")
                print(f"  Compressed: {compressed_mb:.2f} MB")
                print(f"  Ratio: {info['compression_ratio']:.2f}:1")
                print(f"  Savings: {info['space_savings_percent']:.1f}%")
                print()

        if "total" in report:
            total = report["total"]
            original_mb = total["original_size"] / (1024 * 1024)
            compressed_mb = total["compressed_size"] / (1024 * 1024)

            print(f"Total Original Size: {original_mb:.2f} MB")
            print(f"Total Compressed Size: {compressed_mb:.2f} MB")
            print(f"Overall Compression Ratio: {total['compression_ratio']:.2f}:1")
            print(f"Space Savings: {total['space_savings_percent']:.1f}%")

    def should_use_compression(self) -> bool:
        """Check if compression should be used."""
        return self.compress_data and self.compression_type != "none"
