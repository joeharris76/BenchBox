"""Performance tests for compression functionality.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import tempfile
import time
from pathlib import Path

import pytest

from benchbox.core.ssb.generator import SSBDataGenerator
from benchbox.utils.compression import CompressionManager


class TestCompressionPerformance:
    """Performance tests for compression functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.compression_manager = CompressionManager()

    def generate_test_data(self, size_mb: float) -> str:
        """Generate test data of specified size."""
        # Create realistic CSV data
        base_row = "12345,Product Name with Special Characters & Symbols,Category A,99.99,2023-01-01 10:00:00,Description with unicode café résumé\n"
        row_size = len(base_row.encode("utf-8"))
        num_rows = int((size_mb * 1024 * 1024) // row_size)

        header = "id,name,category,price,timestamp,description\n"
        data = header + base_row * num_rows
        return data

    @pytest.mark.performance
    def test_compression_speed_comparison(self):
        """Compare compression speeds between algorithms."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create test data (1MB)
            test_data = self.generate_test_data(1)
            original_file = temp_path / "test_data.csv"
            original_file.write_text(test_data)

            compression_results = {}

            # Test each compression type
            for compression_type in ["gzip", "zstd"]:
                try:
                    compressor = self.compression_manager.get_compressor(compression_type)
                    compressed_file = temp_path / f"test_{compression_type}{compressor.get_file_extension()}"

                    # Measure compression time
                    start_time = time.time()
                    compressor.compress_file(original_file, compressed_file)
                    compression_time = time.time() - start_time

                    # Measure decompression time
                    decompressed_file = temp_path / f"decompressed_{compression_type}.csv"
                    start_time = time.time()
                    compressor.decompress_file(compressed_file, decompressed_file)
                    decompression_time = time.time() - start_time

                    # Get file sizes
                    original_size = original_file.stat().st_size
                    compressed_size = compressed_file.stat().st_size

                    compression_results[compression_type] = {
                        "compression_time": compression_time,
                        "decompression_time": decompression_time,
                        "original_size": original_size,
                        "compressed_size": compressed_size,
                        "compression_ratio": original_size / compressed_size,
                        "compression_speed_mbps": (original_size / (1024 * 1024)) / compression_time,
                        "decompression_speed_mbps": (original_size / (1024 * 1024)) / decompression_time,
                    }

                except Exception as e:
                    pytest.skip(f"Compression type {compression_type} not available: {e}")

            # Print results for analysis
            print("\nCompression Performance Results:")
            print("-" * 80)
            for comp_type, results in compression_results.items():
                print(f"{comp_type.upper()}:")
                print(f"  Compression ratio: {results['compression_ratio']:.2f}:1")
                print(f"  Compression speed: {results['compression_speed_mbps']:.2f} MB/s")
                print(f"  Decompression speed: {results['decompression_speed_mbps']:.2f} MB/s")
                print(f"  Compression time: {results['compression_time']:.3f}s")
                print(f"  Decompression time: {results['decompression_time']:.3f}s")
                print(f"  Space savings: {(1 - results['compressed_size'] / results['original_size']) * 100:.1f}%")
                print()

            # Performance assertions (reasonable bounds)
            for comp_type, results in compression_results.items():
                # Should achieve some compression
                assert results["compression_ratio"] > 1.0

                # Should complete in reasonable time (less than 10 seconds for 1MB)
                assert results["compression_time"] < 10.0
                assert results["decompression_time"] < 10.0

                # Should achieve reasonable speeds (at least 0.1 MB/s)
                assert results["compression_speed_mbps"] > 0.1
                assert results["decompression_speed_mbps"] > 0.1

    @pytest.mark.performance
    def test_streaming_vs_file_compression_performance(self):
        """Compare streaming vs file-based compression performance."""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Generate test data
            test_data = self.generate_test_data(1)  # 1MB
            original_file = temp_path / "test_data.csv"
            original_file.write_text(test_data)

            compressor = self.compression_manager.get_compressor("gzip")

            # Test file-based compression
            compressed_file1 = temp_path / "file_compressed.csv.gz"
            start_time = time.time()
            compressor.compress_file(original_file, compressed_file1)
            file_compression_time = time.time() - start_time

            # Test streaming compression
            compressed_file2 = temp_path / "stream_compressed.csv.gz"
            start_time = time.time()
            with compressor.open_for_write(compressed_file2, "wt") as f:
                f.write(test_data)
            stream_compression_time = time.time() - start_time

            # Compare results
            file_size1 = compressed_file1.stat().st_size
            file_size2 = compressed_file2.stat().st_size

            print("\nStreaming vs File Compression:")
            print(f"File-based compression time: {file_compression_time:.3f}s")
            print(f"Streaming compression time: {stream_compression_time:.3f}s")
            print(f"File-based result size: {file_size1} bytes")
            print(f"Streaming result size: {file_size2} bytes")

            # Results should be similar
            assert abs(file_size1 - file_size2) < 1000  # Within 1KB difference

            # Both should complete in reasonable time
            assert file_compression_time < 10.0
            assert stream_compression_time < 10.0

    @pytest.mark.performance
    def test_compression_with_different_data_sizes(self):
        """Test compression performance with different data sizes."""
        data_sizes_mb = [0.1, 0.5, 1.0, 2.0]  # Different sizes to test

        for size_mb in data_sizes_mb:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Generate test data
                test_data = self.generate_test_data(size_mb)
                original_file = temp_path / f"test_{size_mb}mb.csv"
                original_file.write_text(test_data)

                compressor = self.compression_manager.get_compressor("gzip")
                compressed_file = temp_path / f"test_{size_mb}mb.csv.gz"

                # Measure compression
                start_time = time.time()
                compressor.compress_file(original_file, compressed_file)
                compression_time = time.time() - start_time

                original_size = original_file.stat().st_size
                compressed_size = compressed_file.stat().st_size
                compression_ratio = original_size / compressed_size

                print(f"Size: {size_mb}MB - Time: {compression_time:.3f}s - Ratio: {compression_ratio:.2f}:1")

                # Performance should scale reasonably with size
                assert compression_time < size_mb * 10  # Max 10 seconds per MB
                assert compression_ratio > 1.0  # Should achieve compression

    @pytest.mark.performance
    @pytest.mark.requires_zstd
    def test_ssb_benchmark_compression_performance(self):
        """Test SSB benchmark with compression performance (zstd)."""
        scale_factors = [0.01, 0.1]  # Small scale factors for testing

        for scale_factor in scale_factors:
            with tempfile.TemporaryDirectory() as temp_dir:
                print(f"\nTesting SSB benchmark compression at scale {scale_factor}")

                # Test without compression
                generator_uncompressed = SSBDataGenerator(
                    scale_factor=scale_factor,
                    output_dir=temp_dir + "/uncompressed",
                    compress_data=False,
                )

                start_time = time.time()
                files_uncompressed = generator_uncompressed.generate_data(tables=["date"])
                uncompressed_time = time.time() - start_time

                uncompressed_size = Path(files_uncompressed["date"]).stat().st_size

                # Test with compression
                generator_compressed = SSBDataGenerator(
                    scale_factor=scale_factor,
                    output_dir=temp_dir + "/compressed",
                    compress_data=True,
                    compression_type="zstd",
                )

                start_time = time.time()
                files_compressed = generator_compressed.generate_data(tables=["date"])
                compressed_time = time.time() - start_time

                compressed_size = Path(files_compressed["date"]).stat().st_size

                # Calculate metrics
                compression_ratio = uncompressed_size / compressed_size
                time_overhead_percent = (
                    ((compressed_time - uncompressed_time) / uncompressed_time * 100) if uncompressed_time > 0 else 0
                )

                print(f"  Scale {scale_factor}:")
                print(f"    Uncompressed: {uncompressed_size:,} bytes in {uncompressed_time:.3f}s")
                print(f"    Compressed: {compressed_size:,} bytes in {compressed_time:.3f}s")
                print(f"    Compression ratio: {compression_ratio:.2f}:1")
                print(f"    Time overhead: {time_overhead_percent:.1f}%")

                # Assertions
                assert compression_ratio > 1.0  # Should achieve compression
                assert compressed_time < 60.0  # Should complete in reasonable time
                assert time_overhead_percent < 500  # Compression overhead should be reasonable

    @pytest.mark.performance
    def test_compression_memory_usage(self):
        """Test compression memory usage patterns."""
        import os

        import psutil

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Create larger test file
            test_data = self.generate_test_data(5)  # 5MB
            original_file = temp_path / "large_test.csv"
            original_file.write_text(test_data)

            compressor = self.compression_manager.get_compressor("gzip")
            compressed_file = temp_path / "large_test.csv.gz"

            # Monitor memory usage
            process = psutil.Process(os.getpid())
            memory_before = process.memory_info().rss

            # Perform compression
            compressor.compress_file(original_file, compressed_file)

            memory_after = process.memory_info().rss
            memory_used = memory_after - memory_before

            print("\nMemory usage for 5MB compression:")
            print(f"  Memory before: {memory_before / (1024 * 1024):.2f} MB")
            print(f"  Memory after: {memory_after / (1024 * 1024):.2f} MB")
            print(f"  Memory used: {memory_used / (1024 * 1024):.2f} MB")

            # Memory usage should be reasonable (less than 50MB for 5MB file)
            assert memory_used < 50 * 1024 * 1024  # 50MB limit

    def test_concurrent_compression_safety(self):
        """Test that compression is safe under concurrent usage."""
        import concurrent.futures

        def compress_data(thread_id):
            """Compression task for concurrent testing."""
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)

                # Generate unique test data for this thread
                test_data = self.generate_test_data(0.5) + f"thread_{thread_id}\n"
                original_file = temp_path / f"test_{thread_id}.csv"
                original_file.write_text(test_data)

                compressor = self.compression_manager.get_compressor("gzip")
                compressed_file = temp_path / f"test_{thread_id}.csv.gz"

                # Compress
                compressor.compress_file(original_file, compressed_file)

                # Decompress and verify
                decompressed_file = temp_path / f"decompressed_{thread_id}.csv"
                compressor.decompress_file(compressed_file, decompressed_file)

                # Verify integrity
                original_content = original_file.read_text()
                decompressed_content = decompressed_file.read_text()

                assert original_content == decompressed_content
                return thread_id

        # Run concurrent compression tasks
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(compress_data, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]

        # All threads should complete successfully
        assert len(results) == 10
        assert set(results) == set(range(10))
