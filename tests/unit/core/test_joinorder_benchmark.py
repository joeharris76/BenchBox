"""Unit tests for JoinOrder benchmark configuration handling."""

from datetime import datetime

import pytest

from benchbox.core.benchmark_loader import get_benchmark_instance
from benchbox.core.config import BenchmarkConfig, SystemProfile
from benchbox.core.joinorder.benchmark import JoinOrderBenchmark

pytestmark = pytest.mark.fast


def _make_system_profile() -> SystemProfile:
    """Create a minimal system profile for loader tests."""

    return SystemProfile(
        os_name="TestOS",
        os_version="1.0",
        architecture="x86_64",
        cpu_model="Test CPU",
        cpu_cores_physical=4,
        cpu_cores_logical=8,
        memory_total_gb=16.0,
        memory_available_gb=12.0,
        python_version="3.11.0",
        disk_space_gb=256.0,
        timestamp=datetime.now(),
    )


@pytest.mark.requires_zstd
def test_joinorder_benchmark_propagates_compression(tmp_path) -> None:
    """JoinOrderBenchmark should respect compression kwargs and pass them to the generator."""

    benchmark = JoinOrderBenchmark(
        scale_factor=0.1,
        output_dir=tmp_path,
        compress_data=True,
        compression_type="zstd",
        compression_level=7,
        parallel=2,
    )

    generator = benchmark._generator

    # Compression settings are stored in the generator, not the benchmark
    assert benchmark.parallel == 2
    assert generator.compress_data is True
    assert generator.compression_type == "zstd"
    assert generator.compression_level == 7


def test_joinorder_benchmark_propagates_compression_gzip(tmp_path) -> None:
    """JoinOrderBenchmark should respect gzip compression settings."""

    benchmark = JoinOrderBenchmark(
        scale_factor=0.1,
        output_dir=tmp_path,
        compress_data=True,
        compression_type="gzip",
        compression_level=6,
        parallel=2,
    )

    generator = benchmark._generator

    # Compression settings are stored in the generator, not the benchmark
    assert benchmark.parallel == 2
    assert generator.compress_data is True
    assert generator.compression_type == "gzip"
    assert generator.compression_level == 6


@pytest.mark.requires_zstd
def test_get_benchmark_instance_handles_joinorder_compression() -> None:
    """Loader should instantiate JoinOrderBenchmark with compression and force_regenerate options."""

    config = BenchmarkConfig(
        name="joinorder",
        display_name="JoinOrder",
        scale_factor=0.1,
        compress_data=True,
        compression_type="zstd",
        compression_level=5,
        options={"force_regenerate": True},
    )

    profile = _make_system_profile()

    instance = get_benchmark_instance(config, profile)

    generator = instance._generator

    assert isinstance(instance, JoinOrderBenchmark)
    assert instance.parallel == profile.cpu_cores_logical
    # Compression settings are stored in the generator, not the benchmark
    assert instance.force_regenerate is True
    assert generator.compress_data is True
    assert generator.compression_type == "zstd"
    assert generator.compression_level == 5
    assert generator.force_regenerate is True
