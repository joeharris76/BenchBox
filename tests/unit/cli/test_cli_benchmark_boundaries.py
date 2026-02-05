"""Tests for CLI benchmark boundaries (no direct core class imports)."""

from unittest.mock import MagicMock, patch

import pytest

from benchbox.cli.execution_pipeline import BenchmarkLoadingStage
from benchbox.core.config import BenchmarkConfig

pytestmark = pytest.mark.fast


def test_pipeline_uses_core_benchmark_loader():
    stage = BenchmarkLoadingStage()
    cfg = BenchmarkConfig(name="tpch", display_name="TPC-H", scale_factor=0.01)

    with patch("benchbox.core.benchmark_loader.get_benchmark_instance") as mock_loader:
        inst = MagicMock()
        mock_loader.return_value = inst

        loaded = stage._load_benchmark_instance(cfg)

        assert loaded is inst
        mock_loader.assert_called_once()
