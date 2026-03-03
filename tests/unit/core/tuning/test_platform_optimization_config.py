"""Tests for platform optimization strategy configuration."""

from __future__ import annotations

import pytest

from benchbox.core.tuning.interface import (
    PlatformOptimizationConfiguration,
    TuningType,
    UnifiedTuningConfiguration,
)

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_platform_optimization_defaults_include_sorted_ingestion_and_databricks_strategy() -> None:
    config = PlatformOptimizationConfiguration()
    assert config.sorted_ingestion_mode == "off"
    assert config.sorted_ingestion_method == "auto"
    assert config.databricks_clustering_strategy == "z_order"
    assert config.liquid_clustering_enabled is False
    assert config.liquid_clustering_columns == []


def test_platform_optimization_from_dict_supports_deep_sort_aliases() -> None:
    config = PlatformOptimizationConfiguration.from_dict(
        {
            "deep_sort_mode": "force",
            "deep_sort_method": "ctas",
            "databricks_clustering_strategy": "liquid_clustering",
            "liquid_clustering_enabled": True,
            "liquid_clustering_columns": ["event_time"],
        }
    )
    assert config.sorted_ingestion_mode == "force"
    assert config.sorted_ingestion_method == "ctas"
    assert config.databricks_clustering_strategy == "liquid_clustering"
    assert config.liquid_clustering_enabled is True
    assert config.liquid_clustering_columns == ["event_time"]


def test_platform_optimization_accepts_hilbert_method() -> None:
    config = PlatformOptimizationConfiguration(sorted_ingestion_mode="force", sorted_ingestion_method="hilbert")
    assert config.sorted_ingestion_method == "hilbert"


def test_platform_optimization_rejects_invalid_sorted_ingestion_mode() -> None:
    with pytest.raises(ValueError, match="Invalid sorted_ingestion_mode"):
        PlatformOptimizationConfiguration(sorted_ingestion_mode="invalid")  # type: ignore[arg-type]


def test_platform_optimization_rejects_method_when_mode_off() -> None:
    with pytest.raises(ValueError, match="sorted_ingestion_method must be 'auto'"):
        PlatformOptimizationConfiguration(sorted_ingestion_mode="off", sorted_ingestion_method="ctas")


def test_platform_optimization_rejects_liquid_columns_without_enablement() -> None:
    with pytest.raises(ValueError, match="liquid_clustering_columns requires liquid_clustering_enabled=true"):
        PlatformOptimizationConfiguration(liquid_clustering_columns=["event_time"])


def test_unified_tuning_enable_disable_liquid_clustering() -> None:
    config = UnifiedTuningConfiguration()
    config.enable_platform_optimization(TuningType.LIQUID_CLUSTERING, columns=["event_time"])

    assert config.platform_optimizations.liquid_clustering_enabled is True
    assert config.platform_optimizations.databricks_clustering_strategy == "liquid_clustering"
    assert config.platform_optimizations.liquid_clustering_columns == ["event_time"]
    assert TuningType.LIQUID_CLUSTERING in config.get_enabled_tuning_types()

    config.disable_platform_optimization(TuningType.LIQUID_CLUSTERING)
    assert config.platform_optimizations.liquid_clustering_enabled is False
