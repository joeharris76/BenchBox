"""Integration smoke tests for the TPC-Havoc benchmark with modular variants."""

from __future__ import annotations

import pytest

from benchbox.core.tpchavoc.benchmark import TPCHavocBenchmark

pytestmark = pytest.mark.fast


def test_benchmark_exposes_variant_helpers(tmp_path):
    benchmark = TPCHavocBenchmark(output_dir=tmp_path)

    variant_sql = benchmark.get_query_variant(2, 3)
    assert "select" in variant_sql.lower()

    descriptions = benchmark.get_all_variants_info(2)
    assert len(descriptions) == 10
    assert descriptions[3]["description"]

    all_variants = benchmark.get_all_variants(2)
    assert len(all_variants) == 10

    assert benchmark.get_variant_description(2, 1)
