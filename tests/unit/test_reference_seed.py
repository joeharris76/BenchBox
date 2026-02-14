"""Tests for TPC-H reference seed and qgen defaults mode parity.

Validates that:
1. The TPCH_SF1_REFERENCE_SEED constant matches the octal literal
2. qgen defaults mode (-d flag) produces parameters matching answer files
3. Seed-based mode produces different parameters (confirming -d is distinct)
4. TPCHPowerTestConfig defaults to seed=None (defaults mode)

Copyright 2026 Joe Harris / BenchBox Project

TPC Benchmark™ H (TPC-H) - Copyright © Transaction Processing Performance Council
This implementation is based on the TPC-H specification.

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import pytest

from benchbox.core.tpch.benchmark import TPCH_SF1_REFERENCE_SEED
from benchbox.core.tpch.power_test import TPCHPowerTestConfig


@pytest.mark.unit
@pytest.mark.fast
class TestReferenceSeedConstant:
    """Test the TPCH_SF1_REFERENCE_SEED constant."""

    def test_reference_seed_matches_octal(self):
        """Verify octal 0o0101000000 equals 17039360 decimal."""
        assert TPCH_SF1_REFERENCE_SEED == 17039360
        assert TPCH_SF1_REFERENCE_SEED == 0o0101000000

    def test_reference_seed_is_not_16843008(self):
        """Ensure the old incorrect decimal value is not used."""
        assert TPCH_SF1_REFERENCE_SEED != 16843008


@pytest.mark.unit
@pytest.mark.fast
class TestPowerTestConfigDefaults:
    """Test TPCHPowerTestConfig default values."""

    def test_default_seed_is_none(self):
        """Default seed should be None to trigger qgen defaults mode."""
        config = TPCHPowerTestConfig()
        assert config.seed is None

    def test_explicit_seed_preserved(self):
        """Explicit seed values should be preserved."""
        config = TPCHPowerTestConfig(seed=42)
        assert config.seed == 42

        config_ref = TPCHPowerTestConfig(seed=TPCH_SF1_REFERENCE_SEED)
        assert config_ref.seed == TPCH_SF1_REFERENCE_SEED


@pytest.mark.unit
@pytest.mark.tpch
class TestQgenDefaultsMode:
    """Test qgen defaults mode (-d flag) vs seed-based mode.

    These tests require the qgen binary to be available.
    """

    @pytest.fixture
    def qgen(self):
        """Get QGenBinary instance, skip if qgen not available."""
        try:
            from benchbox.core.tpch.queries import QGenBinary

            return QGenBinary()
        except RuntimeError:
            pytest.skip("qgen binary not available")

    def test_defaults_mode_q3_contains_building(self, qgen):
        """qgen -d for Q3 should produce BUILDING segment (matching answer files)."""
        sql = qgen.generate(query_id=3, seed=None, scale_factor=1.0)
        assert "BUILDING" in sql.upper(), f"Expected BUILDING in Q3 defaults output, got: {sql[:200]}"

    def test_seed1_q3_contains_furniture(self, qgen):
        """qgen -r 1 for Q3 should produce FURNITURE segment (different from defaults)."""
        sql = qgen.generate(query_id=3, seed=1, scale_factor=1.0)
        assert "FURNITURE" in sql.upper(), f"Expected FURNITURE in Q3 seed=1 output, got: {sql[:200]}"

    def test_defaults_mode_differs_from_seed1(self, qgen):
        """Defaults mode and seed=1 should produce different parameters for Q3."""
        sql_defaults = qgen.generate(query_id=3, seed=None, scale_factor=1.0)
        sql_seed1 = qgen.generate(query_id=3, seed=1, scale_factor=1.0)
        assert sql_defaults != sql_seed1, "Defaults mode and seed=1 should produce different SQL"

    def test_defaults_mode_q6_discount(self, qgen):
        """qgen -d for Q6 should produce .06 discount (matching answer files)."""
        sql = qgen.generate(query_id=6, seed=None, scale_factor=1.0)
        assert ".06" in sql, f"Expected .06 discount in Q6 defaults output, got: {sql[:200]}"

    def test_defaults_mode_q5_asia(self, qgen):
        """qgen -d for Q5 should produce ASIA region (matching answer files)."""
        sql = qgen.generate(query_id=5, seed=None, scale_factor=1.0)
        assert "ASIA" in sql.upper(), f"Expected ASIA in Q5 defaults output, got: {sql[:200]}"
