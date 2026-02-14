"""Coverage tests for TPC-DS parameter extraction helpers."""

from __future__ import annotations

import pytest

from benchbox.core.tpcds import parameter_extractor as pe

pytestmark = [pytest.mark.fast, pytest.mark.unit]


def test_extract_common_params_primary_keys() -> None:
    sql = """
    where d_year = 2001
      and d_moy = 11
      and s_state = 'CA'
      and cd_gender = 'F'
      and cd_marital_status = 'M'
      and cd_education_status = 'College'
      and i_manufact_id = 427
    """
    expected = {"year", "month", "state", "gender", "marital_status", "education", "manufact_id"}

    out = pe._extract_common_params(sql, expected)

    assert out == {
        "year": 2001,
        "month": 11,
        "state": "CA",
        "gender": "F",
        "marital_status": "M",
        "education": "College",
        "manufact_id": 427,
    }


def test_extract_common_params_cd_alias_keys_and_missing_values() -> None:
    sql = "where ca_state = 'OR' and cd_gender = 'M' and cd_marital_status = 'S' and cd_education_status = 'Primary'"
    expected = {"state", "cd_gender", "cd_marital_status", "cd_education_status", "month"}

    out = pe._extract_common_params(sql, expected)

    assert out == {
        "state": "OR",
        "cd_gender": "M",
        "cd_marital_status": "S",
        "cd_education_status": "Primary",
    }


def test_extract_tpcds_parameters_filters_and_collects(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDSQGen:
        def generate(self, query_id: int, seed: int, scale_factor: float, stream_id: int | None) -> str:
            assert seed == 7
            assert scale_factor == 0.01
            assert stream_id == 3
            if query_id == 1:
                return "where d_year = 2000 and d_moy = 2"
            if query_id == 2:
                return "select 1"  # no extractable params
            raise RuntimeError("boom")

    monkeypatch.setattr("benchbox.core.tpcds.c_tools.DSQGenBinary", FakeDSQGen)
    monkeypatch.setattr(
        "benchbox.core.tpcds.dataframe_queries.parameters.TPCDS_DEFAULT_PARAMS",
        {
            1: {"year": 1998, "month": 1},
            2: {"state": "TX"},
            3: {"year": 1997},
            4: {},  # skipped empty defaults
        },
        raising=False,
    )

    out = pe.extract_tpcds_parameters(seed=7, scale_factor=0.01, stream_id=3, query_ids=[1, 2, 3, 4, 5])
    assert out == {1: {"year": 2000, "month": 2}}


def test_extract_tpcds_parameters_without_query_ids_uses_default_range(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeDSQGen:
        def __init__(self):
            self.calls = 0

        def generate(self, query_id: int, seed: int, scale_factor: float, stream_id: int | None) -> str:
            self.calls += 1
            return "where d_year = 1999"

    fake = FakeDSQGen()
    monkeypatch.setattr("benchbox.core.tpcds.c_tools.DSQGenBinary", lambda: fake)
    monkeypatch.setattr(
        "benchbox.core.tpcds.dataframe_queries.parameters.TPCDS_DEFAULT_PARAMS",
        {1: {"year": 1998}},
        raising=False,
    )

    out = pe.extract_tpcds_parameters(seed=1, scale_factor=1.0, stream_id=None, query_ids=None)
    assert out == {1: {"year": 1999}}
    assert fake.calls == 1


def test_get_tpcds_extracted_parameters_cache_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_extract(seed: int, scale_factor: float, stream_id: int | None):
        calls.append((seed, scale_factor, stream_id))
        return {1: {"seed": seed, "stream": stream_id}}

    monkeypatch.setattr(pe, "extract_tpcds_parameters", fake_extract)
    pe.clear_cache()

    first = pe.get_tpcds_extracted_parameters(11, 0.1, 2, use_cache=True)
    second = pe.get_tpcds_extracted_parameters(11, 0.1, 2, use_cache=True)
    third = pe.get_tpcds_extracted_parameters(11, 0.1, 2, use_cache=False)

    assert first == second == third == {1: {"seed": 11, "stream": 2}}
    assert calls == [(11, 0.1, 2), (11, 0.1, 2)]


def test_clear_cache_empties_store() -> None:
    pe._cache[(1, 1.0, None)] = {1: {"year": 1998}}
    pe.clear_cache()
    assert pe._cache == {}
