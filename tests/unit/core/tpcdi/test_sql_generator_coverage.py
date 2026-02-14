from __future__ import annotations

from types import SimpleNamespace

import pytest

import benchbox.core.tpcdi.generator.sql as sql_mod
from benchbox.core.tpcdi.generator.sql import TPCDISQLGenerator

pytestmark = [pytest.mark.fast, pytest.mark.unit]


class _Cursor:
    def __init__(self, row=(0,)):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    def __init__(self):
        self.calls: list[tuple[str, tuple]] = []
        self.counts = {"DimDate": 10, "DimTime": 5}

    def execute(self, query: str, params=()):
        self.calls.append((query, params))
        if "COUNT(*) FROM DimDate" in query:
            return _Cursor((self.counts["DimDate"],))
        if "COUNT(*) FROM DimTime" in query:
            return _Cursor((self.counts["DimTime"],))
        return _Cursor((None,))


def test_constructor_and_connection_lifecycle(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sql_mod, "duckdb", None)
    monkeypatch.setattr(TPCDISQLGenerator, "__del__", lambda self: None)
    with pytest.raises(ImportError, match="DuckDB is required"):
        TPCDISQLGenerator()

    fake_duckdb = SimpleNamespace(connect=lambda _s: SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(sql_mod, "duckdb", fake_duckdb)
    gen = TPCDISQLGenerator(scale_factor=0.1, connection=None, enable_progress=False)
    conn = gen._get_connection()
    assert conn is gen._get_connection()
    gen._close_temp_connection()
    assert gen.temp_connection is None


def test_lookup_and_table_generation_paths(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sql_mod, "duckdb", SimpleNamespace(connect=lambda _s: _Conn()))
    conn = _Conn()
    gen = TPCDISQLGenerator(scale_factor=0.1, connection=conn, enable_progress=True)

    gen._setup_lookup_tables(conn)
    assert len(conn.calls) >= 8

    n_date = gen.generate_date_dimension(conn, start_year=2020, end_year=2020)
    n_time = gen.generate_time_dimension(conn)
    assert n_date == 10 and n_time == 5
    assert "DimDate" in gen.generation_stats["generation_times"]
    assert "DimTime" in gen.generation_stats["generation_times"]

    # Exercise other generators with minimal mocked behavior and verify scale math.
    assert gen.generate_company_dimension(conn) == int(gen.base_companies * gen.scale_factor)
    assert gen.generate_security_dimension(conn) == int(gen.base_securities * gen.scale_factor)
    assert gen.generate_customer_dimension(conn) == int(gen.base_customers * gen.scale_factor)
    assert gen.generate_account_dimension(conn) == int(int(gen.base_customers * gen.scale_factor) * 1.5)
    assert gen.generate_trade_facts(conn) == int(gen.base_trades * gen.scale_factor)


def test_generate_all_tables_and_stats(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(sql_mod, "duckdb", SimpleNamespace(connect=lambda _s: _Conn()))
    conn = _Conn()
    gen = TPCDISQLGenerator(scale_factor=0.01, connection=conn, enable_progress=True)

    monkeypatch.setattr(gen, "_setup_lookup_tables", lambda _c: None)
    monkeypatch.setattr(gen, "generate_date_dimension", lambda _c: 2)
    monkeypatch.setattr(gen, "generate_time_dimension", lambda _c: 3)
    monkeypatch.setattr(gen, "generate_company_dimension", lambda _c: 4)
    monkeypatch.setattr(gen, "generate_security_dimension", lambda _c: 5)
    monkeypatch.setattr(gen, "generate_customer_dimension", lambda _c: 6)
    monkeypatch.setattr(gen, "generate_account_dimension", lambda _c: 7)
    monkeypatch.setattr(gen, "generate_trade_facts", lambda _c: 8)

    all_results = gen.generate_all_tables(conn)
    assert set(all_results) == {
        "DimDate",
        "DimTime",
        "DimCompany",
        "DimSecurity",
        "DimCustomer",
        "DimAccount",
        "FactTrade",
    }

    partial = gen.generate_all_tables(conn, tables=["DimDate", "FactTrade", "Unknown"])
    assert partial == {"DimDate": 2, "FactTrade": 8}

    stats = gen.get_generation_stats()
    assert stats["scale_factor"] == 0.01
    assert "estimated_records" in stats
