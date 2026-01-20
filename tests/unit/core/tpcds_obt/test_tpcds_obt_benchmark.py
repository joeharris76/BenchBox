from pathlib import Path
from typing import Any

import pytest

from benchbox.core.tpcds_obt.benchmark import TPCDSOBTBenchmark
from benchbox.core.tpcds_obt.schema import OBT_TABLE_NAME

pytestmark = pytest.mark.fast


class StubGenerator:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.called = False

    def generate(self) -> dict[str, Path]:
        self.called = True
        sample = self.base_dir / "store_sales.dat"
        sample.parent.mkdir(parents=True, exist_ok=True)
        sample.touch()
        return {"store_sales": sample}


class StubTransformer:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.calls: list[dict[str, Any]] = []

    def transform(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        table_path = self.base_dir / f"{OBT_TABLE_NAME}.dat"
        manifest_path = self.base_dir / f"{OBT_TABLE_NAME}_manifest.json"
        table_path.parent.mkdir(parents=True, exist_ok=True)
        table_path.write_text("data\n")
        manifest_path.write_text('{"rows_total": 1}')
        return {"table": table_path, "manifest": manifest_path}


class StubCursor:
    def __init__(self) -> None:
        self.executed = None
        self.params = None

    def execute(self, query: str, params: dict[str, Any] | None = None) -> None:
        self.executed = query
        self.params = params

    def fetchall(self) -> list[tuple]:
        return [("ok",)]


def test_generate_data_invokes_generator_and_transformer(tmp_path: Path) -> None:
    benchmark = TPCDSOBTBenchmark(
        scale_factor=1.0,
        output_dir=tmp_path / "out",
        dimension_mode="minimal",
        channels=["store"],
        output_format="dat",
        force_regenerate=True,
    )
    stub_generator = StubGenerator(tmp_path)
    stub_transformer = StubTransformer(tmp_path)
    benchmark._data_generator = stub_generator  # type: ignore[assignment]
    benchmark._obt_transformer = stub_transformer  # type: ignore[assignment]

    result = benchmark.generate_data()

    assert stub_generator.called is True
    assert len(stub_transformer.calls) == 1
    call = stub_transformer.calls[0]
    assert call["mode"] == "minimal"
    assert call["channels"] == ["store"]
    assert result["table"].exists()
    assert benchmark.tables["tpcds_sales_returns_obt"] == result["table"]
    assert benchmark.manifest == result["manifest"]


def test_get_query_and_execute_query(tmp_path: Path) -> None:
    benchmark = TPCDSOBTBenchmark(scale_factor=1.0, output_dir=tmp_path / "out", force_regenerate=True)

    # Use TPC-DS query 3 - a simple store sales query
    sql = benchmark.get_query(3)
    assert OBT_TABLE_NAME in sql

    cursor = StubCursor()
    results = benchmark.execute_query(3, cursor)
    assert results == [("ok",)]
    assert cursor.executed is not None
