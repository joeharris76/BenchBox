import json
from pathlib import Path

import pytest

from benchbox.core.tpcds_obt import schema
from benchbox.core.tpcds_obt.etl.transformer import SUPPORTED_CHANNELS, TPCDSOBTTransformer

pytestmark = pytest.mark.fast


class FakeConnection:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.statements: list[str] = []
        self.last_sql: str = ""

    def execute(self, sql: str) -> "FakeConnection":
        self.last_sql = " ".join(sql.split())
        self.statements.append(self.last_sql)

        if "COPY" in sql:
            start = sql.find("TO '") + 4
            end = sql.find("'", start)
            out_path = Path(sql[start:end])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("data\n")

        return self

    def fetchone(self) -> tuple[int]:
        if "has_return" in self.last_sql:
            return (5,)
        return (15,)

    def fetchall(self) -> list[tuple[str, int]]:
        return [("store", 10), ("web", 5)]

    def close(self) -> None:
        return None


class FakeDuckDB:
    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.last_connection: FakeConnection | None = None

    def connect(self, _: str) -> FakeConnection:
        self.last_connection = FakeConnection(self.tmp_path)
        return self.last_connection


def test_union_query_contains_channel_literal() -> None:
    transformer = TPCDSOBTTransformer()
    columns = schema.get_obt_columns("minimal")

    sql = transformer._build_union_query(columns, ["store"])

    assert "CREATE OR REPLACE TABLE tpcds_sales_returns_obt AS" in sql
    assert "CAST('store' AS VARCHAR(10)) AS channel" in sql
    assert "FROM store_sales ss" in sql


def test_transform_with_fake_duckdb_writes_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    fake_duckdb = FakeDuckDB(tmp_path)
    transformer = TPCDSOBTTransformer(duckdb_module=fake_duckdb)

    def fake_resolve(base_dir: Path, table_name: str) -> Path:
        path = base_dir / f"{table_name}.dat"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        return path

    monkeypatch.setattr(transformer, "_resolve_source_path", fake_resolve)

    result = transformer.transform(
        tpcds_dir=tmp_path,
        output_dir=tmp_path / "out",
        mode="minimal",
        channels=SUPPORTED_CHANNELS,
        output_format="dat",
        scale_factor=1.0,
    )

    manifest_path = result["manifest"]
    output_path = result["table"]

    assert output_path.exists()
    manifest = json.loads(Path(manifest_path).read_text())
    assert manifest["table"] == "tpcds_sales_returns_obt"
    assert manifest["rows_total"] == 15
    assert manifest["rows_with_returns"] == 5
    assert manifest["channels"] == list(SUPPORTED_CHANNELS)
    assert manifest["column_count"] == len(schema.get_obt_columns("minimal"))
