from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from benchbox.core.tpcds.benchmark.runner import TPCDSBenchmark
from benchbox.core.validation import ValidationResult

pytestmark = pytest.mark.fast


@pytest.fixture
def tpcds_benchmark(tmp_path, monkeypatch):
    class FakeQueryManager:
        def get_query(self, query_id, **kwargs):
            return f"SELECT {query_id}"

        def get_all_queries(self, **kwargs):
            return {1: "SELECT 1"}

    class FakeDataGenerator:
        def __init__(self, scale_factor=1.0, parallel=1, output_dir=None, **kwargs):
            self.scale_factor = scale_factor
            self.parallel = parallel
            self.output_dir = Path(output_dir) if output_dir else Path.cwd()

        def generate(self):
            return {"store_sales": self.output_dir / "store_sales.dat"}

    class FakeCTools:
        def get_tools_info(self):
            return {"available_tools": []}

    monkeypatch.setattr("benchbox.core.tpcds.benchmark.runner.TPCDSQueryManager", FakeQueryManager)
    monkeypatch.setattr("benchbox.core.tpcds.benchmark.runner.TPCDSDataGenerator", FakeDataGenerator)
    monkeypatch.setattr("benchbox.core.tpcds.benchmark.runner.TPCDSCTools", FakeCTools)

    return TPCDSBenchmark(scale_factor=1.0, output_dir=tmp_path, verbose=False)


def test_get_query_validates_inputs(tpcds_benchmark):
    with pytest.raises(TypeError):
        tpcds_benchmark.get_query("1")
    with pytest.raises(ValueError):
        tpcds_benchmark.get_query(0)
    with pytest.raises(TypeError):
        tpcds_benchmark.get_query(1, seed="x")
    with pytest.raises(ValueError):
        tpcds_benchmark.get_query(1, scale_factor=0)


def test_get_query_variant_fallback_without_dsqgen(tpcds_benchmark):
    class NoDSQGen:
        def get_query(self, query_id, **kwargs):
            return f"SELECT {query_id}"

    tpcds_benchmark.query_manager = NoDSQGen()

    query = tpcds_benchmark.get_query(14, variant="a", dialect="duckdb", base_dialect="netezza")

    assert "SELECT" in query


def test_normalize_interval_syntax(tpcds_benchmark):
    raw = "select cast('2000-01-01' as date) + 60 days - 30 days"
    normalized = tpcds_benchmark._normalize_interval_syntax(raw)
    assert "+ INTERVAL 60 DAY" in normalized
    assert "- INTERVAL 30 DAY" in normalized


def test_fix_query58_ambiguity(tpcds_benchmark):
    query = (
        "WITH ss_items AS (SELECT 1 AS item_id), cs_items AS (SELECT 1 AS item_id), "
        'ws_items AS (SELECT 1 AS item_id) SELECT * FROM ss_items ORDER BY "item_id"'
    )
    fixed = tpcds_benchmark._fix_query58_ambiguity(query)
    assert 'ORDER BY "ss_items"."item_id"' in fixed


def test_get_create_tables_sql_uses_tuning_flags(tpcds_benchmark, monkeypatch):
    captured = {}

    def fake_get_create_all_tables_sql(enable_primary_keys=False, enable_foreign_keys=False):
        captured["pk"] = enable_primary_keys
        captured["fk"] = enable_foreign_keys
        return "-- schema"

    monkeypatch.setattr("benchbox.core.tpcds.schema.get_create_all_tables_sql", fake_get_create_all_tables_sql)

    tuning = SimpleNamespace(
        primary_keys=SimpleNamespace(enabled=True),
        foreign_keys=SimpleNamespace(enabled=False),
    )

    sql = tpcds_benchmark.get_create_tables_sql(tuning_config=tuning)

    assert sql == "-- schema"
    assert captured == {"pk": True, "fk": False}


def test_get_all_streams_info_skips_invalid(tpcds_benchmark, monkeypatch):
    def fake_get_stream_info(stream_id):
        if stream_id == 1:
            raise ValueError("bad stream")
        return {"stream_id": stream_id}

    monkeypatch.setattr(tpcds_benchmark, "get_stream_info", fake_get_stream_info)

    all_info = tpcds_benchmark.get_all_streams_info(num_streams=3)

    assert [item["stream_id"] for item in all_info] == [0, 2]


def test_validate_data_integrity_combines_results(tpcds_benchmark, monkeypatch):
    file_result = ValidationResult(is_valid=True, warnings=["w1"], details={"files": "ok"})
    db_result = ValidationResult(is_valid=False, errors=["db-fail"], details={"db": "bad"})

    monkeypatch.setattr(tpcds_benchmark, "validate_generated_data", lambda: file_result)
    monkeypatch.setattr(tpcds_benchmark, "validate_loaded_data", lambda _conn: db_result)

    combined = tpcds_benchmark.validate_data_integrity(connection=object())

    assert combined.is_valid is False
    assert combined.errors == ["db-fail"]
    assert combined.warnings == ["w1"]
    assert combined.details["file_validation"] == {"files": "ok"}
    assert combined.details["database_validation"] == {"db": "bad"}


def test_get_benchmark_info_includes_tools(tpcds_benchmark):
    info = tpcds_benchmark.get_benchmark_info()
    assert info["name"] == "TPC-DS"
    assert info["maintenance_test_supported"] is True
    assert "available_tools" in info["c_tools_info"]


class _Conn:
    def __init__(self, fail_on=None):
        self.fail_on = fail_on
        self.last_query = ""
        self.closed = False

    def execute(self, query, *args, **kwargs):
        self.last_query = str(query)
        if self.fail_on and self.fail_on in self.last_query:
            raise RuntimeError("forced failure")
        return self

    def fetchall(self):
        return [(1,)]

    def fetchone(self):
        return (0,)

    def close(self):
        self.closed = True


def test_run_throughput_test_success(tpcds_benchmark, monkeypatch):
    monkeypatch.setattr(tpcds_benchmark, "get_query", lambda qid, **kwargs: f"SELECT {qid}")

    result = tpcds_benchmark.run_throughput_test(
        connection_factory=lambda: _Conn(),
        num_streams=1,
        query_timeout=1,
        stream_timeout=10,
        max_retries=0,
    )

    assert result.streams_executed == 1
    assert result.streams_successful == 1
    assert result.success is True
    assert result.throughput_at_size > 0
    assert result.stream_results[0]["queries_executed"] == 99


def test_run_power_test_collects_query_error(tpcds_benchmark, monkeypatch):
    monkeypatch.setattr(tpcds_benchmark, "get_query", lambda qid, **kwargs: f"SELECT {qid}")

    conn = _Conn(fail_on="SELECT 42")
    result = tpcds_benchmark.run_power_test(connection=conn, warm_up=False, verbose=False)

    assert result["total_time"] >= 0
    assert len(result["query_results"]) == 99
    assert result["errors"]
    assert result["query_results"][42]["status"] == "failed"


def test_run_maintenance_test_maps_operations(tpcds_benchmark, monkeypatch):
    class FakeMaintenanceTest:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run(self, config):
            op = SimpleNamespace(
                operation_type="insert",
                table_name="store_sales",
                start_time=1.0,
                end_time=2.0,
                duration=1.0,
                rows_affected=10,
                success=True,
                error=None,
            )
            return {
                "total_time": 1.0,
                "total_operations": 1,
                "successful_operations": 1,
                "failed_operations": 0,
                "overall_throughput": 1.0,
                "operations": [op],
                "errors": [],
            }

    monkeypatch.setattr("benchbox.core.tpcds.maintenance_test.TPCDSMaintenanceTest", FakeMaintenanceTest)

    result = tpcds_benchmark.run_maintenance_test(connection=_Conn())
    assert result.total_operations == 1
    assert result.successful_operations == 1
    assert result.maintenance_operations[0]["operation"] == "insert_store_sales"


def test_validate_maintenance_data_integrity(tpcds_benchmark):
    conn = _Conn()
    result = tpcds_benchmark.validate_maintenance_data_integrity(connection=conn)
    assert len(result["validation_checks"]) == 3
    assert result["integrity_score"] == 1.0
    assert conn.closed is True


def test_run_official_benchmark_aggregates(tpcds_benchmark, monkeypatch):
    monkeypatch.setattr(
        tpcds_benchmark,
        "run_power_test",
        lambda **kwargs: {"total_time": 1.0, "power_at_size": 16.0},
    )
    monkeypatch.setattr(
        tpcds_benchmark,
        "run_throughput_test",
        lambda **kwargs: SimpleNamespace(throughput_at_size=9.0),
    )
    monkeypatch.setattr(
        tpcds_benchmark,
        "run_maintenance_test",
        lambda **kwargs: SimpleNamespace(successful_operations=1, total_operations=1),
    )

    result = tpcds_benchmark.run_official_benchmark(connection=_Conn(), num_streams=1)
    assert result["success"] is True
    assert result["power_at_size"] == 16.0
    assert result["throughput_at_size"] == 9.0
    assert result["qphds_at_size"] == pytest.approx(12.0)
