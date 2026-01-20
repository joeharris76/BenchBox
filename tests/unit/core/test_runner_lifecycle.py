"""Unit tests for core lifecycle runner."""

import json
import logging
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from benchbox.base import BaseBenchmark
from benchbox.core.config import BenchmarkConfig, DatabaseConfig, SystemProfile
from benchbox.core.results.models import BenchmarkResults
from benchbox.core.runner import LifecyclePhases, ValidationOptions, run_benchmark_lifecycle
from benchbox.core.validation import ValidationResult
from benchbox.utils.verbosity import VerbosityMixin, VerbositySettings

pytestmark = pytest.mark.fast


def _mk_system_profile():
    return SystemProfile(
        os_name="Linux",
        os_version="6.6",
        architecture="x86_64",
        cpu_model="Test CPU",
        cpu_cores_physical=4,
        cpu_cores_logical=8,
        memory_total_gb=16.0,
        memory_available_gb=12.0,
        python_version="3.11",
        disk_space_gb=256.0,
        timestamp=datetime.now(),
        hostname="host",
    )


@pytest.mark.unit
def test_data_only_mode_returns_result_without_adapter():
    cfg = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="data_only")
    bench = MagicMock()
    bench.output_dir = None
    bench.tables = None

    expected = BenchmarkResults(
        benchmark_name="TPC-H",
        platform="data_only",
        scale_factor=cfg.scale_factor,
        execution_id="eid",
        timestamp=datetime.now(),
        duration_seconds=0.0,
        query_definitions={},
        total_queries=0,
        successful_queries=0,
        failed_queries=0,
        total_execution_time=0.0,
        average_query_time=0.0,
        execution_phases=None,
        test_execution_type="data_only",
    )

    def _create_enhanced(platform, query_results, **kwargs):
        return expected

    bench.create_enhanced_benchmark_result = _create_enhanced
    bench.generate_data = MagicMock()

    res = run_benchmark_lifecycle(
        benchmark_config=cfg,
        database_config=None,
        system_profile=_mk_system_profile(),
        output_root=None,
        benchmark_instance=bench,
    )

    assert isinstance(res, BenchmarkResults)
    assert res.platform == "data_only"
    # No adapter interaction


@pytest.mark.unit
def test_load_only_mode_invokes_adapter_load(tmp_path):
    cfg = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="load_only")
    db = DatabaseConfig(type="duckdb", name="test")
    bench = MagicMock()
    bench.output_dir = tmp_path
    bench.tables = None

    def _create_enhanced(platform, query_results, **kwargs):
        return BenchmarkResults(
            benchmark_name="TPC-H",
            platform=platform,
            scale_factor=cfg.scale_factor,
            execution_id="eid",
            timestamp=datetime.now(),
            duration_seconds=kwargs.get("duration_seconds", 0.0),
            query_definitions={},
            total_queries=len(query_results),
            successful_queries=0,
            failed_queries=0,
            total_execution_time=kwargs.get("duration_seconds", 0.0),
            average_query_time=0.0,
            execution_phases=kwargs.get("phases"),
            test_execution_type="load_only",
        )

    bench.create_enhanced_benchmark_result = _create_enhanced
    bench.generate_data = MagicMock()

    class DummyAdapter:
        platform_name = "duckdb"

        def __init__(self):
            self.load_called = False
            self.schema_created = False
            self.closed = False

        def create_connection(self, **kwargs):  # pragma: no cover - simple mock
            return Mock()

        def close_connection(self, _conn):  # pragma: no cover - simple mock
            self.closed = True

        def create_schema(self, benchmark, connection):
            self.schema_created = True
            return 0.1

        def load_data(self, benchmark, connection, data_dir):
            self.load_called = True
            return {"table1": 10}, 0.5, None

    adapter_instance = DummyAdapter()

    with patch("benchbox.core.runner.runner.get_platform_adapter", return_value=adapter_instance):
        res = run_benchmark_lifecycle(
            benchmark_config=cfg,
            database_config=db,
            system_profile=_mk_system_profile(),
            platform_config={"database_path": "test.duckdb"},
            phases=LifecyclePhases(generate=False, load=True, execute=False),
            output_root=str(tmp_path),
            benchmark_instance=bench,
        )

    assert adapter_instance.schema_created is True
    assert adapter_instance.load_called is True
    assert isinstance(res, BenchmarkResults)
    assert res.platform == "duckdb"


@pytest.mark.unit
def test_standard_mode_delegates_to_adapter_run_benchmark():
    cfg = BenchmarkConfig(name="tpch", display_name="TPC-H", test_execution_type="standard")
    db = DatabaseConfig(type="duckdb", name="test")
    bench = MagicMock()
    bench.output_dir = None
    bench.tables = None

    expected = BenchmarkResults(
        benchmark_name="TPC-H",
        platform="duckdb",
        scale_factor=cfg.scale_factor,
        execution_id="eid",
        timestamp=datetime.now(),
        duration_seconds=1.0,
        query_definitions={},
        total_queries=1,
        successful_queries=1,
        failed_queries=0,
        total_execution_time=1.0,
        average_query_time=1.0,
        execution_phases=None,
        test_execution_type="standard",
    )

    mock_adapter = MagicMock()
    mock_adapter.run_benchmark.return_value = expected
    mock_adapter.load_data.return_value = ({"table1": 10}, 0.5, None)
    mock_adapter.create_connection.return_value = Mock()
    mock_adapter.close_connection = Mock()
    with patch("benchbox.core.runner.runner.get_platform_adapter", return_value=mock_adapter):
        res = run_benchmark_lifecycle(
            benchmark_config=cfg,
            database_config=db,
            system_profile=_mk_system_profile(),
            platform_config={"database_path": "test.duckdb"},
            output_root=None,
            benchmark_instance=bench,
        )

    mock_adapter.run_benchmark.assert_called_once()
    assert res is expected


@pytest.mark.unit
def test_preflight_and_manifest_validation(tmp_path):
    cfg = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=1.0,
        options={
            "enable_preflight_validation": True,
            "enable_postgen_manifest_validation": True,
        },
    )
    bench = Mock()
    bench.name = "tpch"
    bench.scale_factor = 1.0
    bench.tables = None
    bench.generate_data = Mock()
    bench.validate_preflight.return_value = ValidationResult(True, [], [])
    bench.validate_manifest.return_value = ValidationResult(True, [], [])

    expected = BenchmarkResults(
        benchmark_name="TPC-H",
        platform="unknown",
        scale_factor=cfg.scale_factor,
        execution_id="eid",
        timestamp=datetime.now(),
        duration_seconds=0.0,
        query_definitions={},
        total_queries=0,
        successful_queries=0,
        failed_queries=0,
        total_execution_time=0.0,
        average_query_time=0.0,
    )

    bench.create_enhanced_benchmark_result.return_value = expected
    manifest_path = Path(tmp_path) / "_datagen_manifest.json"
    manifest_path.write_text('{"benchmark": "tpch", "scale_factor": 1.0, "tables": {}}')
    bench.output_dir = tmp_path

    with patch("benchbox.core.runner.runner.get_benchmark_instance", return_value=bench):
        result = run_benchmark_lifecycle(
            benchmark_config=cfg,
            database_config=None,
            system_profile=_mk_system_profile(),
            phases=LifecyclePhases(generate=True, load=False, execute=False),
            validation_opts=ValidationOptions(
                enable_preflight_validation=True,
                enable_postgen_manifest_validation=True,
            ),
            output_root=str(tmp_path),
        )

    assert result is expected
    stages = result.validation_details.get("stages", [])
    stage_names = {stage["stage"] for stage in stages}
    assert {"preflight", "post_generation_manifest"} <= stage_names
    assert result.validation_status in {"PASSED", "WARNINGS"}


@pytest.mark.unit
def test_preflight_validation_failure_raises(tmp_path):
    cfg = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=1.0,
        options={"enable_preflight_validation": True},
    )
    bench = Mock()
    bench.name = "tpch"
    bench.scale_factor = 1.0
    bench.tables = None
    bench.validate_preflight.return_value = ValidationResult(False, ["disk"], [])
    bench.output_dir = tmp_path

    with patch("benchbox.core.runner.runner.get_benchmark_instance", return_value=bench):
        with pytest.raises(RuntimeError, match="Preflight validation failed: disk"):
            run_benchmark_lifecycle(
                benchmark_config=cfg,
                database_config=None,
                system_profile=_mk_system_profile(),
                phases=LifecyclePhases(generate=True, load=False, execute=False),
                validation_opts=ValidationOptions(enable_preflight_validation=True),
                output_root=str(tmp_path),
            )


@pytest.mark.unit
def test_postload_validation_invoked(tmp_path):
    cfg = BenchmarkConfig(
        name="tpch",
        display_name="TPC-H",
        scale_factor=1.0,
        options={"enable_postload_validation": True},
    )
    db = DatabaseConfig(type="duckdb", name="duckdb")
    bench = Mock()
    bench.scale_factor = 1.0
    bench.tables = {}
    bench.output_dir = str(tmp_path)
    bench.create_enhanced_benchmark_result.return_value = BenchmarkResults(
        benchmark_name="TPC-H",
        platform="duckdb",
        scale_factor=1.0,
        execution_id="eid",
        timestamp=datetime.now(),
        duration_seconds=0.0,
        query_definitions={},
        total_queries=0,
        successful_queries=0,
        failed_queries=0,
        total_execution_time=0.0,
        average_query_time=0.0,
    )

    mock_adapter = Mock()
    mock_adapter.platform_name = "duckdb"
    mock_adapter.create_connection.return_value = Mock()
    mock_adapter.close_connection = Mock()
    mock_adapter.run_benchmark.return_value = bench.create_enhanced_benchmark_result.return_value

    validation_result = ValidationResult(True, [], [])

    with (
        patch("benchbox.core.runner.runner.get_benchmark_instance", return_value=bench),
        patch("benchbox.core.runner.runner.get_platform_adapter", return_value=mock_adapter),
        patch("benchbox.core.validation.DatabaseValidationEngine") as mock_db_engine,
    ):
        mock_db_engine.return_value.validate_loaded_data.return_value = validation_result

        result = run_benchmark_lifecycle(
            benchmark_config=cfg,
            database_config=db,
            system_profile=_mk_system_profile(),
            platform_config={"database_path": str(tmp_path / "db.duckdb")},
            phases=LifecyclePhases(generate=False, load=False, execute=True),
            validation_opts=ValidationOptions(enable_postload_validation=True),
        )

    stages = result.validation_details.get("stages", [])
    assert any(stage["stage"] == "post_load" for stage in stages)
    mock_db_engine.return_value.validate_loaded_data.assert_called_once()


@pytest.mark.unit
def test_run_benchmark_lifecycle_propagates_verbosity(tmp_path):
    """Verbosity settings should reach both benchmark and adapter."""

    class StubBenchmark(VerbosityMixin):
        def __init__(self):
            self.logger = logging.getLogger("stub-benchmark")
            self.output_dir = tmp_path
            self.tables = None
            self.apply_verbosity(VerbositySettings.default())

        def generate_data(self):
            self.tables = {"table": tmp_path / "data.tbl"}
            return []

        def create_enhanced_benchmark_result(self, **kwargs):
            return BenchmarkResults(
                benchmark_name="TPC-H",
                platform="stub",
                scale_factor=cfg.scale_factor,
                execution_id="eid",
                timestamp=datetime.now(),
                duration_seconds=0.0,
                query_definitions={},
                total_queries=0,
                successful_queries=0,
                failed_queries=0,
                total_execution_time=0.0,
                average_query_time=0.0,
                execution_phases=None,
                test_execution_type="standard",
            )

    class StubAdapter(VerbosityMixin):
        platform_name = "stub"

        def __init__(self):
            self.logger = logging.getLogger("stub-adapter")
            self.apply_verbosity(VerbositySettings.default())
            self.last_run_kwargs = None
            self.enable_validation = False

        def run_benchmark(self, benchmark, **run_config):
            self.last_run_kwargs = run_config
            return BenchmarkResults(
                benchmark_name="TPC-H",
                platform=self.platform_name,
                scale_factor=cfg.scale_factor,
                execution_id="eid",
                timestamp=datetime.now(),
                duration_seconds=0.1,
                query_definitions={},
                total_queries=0,
                successful_queries=0,
                failed_queries=0,
                total_execution_time=0.1,
                average_query_time=0.0,
                execution_phases=None,
                test_execution_type="standard",
            )

    cfg = BenchmarkConfig(name="tpch", display_name="TPC-H")
    db = DatabaseConfig(type="duckdb", name="test", options={})
    benchmark = StubBenchmark()
    adapter = StubAdapter()

    verbosity = VerbositySettings.from_flags(2, False)

    with patch("benchbox.core.runner.runner.get_platform_adapter", return_value=adapter):
        result = run_benchmark_lifecycle(
            benchmark_config=cfg,
            database_config=db,
            system_profile=_mk_system_profile(),
            platform_config={"database_path": str(tmp_path / "db.duckdb")},
            benchmark_instance=benchmark,
            verbosity=verbosity,
        )

    assert isinstance(result, BenchmarkResults)
    assert benchmark.verbose_level == 2
    assert benchmark.verbose_enabled is True
    assert adapter.verbose_level == 2
    assert adapter.verbose_enabled is True
    assert adapter.last_run_kwargs is not None
    assert adapter.last_run_kwargs["verbose"] is True
    assert adapter.last_run_kwargs["verbose_level"] == 2
    assert adapter.last_run_kwargs["quiet"] is False


@pytest.mark.unit
def test_manifest_reuse_accepts_data_source_alias(tmp_path):
    """Manifest reuse should work when benchmark declares a data-source alias."""

    data_dir = tmp_path / "tpch_sf1"
    data_dir.mkdir()
    customer_path = data_dir / "customer.tbl"
    contents = "1|cust\n"
    customer_path.write_text(contents)

    manifest = {
        "benchmark": "tpch",
        "scale_factor": 1.0,
        "tables": {
            "customer": [
                {
                    "path": "customer.tbl",
                    "size_bytes": customer_path.stat().st_size,
                    "row_count": 1,
                }
            ]
        },
    }
    (data_dir / "_datagen_manifest.json").write_text(json.dumps(manifest))

    class AliasBenchmark(BaseBenchmark):
        def __init__(self, output_dir: Path):
            super().__init__(scale_factor=1.0, output_dir=output_dir)
            self.tables = {}

        def get_data_source_benchmark(self) -> str:
            return "tpch"

        def generate_data(self):  # pragma: no cover - patched during test
            raise AssertionError("generate_data should not be called when manifest is reused")

        def get_queries(self, dialect: str | None = None):
            return {}

        def get_query(self, query_id, *, params=None):
            raise ValueError("Invalid query ID")

    benchmark = AliasBenchmark(data_dir)
    benchmark.generate_data = Mock()

    cfg = BenchmarkConfig(
        name="read_primitives",
        display_name="Primitives",
        scale_factor=1.0,
        compress_data=False,
        options={"force_regenerate": False, "no_regenerate": False},
    )

    result = run_benchmark_lifecycle(
        benchmark_config=cfg,
        database_config=None,
        system_profile=None,
        phases=LifecyclePhases(generate=True, load=False, execute=False),
        benchmark_instance=benchmark,
    )

    assert isinstance(result, BenchmarkResults)
    benchmark.generate_data.assert_not_called()
    assert "customer" in benchmark.tables
    assert benchmark.tables["customer"] == customer_path


@pytest.mark.unit
def test_no_regenerate_respects_alias_manifest(tmp_path):
    """no_regenerate should pass when shared manifest is valid."""

    data_dir = tmp_path / "tpch_sf1"
    data_dir.mkdir()
    orders_path = data_dir / "orders.tbl"
    orders_path.write_text("1|order\n")
    manifest = {
        "benchmark": "tpch",
        "scale_factor": 1.0,
        "tables": {
            "orders": [
                {
                    "path": "orders.tbl",
                    "size_bytes": orders_path.stat().st_size,
                    "row_count": 1,
                }
            ]
        },
    }
    (data_dir / "_datagen_manifest.json").write_text(json.dumps(manifest))

    class AliasBenchmark(BaseBenchmark):
        def __init__(self, output_dir: Path):
            super().__init__(scale_factor=1.0, output_dir=output_dir)
            self.tables = {}

        def get_data_source_benchmark(self) -> str:
            return "tpch"

        def generate_data(self):  # pragma: no cover - patched during test
            raise AssertionError("generate_data should not run when no_regenerate is set")

        def get_queries(self, dialect: str | None = None):
            return {}

        def get_query(self, query_id, *, params=None):
            raise ValueError("Invalid query ID")

    benchmark = AliasBenchmark(data_dir)
    benchmark.generate_data = Mock()

    cfg = BenchmarkConfig(
        name="read_primitives",
        display_name="Primitives",
        scale_factor=1.0,
        compress_data=False,
        options={"force_regenerate": False, "no_regenerate": True},
    )

    result = run_benchmark_lifecycle(
        benchmark_config=cfg,
        database_config=None,
        system_profile=None,
        phases=LifecyclePhases(generate=True, load=False, execute=False),
        benchmark_instance=benchmark,
    )

    assert isinstance(result, BenchmarkResults)
    benchmark.generate_data.assert_not_called()
    assert "orders" in benchmark.tables
