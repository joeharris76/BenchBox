from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from benchbox.platforms.base.spark_execution_mixin import SparkDataLoadMixin

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


class _DummySparkAdapter(SparkDataLoadMixin):
    def __init__(self) -> None:
        self.logger = MagicMock()

    def log_verbose(self, msg: str) -> None:
        return None

    def _normalize_and_validate_file_paths(self, file_paths):
        return [Path(path) for path in (file_paths if isinstance(file_paths, list) else [file_paths])]


def _make_dataframe(columns: list[str] | None = None) -> MagicMock:
    df = MagicMock()
    df.columns = columns or []
    df.cache.return_value = df
    df.count.return_value = 5
    df.write.mode.return_value = df.write
    return df


def _run_load(adapter: _DummySparkAdapter, spark: MagicMock, table_path: Path) -> None:
    benchmark = MagicMock()
    with patch("benchbox.platforms.base.data_loading.DataSourceResolver.resolve") as mock_resolve:
        mock_resolve.return_value = SimpleNamespace(source_type="benchmark_tables", tables={"orders": [table_path]})
        stats, _, _ = adapter._load_data_spark(benchmark, table_path.parent, spark)
    assert stats["orders"] == 5


def test_load_data_spark_uses_delta_reader_for_delta_directory(tmp_path: Path) -> None:
    adapter = _DummySparkAdapter()
    spark = MagicMock()
    delta_dir = tmp_path / "orders"
    (delta_dir / "_delta_log").mkdir(parents=True)
    delta_df = _make_dataframe(["id"])
    spark.read.format.return_value.load.return_value = delta_df

    _run_load(adapter, spark, delta_dir)

    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with(str(delta_dir))


def test_load_data_spark_uses_iceberg_reader_for_iceberg_directory(tmp_path: Path) -> None:
    adapter = _DummySparkAdapter()
    spark = MagicMock()
    iceberg_dir = tmp_path / "orders"
    (iceberg_dir / "metadata").mkdir(parents=True)
    iceberg_df = _make_dataframe(["id"])
    spark.read.format.return_value.load.return_value = iceberg_df

    _run_load(adapter, spark, iceberg_dir)

    spark.read.format.assert_called_once_with("iceberg")
    spark.read.format.return_value.load.assert_called_once_with(str(iceberg_dir))


def test_load_data_spark_uses_hudi_reader_for_hudi_directory(tmp_path: Path) -> None:
    adapter = _DummySparkAdapter()
    spark = MagicMock()
    hudi_dir = tmp_path / "orders"
    (hudi_dir / ".hoodie").mkdir(parents=True)
    hudi_df = _make_dataframe(["id"])
    spark.read.format.return_value.load.return_value = hudi_df

    _run_load(adapter, spark, hudi_dir)

    spark.read.format.assert_called_once_with("hudi")
    spark.read.format.return_value.load.assert_called_once_with(str(hudi_dir))


def test_load_data_spark_parquet_path_unchanged(tmp_path: Path) -> None:
    adapter = _DummySparkAdapter()
    spark = MagicMock()
    parquet_path = tmp_path / "orders.parquet"
    parquet_path.write_bytes(b"PAR1")
    parquet_df = _make_dataframe(["id"])
    spark.read.parquet.return_value = parquet_df

    _run_load(adapter, spark, parquet_path)

    spark.read.parquet.assert_called_once_with(str(parquet_path))
    spark.read.format.assert_not_called()
