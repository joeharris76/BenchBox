from __future__ import annotations

import pytest

import benchbox.platforms.dataframe as dataframe_pkg

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def test_dataframe_package_exports_and_flags():
    assert "PolarsDataFrameAdapter" in dataframe_pkg.__all__
    assert "PandasDataFrameAdapter" in dataframe_pkg.__all__
    assert hasattr(dataframe_pkg, "POLARS_AVAILABLE")
    assert hasattr(dataframe_pkg, "PANDAS_AVAILABLE")
    assert hasattr(dataframe_pkg, "DATAFRAME_PLATFORMS")


def test_dataframe_package_optional_exports_are_defined():
    # Optional adapters may be None when dependencies are unavailable.
    assert hasattr(dataframe_pkg, "ModinDataFrameAdapter")
    assert hasattr(dataframe_pkg, "CuDFDataFrameAdapter")
    assert hasattr(dataframe_pkg, "DaskDataFrameAdapter")
    assert hasattr(dataframe_pkg, "DataFusionDataFrameAdapter")
    assert hasattr(dataframe_pkg, "PySparkDataFrameAdapter")
