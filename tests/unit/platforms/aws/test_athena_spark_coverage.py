"""Additional coverage tests for Athena Spark adapter."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from benchbox.platforms.aws import AthenaSparkAdapter

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


def _adapter() -> AthenaSparkAdapter:
    with (
        patch("benchbox.platforms.aws.athena_spark_adapter.BOTO3_AVAILABLE", True),
        patch("benchbox.platforms.aws.athena_spark_adapter.boto3", MagicMock()),
        patch("benchbox.platforms.aws.athena_spark_adapter.CloudSparkStaging") as mock_staging,
    ):
        mock_staging.from_uri.return_value = MagicMock()
        return AthenaSparkAdapter(workgroup="wg", s3_staging_dir="s3://bucket/path")


def test_client_helpers_cache_clients() -> None:
    adapter = _adapter()

    athena_client = MagicMock()
    glue_client = MagicMock()
    s3_client = MagicMock()

    with patch("benchbox.platforms.aws.athena_spark_adapter.boto3") as mock_boto:
        mock_boto.client.side_effect = [athena_client, glue_client, s3_client]
        assert adapter._get_athena_client() is adapter._get_athena_client()
        assert adapter._get_glue_client() is adapter._get_glue_client()
        assert adapter._get_s3_client() is adapter._get_s3_client()


def test_close_clears_session_when_terminate_fails() -> None:
    adapter = _adapter()
    adapter._session_id = "session-1"
    client = MagicMock()
    client.terminate_session.side_effect = RuntimeError("terminate failed")

    with patch.object(adapter, "_get_athena_client", return_value=client):
        adapter.close()

    assert adapter._session_id is None


def test_apply_tuning_configuration_collects_results() -> None:
    adapter = _adapter()
    adapter.apply_primary_keys = MagicMock(return_value={"pk": 1})
    adapter.apply_foreign_keys = MagicMock(return_value={"fk": 1})
    adapter.apply_platform_optimizations = MagicMock(return_value={"opt": 1})

    config = SimpleNamespace(scale_factor=10.0, primary_keys=["a"], foreign_keys=["b"], platform={"x": 1})
    result = adapter.apply_tuning_configuration(config)

    assert adapter._scale_factor == 10.0
    assert result["primary_keys"] == {"pk": 1}
    assert result["foreign_keys"] == {"fk": 1}
    assert result["platform_optimizations"] == {"opt": 1}
