"""Cloud Spark adapter integration smoke tests.

Tests for:
- AWS Athena Spark
- AWS EMR Serverless
- GCP Dataproc
- GCP Dataproc Serverless

These adapters use session-based/batch-based execution patterns that differ from
traditional SQL adapters. The stubs simulate the AWS/GCP APIs without requiring
real cloud services.
"""

import pytest

from benchbox.core.exceptions import ConfigurationError

from .common import (
    CloudSparkStubState,
    install_athena_spark_stub,
    install_dataproc_serverless_stub,
    install_dataproc_stub,
    install_emr_serverless_stub,
)  # fmt: skip

# ---------------------------------------------------------------------------
# AWS Athena Spark Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_athena_spark_requires_workgroup(monkeypatch):
    """Test that Athena Spark adapter requires workgroup configuration."""
    install_athena_spark_stub(monkeypatch)

    from benchbox.platforms.aws import AthenaSparkAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        AthenaSparkAdapter(s3_staging_dir="s3://bucket/path")

    assert "workgroup" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_athena_spark_requires_s3_staging(monkeypatch):
    """Test that Athena Spark adapter requires S3 staging directory."""
    install_athena_spark_stub(monkeypatch)

    from benchbox.platforms.aws import AthenaSparkAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        AthenaSparkAdapter(workgroup="spark-workgroup")

    assert "s3" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_athena_spark_validates_s3_path(monkeypatch):
    """Test that Athena Spark adapter validates S3 path format."""
    install_athena_spark_stub(monkeypatch)

    from benchbox.platforms.aws import AthenaSparkAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        AthenaSparkAdapter(
            workgroup="spark-workgroup",
            s3_staging_dir="/invalid/path",  # Not an S3 path
        )

    assert "s3://" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_athena_spark_platform_info(monkeypatch):
    """Test Athena Spark platform info retrieval."""
    state: CloudSparkStubState = install_athena_spark_stub(monkeypatch)

    from benchbox.platforms.aws import AthenaSparkAdapter

    adapter = AthenaSparkAdapter(
        workgroup="spark-workgroup",
        s3_staging_dir=f"s3://{state.bucket}/staging/",
        region=state.region,
    )

    info = adapter.get_platform_info()

    assert info["platform"] == "athena-spark"
    assert info["vendor"] == "AWS"
    assert info["region"] == state.region
    assert info["supports_sql"] is True
    assert info["supports_dataframe"] is True


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_athena_spark_configure_for_benchmark(monkeypatch):
    """Test Athena Spark benchmark configuration via CloudSparkConfigMixin."""
    state: CloudSparkStubState = install_athena_spark_stub(monkeypatch)

    from benchbox.platforms.aws import AthenaSparkAdapter

    adapter = AthenaSparkAdapter(
        workgroup="spark-workgroup",
        s3_staging_dir=f"s3://{state.bucket}/staging/",
        region=state.region,
    )

    # Should not raise - uses inherited CloudSparkConfigMixin
    adapter.configure_for_benchmark(None, "tpch")
    assert adapter._benchmark_type == "tpch"
    assert adapter._spark_config is not None


# ---------------------------------------------------------------------------
# AWS EMR Serverless Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_emr_serverless_requires_s3_staging(monkeypatch):
    """Test that EMR Serverless adapter requires S3 staging directory."""
    install_emr_serverless_stub(monkeypatch)

    from benchbox.platforms.aws import EMRServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        EMRServerlessAdapter(
            application_id="app-12345",
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
        )

    assert "s3" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_emr_serverless_requires_execution_role(monkeypatch):
    """Test that EMR Serverless adapter requires execution role ARN."""
    install_emr_serverless_stub(monkeypatch)

    from benchbox.platforms.aws import EMRServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        EMRServerlessAdapter(
            application_id="app-12345",
            s3_staging_dir="s3://bucket/path",
        )

    assert "execution_role" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_emr_serverless_requires_application_id_or_create(monkeypatch):
    """Test that EMR Serverless requires application_id or create_application."""
    install_emr_serverless_stub(monkeypatch)

    from benchbox.platforms.aws import EMRServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        EMRServerlessAdapter(
            s3_staging_dir="s3://bucket/path",
            execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
        )

    assert "application" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_emr_serverless_platform_info(monkeypatch):
    """Test EMR Serverless platform info retrieval."""
    state: CloudSparkStubState = install_emr_serverless_stub(monkeypatch)

    from benchbox.platforms.aws import EMRServerlessAdapter

    adapter = EMRServerlessAdapter(
        application_id=state.application_id,
        s3_staging_dir=f"s3://{state.bucket}/staging/",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
        region=state.region,
    )

    info = adapter.get_platform_info()

    assert info["platform"] == "emr-serverless"
    assert info["vendor"] == "AWS"
    assert info["region"] == state.region
    assert info["application_id"] == state.application_id


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_emr_serverless_configure_for_benchmark(monkeypatch):
    """Test EMR Serverless benchmark configuration via CloudSparkConfigMixin."""
    state: CloudSparkStubState = install_emr_serverless_stub(monkeypatch)

    from benchbox.platforms.aws import EMRServerlessAdapter

    adapter = EMRServerlessAdapter(
        application_id=state.application_id,
        s3_staging_dir=f"s3://{state.bucket}/staging/",
        execution_role_arn="arn:aws:iam::123456789012:role/EMRRole",
        region=state.region,
    )

    adapter.configure_for_benchmark(None, "tpcds")
    assert adapter._benchmark_type == "tpcds"
    assert adapter._spark_config is not None


# ---------------------------------------------------------------------------
# GCP Dataproc Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_requires_project_id(monkeypatch):
    """Test that Dataproc adapter requires project_id configuration."""
    install_dataproc_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocAdapter(gcs_staging_dir="gs://bucket/path")

    assert "project_id" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_requires_gcs_staging(monkeypatch):
    """Test that Dataproc adapter requires GCS staging directory."""
    install_dataproc_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocAdapter(project_id="smoke-project")

    assert "gcs" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_validates_gcs_path(monkeypatch):
    """Test that Dataproc adapter validates GCS path format."""
    install_dataproc_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocAdapter(
            project_id="smoke-project",
            gcs_staging_dir="/invalid/path",  # Not a GCS path
        )

    assert "gs://" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_platform_info(monkeypatch):
    """Test Dataproc platform info retrieval."""
    state: CloudSparkStubState = install_dataproc_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocAdapter

    adapter = DataprocAdapter(
        project_id=state.project_id,
        region=state.region,
        gcs_staging_dir=f"gs://{state.bucket}/staging/",
    )

    info = adapter.get_platform_info()

    assert info["platform"] == "dataproc"
    assert info["vendor"] == "Google Cloud"
    assert info["project_id"] == state.project_id
    assert info["region"] == state.region


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_configure_for_benchmark(monkeypatch):
    """Test Dataproc benchmark configuration via CloudSparkConfigMixin."""
    state: CloudSparkStubState = install_dataproc_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocAdapter

    adapter = DataprocAdapter(
        project_id=state.project_id,
        region=state.region,
        gcs_staging_dir=f"gs://{state.bucket}/staging/",
    )

    adapter.configure_for_benchmark(None, "ssb")
    assert adapter._benchmark_type == "ssb"
    assert adapter._spark_config is not None


# ---------------------------------------------------------------------------
# GCP Dataproc Serverless Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_serverless_requires_project_id(monkeypatch):
    """Test that Dataproc Serverless adapter requires project_id."""
    install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocServerlessAdapter(gcs_staging_dir="gs://bucket/path")

    assert "project_id" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_serverless_requires_gcs_staging(monkeypatch):
    """Test that Dataproc Serverless adapter requires GCS staging."""
    install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocServerlessAdapter(project_id="smoke-project")

    assert "gcs" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_serverless_validates_gcs_path(monkeypatch):
    """Test that Dataproc Serverless validates GCS path format."""
    install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    with pytest.raises(ConfigurationError) as excinfo:
        DataprocServerlessAdapter(
            project_id="smoke-project",
            gcs_staging_dir="s3://wrong-cloud/path",  # Not a GCS path
        )

    assert "gs://" in str(excinfo.value).lower()


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_serverless_platform_info(monkeypatch):
    """Test Dataproc Serverless platform info retrieval."""
    state: CloudSparkStubState = install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    adapter = DataprocServerlessAdapter(
        project_id=state.project_id,
        region=state.region,
        gcs_staging_dir=f"gs://{state.bucket}/staging/",
    )

    info = adapter.get_platform_info()

    assert info["platform"] == "dataproc-serverless"
    assert info["vendor"] == "Google Cloud"
    assert info["project_id"] == state.project_id
    assert info["region"] == state.region


@pytest.mark.integration
@pytest.mark.platform_smoke
def test_dataproc_serverless_configure_for_benchmark(monkeypatch):
    """Test Dataproc Serverless benchmark config via CloudSparkConfigMixin."""
    state: CloudSparkStubState = install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    adapter = DataprocServerlessAdapter(
        project_id=state.project_id,
        region=state.region,
        gcs_staging_dir=f"gs://{state.bucket}/staging/",
    )

    adapter.configure_for_benchmark(None, "tpch")
    assert adapter._benchmark_type == "tpch"
    assert adapter._spark_config is not None


# ---------------------------------------------------------------------------
# Cross-Platform Mixin Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.platform_smoke
@pytest.mark.parametrize(
    "benchmark_type",
    [
        pytest.param("tpch", id="tpch"),
        pytest.param("tpcds", id="tpcds"),
        pytest.param("ssb", id="ssb"),
        pytest.param("unknown", id="unknown-defaults-to-tpch"),
    ],
)
def test_cloud_spark_config_mixin_benchmark_types(monkeypatch, benchmark_type):
    """Test CloudSparkConfigMixin handles all benchmark types."""
    state: CloudSparkStubState = install_dataproc_serverless_stub(monkeypatch)

    from benchbox.platforms.gcp import DataprocServerlessAdapter

    adapter = DataprocServerlessAdapter(
        project_id=state.project_id,
        region=state.region,
        gcs_staging_dir=f"gs://{state.bucket}/staging/",
    )

    # Should not raise for any benchmark type
    adapter.configure_for_benchmark(None, benchmark_type)
    assert adapter._benchmark_type == benchmark_type.lower()
    assert isinstance(adapter._spark_config, dict)
