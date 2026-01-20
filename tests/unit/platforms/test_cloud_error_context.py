"""Tests for cloud platform error context helpers.

These tests verify that error pattern matching provides actionable
error messages for common cloud platform failures.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import pytest

from benchbox.core.exceptions import ConfigurationError
from benchbox.platforms.cloud_error_context import (
    BIGQUERY_ERROR_PATTERNS,
    DATABRICKS_ERROR_PATTERNS,
    REDSHIFT_ERROR_PATTERNS,
    SNOWFLAKE_ERROR_PATTERNS,
    CloudErrorContext,
    detect_error_type,
    enhance_cloud_error,
    wrap_cloud_operation,
)


class TestCloudErrorContext:
    """Tests for CloudErrorContext dataclass."""

    def test_format_message_basic(self):
        """Test basic message formatting."""
        ctx = CloudErrorContext(
            error_type="auth",
            original_message="Authentication failed",
            platform="BigQuery",
            suggestions=["Check credentials", "Verify project ID"],
        )

        message = ctx.format_message()

        assert "BigQuery auth error:" in message
        assert "Authentication failed" in message
        assert "Suggestions:" in message
        assert "1. Check credentials" in message
        assert "2. Verify project ID" in message

    def test_format_message_with_diagnostic_commands(self):
        """Test message formatting with diagnostic commands."""
        ctx = CloudErrorContext(
            error_type="network",
            original_message="Connection refused",
            platform="Redshift",
            suggestions=["Check security group"],
            diagnostic_commands=["aws ec2 describe-security-groups"],
        )

        message = ctx.format_message()

        assert "Diagnostic commands:" in message
        assert "$ aws ec2 describe-security-groups" in message

    def test_format_message_with_documentation_url(self):
        """Test message formatting with documentation URL."""
        ctx = CloudErrorContext(
            error_type="quota",
            original_message="Quota exceeded",
            platform="Snowflake",
            suggestions=["Request quota increase"],
            documentation_url="https://docs.snowflake.com/quotas",
        )

        message = ctx.format_message()

        assert "Documentation:" in message
        assert "https://docs.snowflake.com/quotas" in message

    def test_format_message_complete(self):
        """Test message formatting with all fields."""
        ctx = CloudErrorContext(
            error_type="permission",
            original_message="Access denied to dataset",
            platform="Databricks",
            suggestions=["Check cluster permissions", "Verify role assignment"],
            diagnostic_commands=["databricks clusters list", "databricks workspace list /"],
            documentation_url="https://docs.databricks.com/security",
        )

        message = ctx.format_message()

        # All sections present
        assert "Databricks permission error:" in message
        assert "Access denied to dataset" in message
        assert "Suggestions:" in message
        assert "1. Check cluster permissions" in message
        assert "2. Verify role assignment" in message
        assert "Diagnostic commands:" in message
        assert "$ databricks clusters list" in message
        assert "$ databricks workspace list /" in message
        assert "Documentation:" in message
        assert "https://docs.databricks.com/security" in message


class TestBigQueryErrorPatterns:
    """Tests for BigQuery error pattern matching."""

    def test_quota_error_detection(self):
        """Test detection of quota errors."""
        error_msg = "rateLimitExceeded: Job exceeded rate limits"

        ctx = detect_error_type(error_msg, "bigquery")

        assert ctx is not None
        assert ctx.error_type == "quota"
        assert ctx.platform == "Bigquery"
        assert "quota limits" in ctx.suggestions[0].lower()
        assert ctx.documentation_url is not None

    def test_permission_error_detection(self):
        """Test detection of permission errors."""
        error_msg = "403 Forbidden: User does not have bigquery.jobs.create permission"

        ctx = detect_error_type(error_msg, "bigquery")

        assert ctx is not None
        assert ctx.error_type == "permission"
        assert any("role" in s.lower() for s in ctx.suggestions)

    def test_not_found_error_detection(self):
        """Test detection of not found errors."""
        error_msg = "404 Not found: Dataset my_project:my_dataset was not found"

        ctx = detect_error_type(error_msg, "bigquery")

        assert ctx is not None
        assert ctx.error_type == "config"
        assert any("dataset" in s.lower() for s in ctx.suggestions)

    def test_credentials_error_detection(self):
        """Test detection of credentials errors."""
        error_msg = "could not automatically determine credentials"

        ctx = detect_error_type(error_msg, "bigquery")

        assert ctx is not None
        assert ctx.error_type == "auth"
        assert any("google_application_credentials" in s.lower() for s in ctx.suggestions)


class TestRedshiftErrorPatterns:
    """Tests for Redshift error pattern matching."""

    def test_network_error_detection(self):
        """Test detection of network errors."""
        error_msg = "Connection refused to cluster endpoint"

        ctx = detect_error_type(error_msg, "redshift")

        assert ctx is not None
        assert ctx.error_type == "network"
        assert any("security group" in s.lower() for s in ctx.suggestions)
        assert ctx.diagnostic_commands is not None

    def test_cluster_paused_detection(self):
        """Test detection of paused cluster."""
        error_msg = "Cluster my-cluster is currently PAUSED"

        ctx = detect_error_type(error_msg, "redshift")

        assert ctx is not None
        assert ctx.error_type == "cluster_state"
        assert any("resume" in s.lower() for s in ctx.suggestions)

    def test_permission_error_detection(self):
        """Test detection of permission errors."""
        error_msg = "User is not authorized to perform this action"

        ctx = detect_error_type(error_msg, "redshift")

        assert ctx is not None
        assert ctx.error_type == "permission"
        assert any("grant" in s.lower() for s in ctx.suggestions)

    def test_auth_error_detection(self):
        """Test detection of authentication errors."""
        error_msg = "FATAL: password authentication failed for user admin"

        ctx = detect_error_type(error_msg, "redshift")

        assert ctx is not None
        assert ctx.error_type == "auth"
        assert any("credentials" in s.lower() or "password" in s.lower() for s in ctx.suggestions)


class TestSnowflakeErrorPatterns:
    """Tests for Snowflake error pattern matching."""

    def test_warehouse_suspended_detection(self):
        """Test detection of suspended warehouse."""
        error_msg = "Warehouse COMPUTE_WH is SUSPENDED"

        ctx = detect_error_type(error_msg, "snowflake")

        assert ctx is not None
        assert ctx.error_type == "cluster_state"
        assert any("resume" in s.lower() for s in ctx.suggestions)

    def test_role_error_detection(self):
        """Test detection of role errors."""
        error_msg = "Role 'ANALYST' does not exist or not authorized"

        ctx = detect_error_type(error_msg, "snowflake")

        assert ctx is not None
        assert ctx.error_type == "permission"
        assert any("role" in s.lower() for s in ctx.suggestions)

    def test_account_not_found_detection(self):
        """Test detection of invalid account."""
        error_msg = "Host myaccount.snowflakecomputing.com not found"

        ctx = detect_error_type(error_msg, "snowflake")

        assert ctx is not None
        assert ctx.error_type == "auth"
        assert any("account" in s.lower() for s in ctx.suggestions)

    def test_auth_failed_detection(self):
        """Test detection of authentication failure."""
        error_msg = "Incorrect username or password was specified"

        ctx = detect_error_type(error_msg, "snowflake")

        assert ctx is not None
        assert ctx.error_type == "auth"
        assert any("password" in s.lower() or "username" in s.lower() for s in ctx.suggestions)


class TestDatabricksErrorPatterns:
    """Tests for Databricks error pattern matching."""

    def test_cluster_terminated_detection(self):
        """Test detection of terminated cluster."""
        error_msg = "Cluster 0123-456789-abcd is TERMINATED"

        ctx = detect_error_type(error_msg, "databricks")

        assert ctx is not None
        assert ctx.error_type == "cluster_state"
        assert any("running" in s.lower() or "start" in s.lower() for s in ctx.suggestions)

    def test_token_expired_detection(self):
        """Test detection of expired token."""
        error_msg = "401 Unauthorized: Invalid token or token expired"

        ctx = detect_error_type(error_msg, "databricks")

        assert ctx is not None
        assert ctx.error_type == "auth"
        assert any("token" in s.lower() for s in ctx.suggestions)

    def test_permission_denied_detection(self):
        """Test detection of permission denied."""
        error_msg = "PERMISSION_DENIED: User does not have access to this resource"

        ctx = detect_error_type(error_msg, "databricks")

        assert ctx is not None
        assert ctx.error_type == "permission"
        assert any("permission" in s.lower() or "access" in s.lower() for s in ctx.suggestions)

    def test_network_error_detection(self):
        """Test detection of network errors."""
        error_msg = "could not resolve: workspace.cloud.databricks.com"

        ctx = detect_error_type(error_msg, "databricks")

        assert ctx is not None
        assert ctx.error_type == "network"
        assert any("url" in s.lower() or "connectivity" in s.lower() for s in ctx.suggestions)


class TestDetectErrorType:
    """Tests for detect_error_type function."""

    def test_unknown_platform_returns_none(self):
        """Test that unknown platforms return None."""
        ctx = detect_error_type("Some error", "unknown_platform")

        assert ctx is None

    def test_unmatched_error_returns_none(self):
        """Test that unmatched errors return None."""
        ctx = detect_error_type("Random internal error xyz123", "bigquery")

        assert ctx is None

    def test_case_insensitive_matching(self):
        """Test that pattern matching is case insensitive."""
        # Lowercase
        ctx1 = detect_error_type("quota exceeded", "bigquery")
        # Mixed case
        ctx2 = detect_error_type("QUOTA EXCEEDED", "bigquery")

        assert ctx1 is not None
        assert ctx2 is not None
        assert ctx1.error_type == ctx2.error_type

    def test_platform_name_normalized(self):
        """Test that platform name is case insensitive."""
        ctx1 = detect_error_type("quota exceeded", "BIGQUERY")
        ctx2 = detect_error_type("quota exceeded", "BigQuery")
        ctx3 = detect_error_type("quota exceeded", "bigquery")

        assert ctx1 is not None
        assert ctx2 is not None
        assert ctx3 is not None


class TestEnhanceCloudError:
    """Tests for enhance_cloud_error function."""

    def test_known_error_enhanced(self):
        """Test that known errors get enhanced messages."""
        original = ValueError("rateLimitExceeded: Quota exceeded")

        enhanced = enhance_cloud_error(original, "bigquery")

        assert isinstance(enhanced, ConfigurationError)
        assert "quota" in enhanced.args[0].lower()
        assert enhanced.details["platform"] == "bigquery"
        assert enhanced.details["error_type"] == "quota"
        assert enhanced.details["original_type"] == "ValueError"

    def test_unknown_error_gets_generic_enhancement(self):
        """Test that unknown errors get generic enhancement."""
        original = RuntimeError("Some internal error")

        enhanced = enhance_cloud_error(original, "bigquery")

        assert isinstance(enhanced, ConfigurationError)
        assert "bigquery" in enhanced.args[0].lower()
        assert enhanced.details["error_type"] == "unknown"

    def test_original_error_preserved(self):
        """Test that original error message is preserved."""
        original_msg = "Specific error message abc123"
        original = Exception(original_msg)

        enhanced = enhance_cloud_error(original, "snowflake")

        assert original_msg in enhanced.details["original_error"]


class TestWrapCloudOperation:
    """Tests for wrap_cloud_operation decorator."""

    def test_successful_operation_unchanged(self):
        """Test that successful operations return normally."""

        @wrap_cloud_operation("bigquery", "test_operation")
        def successful_func():
            return "success"

        result = successful_func()
        assert result == "success"

    def test_exception_enhanced(self):
        """Test that exceptions are enhanced."""

        @wrap_cloud_operation("bigquery", "connection")
        def failing_func():
            raise ValueError("rateLimitExceeded: Too many requests")

        with pytest.raises(ConfigurationError) as exc_info:
            failing_func()

        assert exc_info.value.details["platform"] == "bigquery"
        assert exc_info.value.details["operation"] == "connection"

    def test_configuration_error_not_double_wrapped(self):
        """Test that ConfigurationError is not double-wrapped."""
        original_error = ConfigurationError("Already enhanced")

        @wrap_cloud_operation("bigquery", "operation")
        def already_wrapped():
            raise original_error

        with pytest.raises(ConfigurationError) as exc_info:
            already_wrapped()

        # Should be the same error, not wrapped
        assert exc_info.value is original_error

    def test_decorator_preserves_function_metadata(self):
        """Test that decorator preserves function name and docstring."""

        @wrap_cloud_operation("snowflake", "connection")
        def my_function():
            """My docstring."""

        assert my_function.__name__ == "my_function"
        assert my_function.__doc__ == "My docstring."


class TestErrorPatternCoverage:
    """Tests to verify error pattern quality and coverage."""

    def test_bigquery_patterns_have_required_fields(self):
        """Test that BigQuery patterns are complete."""
        for pattern, error_type, suggestions, commands, doc_url in BIGQUERY_ERROR_PATTERNS:
            assert pattern, "Pattern should not be empty"
            assert error_type in ("auth", "network", "quota", "config", "permission", "cluster_state")
            assert len(suggestions) > 0, "Should have at least one suggestion"
            assert doc_url is None or doc_url.startswith("http")

    def test_redshift_patterns_have_required_fields(self):
        """Test that Redshift patterns are complete."""
        for pattern, error_type, suggestions, commands, doc_url in REDSHIFT_ERROR_PATTERNS:
            assert pattern, "Pattern should not be empty"
            assert error_type in ("auth", "network", "quota", "config", "permission", "cluster_state")
            assert len(suggestions) > 0, "Should have at least one suggestion"
            assert doc_url is None or doc_url.startswith("http")

    def test_snowflake_patterns_have_required_fields(self):
        """Test that Snowflake patterns are complete."""
        for pattern, error_type, suggestions, commands, doc_url in SNOWFLAKE_ERROR_PATTERNS:
            assert pattern, "Pattern should not be empty"
            assert error_type in ("auth", "network", "quota", "config", "permission", "cluster_state")
            assert len(suggestions) > 0, "Should have at least one suggestion"
            assert doc_url is None or doc_url.startswith("http")

    def test_databricks_patterns_have_required_fields(self):
        """Test that Databricks patterns are complete."""
        for pattern, error_type, suggestions, commands, doc_url in DATABRICKS_ERROR_PATTERNS:
            assert pattern, "Pattern should not be empty"
            assert error_type in ("auth", "network", "quota", "config", "permission", "cluster_state")
            assert len(suggestions) > 0, "Should have at least one suggestion"
            assert doc_url is None or doc_url.startswith("http")

    def test_all_platforms_covered(self):
        """Test that all expected platforms have error patterns."""
        from benchbox.platforms.cloud_error_context import PLATFORM_ERROR_PATTERNS

        expected_platforms = {"bigquery", "redshift", "snowflake", "databricks"}
        actual_platforms = set(PLATFORM_ERROR_PATTERNS.keys())

        assert expected_platforms == actual_platforms
