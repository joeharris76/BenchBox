"""Cloud platform error context helpers.

Provides actionable error messages for common cloud platform failures.
Each platform adapter can use these helpers to enhance error messages
with diagnostic hints and fix suggestions.

Usage:
    from benchbox.platforms.cloud_error_context import enhance_cloud_error

    try:
        connection = platform.connect()
    except Exception as e:
        enhanced = enhance_cloud_error(e, "bigquery")
        raise enhanced from e

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from benchbox.core.exceptions import ConfigurationError


@dataclass
class CloudErrorContext:
    """Context for cloud platform errors with actionable suggestions."""

    error_type: str  # auth, network, quota, config, permission, cluster_state
    original_message: str
    platform: str
    suggestions: list[str]
    diagnostic_commands: list[str] | None = None
    documentation_url: str | None = None

    def format_message(self) -> str:
        """Format the error message with context and suggestions."""
        lines = [
            f"{self.platform} {self.error_type} error:",
            f"  {self.original_message}",
            "",
            "Suggestions:",
        ]

        for i, suggestion in enumerate(self.suggestions, 1):
            lines.append(f"  {i}. {suggestion}")

        if self.diagnostic_commands:
            lines.append("")
            lines.append("Diagnostic commands:")
            for cmd in self.diagnostic_commands:
                lines.append(f"  $ {cmd}")

        if self.documentation_url:
            lines.append("")
            lines.append(f"Documentation: {self.documentation_url}")

        return "\n".join(lines)


# Error pattern matchers for each platform
# Format: (regex_pattern, error_type, suggestions, diagnostic_commands, doc_url)

BIGQUERY_ERROR_PATTERNS: list[tuple[str, str, list[str], list[str] | None, str | None]] = [
    (
        r"(quota|rateLimitExceeded|Resource quota exceeded)",
        "quota",
        [
            "Check your BigQuery quota limits in Google Cloud Console",
            "Request a quota increase if running large benchmarks",
            "Use smaller scale factor or run during off-peak hours",
            "Check if slot reservations are available for your project",
        ],
        ["gcloud bigquery show-reservation --project=YOUR_PROJECT"],
        "https://cloud.google.com/bigquery/quotas",
    ),
    (
        r"(403|Permission|Access Denied|does not have.*permission)",
        "permission",
        [
            "Verify service account has BigQuery roles assigned",
            "Required roles: bigquery.dataEditor, bigquery.jobUser",
            "Check IAM permissions in Google Cloud Console",
            "Ensure service account has access to the project and dataset",
        ],
        [
            "gcloud projects get-iam-policy YOUR_PROJECT",
            "gcloud auth application-default print-access-token",
        ],
        "https://cloud.google.com/bigquery/docs/access-control",
    ),
    (
        r"(404|Not found|Project.*not found|Dataset.*not found)",
        "config",
        [
            "Verify project ID is correct",
            "Check that dataset exists in the project",
            "Ensure credentials are for the correct Google Cloud project",
            "Create dataset if it doesn't exist: CREATE SCHEMA dataset_name",
        ],
        [
            "gcloud config list project",
            "bq ls --project_id=YOUR_PROJECT",
        ],
        "https://cloud.google.com/bigquery/docs/datasets",
    ),
    (
        r"(could not.*credentials|GOOGLE_APPLICATION_CREDENTIALS|authentication)",
        "auth",
        [
            "Set GOOGLE_APPLICATION_CREDENTIALS environment variable",
            "Or run: gcloud auth application-default login",
            "Verify service account key file exists and is readable",
            "Check that credentials are not expired",
        ],
        [
            "echo $GOOGLE_APPLICATION_CREDENTIALS",
            "gcloud auth application-default print-access-token 2>&1",
        ],
        "https://cloud.google.com/docs/authentication/getting-started",
    ),
]

REDSHIFT_ERROR_PATTERNS: list[tuple[str, str, list[str], list[str] | None, str | None]] = [
    (
        r"(Connection refused|timeout|Unable to connect|could not connect)",
        "network",
        [
            "Check Redshift cluster is in 'Available' state",
            "Verify security group allows inbound traffic on port 5439",
            "Ensure your IP is whitelisted in cluster security group",
            "Check if VPC endpoint is required for private clusters",
        ],
        [
            "aws redshift describe-clusters --cluster-identifier YOUR_CLUSTER",
            "aws ec2 describe-security-groups --group-ids YOUR_SG_ID",
        ],
        "https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-connect-to-cluster.html",
    ),
    (
        r"(cluster.*paused|PAUSED|maintenance|upgrading)",
        "cluster_state",
        [
            "Redshift cluster is paused or in maintenance",
            "Resume cluster in AWS Console or via CLI",
            "Wait for maintenance window to complete",
            "Check cluster events for maintenance schedule",
        ],
        [
            "aws redshift describe-clusters --cluster-identifier YOUR_CLUSTER",
            "aws redshift resume-cluster --cluster-identifier YOUR_CLUSTER",
        ],
        "https://docs.aws.amazon.com/redshift/latest/mgmt/managing-cluster-operations.html",
    ),
    (
        r"(permission|not authorized|Access denied|role.*required)",
        "permission",
        [
            "Verify IAM role has redshift:GetClusterCredentials permission",
            "Check database user has required permissions (CREATE, SELECT)",
            "Grant USAGE on schema: GRANT USAGE ON SCHEMA public TO user",
            "Grant table permissions: GRANT ALL ON ALL TABLES IN SCHEMA public TO user",
        ],
        [
            "aws redshift describe-cluster-db-revisions",
            "aws iam get-role-policy --role-name YOUR_ROLE --policy-name YOUR_POLICY",
        ],
        "https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html",
    ),
    (
        r"(FATAL.*password authentication failed|invalid.*credentials)",
        "auth",
        [
            "Verify Redshift master user credentials",
            "Check if using IAM authentication (requires role configuration)",
            "Password may have expired or been changed",
            "Try connecting with AWS Secrets Manager for credential rotation",
        ],
        ["aws redshift describe-clusters --cluster-identifier YOUR_CLUSTER | grep MasterUsername"],
        "https://docs.aws.amazon.com/redshift/latest/mgmt/generating-iam-credentials-overview.html",
    ),
]

SNOWFLAKE_ERROR_PATTERNS: list[tuple[str, str, list[str], list[str] | None, str | None]] = [
    (
        r"(warehouse.*suspended|Warehouse.*SUSPENDED|warehouse is disabled)",
        "cluster_state",
        [
            "Snowflake warehouse is suspended (auto-suspend)",
            "Warehouse will auto-resume on next query (may take 30-60s)",
            "Or manually resume: ALTER WAREHOUSE warehouse_name RESUME",
            "Consider increasing AUTO_SUSPEND time for benchmarks",
        ],
        None,
        "https://docs.snowflake.com/en/user-guide/warehouses-overview",
    ),
    (
        r"(role.*not.*exist|Role .* does not exist|insufficient privileges)",
        "permission",
        [
            "Verify you have the correct role assigned",
            "Check role has USAGE on warehouse: GRANT USAGE ON WAREHOUSE wh TO ROLE role",
            "Check role has USAGE on database: GRANT USAGE ON DATABASE db TO ROLE role",
            "Use: USE ROLE role_name; to switch to correct role",
        ],
        None,
        "https://docs.snowflake.com/en/user-guide/security-access-control-overview",
    ),
    (
        r"(account.*not.*found|Invalid account|Host .* not found)",
        "auth",
        [
            "Verify Snowflake account identifier is correct",
            "Format: account_name (not full URL)",
            "For orgs: org_name-account_name",
            "Check region suffix if using legacy format: account.region.cloud",
        ],
        None,
        "https://docs.snowflake.com/en/user-guide/admin-account-identifier",
    ),
    (
        r"(authentication.*failed|incorrect.*password|Invalid credentials)",
        "auth",
        [
            "Verify username and password are correct",
            "Check if MFA is required and not configured",
            "If using key-pair auth, verify private key is correct",
            "Password may have expired - contact account admin",
        ],
        None,
        "https://docs.snowflake.com/en/user-guide/admin-user-management",
    ),
]

DATABRICKS_ERROR_PATTERNS: list[tuple[str, str, list[str], list[str] | None, str | None]] = [
    (
        r"(TERMINATED|cluster.*terminated|Starting cluster|Cluster.*not running)",
        "cluster_state",
        [
            "Databricks cluster is not running or starting",
            "Wait for cluster to reach 'Running' state (may take 3-5 minutes)",
            "Start cluster manually in Databricks workspace",
            "Consider using serverless SQL warehouse for faster startup",
        ],
        ["databricks clusters list --output JSON | jq '.clusters[] | {id, state, cluster_name}'"],
        "https://docs.databricks.com/clusters/index.html",
    ),
    (
        r"(token.*expired|401|Invalid token|authentication failed)",
        "auth",
        [
            "Databricks access token may have expired",
            "Generate new token in User Settings > Developer > Access Tokens",
            "Token lifetime can be extended in admin settings",
            "Consider using service principal for long-running workloads",
        ],
        ["echo $DATABRICKS_TOKEN | cut -c1-10"],
        "https://docs.databricks.com/dev-tools/auth.html",
    ),
    (
        r"(403|Access denied|not authorized|PERMISSION_DENIED)",
        "permission",
        [
            "Check you have access to the workspace and cluster",
            "Verify cluster permissions allow your user/group",
            "For SQL warehouse, check SQL permissions in Data Explorer",
            "Admin may need to grant CAN_ATTACH_TO permission",
        ],
        ["databricks workspace list /"],
        "https://docs.databricks.com/security/access-control/index.html",
    ),
    (
        r"(could not resolve|Connection refused|timeout|no route to host)",
        "network",
        [
            "Check Databricks workspace URL is correct",
            "Verify network connectivity to Databricks cloud",
            "If using VPC peering, check route tables and security groups",
            "Corporate firewall may block Databricks endpoints",
        ],
        ["curl -s https://YOUR_WORKSPACE.cloud.databricks.com/api/2.0/clusters/list 2>&1 | head -1"],
        "https://docs.databricks.com/administration-guide/cloud-configurations/aws/customer-managed-vpc.html",
    ),
]

# Platform pattern registry
PLATFORM_ERROR_PATTERNS: dict[str, list[tuple[str, str, list[str], list[str] | None, str | None]]] = {
    "bigquery": BIGQUERY_ERROR_PATTERNS,
    "redshift": REDSHIFT_ERROR_PATTERNS,
    "snowflake": SNOWFLAKE_ERROR_PATTERNS,
    "databricks": DATABRICKS_ERROR_PATTERNS,
}


def detect_error_type(
    error_message: str,
    platform: str,
) -> CloudErrorContext | None:
    """Detect error type and return context with suggestions.

    Args:
        error_message: The original error message
        platform: Platform name (bigquery, redshift, snowflake, databricks)

    Returns:
        CloudErrorContext with suggestions, or None if error type not recognized
    """
    patterns = PLATFORM_ERROR_PATTERNS.get(platform.lower())
    if not patterns:
        return None

    error_message.lower()

    for pattern, error_type, suggestions, commands, doc_url in patterns:
        if re.search(pattern, error_message, re.IGNORECASE):
            return CloudErrorContext(
                error_type=error_type,
                original_message=error_message,
                platform=platform.title(),
                suggestions=suggestions,
                diagnostic_commands=commands,
                documentation_url=doc_url,
            )

    return None


def enhance_cloud_error(
    exception: Exception,
    platform: str,
) -> ConfigurationError:
    """Enhance a cloud platform exception with actionable context.

    Args:
        exception: The original exception
        platform: Platform name (bigquery, redshift, snowflake, databricks)

    Returns:
        ConfigurationError with enhanced message and suggestions
    """
    error_message = str(exception)
    context = detect_error_type(error_message, platform)

    if context:
        enhanced_message = context.format_message()
        return ConfigurationError(
            enhanced_message,
            details={
                "platform": platform,
                "error_type": context.error_type,
                "original_error": error_message,
                "original_type": type(exception).__name__,
            },
        )

    # No pattern matched - return generic enhancement
    return ConfigurationError(
        f"{platform.title()} error: {error_message}\n\nCheck {platform} documentation for troubleshooting guidance.",
        details={
            "platform": platform,
            "error_type": "unknown",
            "original_error": error_message,
            "original_type": type(exception).__name__,
        },
    )


def wrap_cloud_operation(
    platform: str,
    operation: str,
):
    """Decorator to wrap cloud operations with enhanced error context.

    Usage:
        @wrap_cloud_operation("bigquery", "connection")
        def create_connection(self):
            return bigquery.Client()

    Args:
        platform: Platform name
        operation: Operation description (for error messages)
    """
    from functools import wraps

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ConfigurationError:
                # Already enhanced, re-raise
                raise
            except Exception as e:
                enhanced = enhance_cloud_error(e, platform)
                enhanced.details["operation"] = operation
                raise enhanced from e

        return wrapper

    return decorator
