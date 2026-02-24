"""AWS Athena credentials setup and validation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import os
from typing import Optional, Union

from rich.console import Console
from rich.prompt import Confirm

from benchbox.platforms.credentials.helpers import prompt_secure_field, prompt_with_default
from benchbox.security.credentials import CredentialManager, CredentialStatus
from benchbox.utils.printing import QuietConsoleProxy


def setup_athena_credentials(cred_manager: CredentialManager, console: Union[Console, QuietConsoleProxy]) -> None:
    """Interactive setup for AWS Athena credentials.

    Args:
        cred_manager: Credential manager instance
        console: Rich console for output
    """
    console.print("\n📋 [bold]You'll need:[/bold]")
    console.print("  • AWS credentials (access key + secret, or profile name)")
    console.print("  • S3 bucket for query results and data staging")
    console.print("  • Athena workgroup (optional, defaults to 'primary')")
    console.print("  • AWS region (defaults to us-east-1)\n")

    console.print("[dim]Need help? Visit: https://docs.aws.amazon.com/athena/latest/ug/setting-up.html[/dim]\n")

    # Load existing credentials to use as defaults
    existing_creds = cred_manager.get_platform_credentials("athena")

    # Only offer auto-detection if no existing credentials
    if existing_creds:
        console.print("ℹ️  [cyan]Existing credentials found - updating configuration[/cyan]\n")
        auto_config = None
    else:
        # Try auto-detection from environment variables
        auto_config = None
        try_auto = Confirm.ask("🔍 Attempt auto-detection from environment variables?", default=True)

        if try_auto:
            console.print("\n[dim]Checking environment variables...[/dim]")
            auto_config = _auto_detect_athena(console)

    # Get credentials (use auto-detected or prompt)
    if auto_config:
        region = auto_config.get("region")
        workgroup = auto_config.get("workgroup")
        s3_staging_dir = auto_config.get("s3_staging_dir")
        s3_output_location = auto_config.get("s3_output_location")
        aws_access_key_id = auto_config.get("aws_access_key_id")
        aws_secret_access_key = auto_config.get("aws_secret_access_key")
        aws_profile = auto_config.get("aws_profile")

        console.print(f"\n✅ Found region: [cyan]{region}[/cyan]")
        if workgroup:
            console.print(f"✅ Found workgroup: [cyan]{workgroup}[/cyan]")
        if s3_staging_dir:
            console.print(f"✅ Found S3 staging dir: [cyan]{s3_staging_dir}[/cyan]")
        if s3_output_location:
            console.print(f"✅ Found S3 output location: [cyan]{s3_output_location}[/cyan]")
        if aws_profile:
            console.print(f"✅ Found AWS profile: [cyan]{aws_profile}[/cyan]")
        elif aws_access_key_id:
            console.print("✅ Found AWS access key")
    else:
        console.print("\n[bold]AWS Configuration:[/bold]")

        # Use existing credentials as defaults if available
        current_region = existing_creds.get("region") if existing_creds else None
        current_workgroup = existing_creds.get("workgroup") if existing_creds else None
        current_s3_staging_dir = existing_creds.get("s3_staging_dir") if existing_creds else None
        current_s3_output_location = existing_creds.get("s3_output_location") if existing_creds else None
        current_aws_access_key_id = existing_creds.get("aws_access_key_id") if existing_creds else None
        current_aws_secret_access_key = existing_creds.get("aws_secret_access_key") if existing_creds else None
        current_aws_profile = existing_creds.get("aws_profile") if existing_creds else None

        region = prompt_with_default(
            "AWS Region",
            current_value=current_region,
            default_if_none="us-east-1",
        )

        workgroup = prompt_with_default(
            "Athena Workgroup",
            current_value=current_workgroup,
            default_if_none="primary",
        )

        # S3 configuration
        console.print("\n[bold]S3 Configuration (Required):[/bold]")
        console.print("[dim]Athena requires S3 for query results and data staging.[/dim]\n")

        s3_staging_dir = prompt_with_default(
            "S3 staging directory (e.g., s3://my-bucket/athena-data/)",
            current_value=current_s3_staging_dir,
        )

        if not s3_staging_dir:
            console.print("[red]❌ S3 staging directory is required for Athena[/red]")
            return

        # Output location defaults to staging dir if not specified
        s3_output_location = prompt_with_default(
            "S3 output location for query results (leave empty to use staging dir)",
            current_value=current_s3_output_location,
            default_if_none="",
        )

        if not s3_output_location:
            # Derive from staging dir
            if s3_staging_dir.endswith("/"):
                s3_output_location = f"{s3_staging_dir}athena-results/"
            else:
                s3_output_location = f"{s3_staging_dir}/athena-results/"

        # AWS Authentication
        console.print("\n[bold]AWS Authentication:[/bold]")
        console.print("1. AWS Profile (recommended if using AWS CLI)")
        console.print("2. Access Key + Secret Key\n")

        # Default to method 1 if profile exists, method 2 if access keys exist
        from rich.prompt import IntPrompt

        default_method = 1 if current_aws_profile else (2 if current_aws_access_key_id else 1)
        auth_method = IntPrompt.ask("Choose authentication method [1-2]", default=default_method)

        if auth_method == 1:
            aws_profile = prompt_with_default(
                "AWS Profile name (from ~/.aws/credentials)",
                current_value=current_aws_profile,
                default_if_none="default",
            )
            aws_access_key_id = None
            aws_secret_access_key = None
        else:
            aws_profile = None
            aws_access_key_id = prompt_with_default(
                "AWS Access Key ID",
                current_value=current_aws_access_key_id,
            )
            if not aws_access_key_id:
                console.print("[red]❌ AWS Access Key ID is required[/red]")
                return

            aws_secret_access_key = prompt_secure_field(
                "AWS Secret Access Key",
                current_value=current_aws_secret_access_key,
                console=console,
            )
            if not aws_secret_access_key:
                console.print("[red]❌ AWS Secret Access Key is required[/red]")
                return

    # Build credentials
    credentials = {
        "region": region,
        "workgroup": workgroup,
        "s3_staging_dir": s3_staging_dir,
        "s3_output_location": s3_output_location,
    }

    if aws_profile:
        credentials["aws_profile"] = aws_profile
    if aws_access_key_id:
        credentials["aws_access_key_id"] = aws_access_key_id
    if aws_secret_access_key:
        credentials["aws_secret_access_key"] = aws_secret_access_key

    # Validate credentials
    console.print("\n🧪 [bold]Validating credentials...[/bold]")

    # Save temporarily for validation
    cred_manager.set_platform_credentials("athena", credentials, CredentialStatus.NOT_VALIDATED)

    success, error = validate_athena_credentials(cred_manager, console)

    if success:
        cred_manager.update_validation_status("athena", CredentialStatus.VALID)
        cred_manager.save_credentials()

        console.print("\n[green]✅ Athena credentials validated and saved![/green]")
        console.print(f"   Location: [cyan]{cred_manager.credentials_path}[/cyan]")
        console.print("   Status: [green]Ready to use[/green]\n")

        console.print("[bold]Try it:[/bold]")
        console.print("  benchbox run --platform athena --benchmark tpch --scale 0.01")
    else:
        cred_manager.update_validation_status("athena", CredentialStatus.INVALID, error)
        cred_manager.save_credentials()

        console.print("\n[red]❌ Validation failed[/red]")
        if error:
            console.print(f"   Error: {error}")
        console.print("\n[yellow]Credentials saved but marked as invalid.[/yellow]")
        console.print("Fix the issues and run: benchbox setup --platform athena --validate-only")


def validate_athena_credentials(
    cred_manager: CredentialManager, console: Optional[Union[Console, QuietConsoleProxy]] = None
) -> tuple[bool, Optional[str]]:
    """Validate Athena credentials by testing connection.

    Args:
        cred_manager: Credential manager instance
        console: Optional console for detailed output

    Returns:
        Tuple of (success, error_message)
    """
    creds = cred_manager.get_platform_credentials("athena")

    if not creds:
        return False, "No credentials found"

    preflight_error = _check_athena_prerequisites(creds)
    if preflight_error:
        return False, preflight_error

    try:
        from pyathena import connect as athena_connect
    except ImportError:
        from benchbox.utils.dependencies import get_install_command

        return False, f"pyathena not installed. Run: {get_install_command('athena')}"

    try:
        import boto3  # noqa: F401 - needed for Athena operations
    except ImportError:
        from benchbox.utils.dependencies import get_install_command

        return False, f"boto3 not installed. Run: {get_install_command('athena')}"

    connect_kwargs = _build_athena_connect_kwargs(creds)

    try:
        if console:
            console.print("[dim]Testing Athena connection...[/dim]")

        conn = athena_connect(**connect_kwargs)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()

        return True, None

    except Exception as e:
        return False, _classify_athena_error(str(e), connect_kwargs.get("work_group", "primary"))


def _check_athena_prerequisites(creds: dict) -> Optional[str]:
    """Check required Athena credential fields and AWS authentication availability."""
    if not creds.get("s3_staging_dir") and not creds.get("s3_output_location"):
        return "S3 staging directory or output location is required"

    has_profile = bool(creds.get("aws_profile"))
    has_keys = bool(creds.get("aws_access_key_id") and creds.get("aws_secret_access_key"))
    has_env = bool(os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"))
    has_default_profile = os.path.exists(os.path.expanduser("~/.aws/credentials"))

    if not any([has_profile, has_keys, has_env, has_default_profile]):
        return "No AWS authentication configured. Provide profile or access keys."

    return None


def _build_athena_connect_kwargs(creds: dict) -> dict:
    """Build pyathena connection keyword arguments from credentials."""
    connect_kwargs = {
        "s3_staging_dir": creds.get("s3_output_location") or creds.get("s3_staging_dir"),
        "region_name": creds.get("region", "us-east-1"),
        "work_group": creds.get("workgroup", "primary"),
    }

    if creds.get("aws_access_key_id") and creds.get("aws_secret_access_key"):
        connect_kwargs["aws_access_key_id"] = creds["aws_access_key_id"]
        connect_kwargs["aws_secret_access_key"] = creds["aws_secret_access_key"]
    elif creds.get("aws_profile"):
        connect_kwargs["profile_name"] = creds["aws_profile"]

    return connect_kwargs


def _classify_athena_error(error_msg: str, workgroup: str) -> str:
    """Classify Athena connection errors into user-friendly messages."""
    _error_patterns = [
        (("Access Denied", "AccessDenied"), "Access denied. Check S3 bucket permissions and IAM policies."),
        (("InvalidAccessKeyId",), "Invalid AWS Access Key ID."),
        (("SignatureDoesNotMatch",), "Invalid AWS Secret Access Key."),
        (("NoSuchBucket",), "S3 bucket does not exist. Check the bucket name."),
    ]
    for patterns, message in _error_patterns:
        if any(p in error_msg for p in patterns):
            return message

    if "workgroup" in error_msg.lower() and "not found" in error_msg.lower():
        return f"Workgroup '{workgroup}' not found. Check workgroup name."

    return f"Connection failed: {error_msg}"


def _auto_detect_athena(console: Union[Console, QuietConsoleProxy]) -> Optional[dict]:
    """Attempt to auto-detect Athena configuration from environment variables.

    Args:
        console: Rich console for output

    Returns:
        Dictionary with detected config or None
    """
    env_vars = {
        "region": os.getenv("AWS_DEFAULT_REGION") or os.getenv("AWS_REGION"),
        "workgroup": os.getenv("ATHENA_WORKGROUP"),
        "s3_staging_dir": os.getenv("ATHENA_S3_STAGING_DIR"),
        "s3_output_location": os.getenv("ATHENA_S3_OUTPUT_LOCATION"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "aws_profile": os.getenv("AWS_PROFILE"),
    }

    # Check if we have the minimum required fields
    has_s3 = bool(env_vars.get("s3_staging_dir") or env_vars.get("s3_output_location"))
    has_auth = bool(
        env_vars.get("aws_profile")
        or (env_vars.get("aws_access_key_id") and env_vars.get("aws_secret_access_key"))
        or os.path.exists(os.path.expanduser("~/.aws/credentials"))
    )

    if not has_s3:
        console.print("  ⚠️  Missing: ATHENA_S3_STAGING_DIR or ATHENA_S3_OUTPUT_LOCATION")
        return None

    if not has_auth:
        console.print(
            "  ⚠️  Missing AWS credentials (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, AWS_PROFILE, or ~/.aws/credentials)"
        )
        return None

    # Set defaults
    if not env_vars.get("region"):
        env_vars["region"] = "us-east-1"
    if not env_vars.get("workgroup"):
        env_vars["workgroup"] = "primary"

    console.print("  ✓ Found required environment variables")

    return env_vars


__all__ = ["setup_athena_credentials", "validate_athena_credentials"]
