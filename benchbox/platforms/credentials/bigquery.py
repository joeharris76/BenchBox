"""BigQuery credentials setup and validation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
import os
from pathlib import Path
from typing import Optional, Union

from rich.console import Console
from rich.prompt import Confirm, Prompt

from benchbox.platforms.credentials.helpers import prompt_with_default
from benchbox.security.credentials import CredentialManager, CredentialStatus
from benchbox.utils.printing import QuietConsoleProxy


def setup_bigquery_credentials(cred_manager: CredentialManager, console: Union[Console, QuietConsoleProxy]) -> None:
    """Interactive setup for BigQuery credentials.

    Args:
        cred_manager: Credential manager instance
        console: Rich console for output
    """
    console.print("\n📋 [bold]You'll need:[/bold]")
    console.print("  • Google Cloud project ID")
    console.print("  • Service account JSON key file")
    console.print("  • Dataset name (will be created if it doesn't exist)")
    console.print("  • Data location (e.g., US, EU, us-central1)\n")

    console.print("[dim]Need help? Visit: https://cloud.google.com/iam/docs/service-accounts-create[/dim]\n")

    # Load existing credentials to use as defaults
    existing_creds = cred_manager.get_platform_credentials("bigquery")

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
            auto_config = _auto_detect_bigquery(console)

    # Get credentials (use auto-detected or prompt)
    if auto_config:
        project_id = auto_config.get("project_id")
        credentials_path = auto_config.get("credentials_path")
        dataset_id = auto_config.get("dataset_id")
        location = auto_config.get("location")
        storage_bucket = auto_config.get("storage_bucket")

        console.print(f"\n✅ Found credentials file: [cyan]{credentials_path}[/cyan]")
        console.print(f"✅ Found project: [cyan]{project_id}[/cyan]")
        console.print(f"✅ Found dataset: [cyan]{dataset_id}[/cyan]")
        console.print(f"✅ Found location: [cyan]{location}[/cyan]")
        if storage_bucket:
            console.print(f"✅ Found Cloud Storage bucket: [cyan]{storage_bucket}[/cyan]")
    else:
        console.print("\n[bold]BigQuery Configuration:[/bold]")

        # Use existing credentials as defaults if available
        current_project_id = existing_creds.get("project_id") if existing_creds else None
        current_credentials_path = existing_creds.get("credentials_path") if existing_creds else None
        current_dataset_id = existing_creds.get("dataset_id") if existing_creds else None
        current_location = existing_creds.get("location") if existing_creds else None
        current_storage_bucket = existing_creds.get("storage_bucket") if existing_creds else None

        project_id = prompt_with_default(
            "Project ID (e.g., my-gcp-project-123456)",
            current_value=current_project_id,
        )

        if not project_id:
            console.print("[red]❌ Project ID is required[/red]")
            return

        credentials_path = prompt_with_default(
            "Path to service account JSON key file",
            current_value=current_credentials_path,
        )

        if not credentials_path:
            console.print("[red]❌ Service account JSON key file path is required[/red]")
            return

        # Expand path and validate file exists
        credentials_path = os.path.expanduser(credentials_path)
        credentials_path_obj = Path(credentials_path)

        if not credentials_path_obj.exists():
            console.print(f"[red]❌ File not found: {credentials_path}[/red]")
            return

        if not credentials_path_obj.is_file():
            console.print(f"[red]❌ Not a file: {credentials_path}[/red]")
            return

        # Validate it's valid JSON
        try:
            with open(credentials_path) as f:
                json_content = json.load(f)
                # Basic validation - check for required service account fields
                if "type" not in json_content or json_content.get("type") != "service_account":
                    console.print("[red]❌ File does not appear to be a valid service account JSON key[/red]")
                    console.print("[dim]Expected: 'type': 'service_account'[/dim]")
                    return
        except json.JSONDecodeError as e:
            console.print(f"[red]❌ Invalid JSON file: {e}[/red]")
            return
        except Exception as e:
            console.print(f"[red]❌ Error reading file: {e}[/red]")
            return

        console.print("[green]✓[/green] Validated service account JSON file")

        dataset_id = prompt_with_default("Dataset name", current_value=current_dataset_id, default_if_none="benchbox")

        if not dataset_id:
            console.print("[red]❌ Dataset name is required[/red]")
            return

        location = prompt_with_default("Data location", current_value=current_location, default_if_none="US")

        if not location:
            console.print("[red]❌ Location is required[/red]")
            return

        # Optional Cloud Storage bucket
        console.print("\n[bold]Cloud Storage Configuration (Recommended):[/bold]")
        console.print("[dim]Configuring Cloud Storage enables efficient data loading.[/dim]")
        console.print("[dim]Without Cloud Storage, data loading will be slower using direct streaming.[/dim]\n")

        configure_storage = Confirm.ask("Configure Cloud Storage bucket for efficient data loading?", default=True)

        if configure_storage:
            storage_bucket = prompt_with_default(
                "Cloud Storage bucket name (e.g., my-benchbox-data)",
                current_value=current_storage_bucket,
                default_if_none="",
            )
            if not storage_bucket:
                storage_bucket = None
        else:
            storage_bucket = None

    # Build credentials
    credentials = {
        "project_id": project_id,
        "credentials_path": credentials_path,
        "dataset_id": dataset_id,
        "location": location,
    }

    if storage_bucket:
        credentials["storage_bucket"] = storage_bucket

    # Validate credentials
    console.print("\n🧪 [bold]Validating credentials...[/bold]")

    # Save temporarily for validation
    cred_manager.set_platform_credentials("bigquery", credentials, CredentialStatus.NOT_VALIDATED)

    success, error = validate_bigquery_credentials(cred_manager)

    if success:
        cred_manager.update_validation_status("bigquery", CredentialStatus.VALID)
        cred_manager.save_credentials()

        console.print("\n[green]✅ BigQuery credentials validated and saved![/green]")
        console.print(f"   Location: [cyan]{cred_manager.credentials_path}[/cyan]")
        console.print("   Status: [green]Ready to use[/green]\n")

        if storage_bucket:
            console.print("[green]✅ Cloud Storage staging configured for efficient data loading[/green]\n")
        else:
            console.print(
                "[yellow]⚠️  No Cloud Storage staging configured - data loading will use direct streaming (slower)[/yellow]\n"
            )

        # Prompt for default output location (optional)
        _prompt_default_output_location(cred_manager, console, credentials, storage_bucket)

        console.print("[bold]Try it:[/bold]")
        console.print("  benchbox run --platform bigquery --benchmark tpch --scale 0.01")
    else:
        cred_manager.update_validation_status("bigquery", CredentialStatus.INVALID, error)
        cred_manager.save_credentials()

        console.print("\n[red]❌ Validation failed[/red]")
        if error:
            console.print(f"   Error: {error}")
        console.print("\n[yellow]Credentials saved but marked as invalid.[/yellow]")
        console.print("Fix the issues and run: benchbox setup --platform bigquery --validate-only")


def _prompt_default_output_location(
    cred_manager: CredentialManager,
    console: Union[Console, QuietConsoleProxy],
    credentials: dict,
    storage_bucket: Optional[str],
) -> None:
    """Prompt for default cloud output location for BigQuery.

    Args:
        cred_manager: Credential manager instance
        console: Rich console for output
        credentials: Current credentials dictionary
        storage_bucket: Optional configured storage bucket (for suggestions)
    """
    from benchbox.utils.cloud_storage import is_cloud_path

    console.print("\n[bold]Default Output Location (Optional):[/bold]")
    console.print("Configure a default cloud path for benchmark data storage.")
    console.print("This prevents needing to specify --output for every run.\n")

    wants_default = Confirm.ask("Configure default output location?", default=True)

    if not wants_default:
        console.print("[dim]You can add --output <cloud-path> when running benchmarks[/dim]\n")
        return

    # Show examples - suggest storage bucket if configured
    console.print("\n[bold cyan]Example paths for BigQuery:[/bold cyan]")
    if storage_bucket:
        console.print(f"  • [dim]gs://{storage_bucket}/benchbox-data[/dim] (using your staging bucket)")
        console.print(f"  • [dim]gs://{storage_bucket}/data[/dim]")
    else:
        console.print("  • [dim]gs://my-bucket/benchbox-data[/dim]")
        console.print("  • [dim]gs://my-bucket/data[/dim]")

    console.print("\n[bold]Google Cloud Storage:[/bold]")
    console.print("  gs://<bucket>/<path>")
    console.print("\n[dim]Note: Ensure the bucket exists and the service account has[/dim]")
    console.print("[dim]storage.objects.create permission[/dim]\n")

    # Suggest default based on storage bucket
    suggested_default = f"gs://{storage_bucket}/benchbox-data" if storage_bucket else ""

    # Prompt for path with validation
    while True:
        cloud_path = Prompt.ask("[bold]Enter default cloud storage path[/bold]", default=suggested_default)

        if not cloud_path:
            console.print("[yellow]Skipping default output location[/yellow]\n")
            return

        # Validate cloud path format
        if not is_cloud_path(cloud_path):
            console.print(f"[yellow]⚠️  Warning: '{cloud_path}' doesn't look like a cloud path[/yellow]")
            console.print("[dim]Expected format: gs://<bucket>/<path>[/dim]")
            proceed = Confirm.ask("Use this path anyway?", default=False)
            if not proceed:
                continue

        # Confirm the path
        console.print(f"\n[green]✓[/green] Will use: [cyan]{cloud_path}[/cyan]")
        confirmed = Confirm.ask("Is this correct?", default=True)
        if confirmed:
            # Set credentials with default_output_location
            credentials["default_output_location"] = cloud_path
            cred_manager.set_platform_credentials("bigquery", credentials, CredentialStatus.VALID)
            cred_manager.save_credentials()
            console.print("[green]✅ Default output location saved![/green]\n")
            return


def validate_bigquery_credentials(cred_manager: CredentialManager) -> tuple[bool, Optional[str]]:
    """Validate BigQuery credentials by testing connection.

    Args:
        cred_manager: Credential manager instance

    Returns:
        Tuple of (success, error_message)
    """
    creds = cred_manager.get_platform_credentials("bigquery")

    if not creds:
        return False, "No credentials found"

    required_fields = ["project_id", "credentials_path"]
    missing = [field for field in required_fields if not creds.get(field)]

    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"

    # Validate credentials file exists
    credentials_path = creds.get("credentials_path")
    credentials_path = os.path.expanduser(credentials_path)

    if not os.path.exists(credentials_path):
        return False, f"Credentials file not found: {credentials_path}"

    if not os.path.isfile(credentials_path):
        return False, f"Credentials path is not a file: {credentials_path}"

    # Validate JSON structure
    try:
        with open(credentials_path) as f:
            json_content = json.load(f)
            if "type" not in json_content or json_content.get("type") != "service_account":
                return False, "Credentials file is not a valid service account JSON key"
    except json.JSONDecodeError:
        return False, "Credentials file is not valid JSON"
    except Exception as e:
        return False, f"Error reading credentials file: {e}"

    # Try to import BigQuery client
    try:
        from google.cloud import bigquery
    except ImportError:
        return False, "BigQuery client library not installed. Run: pip install google-cloud-bigquery"

    # Test connection
    try:
        # Create client with credentials
        client = bigquery.Client.from_service_account_json(
            credentials_path,
            project=creds["project_id"],
        )

        # Test basic query
        query = "SELECT 1 as test"
        query_job = client.query(query)
        query_job.result()

        # Test project access
        query = "SELECT @@project_id as project"
        query_job = client.query(query)
        results = list(query_job.result())
        if not results or not results[0].project:
            return False, "Could not verify project access"

        # Test dataset listing (checks permissions)
        try:
            list(client.list_datasets(max_results=1))
        except Exception:
            # This is not fatal - user might not have dataset list permissions
            # But they can still create and use datasets
            pass

        # If storage bucket is configured, test Cloud Storage access
        storage_bucket = creds.get("storage_bucket")
        if storage_bucket:
            try:
                from google.cloud import storage

                storage_client = storage.Client.from_service_account_json(
                    credentials_path,
                    project=creds["project_id"],
                )

                # Test bucket access
                bucket = storage_client.bucket(storage_bucket)
                # Just check if we can access bucket metadata (doesn't require objects to exist)
                bucket.reload()

            except ImportError:
                return False, "Cloud Storage client library not installed. Run: pip install google-cloud-storage"
            except Exception as e:
                error_msg = str(e)
                if "not found" in error_msg.lower() or "404" in error_msg:
                    return False, f"Cloud Storage bucket '{storage_bucket}' not found"
                elif "forbidden" in error_msg.lower() or "403" in error_msg:
                    return (
                        False,
                        f"No access to Cloud Storage bucket '{storage_bucket}'. Service account needs Storage Object Admin role",
                    )
                else:
                    return False, f"Cloud Storage access failed: {error_msg}"

        return True, None

    except Exception as e:
        error_msg = str(e)
        # Make error more user-friendly
        if "could not find default credentials" in error_msg.lower():
            return False, "Could not load credentials. Check that the service account JSON file is valid."
        elif "permission denied" in error_msg.lower() or "forbidden" in error_msg.lower():
            return False, "Permission denied. Service account needs BigQuery Data Editor and BigQuery Job User roles."
        elif "not found" in error_msg.lower() and "project" in error_msg.lower():
            return False, f"Project '{creds.get('project_id')}' not found or not accessible."
        elif "invalid" in error_msg.lower() and ("credentials" in error_msg.lower() or "key" in error_msg.lower()):
            return False, "Invalid service account credentials. Check that the JSON key file is correct."
        else:
            return False, f"Connection failed: {error_msg}"


def _auto_detect_bigquery(console: Union[Console, QuietConsoleProxy]) -> Optional[dict]:
    """Attempt to auto-detect BigQuery configuration from environment variables.

    Args:
        console: Rich console for output

    Returns:
        Dictionary with detected config or None
    """
    env_vars = {
        "credentials_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        "project_id": os.getenv("BIGQUERY_PROJECT"),
        "dataset_id": os.getenv("BIGQUERY_DATASET"),
        "location": os.getenv("BIGQUERY_LOCATION"),
        "storage_bucket": os.getenv("BIGQUERY_STORAGE_BUCKET"),
    }

    # Check if we have the required fields
    required = ["credentials_path", "project_id"]
    found_required = all(env_vars.get(field) for field in required)

    if not found_required:
        missing = []
        if not env_vars.get("credentials_path"):
            missing.append("GOOGLE_APPLICATION_CREDENTIALS")
        if not env_vars.get("project_id"):
            missing.append("BIGQUERY_PROJECT")
        console.print(f"  ⚠️  Missing environment variables: {', '.join(missing)}")
        return None

    # Validate credentials file exists
    credentials_path = os.path.expanduser(env_vars["credentials_path"])
    if not os.path.exists(credentials_path):
        console.print(f"  ⚠️  Credentials file not found: {credentials_path}")
        return None

    # Set defaults for optional fields
    if not env_vars.get("dataset_id"):
        env_vars["dataset_id"] = "benchbox"
    if not env_vars.get("location"):
        env_vars["location"] = "US"

    console.print("  ✓ Found all required environment variables")

    # Check for Cloud Storage configuration
    if env_vars.get("storage_bucket"):
        console.print("  ✓ Found Cloud Storage staging configuration")

    return env_vars


__all__ = ["setup_bigquery_credentials", "validate_bigquery_credentials"]
