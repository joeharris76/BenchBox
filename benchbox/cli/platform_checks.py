"""Platform requirement checks for interactive mode.

Provides early validation of platform requirements (credentials, cloud storage)
immediately after platform selection in the interactive flow.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from typing import Optional

from benchbox.security.credentials import CredentialManager, CredentialStatus


def check_and_setup_platform_credentials(
    platform: str,
    console_obj,
    interactive: bool = True,
) -> bool:
    """Check platform credentials and offer setup if missing.

    This function checks if credentials exist for the specified platform and
    optionally offers interactive setup if they're missing.

    Args:
        platform: Platform name (snowflake, bigquery, databricks, redshift)
        console_obj: Console object for output
        interactive: If True, offer interactive setup; if False, just check

    Returns:
        True if credentials are available or successfully configured,
        False if credentials are missing and setup was declined/failed
    """
    cred_manager = CredentialManager()

    # Check if credentials exist for this platform
    platform_creds = cred_manager.get_platform_credentials(platform)

    if platform_creds:
        # Credentials exist - could optionally validate them here
        return True

    # No credentials found
    if not interactive:
        return False

    # Offer interactive setup
    from rich.prompt import Confirm

    from benchbox.cli.commands.setup import run_platform_credential_setup

    console_obj.print(f"\n[yellow]⚠️  {platform.capitalize()} credentials not found[/yellow]")
    console_obj.print(f"\nTo use {platform.capitalize()}, you need to configure credentials.")

    # Ask if user wants to set up now
    if not Confirm.ask("\n🔧 Would you like to set up credentials now?", default=True):
        console_obj.print("[yellow]Skipping credential setup[/yellow]")
        console_obj.print(f"\n[dim]To set up later, run: benchbox setup --platform {platform}[/dim]")
        return False

    # Run interactive setup
    success = run_platform_credential_setup(platform, console_obj, show_welcome=True)

    if success:
        console_obj.print("\n[green]✅ Credentials configured successfully![/green]\n")
        return True
    else:
        console_obj.print("\n[red]❌ Credential setup failed[/red]")
        return False


def check_platform_credential_status(platform: str) -> tuple[bool, Optional[CredentialStatus]]:
    """Check credential status for a platform without prompting.

    Args:
        platform: Platform name

    Returns:
        Tuple of (credentials_exist: bool, status: CredentialStatus or None)
    """
    cred_manager = CredentialManager()
    platform_creds = cred_manager.get_platform_credentials(platform)

    if not platform_creds:
        return (False, CredentialStatus.MISSING)

    # Credentials exist - use the built-in method to get status
    status_enum = cred_manager.get_credential_status(platform)

    return (True, status_enum)
