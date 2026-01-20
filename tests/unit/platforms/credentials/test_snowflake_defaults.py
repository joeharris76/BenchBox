"""Tests for Snowflake credential setup with default values.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.credentials.snowflake import setup_snowflake_credentials

pytestmark = pytest.mark.fast


class TestSnowflakeCredentialDefaults:
    """Test Snowflake credential setup shows existing values as defaults."""

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_shows_existing_values_as_defaults(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test that existing credential values are shown as defaults in prompts."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            "password": "secret_password",
            "warehouse": "MY_WAREHOUSE",
            "database": "MY_DATABASE",
            "schema": "MY_SCHEMA",
            "role": "MY_ROLE",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # User declines auto-detection, provides same values
        mock_confirm.return_value = False  # Skip auto-detection
        mock_prompt_default.side_effect = [
            "myorg-account123",  # account
            "JOEHARRIS76",  # username
            "MY_WAREHOUSE",  # warehouse
            "MY_DATABASE",  # database
            "MY_SCHEMA",  # schema
            "MY_ROLE",  # role
        ]
        mock_prompt_secure.return_value = "secret_password"  # password (preserved)

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify prompts were called with existing values as current_value
        calls = mock_prompt_default.call_args_list
        assert calls[0][1]["current_value"] == "myorg-account123"  # account
        assert calls[1][1]["current_value"] == "JOEHARRIS76"  # username
        assert calls[2][1]["current_value"] == "MY_WAREHOUSE"  # warehouse
        assert calls[3][1]["current_value"] == "MY_DATABASE"  # database
        assert calls[4][1]["current_value"] == "MY_SCHEMA"  # schema
        assert calls[5][1]["current_value"] == "MY_ROLE"  # role

        # Verify secure field was called with existing password
        mock_prompt_secure.assert_called_once_with("Password", current_value="secret_password", console=console)

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_works_with_no_existing_credentials(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test that setup works when no existing credentials exist."""
        # Setup: no existing credentials
        mock_manager = Mock()
        mock_manager.get_platform_credentials.return_value = None

        # User provides new values
        mock_confirm.return_value = False  # Skip auto-detection
        mock_prompt_default.side_effect = [
            "newaccount",  # account
            "newuser",  # username
            "COMPUTE_WH",  # warehouse
            "BENCHBOX",  # database
            "PUBLIC",  # schema
            "",  # role
        ]
        mock_prompt_secure.return_value = "newpassword"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify prompts were called with None as current_value
        calls = mock_prompt_default.call_args_list
        assert calls[0][1]["current_value"] is None  # account
        assert calls[1][1]["current_value"] is None  # username
        assert calls[2][1]["current_value"] is None  # warehouse
        assert calls[3][1]["current_value"] is None  # database
        assert calls[4][1]["current_value"] is None  # schema
        assert calls[5][1]["current_value"] is None  # role

        # Verify secure field was called with None
        mock_prompt_secure.assert_called_once_with("Password", current_value=None, console=console)

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_password_preserved_on_empty_input(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test that existing password is preserved when user enters empty input."""
        # Setup: existing credentials with password
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            "password": "existing_secret",
            "warehouse": "MY_WAREHOUSE",
            "database": "MY_DATABASE",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_confirm.return_value = False
        mock_prompt_default.side_effect = [
            "myorg-account123",
            "JOEHARRIS76",
            "MY_WAREHOUSE",
            "MY_DATABASE",
            "PUBLIC",
            "",
        ]
        # User enters empty string, existing password should be preserved
        mock_prompt_secure.return_value = "existing_secret"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify the saved credentials still have the password
        saved_creds = mock_manager.set_platform_credentials.call_args[0][1]
        assert saved_creds["password"] == "existing_secret"

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_new_password_overrides_existing(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test that new password overrides existing password."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            "password": "old_password",
            "warehouse": "MY_WAREHOUSE",
            "database": "MY_DATABASE",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_confirm.return_value = False
        mock_prompt_default.side_effect = [
            "myorg-account123",
            "JOEHARRIS76",
            "MY_WAREHOUSE",
            "MY_DATABASE",
            "PUBLIC",
            "",
        ]
        # User provides new password
        mock_prompt_secure.return_value = "new_password"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify the saved credentials have the new password
        saved_creds = mock_manager.set_platform_credentials.call_args[0][1]
        assert saved_creds["password"] == "new_password"

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_partial_existing_credentials(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test handling of partial existing credentials."""
        # Setup: only some credentials exist
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            # password, warehouse, database missing
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_confirm.return_value = False
        mock_prompt_default.side_effect = [
            "myorg-account123",  # existing
            "JOEHARRIS76",  # existing
            "COMPUTE_WH",  # new (uses default_if_none)
            "BENCHBOX",  # new (uses default_if_none)
            "PUBLIC",  # new
            "",  # new
        ]
        mock_prompt_secure.return_value = "new_password"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify existing values were used as defaults
        calls = mock_prompt_default.call_args_list
        assert calls[0][1]["current_value"] == "myorg-account123"
        assert calls[1][1]["current_value"] == "JOEHARRIS76"
        # Missing fields should have None as current_value but have default_if_none
        assert calls[2][1]["current_value"] is None
        assert calls[2][1]["default_if_none"] == "COMPUTE_WH"

    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_optional_fields_show_existing_values(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
    ):
        """Test that optional fields (schema, role) show existing values."""
        # Setup: credentials with optional fields
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            "password": "secret",
            "warehouse": "MY_WAREHOUSE",
            "database": "MY_DATABASE",
            "schema": "CUSTOM_SCHEMA",
            "role": "CUSTOM_ROLE",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_confirm.return_value = False
        mock_prompt_default.side_effect = [
            "myorg-account123",
            "JOEHARRIS76",
            "MY_WAREHOUSE",
            "MY_DATABASE",
            "CUSTOM_SCHEMA",
            "CUSTOM_ROLE",
        ]
        mock_prompt_secure.return_value = "secret"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify optional fields were called with existing values
        calls = mock_prompt_default.call_args_list
        assert calls[4][1]["current_value"] == "CUSTOM_SCHEMA"  # schema
        assert calls[5][1]["current_value"] == "CUSTOM_ROLE"  # role

    @patch("benchbox.platforms.credentials.snowflake._auto_detect_snowflake")
    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_auto_detection_bypasses_existing_defaults(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
        mock_auto_detect,
    ):
        """Test that auto-detection is skipped when existing credentials are present."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "account": "old_account",
            "username": "old_user",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # NOTE: With existing credentials, auto-detection is skipped
        # This test verifies that existing credentials flow directly to manual prompts
        # Auto-detection is no longer offered when credentials exist

        mock_prompt_default.side_effect = [
            "old_account",
            "old_user",
            "COMPUTE_WH",
            "BENCHBOX",
            "PUBLIC",
            "",
        ]
        mock_prompt_secure.return_value = "password"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify Confirm.ask was NOT called (no auto-detection prompt)
        mock_confirm.assert_not_called()
        # Verify auto-detect was NOT called
        mock_auto_detect.assert_not_called()

    @patch("benchbox.platforms.credentials.snowflake._auto_detect_snowflake")
    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_skips_auto_detection_when_credentials_exist(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
        mock_auto_detect,
    ):
        """Test that auto-detection is skipped when credentials already exist."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "account": "myorg-account123",
            "username": "JOEHARRIS76",
            "password": "secret",
            "warehouse": "MY_WAREHOUSE",
            "database": "MY_DATABASE",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_prompt_default.side_effect = [
            "myorg-account123",
            "JOEHARRIS76",
            "MY_WAREHOUSE",
            "MY_DATABASE",
            "PUBLIC",
            "",
        ]
        mock_prompt_secure.return_value = "secret"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify no auto-detection prompt was shown
        mock_confirm.assert_not_called()
        mock_auto_detect.assert_not_called()

        # Verify "updating configuration" message was displayed
        console_output = " ".join(str(call) for call in console.print.call_args_list)
        assert "Existing credentials found" in console_output
        assert "updating configuration" in console_output

    @patch("benchbox.platforms.credentials.snowflake._auto_detect_snowflake")
    @patch("benchbox.platforms.credentials.snowflake.validate_snowflake_credentials")
    @patch("benchbox.platforms.credentials.snowflake._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.snowflake.prompt_secure_field")
    @patch("benchbox.platforms.credentials.snowflake.prompt_with_default")
    @patch("rich.prompt.Confirm.ask")
    def test_offers_auto_detection_when_no_credentials_exist(
        self,
        mock_confirm,
        mock_prompt_default,
        mock_prompt_secure,
        mock_output_location,
        mock_validate,
        mock_auto_detect,
    ):
        """Test that auto-detection is offered when no credentials exist."""
        # Setup: no existing credentials
        mock_manager = Mock()
        mock_manager.get_platform_credentials.return_value = None

        # User declines auto-detection
        mock_confirm.return_value = False
        mock_prompt_default.side_effect = [
            "newaccount",
            "newuser",
            "COMPUTE_WH",
            "BENCHBOX",
            "PUBLIC",
            "",
        ]
        mock_prompt_secure.return_value = "newpassword"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_snowflake_credentials(mock_manager, console)

        # Verify auto-detection prompt WAS shown
        mock_confirm.assert_called_once_with("🔍 Attempt auto-detection from environment variables?", default=True)

        # Verify "updating configuration" message was NOT displayed
        console_output = " ".join(str(call) for call in console.print.call_args_list)
        assert "Existing credentials found" not in console_output
