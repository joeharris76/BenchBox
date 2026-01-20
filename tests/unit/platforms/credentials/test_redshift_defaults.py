"""Tests for Redshift credential setup with default values.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.credentials.redshift import setup_redshift_credentials

pytestmark = pytest.mark.fast


class TestRedshiftCredentialDefaults:
    """Test Redshift credential setup shows existing values as defaults."""

    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_shows_existing_values_as_defaults(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
    ):
        """Test that existing credential values are shown as defaults in prompts."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "host": "my-cluster.abc123.us-east-1.redshift.amazonaws.com",
            "port": 5439,
            "database": "mydb",
            "username": "admin",
            "password": "secret",
            "schema": "public",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # With existing credentials, no auto-detection prompt - only S3 config
        mock_confirm.return_value = False  # Don't configure S3
        mock_int_prompt.return_value = 5439  # port
        mock_prompt_default.side_effect = [
            "my-cluster.abc123.us-east-1.redshift.amazonaws.com",  # host
            "mydb",  # database
            "admin",  # username
            "public",  # schema
        ]
        mock_prompt_secure.return_value = "secret"  # password

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify prompts were called with existing values as current_value
        calls = mock_prompt_default.call_args_list
        assert calls[0][1]["current_value"] == "my-cluster.abc123.us-east-1.redshift.amazonaws.com"  # host
        assert calls[1][1]["current_value"] == "mydb"  # database
        assert calls[2][1]["current_value"] == "admin"  # username
        assert calls[3][1]["current_value"] == "public"  # schema

        # Verify secure field was called with existing password
        mock_prompt_secure.assert_called_once_with("Password", current_value="secret", console=console)

    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_works_with_no_existing_credentials(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
    ):
        """Test that setup works when no existing credentials exist."""
        # Setup: no existing credentials
        mock_manager = Mock()
        mock_manager.get_platform_credentials.return_value = None

        # User provides new values
        mock_confirm.side_effect = [
            False,  # Skip auto-detection
            False,  # Don't configure S3
        ]
        mock_int_prompt.return_value = 5439  # port (default)
        mock_prompt_default.side_effect = [
            "new-cluster.xyz.us-east-1.redshift.amazonaws.com",  # host
            "dev",  # database (default)
            "newuser",  # username
            "public",  # schema (default)
        ]
        mock_prompt_secure.return_value = "newpassword"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify prompts were called with None as current_value
        calls = mock_prompt_default.call_args_list
        assert calls[0][1]["current_value"] is None  # host
        assert calls[1][1]["current_value"] is None  # database
        assert calls[1][1]["default_if_none"] == "dev"  # database default
        assert calls[2][1]["current_value"] is None  # username
        assert calls[3][1]["current_value"] is None  # schema
        assert calls[3][1]["default_if_none"] == "public"  # schema default

        # Verify secure field was called with None
        mock_prompt_secure.assert_called_once_with("Password", current_value=None, console=console)

    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_password_preserved_on_empty_input(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
    ):
        """Test that existing password is preserved when user enters empty input."""
        # Setup: existing credentials with password
        mock_manager = Mock()
        existing_creds = {
            "host": "my-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "mydb",
            "username": "admin",
            "password": "existing_password",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # With existing credentials, no auto-detection prompt
        mock_confirm.return_value = False  # Don't configure S3
        mock_int_prompt.return_value = 5439
        mock_prompt_default.side_effect = [
            "my-cluster.redshift.amazonaws.com",
            "mydb",
            "admin",
            "public",
        ]
        # User enters empty string, existing password should be preserved
        mock_prompt_secure.return_value = "existing_password"

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify the saved credentials still have the password
        saved_creds = mock_manager.set_platform_credentials.call_args[0][1]
        assert saved_creds["password"] == "existing_password"

    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_s3_credentials_show_existing_values(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
    ):
        """Test that S3 configuration shows existing values."""
        # Setup: existing credentials with S3 config (IAM role)
        mock_manager = Mock()
        existing_creds = {
            "host": "my-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "mydb",
            "username": "admin",
            "password": "secret",
            "s3_bucket": "my-benchbox-data",
            "iam_role": "arn:aws:iam::123456789012:role/RedshiftS3AccessRole",
            "aws_region": "us-east-1",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # With existing credentials, no auto-detection prompt
        mock_confirm.return_value = True  # Configure S3
        mock_int_prompt.side_effect = [
            5439,  # port
            1,  # IAM role auth method
        ]
        mock_prompt_default.side_effect = [
            "my-cluster.redshift.amazonaws.com",  # host
            "mydb",  # database
            "admin",  # username
            "public",  # schema
            "my-benchbox-data",  # s3_bucket
            "arn:aws:iam::123456789012:role/RedshiftS3AccessRole",  # iam_role
            "us-east-1",  # aws_region
        ]
        mock_prompt_secure.return_value = "secret"  # password

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify S3 prompts were called with existing values
        calls = mock_prompt_default.call_args_list
        assert calls[4][1]["current_value"] == "my-benchbox-data"  # s3_bucket
        assert calls[5][1]["current_value"] == "arn:aws:iam::123456789012:role/RedshiftS3AccessRole"  # iam_role
        assert calls[6][1]["current_value"] == "us-east-1"  # aws_region

    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_s3_access_keys_show_existing_values(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
    ):
        """Test that S3 access keys show existing values."""
        # Setup: existing credentials with S3 config (access keys)
        mock_manager = Mock()
        existing_creds = {
            "host": "my-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "mydb",
            "username": "admin",
            "password": "secret",
            "s3_bucket": "my-bucket",
            "aws_access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "aws_secret_access_key": "secret_key",
            "aws_region": "us-west-2",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # With existing credentials, no auto-detection prompt
        mock_confirm.return_value = True  # Configure S3
        mock_int_prompt.side_effect = [
            5439,  # port
            2,  # Access keys auth method
        ]
        mock_prompt_default.side_effect = [
            "my-cluster.redshift.amazonaws.com",  # host
            "mydb",  # database
            "admin",  # username
            "public",  # schema
            "my-bucket",  # s3_bucket
            "AKIAIOSFODNN7EXAMPLE",  # aws_access_key_id
            "us-west-2",  # aws_region
        ]
        mock_prompt_secure.side_effect = [
            "secret",  # password
            "secret_key",  # aws_secret_access_key
        ]

        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify AWS access key and secret key prompts were called with existing values
        default_calls = mock_prompt_default.call_args_list
        assert default_calls[5][1]["current_value"] == "AKIAIOSFODNN7EXAMPLE"  # access_key_id

        secure_calls = mock_prompt_secure.call_args_list
        assert secure_calls[1][0][0] == "AWS Secret Access Key"  # field name
        assert secure_calls[1][1]["current_value"] == "secret_key"  # secret_access_key

    @patch("benchbox.platforms.credentials.redshift._auto_detect_redshift")
    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_auto_detection_bypasses_existing_defaults(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
        mock_auto_detect,
    ):
        """Test that auto-detection is skipped when existing credentials are present."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "host": "old-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "olddb",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        # NOTE: With existing credentials, auto-detection is skipped
        mock_confirm.return_value = False  # Don't configure S3
        mock_int_prompt.return_value = 5439
        mock_prompt_default.side_effect = [
            "old-cluster.redshift.amazonaws.com",
            "olddb",
            "admin",
            "public",
        ]
        mock_prompt_secure.return_value = "password"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify Confirm.ask was NOT called for auto-detection
        # (only called for S3 configuration)
        assert mock_confirm.call_count == 1
        storage_call = mock_confirm.call_args_list[0]
        assert "s3" in str(storage_call).lower()
        # Verify auto-detect was NOT called
        mock_auto_detect.assert_not_called()

    @patch("benchbox.platforms.credentials.redshift._auto_detect_redshift")
    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_skips_auto_detection_when_credentials_exist(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
        mock_auto_detect,
    ):
        """Test that auto-detection is skipped when credentials already exist."""
        # Setup: existing credentials
        mock_manager = Mock()
        existing_creds = {
            "host": "my-cluster.redshift.amazonaws.com",
            "port": 5439,
            "database": "mydb",
            "username": "admin",
            "password": "secret",
        }
        mock_manager.get_platform_credentials.return_value = existing_creds

        mock_confirm.return_value = False  # Don't configure S3
        mock_int_prompt.return_value = 5439
        mock_prompt_default.side_effect = [
            "my-cluster.redshift.amazonaws.com",
            "mydb",
            "admin",
            "public",
        ]
        mock_prompt_secure.return_value = "secret"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify no auto-detection prompt was shown (only S3 config prompt)
        assert mock_confirm.call_count == 1
        mock_auto_detect.assert_not_called()

        # Verify "updating configuration" message was displayed
        console_output = " ".join(str(call) for call in console.print.call_args_list)
        assert "Existing credentials found" in console_output
        assert "updating configuration" in console_output

    @patch("benchbox.platforms.credentials.redshift._auto_detect_redshift")
    @patch("benchbox.platforms.credentials.redshift._prompt_default_output_location")
    @patch("benchbox.platforms.credentials.redshift.validate_redshift_credentials")
    @patch("benchbox.platforms.credentials.redshift.prompt_secure_field")
    @patch("benchbox.platforms.credentials.redshift.prompt_with_default")
    @patch("rich.prompt.IntPrompt.ask")
    @patch("rich.prompt.Confirm.ask")
    def test_offers_auto_detection_when_no_credentials_exist(
        self,
        mock_confirm,
        mock_int_prompt,
        mock_prompt_default,
        mock_prompt_secure,
        mock_validate,
        mock_output_location,
        mock_auto_detect,
    ):
        """Test that auto-detection is offered when no credentials exist."""
        # Setup: no existing credentials
        mock_manager = Mock()
        mock_manager.get_platform_credentials.return_value = None

        # User declines auto-detection and S3
        mock_confirm.side_effect = [False, False]  # auto-detect, S3
        mock_int_prompt.return_value = 5439
        mock_prompt_default.side_effect = [
            "new-cluster.redshift.amazonaws.com",
            "dev",
            "newuser",
            "public",
        ]
        mock_prompt_secure.return_value = "newpassword"
        mock_validate.return_value = (True, None)
        console = Mock()

        setup_redshift_credentials(mock_manager, console)

        # Verify auto-detection prompt WAS shown
        auto_detect_call = mock_confirm.call_args_list[0]
        assert "auto-detection" in str(auto_detect_call).lower()

        # Verify "updating configuration" message was NOT displayed
        console_output = " ".join(str(call) for call in console.print.call_args_list)
        assert "Existing credentials found" not in console_output
