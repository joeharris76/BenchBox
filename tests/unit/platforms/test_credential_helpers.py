"""Tests for credential setup helper utilities.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from unittest.mock import Mock, patch

import pytest

from benchbox.platforms.credentials.helpers import (
    format_prompt_with_current,
    prompt_secure_field,
    prompt_with_default,
)

pytestmark = pytest.mark.fast


class TestFormatPromptWithCurrent:
    """Test prompt text formatting with current values."""

    def test_formats_with_current_value(self):
        """Test that current value is shown in formatted prompt."""
        result = format_prompt_with_current("Username", "john_doe")
        assert result == "Username (current: john_doe)"

    def test_formats_without_current_value(self):
        """Test that prompt is unchanged when no current value."""
        result = format_prompt_with_current("Username", None)
        assert result == "Username"

    def test_formats_with_empty_string_current_value(self):
        """Test that empty string is not treated as a current value."""
        result = format_prompt_with_current("Username", "")
        assert result == "Username"

    def test_formats_with_complex_current_value(self):
        """Test formatting with complex value containing special characters."""
        result = format_prompt_with_current("Account", "myorg-account123.example.com")
        assert result == "Account (current: myorg-account123.example.com)"


class TestPromptWithDefault:
    """Test prompting with default values."""

    @patch("rich.prompt.Prompt.ask")
    def test_shows_current_value_as_default(self, mock_ask):
        """Test that current value is used as default in prompt."""
        mock_ask.return_value = "john_doe"

        result = prompt_with_default("Username", current_value="john_doe")

        assert result == "john_doe"
        mock_ask.assert_called_once_with("Username (current: john_doe)", default="john_doe", password=False)

    @patch("rich.prompt.Prompt.ask")
    def test_uses_default_if_none_when_no_current_value(self, mock_ask):
        """Test that default_if_none is used when no current value."""
        mock_ask.return_value = "COMPUTE_WH"

        result = prompt_with_default("Warehouse", current_value=None, default_if_none="COMPUTE_WH")

        assert result == "COMPUTE_WH"
        mock_ask.assert_called_once_with("Warehouse", default="COMPUTE_WH", password=False)

    @patch("rich.prompt.Prompt.ask")
    def test_current_value_takes_precedence_over_default_if_none(self, mock_ask):
        """Test that current_value takes precedence over default_if_none."""
        mock_ask.return_value = "MY_WH"

        result = prompt_with_default("Warehouse", current_value="MY_WH", default_if_none="COMPUTE_WH")

        assert result == "MY_WH"
        mock_ask.assert_called_once_with("Warehouse (current: MY_WH)", default="MY_WH", password=False)

    @patch("rich.prompt.Prompt.ask")
    def test_returns_none_when_empty_input_and_no_defaults(self, mock_ask):
        """Test that None is returned when user provides empty input and no defaults exist."""
        mock_ask.return_value = ""

        result = prompt_with_default("Optional field", current_value=None, default_if_none=None)

        assert result is None

    @patch("rich.prompt.Prompt.ask")
    def test_returns_user_input_when_changed(self, mock_ask):
        """Test that new user input overrides current value."""
        mock_ask.return_value = "new_value"

        result = prompt_with_default("Username", current_value="old_value")

        assert result == "new_value"

    @patch("rich.prompt.Prompt.ask")
    def test_password_mode_enabled(self, mock_ask):
        """Test that password mode is passed through correctly."""
        mock_ask.return_value = "secret"

        result = prompt_with_default("Password", current_value=None, password=True)

        assert result == "secret"
        mock_ask.assert_called_once_with("Password", default=None, password=True)


class TestPromptSecureField:
    """Test secure field prompting with value preservation."""

    @patch("rich.prompt.Prompt.ask")
    def test_shows_set_indicator_when_value_exists(self, mock_ask):
        """Test that ****SET**** indicator is shown when current value exists."""
        mock_ask.return_value = "new_password"

        result = prompt_secure_field("Password", current_value="existing_password")

        assert result == "new_password"
        mock_ask.assert_called_once_with("Password [current: ****SET****]", password=True, default="")

    @patch("rich.prompt.Prompt.ask")
    def test_preserves_existing_value_on_empty_input(self, mock_ask):
        """Test that existing value is preserved when user enters empty string."""
        mock_ask.return_value = ""

        result = prompt_secure_field("Password", current_value="existing_password")

        assert result == "existing_password"

    @patch("rich.prompt.Prompt.ask")
    def test_returns_new_value_when_provided(self, mock_ask):
        """Test that new value overrides existing value."""
        mock_ask.return_value = "new_password"

        result = prompt_secure_field("Password", current_value="old_password")

        assert result == "new_password"

    @patch("rich.prompt.Prompt.ask")
    def test_returns_none_when_no_existing_value_and_empty_input(self, mock_ask):
        """Test that None is returned when no existing value and user enters empty."""
        mock_ask.return_value = ""

        result = prompt_secure_field("Password", current_value=None)

        assert result is None

    @patch("rich.prompt.Prompt.ask")
    def test_no_indicator_when_no_existing_value(self, mock_ask):
        """Test that no SET indicator is shown when no existing value."""
        mock_ask.return_value = "new_password"

        result = prompt_secure_field("Password", current_value=None)

        assert result == "new_password"
        mock_ask.assert_called_once_with("Password", password=True, default="")

    @patch("rich.prompt.Prompt.ask")
    def test_shows_preservation_hint_with_console(self, mock_ask):
        """Test that preservation hint is shown when console is provided."""
        mock_ask.return_value = ""
        mock_console = Mock()

        result = prompt_secure_field("Access token", current_value="token123", console=mock_console)

        assert result == "token123"
        mock_console.print.assert_called_once_with("[dim]Leave empty to preserve current access token[/dim]")

    @patch("rich.prompt.Prompt.ask")
    def test_no_hint_without_console(self, mock_ask):
        """Test that no hint is shown when console is not provided."""
        mock_ask.return_value = ""

        result = prompt_secure_field("Password", current_value="secret", console=None)

        assert result == "secret"

    @patch("rich.prompt.Prompt.ask")
    def test_handles_token_field_name(self, mock_ask):
        """Test secure field handling with token field name."""
        mock_ask.return_value = ""

        result = prompt_secure_field("Access token", current_value="existing_token")

        assert result == "existing_token"
        mock_ask.assert_called_once_with("Access token [current: ****SET****]", password=True, default="")
