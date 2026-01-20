"""
Test cases for CLI validation functionality.
"""

from unittest.mock import Mock, patch

import pytest
from rich.console import Console

from benchbox.cli.validation import CLIValidationRunner, ValidationDisplay
from benchbox.core.validation import (
    PlatformValidationResult,
    ValidationResult,
    ValidationService,
    ValidationSummary,
)

pytestmark = pytest.mark.fast


class TestValidationDisplay:
    """Test cases for ValidationDisplay."""

    def setup_method(self):
        """Set up test fixtures."""
        self.console = Mock(spec=Console)
        self.display = ValidationDisplay(self.console)

    def test_init(self):
        """Test ValidationDisplay initialization."""
        assert isinstance(self.display, ValidationDisplay)
        assert self.display.console == self.console

    def test_init_default_console(self):
        """Test ValidationDisplay initialization with default console."""
        display = ValidationDisplay()
        assert hasattr(display, "console")
        assert display.console is not None

    def test_display_validation_result_passed(self):
        """Test displaying a passed validation result."""
        result = ValidationResult(
            is_valid=True,
            errors=[],
            warnings=["Minor warning"],
            details={"benchmark_type": "tpcds"},
        )

        self.display.display_validation_result(result, "Test Validation")

        # Verify console.print was called
        self.console.print.assert_called_once()
        args = self.console.print.call_args[0]
        # Check that a Panel was passed
        assert hasattr(args[0], "title")

    def test_display_validation_result_failed(self):
        """Test displaying a failed validation result."""
        result = ValidationResult(
            is_valid=False,
            errors=["Critical error", "Another error"],
            warnings=["Warning message"],
            details={"benchmark_type": "tpcds"},
        )

        self.display.display_validation_result(result, "Failed Validation")

        # Verify console.print was called
        self.console.print.assert_called_once()

    def test_display_validation_result_none(self):
        """Test displaying None validation result."""
        self.display.display_validation_result(None, "Missing Validation")

        # Verify warning message was printed
        self.console.print.assert_called_once()
        args = self.console.print.call_args[0]
        assert "Warning:" in args[0]

    def test_display_validation_summary(self):
        """Test displaying validation summary."""
        summary = ValidationSummary(
            total_validations=5,
            passed_validations=3,
            failed_validations=2,
            warnings_count=4,
        )

        self.display.display_validation_summary(summary, "Test Summary")

        # Verify console.print was called
        self.console.print.assert_called_once()


class TestCLIValidationRunner:
    """Test cases for CLIValidationRunner."""

    def setup_method(self):
        self.console = Mock(spec=Console)
        self.service = Mock(spec=ValidationService)
        self.runner = CLIValidationRunner(self.console, show_details=True, service=self.service)

    def test_init(self):
        assert isinstance(self.runner, CLIValidationRunner)
        assert self.runner.console == self.console
        assert self.runner.show_details is True
        assert isinstance(self.runner.service, ValidationService)

    def test_init_default_console(self):
        runner = CLIValidationRunner()
        assert hasattr(runner, "console")
        assert runner.console is not None

    def test_run_preflight_validation(self, tmp_path):
        mock_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_preflight.return_value = mock_result

        result = self.runner.run_preflight_validation("tpcds", 1.0, tmp_path, quiet=False)

        self.service.run_preflight.assert_called_once_with("tpcds", 1.0, tmp_path)
        assert result == mock_result
        self.console.print.assert_called()

    def test_run_preflight_validation_quiet(self, tmp_path):
        mock_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_preflight.return_value = mock_result

        result = self.runner.run_preflight_validation("tpcds", 1.0, tmp_path, quiet=True)

        self.service.run_preflight.assert_called_once_with("tpcds", 1.0, tmp_path)
        assert result == mock_result
        self.console.print.assert_not_called()

    def test_run_data_validation(self, tmp_path):
        manifest_path = tmp_path / "manifest.json"
        mock_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_manifest.return_value = mock_result

        result = self.runner.run_data_validation(manifest_path, quiet=False)

        self.service.run_manifest.assert_called_once_with(manifest_path)
        assert result == mock_result
        self.console.print.assert_called()

    def test_run_database_validation(self):
        mock_connection = Mock()
        mock_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_database.return_value = mock_result

        result = self.runner.run_database_validation(mock_connection, "tpcds", 1.0, quiet=False)

        self.service.run_database.assert_called_once_with(mock_connection, "tpcds", 1.0)
        assert result == mock_result
        self.console.print.assert_called()

    def test_run_platform_validation(self):
        mock_adapter = Mock()
        mock_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_platform.return_value = PlatformValidationResult(capabilities=mock_result)

        result = self.runner.run_platform_validation(mock_adapter, "tpcds", quiet=False)

        self.service.run_platform.assert_called_once_with(mock_adapter, "tpcds", connection=None)
        assert result == mock_result
        self.console.print.assert_called()

    def test_run_platform_validation_with_connection_health(self):
        mock_adapter = Mock()
        mock_connection = Mock()
        capabilities = ValidationResult(is_valid=True, errors=[], warnings=[])
        health_result = ValidationResult(is_valid=True, errors=[], warnings=["warn"])
        self.service.run_platform.return_value = PlatformValidationResult(
            capabilities=capabilities,
            connection_health=health_result,
        )

        result = self.runner.run_platform_validation(
            mock_adapter,
            "tpcds",
            connection=mock_connection,
            quiet=False,
        )

        self.service.run_platform.assert_called_once_with(mock_adapter, "tpcds", connection=mock_connection)
        assert result == capabilities
        assert self.console.print.call_count >= 1

    def test_run_comprehensive_validation(self, tmp_path):
        mock_adapter = Mock()
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text("{}")

        preflight_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        platform_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        data_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        db_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        results_list = [preflight_result, platform_result, data_result, db_result]

        self.service.run_comprehensive.return_value = results_list
        summary = ValidationSummary(4, 4, 0, 0)
        self.service.summarize.return_value = summary

        results = self.runner.run_comprehensive_validation(
            benchmark_type="tpcds",
            scale_factor=1.0,
            output_dir=tmp_path,
            manifest_path=manifest_path,
            connection=Mock(),
            platform_adapter=mock_adapter,
            quiet=False,
        )

        self.service.run_comprehensive.assert_called_once()
        self.service.summarize.assert_called_once_with(results_list)
        assert results == results_list
        self.console.print.assert_called()

    def test_run_comprehensive_validation_quiet(self, tmp_path):
        preflight_result = ValidationResult(is_valid=True, errors=[], warnings=[])
        self.service.run_comprehensive.return_value = [preflight_result]

        results = self.runner.run_comprehensive_validation(
            benchmark_type="tpcds",
            scale_factor=1.0,
            output_dir=tmp_path,
            quiet=True,
        )

        self.service.run_comprehensive.assert_called_once()
        self.service.summarize.assert_not_called()
        assert results == [preflight_result]
        self.console.print.assert_not_called()


# Integration tests for the validation components working together
class TestValidationIntegration:
    """Integration tests for validation components."""

    def test_validation_display_with_real_result(self):
        """Test ValidationDisplay with real ValidationResult."""
        console = Mock(spec=Console)
        display = ValidationDisplay(console)

        result = ValidationResult(
            is_valid=False,
            errors=["Table count mismatch", "Missing critical table"],
            warnings=["File size below threshold"],
            details={
                "benchmark_type": "tpcds",
                "platform": "DuckDB",
                "table_count": 23,
            },
        )

        display.display_validation_result(result, "Data Validation")

        # Verify console.print was called
        console.print.assert_called_once()

        # Get the printed content
        args = console.print.call_args[0]
        panel = args[0]

        # Verify it's a Panel with the correct title
        assert hasattr(panel, "title")
        assert panel.title == "Data Validation"

    def test_cli_runner_with_real_engines(self, tmp_path):
        """Test CLIValidationRunner with real validation engines."""
        console = Mock(spec=Console)
        runner = CLIValidationRunner(console, show_details=False)

        # Test preflight validation
        result = runner.run_preflight_validation("tpcds", 1.0, tmp_path, quiet=True)

        # Should get a real ValidationResult
        assert isinstance(result, ValidationResult)
        assert result.is_valid  # Should pass for valid inputs

    def test_cli_runner_delegates_to_core_engines(self, tmp_path):
        """Verify that CLIValidationRunner correctly calls the core engines."""
        with (
            patch("benchbox.core.validation.DataValidationEngine.validate_preflight_conditions") as mock_preflight,
            patch("benchbox.core.validation.DatabaseValidationEngine.validate_loaded_data") as mock_db_validate,
        ):
            mock_preflight.return_value = ValidationResult(is_valid=True)
            mock_db_validate.return_value = ValidationResult(is_valid=True)

            runner = CLIValidationRunner()
            runner.run_preflight_validation("tpch", 1.0, tmp_path, quiet=True)
            mock_preflight.assert_called_once_with("tpch", 1.0, tmp_path)

            mock_connection = Mock()
            runner.run_database_validation(mock_connection, "tpch", 1.0, quiet=True)
            mock_db_validate.assert_called_once_with(mock_connection, "tpch", 1.0)
