"""Coverage tests for benchbox/cli/exceptions.py and benchbox/cli/commands/run.py.

Wave: coverage-per-module-50-remaining (w9)
Target: push benchbox/cli/commands/run.py from 47% to >=50%.
benchbox/cli/exceptions.py is already at 72%.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

# ===================================================================
# PlatformOptionParamType
# ===================================================================


class TestPlatformOptionParamType:
    def test_convert_valid(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        key, val = pt.convert("memory_limit=8GB", None, None)
        assert key == "memory_limit"
        assert val == "8GB"

    def test_convert_equals_in_value(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        key, val = pt.convert("setting=a=b=c", None, None)
        assert key == "setting"
        assert val == "a=b=c"

    def test_convert_no_equals_fails(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        with pytest.raises(Exception):
            pt.convert("no_equals_sign", None, MagicMock())

    def test_convert_empty_key_fails(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        with pytest.raises(Exception):
            pt.convert("=value", None, MagicMock())

    def test_convert_strips_whitespace(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        key, val = pt.convert("  key  =  value  ", None, None)
        assert key == "key"
        assert val == "value"

    def test_name_attribute(self):
        from benchbox.cli.commands.run import PlatformOptionParamType

        pt = PlatformOptionParamType()
        assert pt.name == "key=value"


# ===================================================================
# _build_execution_context
# ===================================================================


class TestBuildExecutionContext:
    def test_basic(self):
        from benchbox.cli.commands.run import _build_execution_context
        from benchbox.cli.composite_params import CompressionConfig, ForceConfig, ValidationConfig

        ctx = _build_execution_context(
            phases_to_run=["load", "power"],
            seed=42,
            compression=CompressionConfig(enabled=True, type="zstd", level=3),
            mode="sql",
            official=False,
            validation=ValidationConfig(mode="exact"),
            force=ForceConfig(datagen=False, upload=False),
            queries_to_run=["Q1", "Q6"],
            capture_plans=True,
            strict_plan_capture=False,
            non_interactive=True,
            tuning="tuned",
        )
        assert ctx.entry_point == "cli"
        assert ctx.phases == ["load", "power"]
        assert ctx.seed == 42
        assert ctx.compression_enabled is True
        assert ctx.compression_type == "zstd"
        assert ctx.compression_level == 3
        assert ctx.mode == "sql"
        assert ctx.official is False
        assert ctx.validation_mode is None  # "exact" produces None
        assert ctx.force_datagen is False
        assert ctx.query_subset == ["Q1", "Q6"]
        assert ctx.capture_plans is True
        assert ctx.non_interactive is True
        assert ctx.tuning_mode == "tuned"

    def test_mode_defaults_to_sql(self):
        from benchbox.cli.commands.run import _build_execution_context
        from benchbox.cli.composite_params import CompressionConfig, ForceConfig, ValidationConfig

        ctx = _build_execution_context(
            phases_to_run=["power"],
            seed=None,
            compression=CompressionConfig(enabled=False, type="none", level=None),
            mode=None,
            official=False,
            validation=ValidationConfig(mode="loose"),
            force=ForceConfig(datagen=True, upload=False),
            queries_to_run=None,
            capture_plans=False,
            strict_plan_capture=False,
            non_interactive=False,
            tuning="notuning",
        )
        assert ctx.mode == "sql"
        assert ctx.seed is None
        assert ctx.query_subset is None
        assert ctx.tuning_mode is None  # "notuning" produces None
        assert ctx.validation_mode == "loose"
        assert ctx.force_datagen is True

    def test_official_mode(self):
        from benchbox.cli.commands.run import _build_execution_context
        from benchbox.cli.composite_params import CompressionConfig, ForceConfig, ValidationConfig

        ctx = _build_execution_context(
            phases_to_run=["power"],
            seed=12345,
            compression=CompressionConfig(enabled=True, type="gzip", level=6),
            mode="dataframe",
            official=True,
            validation=ValidationConfig(mode="full"),
            force=ForceConfig(datagen=False, upload=True),
            queries_to_run=None,
            capture_plans=False,
            strict_plan_capture=True,
            non_interactive=True,
            tuning="auto",
        )
        assert ctx.official is True
        assert ctx.mode == "dataframe"
        assert ctx.strict_plan_capture is True
        assert ctx.force_upload is True
        assert ctx.tuning_mode == "auto"


# ===================================================================
# _render_post_run_charts
# ===================================================================


class TestRenderPostRunCharts:
    def test_quiet_mode_skips(self):
        from benchbox.cli.commands.run import _render_post_run_charts

        mock_result = MagicMock()
        mock_console = MagicMock()
        _render_post_run_charts(mock_result, mock_console, quiet=True)
        mock_console.print.assert_not_called()

    def test_empty_query_results_skips(self):
        from benchbox.cli.commands.run import _render_post_run_charts

        mock_result = MagicMock()
        mock_result.query_results = []
        mock_console = MagicMock()
        _render_post_run_charts(mock_result, mock_console, quiet=False)
        mock_console.print.assert_not_called()

    def test_renders_charts(self):
        from benchbox.cli.commands.run import _render_post_run_charts

        mock_result = MagicMock()
        mock_result.query_results = [MagicMock()]
        mock_console = MagicMock()

        mock_summary = MagicMock()
        mock_summary.charts = ["chart1", "chart2"]

        with patch(
            "benchbox.core.visualization.post_run_summary.generate_post_run_summary",
            return_value=mock_summary,
        ):
            _render_post_run_charts(mock_result, mock_console, quiet=False)
        assert mock_console.print.call_count == 2

    def test_exception_swallowed(self):
        from benchbox.cli.commands.run import _render_post_run_charts

        mock_result = MagicMock()
        mock_result.query_results = [MagicMock()]
        mock_console = MagicMock()

        with patch(
            "benchbox.core.visualization.post_run_summary.generate_post_run_summary",
            side_effect=ImportError("no module"),
        ):
            _render_post_run_charts(mock_result, mock_console, quiet=False)
        # Should not raise


# ===================================================================
# _describe_platform_options
# ===================================================================


class TestDescribePlatformOptions:
    def test_no_options(self):
        from benchbox.cli.commands.run import _describe_platform_options

        with patch("benchbox.cli.commands.run.console") as mock_console:
            with patch(
                "benchbox.cli.platform_hooks.PlatformHookRegistry.describe_options",
                return_value=[],
            ):
                _describe_platform_options(["duckdb"])
        # Should print "no platform-specific options registered"
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("no platform-specific options registered" in c for c in calls)

    def test_with_options(self):
        from benchbox.cli.commands.run import _describe_platform_options

        with patch("benchbox.cli.commands.run.console") as mock_console:
            with patch(
                "benchbox.cli.platform_hooks.PlatformHookRegistry.describe_options",
                return_value=["memory_limit: Set memory limit", "threads: Set thread count"],
            ):
                _describe_platform_options(["duckdb"])
        calls = [str(c) for c in mock_console.print.call_args_list]
        assert any("memory_limit" in c for c in calls)


# ===================================================================
# setup_verbose_logging
# ===================================================================


class TestSetupVerboseLogging:
    def test_quiet_mode(self):
        from benchbox.cli.commands.run import setup_verbose_logging

        logger_result, settings = setup_verbose_logging(verbose=0, quiet=True)
        assert logger_result is None
        assert settings.quiet is True

    def test_verbose_level_1(self):
        from benchbox.cli.commands.run import setup_verbose_logging

        logger_result, settings = setup_verbose_logging(verbose=1, quiet=False)
        assert logger_result is not None
        assert settings.verbose_enabled is True
        assert settings.very_verbose is False

    def test_verbose_level_2(self):
        from benchbox.cli.commands.run import setup_verbose_logging

        logger_result, settings = setup_verbose_logging(verbose=2, quiet=False)
        assert logger_result is not None
        assert settings.very_verbose is True

    def test_no_verbose_no_quiet(self):
        from benchbox.cli.commands.run import setup_verbose_logging

        logger_result, settings = setup_verbose_logging(verbose=0, quiet=False)
        assert logger_result is None
        assert settings.quiet is False
        assert settings.verbose_enabled is False

    def test_verbosity_settings_passthrough(self):
        from benchbox.cli.commands.run import setup_verbose_logging
        from benchbox.utils.verbosity import VerbositySettings

        vs = VerbositySettings.from_flags(2, False)
        logger_result, settings = setup_verbose_logging(verbose=vs, quiet=False)
        assert settings is vs
        assert logger_result is not None

    def test_verbosity_settings_quiet_override(self):
        from benchbox.cli.commands.run import setup_verbose_logging
        from benchbox.utils.verbosity import VerbositySettings

        vs = VerbositySettings.from_flags(1, False)
        logger_result, settings = setup_verbose_logging(verbose=vs, quiet=True)
        assert settings.quiet is True


# ===================================================================
# CLI exceptions: ErrorHandler branch coverage
# ===================================================================


class TestErrorHandlerBranches:
    def test_handle_database_error(self):
        from benchbox.cli.exceptions import DatabaseError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = DatabaseError("connection refused", include_version=False)
        ctx = ErrorContext(operation="load", stage="connect", database_type="duckdb")
        handler.handle_error(err, ctx)

    def test_handle_database_error_postgresql(self):
        from benchbox.cli.exceptions import DatabaseError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = DatabaseError("connection refused", include_version=False)
        ctx = ErrorContext(operation="load", stage="connect", database_type="postgresql")
        handler.handle_error(err, ctx)

    def test_handle_execution_error_tpch(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler, ExecutionError

        handler = ErrorHandler(console=MagicMock())
        err = ExecutionError("out of memory", include_version=False)
        ctx = ErrorContext(operation="power", stage="query", benchmark_name="tpch")
        handler.handle_error(err, ctx)

    def test_handle_execution_error_no_benchmark(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler, ExecutionError

        handler = ErrorHandler(console=MagicMock())
        err = ExecutionError("timeout", include_version=False)
        ctx = ErrorContext(operation="power", stage="query")
        handler.handle_error(err, ctx)

    def test_handle_validation_error(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler, ValidationError

        handler = ErrorHandler(console=MagicMock())
        err = ValidationError("bad input", include_version=False)
        ctx = ErrorContext(operation="validate", stage="input")
        handler.handle_error(err, ctx)

    def test_handle_cloud_storage_error_s3(self):
        from benchbox.cli.exceptions import CloudStorageError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = CloudStorageError("access denied", details={"provider": "s3"}, include_version=False)
        ctx = ErrorContext(operation="upload", stage="transfer")
        handler.handle_error(err, ctx)

    def test_handle_cloud_storage_error_gcs(self):
        from benchbox.cli.exceptions import CloudStorageError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = CloudStorageError("not found", details={"provider": "gs"}, include_version=False)
        ctx = ErrorContext(operation="upload", stage="transfer")
        handler.handle_error(err, ctx)

    def test_handle_cloud_storage_error_azure(self):
        from benchbox.cli.exceptions import CloudStorageError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = CloudStorageError("forbidden", details={"provider": "azure"}, include_version=False)
        ctx = ErrorContext(operation="upload", stage="transfer")
        handler.handle_error(err, ctx)

    def test_handle_platform_error(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler, PlatformError

        handler = ErrorHandler(console=MagicMock())
        err = PlatformError("driver missing", include_version=False)
        ctx = ErrorContext(operation="connect", stage="init")
        handler.handle_error(err, ctx)

    def test_handle_generic_cli_error(self):
        from benchbox.cli.exceptions import BenchboxCLIError, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = BenchboxCLIError("generic", include_version=False)
        handler.handle_error(err)

    def test_handle_generic_error_with_traceback(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = RuntimeError("unexpected")
        ctx = ErrorContext(operation="run", stage="execute", include_version_info=False)
        handler.handle_error(err, ctx, show_traceback=True)

    def test_handle_cli_error_with_traceback(self):
        from benchbox.cli.exceptions import BenchboxCLIError, ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = BenchboxCLIError("test error", include_version=False)
        ctx = ErrorContext(operation="run", stage="test", include_version_info=False)
        handler.handle_error(err, ctx, show_traceback=True)

    def test_handle_error_with_version_details(self):
        from benchbox.cli.exceptions import BenchboxCLIError, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        err = BenchboxCLIError(
            "test",
            details={"version_warning": "mismatch", "version_info": "1.0"},
            include_version=False,
        )
        handler.handle_error(err)

    def test_show_context_with_source_location(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        ctx = ErrorContext(
            operation="run",
            stage="test",
            source_file="/path/to/benchbox/core/test.py",
            source_line=42,
            source_function="my_function",
            include_version_info=False,
        )
        handler._show_context_info(ctx)

    def test_show_context_non_benchbox_source(self):
        from benchbox.cli.exceptions import ErrorContext, ErrorHandler

        handler = ErrorHandler(console=MagicMock())
        ctx = ErrorContext(
            operation="run",
            stage="test",
            source_file="/some/other/path/module.py",
            source_line=10,
            include_version_info=False,
        )
        handler._show_context_info(ctx)


# ===================================================================
# ValidationRules
# ===================================================================


class TestValidationRules:
    def test_validate_scale_factor_negative(self):
        from benchbox.cli.exceptions import ValidationError, ValidationRules

        with pytest.raises(ValidationError):
            ValidationRules.validate_scale_factor(-1.0)

    def test_validate_scale_factor_zero(self):
        from benchbox.cli.exceptions import ValidationError, ValidationRules

        with pytest.raises(ValidationError):
            ValidationRules.validate_scale_factor(0.0)

    def test_validate_scale_factor_too_large(self):
        from benchbox.cli.exceptions import ValidationError, ValidationRules

        with pytest.raises(ValidationError, match="very large"):
            ValidationRules.validate_scale_factor(200.0)

    def test_validate_scale_factor_valid(self):
        from benchbox.cli.exceptions import ValidationRules

        ValidationRules.validate_scale_factor(1.0)  # should not raise

    def test_validate_benchmark_name_empty(self):
        from benchbox.cli.exceptions import ValidationError, ValidationRules

        with pytest.raises(ValidationError):
            ValidationRules.validate_benchmark_name("", ["tpch", "tpcds"])

    def test_validate_benchmark_name_unknown(self):
        from benchbox.cli.exceptions import ValidationError, ValidationRules

        with pytest.raises(ValidationError, match="Unknown benchmark"):
            ValidationRules.validate_benchmark_name("nonexistent", ["tpch", "tpcds"])

    def test_validate_benchmark_name_case_insensitive(self):
        from benchbox.cli.exceptions import ValidationRules

        ValidationRules.validate_benchmark_name("TPCH", ["tpch", "tpcds"])

    def test_validate_output_directory_local(self, tmp_path):
        from benchbox.cli.exceptions import ValidationRules

        out = str(tmp_path / "new_dir")
        ValidationRules.validate_output_directory(out)
        assert (tmp_path / "new_dir").exists()

    def test_validate_output_directory_cloud(self):
        from benchbox.cli.exceptions import CloudStorageError, ValidationRules

        with patch("benchbox.utils.cloud_storage.is_cloud_path", return_value=True):
            with patch(
                "benchbox.utils.cloud_storage.validate_cloud_credentials",
                return_value={"valid": False, "provider": "s3", "error": "no creds", "env_vars": ["AWS_ACCESS_KEY_ID"]},
            ):
                with pytest.raises(CloudStorageError):
                    ValidationRules.validate_output_directory("s3://bucket/path")


# ===================================================================
# ErrorContext
# ===================================================================


class TestErrorContext:
    def test_basic_creation(self):
        from benchbox.cli.exceptions import ErrorContext

        ctx = ErrorContext(operation="run", stage="init", include_version_info=False)
        assert ctx.operation == "run"
        assert ctx.stage == "init"
        assert ctx.benchmark_name is None

    def test_with_all_fields(self):
        from benchbox.cli.exceptions import ErrorContext

        ctx = ErrorContext(
            operation="run",
            stage="power",
            benchmark_name="tpch",
            database_type="duckdb",
            user_input={"scale": 1.0},
            include_version_info=False,
        )
        assert ctx.benchmark_name == "tpch"
        assert ctx.database_type == "duckdb"


# ===================================================================
# BenchboxCLIError source location
# ===================================================================


class TestBenchboxCLIErrorSourceLocation:
    def test_captures_source_location(self):
        from benchbox.cli.exceptions import BenchboxCLIError

        err = BenchboxCLIError("test", include_version=False)
        assert err.source_file is not None
        assert err.source_line is not None
        assert "test_run_command_coverage_wave_remaining" in err.source_file

    def test_with_details(self):
        from benchbox.cli.exceptions import BenchboxCLIError

        err = BenchboxCLIError("test", details={"key": "value"}, include_version=False)
        assert err.details["key"] == "value"

    def test_configuration_error(self):
        from benchbox.cli.exceptions import ConfigurationError

        err = ConfigurationError("bad config", include_version=False)
        assert err.message == "bad config"
        assert err.source_file is not None


# ===================================================================
# create_error_handler
# ===================================================================


class TestCreateErrorHandler:
    def test_default(self):
        from benchbox.cli.exceptions import ErrorHandler, create_error_handler

        handler = create_error_handler()
        assert isinstance(handler, ErrorHandler)

    def test_with_console(self):
        from benchbox.cli.exceptions import ErrorHandler, create_error_handler

        mock = MagicMock()
        handler = create_error_handler(mock)
        assert isinstance(handler, ErrorHandler)
        assert handler.console is mock
