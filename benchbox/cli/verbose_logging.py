"""Verbose logging configuration for the BenchBox CLI."""

from __future__ import annotations

import logging
import sys

from benchbox.utils.verbosity import VerbositySettings, compute_verbosity


def setup_verbose_logging(
    verbose: int | bool | VerbositySettings = 0,
    quiet: bool = False,
) -> tuple[logging.Logger | None, VerbositySettings]:
    """Configure logging according to verbosity settings.

    Args:
        verbose: Verbosity level or :class:`VerbositySettings` instance. When an
            integer/bool is provided, ``0`` disables verbose logging, ``1``
            enables info-level output, and ``2`` or greater enables debug-level
            output.
        quiet: When True, overrides verbosity and silences non-critical logs.

    Returns:
        A tuple of ``(logger, settings)`` where ``logger`` is the configured
        BenchBox CLI logger (or ``None`` when verbosity is disabled) and
        ``settings`` is the normalized :class:`VerbositySettings` instance.
    """
    settings = _resolve_verbosity_settings(verbose, quiet)
    log_level = _determine_log_level(settings)

    root_logger = _configure_root_logger(log_level, settings)
    _configure_third_party_loggers(settings)

    logger = None
    if settings.verbose_enabled:
        logger = logging.getLogger("benchbox.cli.main")
        if settings.very_verbose:
            logger.debug("Very verbose logging enabled - DEBUG level logging active")
            logger.debug(f"Root logger level: {logging.getLevelName(root_logger.level)}")
        else:
            logger.info("Verbose logging enabled - INFO level logging active")

    return logger, settings


def _resolve_verbosity_settings(verbose: int | bool | VerbositySettings, quiet: bool) -> VerbositySettings:
    """Normalize verbose parameter into VerbositySettings."""
    if isinstance(verbose, VerbositySettings):
        if quiet and not verbose.quiet:
            return VerbositySettings.from_flags(verbose.level, True)
        return verbose
    return compute_verbosity(verbose, quiet)


def _determine_log_level(settings: VerbositySettings) -> int:
    """Map verbosity settings to a logging level."""
    if settings.quiet:
        return logging.CRITICAL
    if settings.very_verbose:
        return logging.DEBUG
    if settings.verbose_enabled:
        return logging.INFO
    return logging.WARNING


def _configure_root_logger(log_level: int, settings: VerbositySettings) -> logging.Logger:
    """Configure the root logger with appropriate handler and formatter."""
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(log_level)

    if settings.very_verbose:
        fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(fmt, datefmt="%H:%M:%S")
    elif settings.verbose_enabled:
        formatter = logging.Formatter("%(levelname)s - %(message)s")
    else:
        formatter = logging.Formatter("%(message)s")

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Keep BenchBox namespace loggers inheriting from root
    for logger_name in ["benchbox", "benchbox.cli", "benchbox.platforms", "benchbox.core", "benchbox.utils"]:
        logging.getLogger(logger_name).setLevel(logging.NOTSET)

    return root_logger


def _configure_third_party_loggers(settings: VerbositySettings) -> None:
    """Configure third-party library loggers to appropriate levels."""
    # Always suppress noisy libraries at WARNING
    _always_warn = ["urllib3", "requests", "py4j", "py4j.java_gateway", "py4j.clientserver", "pyspark", "pyspark.sql"]
    for name in _always_warn:
        logging.getLogger(name).setLevel(logging.WARNING)

    # SQLAlchemy gets INFO only in very-verbose mode
    sa_level = logging.INFO if (settings.very_verbose and not settings.quiet) else logging.WARNING
    logging.getLogger("sqlalchemy").setLevel(sa_level)
