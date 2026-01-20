"""Unified Pydantic schemas for BenchBox configuration and results.

This module provides validated configuration models using Pydantic for
runtime validation, better error messages, and automatic documentation.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from benchbox.core.constants import (
    GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS,
    GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS,
)
from benchbox.utils.verbosity import VerbositySettings


class QueryResult(BaseModel):
    """Individual query execution result."""

    query_id: str
    query_name: str
    sql_text: str
    execution_time_ms: float
    rows_returned: int
    status: str  # "SUCCESS", "ERROR", "TIMEOUT"
    error_message: Optional[str] = None
    resource_usage: Optional[dict[str, Any]] = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: str) -> str:
        """Validate status is one of the allowed values."""
        allowed = {"SUCCESS", "ERROR", "TIMEOUT"}
        if v not in allowed:
            raise ValueError(f"status must be one of {allowed}, got: {v}")
        return v

    @field_validator("execution_time_ms")
    @classmethod
    def validate_execution_time(cls, v: float) -> float:
        """Validate execution time is non-negative."""
        if v < 0:
            raise ValueError(f"execution_time_ms must be non-negative, got: {v}")
        return v

    @field_validator("rows_returned")
    @classmethod
    def validate_rows_returned(cls, v: int) -> int:
        """Validate rows_returned is non-negative."""
        if v < 0:
            raise ValueError(f"rows_returned must be non-negative, got: {v}")
        return v


class RunConfig(BaseModel):
    """Configuration for benchmark run execution."""

    benchmark: Optional[str] = None
    database_type: Optional[str] = None
    query_subset: Optional[list[str]] = None
    concurrent_streams: int = 1
    test_execution_type: str = "standard"
    scale_factor: float = 0.01
    seed: Optional[int] = None
    tuning_config: Optional[dict[str, Any]] = None
    connection: Optional[dict[str, Any]] = None
    options: dict[str, Any] = Field(default_factory=dict)
    enable_postload_validation: bool = False
    capture_plans: bool = False
    strict_plan_capture: bool = False
    driver_package: Optional[str] = None
    driver_version: Optional[str] = None
    driver_version_resolved: Optional[str] = None
    driver_auto_install: bool = False
    verbose: bool = False
    verbose_level: int = 0
    verbose_enabled: bool = False
    very_verbose: bool = False
    quiet: bool = False
    iterations: int = GENERIC_POWER_DEFAULT_MEASUREMENT_ITERATIONS  # Default: 3 measurement iterations
    warm_up_iterations: int = GENERIC_POWER_DEFAULT_WARMUP_ITERATIONS  # Default: 1 warmup iteration
    power_fail_fast: bool = False

    # Format conversion fields
    convert_format: Optional[str] = None
    conversion_compression: str = "snappy"
    conversion_partition_cols: list[str] = Field(default_factory=list)

    @field_validator("scale_factor")
    @classmethod
    def validate_scale_factor(cls, v: float) -> float:
        """Validate scale factor is positive."""
        if v <= 0:
            raise ValueError(f"scale_factor must be positive, got: {v}")
        if v > 100000:
            raise ValueError(f"scale_factor must be ≤ 100000, got: {v}")
        return v

    @field_validator("iterations")
    @classmethod
    def validate_iterations(cls, v: int) -> int:
        """Validate iterations is at least 1."""
        if v < 1:
            return 1
        return v

    @field_validator("warm_up_iterations")
    @classmethod
    def validate_warm_up_iterations(cls, v: int) -> int:
        """Validate warm_up_iterations is non-negative."""
        if v < 0:
            return 0
        return v

    @field_validator("concurrent_streams")
    @classmethod
    def validate_concurrent_streams(cls, v: int) -> int:
        """Validate concurrent_streams is positive."""
        if v < 1:
            raise ValueError(f"concurrent_streams must be at least 1, got: {v}")
        return v

    @field_validator("test_execution_type")
    @classmethod
    def validate_test_execution_type(cls, v: str) -> str:
        """Validate test_execution_type is one of the allowed values."""
        allowed = {"standard", "power", "throughput", "maintenance", "combined", "data_only", "load_only"}
        if v not in allowed:
            raise ValueError(f"test_execution_type must be one of {allowed}, got: {v}")
        return v

    @field_validator("convert_format")
    @classmethod
    def validate_convert_format(cls, v: Optional[str]) -> Optional[str]:
        """Validate convert_format is supported."""
        if v is not None:
            allowed = {"parquet", "delta", "iceberg"}
            v_lower = v.lower()
            if v_lower not in allowed:
                raise ValueError(f"convert_format must be one of {allowed}, got: {v}")
            return v_lower
        return v

    @model_validator(mode="after")
    def populate_driver_metadata(self) -> "RunConfig":
        """Populate driver metadata from options if not explicitly provided."""
        if self.driver_version is None and "driver_version" in self.options:
            option_value = self.options.get("driver_version")
            if isinstance(option_value, str):
                self.driver_version = option_value

        if not self.driver_auto_install:
            auto_install_option = self.options.get("driver_auto_install")
            if isinstance(auto_install_option, bool):
                self.driver_auto_install = auto_install_option

        if self.driver_package is None:
            package_hint = self.options.get("driver_package")
            if isinstance(package_hint, str):
                self.driver_package = package_hint

        if self.driver_version_resolved is None:
            resolved_hint = self.options.get("driver_version_resolved")
            if isinstance(resolved_hint, str):
                self.driver_version_resolved = resolved_hint

        # Apply verbosity settings normalization
        verbosity_source: dict[str, Any] = {
            "verbose": self.verbose,
            "verbose_level": self.verbose_level,
            "verbose_enabled": self.verbose_enabled,
            "very_verbose": self.very_verbose,
            "quiet": self.quiet,
        }
        verbosity = VerbositySettings.from_mapping(verbosity_source)
        self.verbose = verbosity.verbose
        self.verbose_level = verbosity.level
        self.verbose_enabled = verbosity.verbose_enabled
        self.very_verbose = verbosity.very_verbose
        self.quiet = verbosity.quiet

        return self


class BenchmarkConfig(BaseModel):
    """Benchmark configuration."""

    name: str
    display_name: str
    scale_factor: float = 0.01
    queries: Optional[list[str]] = None
    concurrency: int = 1
    capture_plans: bool = False
    strict_plan_capture: bool = False
    options: dict[str, Any] = Field(default_factory=dict)
    compress_data: bool = False
    compression_type: str = "zstd"
    compression_level: Optional[int] = None
    test_execution_type: str = "standard"  # standard, power, throughput, maintenance, combined

    @field_validator("scale_factor")
    @classmethod
    def validate_scale_factor(cls, v: float) -> float:
        """Validate scale factor is positive and within reasonable range."""
        if v <= 0:
            raise ValueError(f"scale_factor must be positive, got: {v}")
        if v > 100000:
            raise ValueError(f"scale_factor must be ≤ 100000, got: {v}")
        return v

    @field_validator("concurrency")
    @classmethod
    def validate_concurrency(cls, v: int) -> int:
        """Validate concurrency is positive."""
        if v < 1:
            raise ValueError(f"concurrency must be at least 1, got: {v}")
        return v

    @field_validator("compression_type")
    @classmethod
    def validate_compression_type(cls, v: str) -> str:
        """Validate compression type is supported."""
        allowed = {"zstd", "gzip", "lz4", "snappy", "none"}
        if v not in allowed:
            raise ValueError(f"compression_type must be one of {allowed}, got: {v}")
        return v

    @field_validator("compression_level")
    @classmethod
    def validate_compression_level(cls, v: Optional[int]) -> Optional[int]:
        """Validate compression level is within range if specified."""
        if v is not None and (v < 1 or v > 22):
            raise ValueError(f"compression_level must be between 1 and 22, got: {v}")
        return v

    @field_validator("test_execution_type")
    @classmethod
    def validate_test_execution_type(cls, v: str) -> str:
        """Validate test_execution_type is one of the allowed values."""
        allowed = {"standard", "power", "throughput", "maintenance", "combined", "data_only", "load_only"}
        if v not in allowed:
            raise ValueError(f"test_execution_type must be one of {allowed}, got: {v}")
        return v


class DatabaseConfig(BaseModel):
    """Database connection configuration.

    This model allows extra fields to support platform-specific configuration
    parameters (e.g., server_hostname for Databricks, account for Snowflake).
    """

    model_config = ConfigDict(extra="allow")

    type: str
    name: str
    connection_string: Optional[str] = None
    options: dict[str, Any] = Field(default_factory=dict)
    driver_package: Optional[str] = None
    driver_version: Optional[str] = None
    driver_version_resolved: Optional[str] = None
    driver_auto_install: bool = False
    execution_mode: Optional[Literal["sql", "dataframe"]] = None

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate database type is not empty."""
        if not v or not v.strip():
            raise ValueError("database type cannot be empty")
        return v.strip()

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate database name is not empty."""
        if not v or not v.strip():
            raise ValueError("database name cannot be empty")
        return v.strip()


class SystemProfile(BaseModel):
    """System profile information for benchmarking."""

    os_name: str
    os_version: str
    architecture: str
    cpu_model: str
    cpu_cores_physical: int
    cpu_cores_logical: int
    memory_total_gb: float
    memory_available_gb: float
    python_version: str
    disk_space_gb: float
    timestamp: datetime
    hostname: Optional[str] = None

    @field_validator("cpu_cores_physical", "cpu_cores_logical")
    @classmethod
    def validate_cpu_cores(cls, v: int) -> int:
        """Validate CPU core count is positive."""
        if v < 1:
            raise ValueError(f"CPU cores must be at least 1, got: {v}")
        return v

    @field_validator("memory_total_gb", "memory_available_gb", "disk_space_gb")
    @classmethod
    def validate_sizes(cls, v: float) -> float:
        """Validate size values are non-negative."""
        if v < 0:
            raise ValueError(f"Size values must be non-negative, got: {v}")
        return v


class LibraryInfo(BaseModel):
    """Information about a detected library."""

    name: str
    version: Optional[str]
    installed: bool
    import_error: Optional[str] = None


class PlatformInfo(BaseModel):
    """Comprehensive information about a platform."""

    name: str
    display_name: str
    description: str
    libraries: list[LibraryInfo]
    available: bool
    enabled: bool
    requirements: list[str]
    installation_command: str
    recommended: bool = False
    category: str = "database"
    supports: list[str] = Field(default_factory=list)
    driver_package: Optional[str] = None
    driver_version_requested: Optional[str] = None
    driver_version_resolved: Optional[str] = None


class DryRunResult(BaseModel):
    """Contains all information gathered during a dry run."""

    benchmark_config: dict[str, Any]
    database_config: dict[str, Any]
    system_profile: dict[str, Any]
    platform_config: dict[str, Any]
    queries: dict[str, str]
    execution_mode: str = "sql"  # "sql" or "dataframe"
    schema_sql: Optional[str] = None  # SQL CREATE TABLE statements (sql mode)
    dataframe_schema: Optional[str] = None  # Python Polars schema code (dataframe mode)
    ddl_preview: Optional[dict[str, dict[str, Any]]] = None  # Per-table DDL with tuning clauses
    post_load_statements: Optional[dict[str, list[str]]] = None  # Per-table post-load operations
    tuning_config: Optional[dict[str, Any]] = None
    constraint_config: Optional[dict[str, Any]] = None
    estimated_resources: Optional[dict[str, Any]] = None
    query_preview: Optional[dict[str, Any]] = None
    warnings: list[str] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)


__all__ = [
    "QueryResult",
    "RunConfig",
    "BenchmarkConfig",
    "DatabaseConfig",
    "SystemProfile",
    "LibraryInfo",
    "PlatformInfo",
    "DryRunResult",
]
