"""High-level orchestration for TPC-DS data generation."""

from __future__ import annotations

import os
import threading
from collections.abc import Iterator
from pathlib import Path

from benchbox.utils.cloud_storage import CloudStorageGeneratorMixin, create_path_handler
from benchbox.utils.compression_mixin import CompressionMixin
from benchbox.utils.data_validation import BenchmarkDataValidator

from .filesystem import FileArtifactMixin
from .runner import DsdgenRunnerMixin
from .streaming import StreamingGenerationMixin


class TPCDSDataGenerator(
    CompressionMixin,
    CloudStorageGeneratorMixin,
    DsdgenRunnerMixin,
    StreamingGenerationMixin,
    FileArtifactMixin,
):
    """Coordinate TPC-DS data generation across local and cloud targets."""

    def __init__(
        self,
        scale_factor: float = 1.0,
        output_dir: str | Path | None = None,
        verbose: int | bool = 0,
        quiet: bool = False,
        parallel: int = 1,
        force_regenerate: bool = False,
        **kwargs,
    ) -> None:
        """Initialize TPC-DS data generator.

        Args:
            scale_factor: Scale factor (1.0 = ~1GB). Must be >= 1.0 because the native
                TPC-DS dsdgen binary crashes with fractional scale factors due to
                internal calculation issues when generating certain tables.
            output_dir: Directory to output generated data
            verbose: Whether to print verbose output during generation
            parallel: Number of parallel processes for data generation
            force_regenerate: Force data regeneration even if valid data exists
            **kwargs: Additional arguments including compression options (compress_data,
                compression_type, compression_level, etc.)

        Raises:
            ValueError: If scale_factor < 1.0 (TPC-DS minimum requirement)
        """
        # Initialize compression mixin
        super().__init__(**kwargs)

        self.scale_factor = scale_factor
        self.output_dir = create_path_handler(output_dir) if output_dir else Path.cwd()
        if isinstance(verbose, bool):
            self.verbose_level = 1 if verbose else 0
        else:
            self.verbose_level = int(verbose or 0)
        self.verbose = self.verbose_level >= 1 and not quiet
        self.very_verbose = self.verbose_level >= 2 and not quiet
        self.quiet = bool(quiet)
        self.parallel = parallel
        self.force_regenerate = force_regenerate

        # Initialize data validator
        self.validator = BenchmarkDataValidator("tpcds", scale_factor)

        # Collect manifest entries during generation
        self._manifest_entries: dict[str, list[dict[str, str | int]]] = {}
        self._manifest_lock = threading.Lock()  # Thread-safe manifest updates

        # Path to dsdgen source - resolve from multiple candidate locations
        self._package_root = self._package_root_dir()
        resolved_path = self.resolve_dsdgen_path()

        # Always record a concrete path for downstream helpers even when
        # bundled sources are absent so precompiled binaries can be used
        self.dsdgen_path = resolved_path or (self._package_root / "_sources/tpc-ds/tools")
        self.dsdgen_available = resolved_path is not None
        self._dsdgen_error: Exception | None = None

        # Validate parameters
        self._validate_parameters()

        # Set tools_dir for compatibility
        self.tools_dir = self.dsdgen_path

        # Check for or build the dsdgen executable when sources are present.
        # When sources are absent, defer raising until generation is requested
        try:
            self.dsdgen_exe = self._find_or_build_dsdgen()
            self.dsdgen_available = self.dsdgen_exe.exists()
        except (FileNotFoundError, RuntimeError, PermissionError) as exc:
            self.dsdgen_exe = None
            self.dsdgen_available = False
            self._dsdgen_error = exc

    def _validate_parameters(self) -> None:
        """Validate input parameters."""
        if self.scale_factor <= 0:
            raise ValueError(f"Scale factor must be positive, got {self.scale_factor}")

        # TPC-DS dsdgen binary requires scale factor >= 1.0 to avoid segmentation faults.
        # Fractional scale factors cause dsdgen to crash when generating certain tables
        # (e.g., call_center) because internal calculations produce invalid chunk sizes.
        if self.scale_factor < 1.0:
            raise ValueError(
                f"TPC-DS requires scale_factor >= 1.0 (got {self.scale_factor}). "
                "The native dsdgen binary crashes with fractional scale factors. "
                "For smaller datasets, use TPC-H (which supports fractional SF) or "
                "use scale_factor=1.0 with a subset of queries."
            )

        if self.scale_factor > 100000:
            raise ValueError(f"Scale factor {self.scale_factor} is too large (max 100000)")

        if self.parallel < 1:
            raise ValueError(f"Parallel processes must be >= 1, got {self.parallel}")

        if self.parallel > 64:
            raise ValueError(f"Too many parallel processes {self.parallel} (max 64)")

    @classmethod
    def _package_root_dir(cls) -> Path:
        """Return the project root directory (not package root).

        This is used to locate resources like sample data in examples/data/
        at the project root level.
        """
        return Path(__file__).parent.parent.parent.parent.parent

    @classmethod
    def _candidate_dsdgen_paths(cls) -> Iterator[Path]:
        """Yield candidate paths where dsdgen sources might be located."""
        package_root = cls._package_root_dir()

        # Primary location: _sources in package root
        yield package_root / "_sources/tpc-ds/tools"

        # Fallback: relative to current file
        yield Path(__file__).parent.parent / "_sources/tpc-ds/tools"

        # Fallback: installed package location
        try:
            import benchbox

            module_path = Path(benchbox.__file__).parent
            yield module_path / "_sources/tpc-ds/tools"
        except ImportError:
            return

    @classmethod
    def resolve_dsdgen_path(cls) -> Path | None:
        """Resolve the dsdgen source directory from candidate locations.

        Returns:
            Path to dsdgen tools directory if found, None otherwise.
        """
        for candidate in cls._candidate_dsdgen_paths():
            if candidate.exists():
                return candidate
        return None

    def has_dsdgen_sources(self) -> bool:
        """Check if dsdgen sources are available.

        Returns:
            True if dsdgen sources are available, False otherwise.
        """
        return self.dsdgen_available

    def _known_table_names(self) -> list[str]:
        """Return the canonical set of TPC-DS table names tracked in manifests."""

        return [
            "call_center",
            "catalog_page",
            "catalog_sales",
            "catalog_returns",
            "customer",
            "customer_address",
            "customer_demographics",
            "date_dim",
            "household_demographics",
            "income_band",
            "inventory",
            "item",
            "promotion",
            "reason",
            "ship_mode",
            "store",
            "store_sales",
            "store_returns",
            "time_dim",
            "warehouse",
            "web_page",
            "web_sales",
            "web_returns",
            "web_site",
            "dbgen_version",
        ]

    def _raise_missing_dsdgen(self) -> None:
        """Raise an error when dsdgen is not available for data generation."""
        message = (
            "TPC-DS native tools are not bundled with this build. "
            "Install the TPC-DS toolkit and place the compiled binaries under "
            f"{self._package_root / '_sources/tpc-ds/tools'} or supply sample data."
        )
        if self._dsdgen_error:
            message += f" Details: {self._dsdgen_error}"
        raise RuntimeError(message)

    def _generate_local(self, output_dir: Path | None = None) -> dict[str, list[Path]]:
        """Generate data locally (original implementation).

        Returns:
            Dictionary mapping table names to lists of file paths (one or more per table)
        """
        # Check if dsdgen is available for data generation
        if not self.dsdgen_available:
            self._raise_missing_dsdgen()

        # Use provided output directory or fall back to instance output_dir
        target_dir = output_dir if output_dir is not None else self.output_dir
        # Create output directory
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            raise PermissionError(f"Cannot create output directory {target_dir}: {e}")

        # Validate output directory is writable
        if not os.access(target_dir, os.W_OK):
            raise PermissionError(f"Output directory {target_dir} is not writable")

        # Initialize validation_result for later use
        validation_result = None

        # Smart data generation: check if valid data already exists
        # Always check target_dir for existing data unless force_regenerate=True
        should_regenerate, validation_result = self.validator.should_regenerate_data(target_dir, self.force_regenerate)

        if not should_regenerate:
            if self.verbose:
                print(f"✅ Valid TPC-DS data found for scale factor {self.scale_factor}")
                self.validator.print_validation_report(validation_result, verbose=False)
                print("   Skipping data generation")
            return self._gather_existing_table_files(target_dir)

        # Data generation needed
        if self.verbose:
            if validation_result is not None and validation_result.issues:
                print(f"⚠️  Data validation failed for scale factor {self.scale_factor}")
                self.validator.print_validation_report(validation_result, verbose=True)
            else:
                print("🔄 Force regeneration requested")
            print("   Generating TPC-DS data...")

        sample_dir = self._get_sample_data_dir()
        if sample_dir is not None:
            if self.verbose:
                print(f"⚡ Using bundled TPC-DS sample dataset for scale factor {self.scale_factor}")
            self._copy_sample_dataset(sample_dir, target_dir)

            # If compression is enabled, compress the copied .dat files
            if self.should_use_compression():
                for dat_file in list(target_dir.glob("*.dat")):
                    if dat_file.suffix == ".dat":  # Only compress .dat files, skip .dst and .idx
                        try:
                            self.compress_existing_file(dat_file, remove_original=True)
                        except Exception:
                            # Best-effort compression; leave validation to catch inconsistencies
                            pass

            table_paths = self._gather_existing_table_files(target_dir)
            if table_paths:
                self._validate_file_format_consistency(target_dir)
                return table_paths

        # Run native dsdgen to generate data directly in target directory
        self._run_dsdgen_native(target_dir)

        # If compression is enabled, normalize any raw .dat files by compressing
        if self.should_use_compression():
            for dat_file in list(target_dir.glob("*.dat")) + list(target_dir.glob("*_*.dat")):
                try:
                    self.compress_existing_file(dat_file, remove_original=True)
                except Exception:
                    # Best-effort compression; leave validation to catch inconsistencies
                    pass

        # Create mapping of table names to generated file paths
        # Use known TPC-DS table names instead of parsing filenames

        table_paths = self._gather_existing_table_files(target_dir)
        if self.should_use_compression() and table_paths and self.verbose:
            print(f"\n📦 Generated {len(table_paths)} tables with streaming {self.compression_type} compression")

        # Validate file format consistency at the very end
        self._validate_file_format_consistency(target_dir)

        # Write manifest with file sizes and row counts (collected during generation when possible)
        self._write_manifest(target_dir, table_paths)

        return table_paths

    def generate(self) -> dict[str, list[Path]]:
        """Generate TPC-DS benchmark data using native C executable.

        Returns:
            Dictionary mapping table names to lists of file paths (one or more per table).
            Multiple files per table are generated when using parallel data generation.

        Raises:
            RuntimeError: If data generation fails
            PermissionError: If output directory cannot be created or written to
            FileNotFoundError: If dsdgen executable is not found
        """
        # Use centralized cloud/local generation handler
        return self._handle_cloud_or_local_generation(self.output_dir, self._generate_local, self.verbose)

    def generate_tables(self, table_names: list[str]) -> dict[str, list[Path]]:
        """Generate specific TPC-DS tables only.

        This method provides selective table generation for scenarios where generating
        all 24 TPC-DS tables is unnecessary (e.g., testing, validation).

        Args:
            table_names: List of table names to generate (e.g., ["customer", "item"])

        Returns:
            Dictionary mapping requested table names to lists of file paths

        Raises:
            RuntimeError: If data generation fails
            PermissionError: If output directory cannot be created or written to
            FileNotFoundError: If dsdgen executable is not found
            ValueError: If invalid table names are provided

        Example:
            generator = TPCDSDataGenerator(scale_factor=1.0, output_dir="/tmp/data")
            files = generator.generate_tables(["customer", "item", "date_dim"])
        """
        # Check if dsdgen is available
        if not self.dsdgen_available:
            self._raise_missing_dsdgen()

        # Validate table names
        known_tables = set(self._known_table_names())
        invalid_tables = [t for t in table_names if t not in known_tables]
        if invalid_tables:
            raise ValueError(f"Invalid table names: {invalid_tables}. Valid TPC-DS tables are: {sorted(known_tables)}")

        # Create output directory
        target_dir = self.output_dir
        try:
            target_dir.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            raise PermissionError(f"Cannot create output directory {target_dir}: {e}")

        # Validate output directory is writable
        if not os.access(target_dir, os.W_OK):
            raise PermissionError(f"Output directory {target_dir} is not writable")

        # Generate each requested table
        if self.verbose:
            print(f"\nGenerating {len(table_names)} TPC-DS tables at scale factor {self.scale_factor}...")

        for table_name in table_names:
            if self.verbose:
                print(f"  - {table_name}")
            self._generate_table_with_streaming(target_dir, table_name)

        # Gather generated files
        table_paths = self._gather_existing_table_files(target_dir)

        # Filter to only requested tables
        filtered_paths = {k: v for k, v in table_paths.items() if k in table_names}

        if self.verbose:
            print(f"\nGenerated {len(filtered_paths)} tables successfully")

        return filtered_paths


__all__ = ["TPCDSDataGenerator"]
