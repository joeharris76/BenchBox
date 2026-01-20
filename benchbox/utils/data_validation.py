"""Data validation utilities for benchmark data generation.

This module provides utilities for validating existing benchmark data
and determining if regeneration is needed.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)


@dataclass
class DataValidationResult:
    """Results from data validation."""

    valid: bool
    tables_validated: dict[str, bool]
    missing_tables: list[str]
    row_count_mismatches: dict[str, tuple[int, int]]  # table: (expected, actual)
    file_size_info: dict[str, int]
    validation_timestamp: datetime
    issues: list[str]


@dataclass
class TableExpectation:
    """Expected data characteristics for a table."""

    name: str
    expected_rows: int
    expected_files: list[str]
    min_file_size: int = 0  # Minimum file size in bytes
    allow_zero_rows: bool = False


class BenchmarkDataValidator:
    """Validates benchmark data and determines if regeneration is needed."""

    # Standard TPC-H row counts for scale factor 1.0
    # Note: nation and region tables have fixed sizes regardless of scale factor
    TPCH_TABLE_EXPECTATIONS = {
        "customer": TableExpectation("customer", 150000, ["customer.tbl"]),
        "lineitem": TableExpectation("lineitem", 6001215, ["lineitem.tbl"]),
        "nation": TableExpectation("nation", 25, ["nation.tbl"]),  # Fixed size table
        "orders": TableExpectation("orders", 1500000, ["orders.tbl"]),
        "part": TableExpectation("part", 200000, ["part.tbl"]),
        "partsupp": TableExpectation("partsupp", 800000, ["partsupp.tbl"]),
        "region": TableExpectation("region", 5, ["region.tbl"]),  # Fixed size table
        "supplier": TableExpectation("supplier", 10000, ["supplier.tbl"]),
    }

    # Standard TPC-DS row counts for scale factor 1.0 (approximate)
    TPCDS_TABLE_EXPECTATIONS = {
        "call_center": TableExpectation("call_center", 6, ["call_center.dat"]),
        "catalog_page": TableExpectation("catalog_page", 11718, ["catalog_page.dat"]),
        "catalog_returns": TableExpectation("catalog_returns", 144067, ["catalog_returns.dat"]),
        "catalog_sales": TableExpectation("catalog_sales", 1441548, ["catalog_sales.dat"]),
        "customer": TableExpectation("customer", 100000, ["customer.dat"]),
        "customer_address": TableExpectation("customer_address", 50000, ["customer_address.dat"]),
        "customer_demographics": TableExpectation("customer_demographics", 1920800, ["customer_demographics.dat"]),
        "date_dim": TableExpectation("date_dim", 73049, ["date_dim.dat"]),
        "household_demographics": TableExpectation("household_demographics", 7200, ["household_demographics.dat"]),
        "income_band": TableExpectation("income_band", 20, ["income_band.dat"]),
        "inventory": TableExpectation("inventory", 11745000, ["inventory.dat"]),
        "item": TableExpectation("item", 18000, ["item.dat"]),
        "promotion": TableExpectation("promotion", 300, ["promotion.dat"]),
        "reason": TableExpectation("reason", 35, ["reason.dat"]),
        "ship_mode": TableExpectation("ship_mode", 20, ["ship_mode.dat"]),
        "store": TableExpectation("store", 12, ["store.dat"]),
        "store_returns": TableExpectation("store_returns", 287514, ["store_returns.dat"]),
        "store_sales": TableExpectation("store_sales", 2880404, ["store_sales.dat"]),
        "time_dim": TableExpectation("time_dim", 86400, ["time_dim.dat"]),
        "warehouse": TableExpectation("warehouse", 5, ["warehouse.dat"]),
        "web_page": TableExpectation("web_page", 60, ["web_page.dat"]),
        "web_returns": TableExpectation("web_returns", 71763, ["web_returns.dat"]),
        "web_sales": TableExpectation("web_sales", 719384, ["web_sales.dat"]),
        "web_site": TableExpectation("web_site", 30, ["web_site.dat"]),
    }

    def __init__(self, benchmark_name: str, scale_factor: float = 1.0):
        """Initialize validator for a specific benchmark.

        Args:
            benchmark_name: Name of the benchmark (tpch, tpcds, etc.)
            scale_factor: Scale factor for row count calculations
        """
        self.benchmark_name = benchmark_name.lower()
        self.scale_factor = scale_factor

        # Get table expectations based on benchmark type
        if self.benchmark_name == "tpch":
            self.table_expectations = self._scale_expectations(self.TPCH_TABLE_EXPECTATIONS)
        elif self.benchmark_name == "tpcds":
            self.table_expectations = self._scale_expectations(self.TPCDS_TABLE_EXPECTATIONS)
        else:
            # For other benchmarks, we'll do basic file existence validation
            self.table_expectations = {}

    def _scale_expectations(self, base_expectations: dict[str, TableExpectation]) -> dict[str, TableExpectation]:
        """Scale row count expectations based on scale factor."""
        scaled = {}
        # Tables that have fixed row counts regardless of scale factor
        fixed_size_tables = {
            "nation",
            "region",
            "call_center",
            "reason",
            "ship_mode",
            "warehouse",
            "income_band",
            "web_site",
            "store",
            "time_dim",
        }

        for table_name, expectation in base_expectations.items():
            if table_name in fixed_size_tables:
                # Don't scale these tables
                scaled_rows = expectation.expected_rows
            else:
                scaled_rows = int(expectation.expected_rows * self.scale_factor)

            scaled[table_name] = TableExpectation(
                name=expectation.name,
                expected_rows=scaled_rows,
                expected_files=expectation.expected_files,
                min_file_size=expectation.min_file_size,
                allow_zero_rows=expectation.allow_zero_rows,
            )
        return scaled

    def validate_data_directory(self, data_dir: Union[str, Path]) -> DataValidationResult:
        """Validate data in the specified directory.

        Args:
            data_dir: Path to the data directory to validate

        Returns:
            DataValidationResult with validation details
        """
        data_path = Path(data_dir)

        if not data_path.exists():
            return DataValidationResult(
                valid=False,
                tables_validated={},
                missing_tables=[],
                row_count_mismatches={},
                file_size_info={},
                validation_timestamp=datetime.now(),
                issues=[f"Data directory does not exist: {data_path}"],
            )

        # If a manifest exists and is valid for this benchmark/scale, use it for validation
        manifest = self._read_manifest(data_path)
        if manifest and self._manifest_matches_config(manifest):
            return self._validate_with_manifest(data_path, manifest)

        # Otherwise, check for expected tables/files by scanning and rebuild manifest
        tables_validated = {}
        missing_tables = []
        row_count_mismatches = {}
        file_size_info = {}
        issues = []

        # If we have specific expectations for this benchmark
        if self.table_expectations:
            for table_name, expectation in self.table_expectations.items():
                table_valid = True

                # Resolve data files that may be compressed or chunked.
                resolved_files = self._resolve_table_files(data_path, table_name, expectation.expected_files)

                if not resolved_files:
                    missing_tables.append(table_name)
                    issues.append(f"Missing data files for table {table_name}")
                    table_valid = False
                else:
                    # Record sizes and check empties
                    for f in resolved_files:
                        try:
                            size = f.stat().st_size
                            file_size_info[f.name] = size
                            if size == 0 and not expectation.allow_zero_rows:
                                table_valid = False
                                issues.append(f"File {f.name} is empty")
                        except Exception as e:
                            table_valid = False
                            issues.append(f"Failed to stat {f}: {e}")

                # For TPC-H/TPC-DS, perform row count validation (best-effort for compressed formats)
                if table_valid and self.benchmark_name in ["tpch", "tpcds"]:
                    try:
                        actual_rows = self._count_rows_paths(resolved_files)
                        expected_rows = expectation.expected_rows

                        # Allow for some variance in row counts (±5%)
                        tolerance = max(1, int(expected_rows * 0.05))
                        if actual_rows > 0 and abs(actual_rows - expected_rows) > tolerance:
                            row_count_mismatches[table_name] = (
                                expected_rows,
                                actual_rows,
                            )
                            table_valid = False
                            issues.append(
                                f"Table {table_name}: expected ~{expected_rows} rows, found {actual_rows} rows"
                            )
                    except Exception as e:
                        # If counting fails (e.g., zstd not available), we do not hard-fail here
                        logger.debug(f"Row counting skipped for {table_name}: {e}")

                tables_validated[table_name] = table_valid
        else:
            # For unknown benchmarks, just check if directory has any data files
            data_files = (
                list(data_path.glob("*.tbl"))
                + list(data_path.glob("*.dat"))
                + list(data_path.glob("*.csv"))
                + list(data_path.glob("*.parquet"))
            )

            if not data_files:
                issues.append("No data files found in directory")
            else:
                for data_file in data_files:
                    file_size = data_file.stat().st_size
                    file_size_info[data_file.name] = file_size
                    if file_size == 0:
                        issues.append(f"Empty data file: {data_file.name}")

        # Overall validation result
        all_valid = (
            not missing_tables and not row_count_mismatches and all(tables_validated.values())
            if tables_validated
            else len(file_size_info) > 0
        )

        result = DataValidationResult(
            valid=all_valid,
            tables_validated=tables_validated,
            missing_tables=missing_tables,
            row_count_mismatches=row_count_mismatches,
            file_size_info=file_size_info,
            validation_timestamp=datetime.now(),
            issues=issues,
        )

        # Build a manifest from the scan for future runs (best-effort)
        try:
            self._write_manifest_from_scan(data_path)
        except Exception as e:
            logger.debug(f"Skipping manifest write after scan: {e}")

        return result

    def _count_rows_in_files(self, data_dir: Path, file_names: list[str]) -> int:
        """Count total rows across multiple data files."""
        total_rows = 0

        for file_name in file_names:
            file_path = data_dir / file_name
            if file_path.exists():
                try:
                    # Fast line counting
                    with open(file_path, "rb") as f:
                        total_rows += sum(1 for _ in f)
                except Exception as e:
                    logger.warning(f"Failed to count rows in {file_path}: {e}")

        return total_rows

    def _count_rows_paths(self, files: list[Path]) -> int:
        """Count rows across a list of file paths (supports .dat, .tbl, .csv, .gz; best-effort for .zst)."""
        total = 0
        for f in files:
            name = f.name.lower()
            try:
                if name.endswith(".gz"):
                    import gzip

                    with gzip.open(f, "rt") as g:
                        total += sum(1 for _ in g)
                elif name.endswith(".zst"):
                    try:
                        import zstandard as zstd  # type: ignore

                        dctx = zstd.ZstdDecompressor()
                        with open(f, "rb") as fh, dctx.stream_reader(fh) as reader:
                            import io

                            text = io.TextIOWrapper(reader)
                            total += sum(1 for _ in text)
                    except Exception:
                        # If zstandard is not available, skip row counting for this file
                        logger.debug(f"Skipping zstd row count for {f}")
                        continue
                else:
                    with open(f, "rb") as fh:
                        total += sum(1 for _ in fh)
            except Exception as e:
                logger.debug(f"Failed to count rows in {f}: {e}")
                continue
        return total

    def _resolve_table_files(self, data_dir: Path, table_name: str, expected_files: list[str]) -> list[Path]:
        """Resolve actual files for a table, considering compression and chunk patterns.

        Supports:
        - Exact files from expectations
        - Compressed variants: .gz, .zst
        - Chunked variants: table_N_M.dat[.ext]
        """
        candidates: list[Path] = []
        # 1) Direct expected files
        for ef in expected_files:
            p = data_dir / ef
            if p.exists():
                candidates.append(p)
            # Compressed variants
            gz = data_dir / f"{ef}.gz"
            zst = data_dir / f"{ef}.zst"
            if gz.exists():
                candidates.append(gz)
            if zst.exists():
                candidates.append(zst)

        # 2) Chunked patterns: table_N_M.dat(.ext)
        for ext in [".dat", ".dat.gz", ".dat.zst"]:
            for pf in data_dir.glob(f"{table_name}_*.{ext.split('.')[-1]}"):
                name = pf.name
                # Normalize compression suffix handling
                core = name[:-3] if name.endswith(".gz") else (name[:-4] if name.endswith(".zst") else name)
                stem = Path(core).stem  # removes .dat
                if stem.startswith(f"{table_name}_"):
                    suffix = stem[len(f"{table_name}_") :]
                    parts = suffix.split("_")
                    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                        candidates.append(pf)

        # Ensure uniqueness
        uniq = []
        seen = set()
        for p in candidates:
            if p.exists() and p not in seen:
                uniq.append(p)
                seen.add(p)
        return uniq

    def should_regenerate_data(
        self, data_dir: Union[str, Path], force_regenerate: bool = False
    ) -> tuple[bool, DataValidationResult]:
        """Determine if data should be regenerated.

        Args:
            data_dir: Path to the data directory
            force_regenerate: If True, always regenerate regardless of validation

        Returns:
            Tuple of (should_regenerate, validation_result)
        """
        if force_regenerate:
            return True, DataValidationResult(
                valid=False,
                tables_validated={},
                missing_tables=[],
                row_count_mismatches={},
                file_size_info={},
                validation_timestamp=datetime.now(),
                issues=["Force regeneration requested"],
            )

        validation_result = self.validate_data_directory(data_dir)
        return not validation_result.valid, validation_result

    def print_validation_report(self, result: DataValidationResult, verbose: bool = True) -> None:
        """Print a human-readable validation report."""
        if result.valid:
            print("✅ Data validation PASSED")
            if verbose:
                print(f"   Validated {len(result.tables_validated)} tables")
                total_size = sum(result.file_size_info.values())
                print(f"   Total data size: {self._format_bytes(total_size)}")
        else:
            print("❌ Data validation FAILED")

            if result.missing_tables:
                print(f"   Missing tables: {', '.join(result.missing_tables)}")

            if result.row_count_mismatches:
                print("   Row count mismatches:")
                for table, (expected, actual) in result.row_count_mismatches.items():
                    print(f"     {table}: expected {expected:,}, found {actual:,}")

            if result.issues and verbose:
                print("   Issues:")
                for issue in result.issues[:5]:  # Show first 5 issues
                    print(f"     - {issue}")
                if len(result.issues) > 5:
                    print(f"     ... and {len(result.issues) - 5} more issues")

    def _format_bytes(self, size_bytes: int) -> str:
        """Format bytes into human readable format."""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math

        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_names[i]}"

    # ---------- Manifest helpers ----------

    def _manifest_path(self, data_dir: Path) -> Path:
        return Path(data_dir) / "_datagen_manifest.json"

    def _read_manifest(self, data_dir: Path) -> Optional[dict]:
        try:
            mp = self._manifest_path(data_dir)
            if not mp.exists():
                return None
            with open(mp) as f:
                return json.load(f)
        except Exception as e:
            logger.debug(f"Failed to read manifest: {e}")
            return None

    def _manifest_matches_config(self, manifest: dict) -> bool:
        try:
            if str(manifest.get("benchmark", "")).lower() != self.benchmark_name:
                return False
            # Tolerate int/float types for scale factor
            return float(manifest.get("scale_factor", -1)) == float(self.scale_factor)
        except Exception:
            return False

    def _validate_with_manifest(self, data_dir: Path, manifest: dict) -> DataValidationResult:
        from benchbox.utils.datagen_manifest import get_table_files

        tables_validated: dict[str, bool] = {}
        missing_tables: list[str] = []
        row_count_mismatches: dict[str, tuple[int, int]] = {}
        file_size_info: dict[str, int] = {}
        issues: list[str] = []

        # If we have concrete expectations, validate them against manifest entries
        if self.table_expectations:
            for table_name, expectation in self.table_expectations.items():
                # Use get_table_files which handles both V1 and V2 manifest formats
                entries = get_table_files(manifest, table_name)
                if not entries:
                    missing_tables.append(table_name)
                    issues.append(f"Missing manifest entries for table {table_name}")
                    tables_validated[table_name] = False
                    continue
                table_ok = True
                actual_rows_total = 0
                for e in entries:
                    rel = e.get("path")
                    size = int(e.get("size_bytes", -1))
                    rc = int(e.get("row_count", 0))
                    if rel is None or size < 0:
                        table_ok = False
                        issues.append(f"Invalid manifest entry for {table_name}")
                        continue
                    fp = data_dir / rel
                    if (not fp.exists()) or fp.stat().st_size != size:
                        table_ok = False
                        issues.append(f"File missing or size mismatch: {rel}")
                    file_size_info[rel] = size
                    actual_rows_total += rc
                # Row count tolerance check when available
                if table_ok and expectation.expected_rows > 0 and actual_rows_total > 0:
                    tol = max(1, int(expectation.expected_rows * 0.05))
                    if abs(actual_rows_total - expectation.expected_rows) > tol:
                        row_count_mismatches[table_name] = (
                            expectation.expected_rows,
                            actual_rows_total,
                        )
                        table_ok = False
                        issues.append(
                            f"Table {table_name}: expected ~{expectation.expected_rows} rows, manifest has {actual_rows_total} rows"
                        )
                tables_validated[table_name] = table_ok
        else:
            # Unknown benchmarks: ensure manifest files exist and are non-empty
            any_files = False
            for table_name in manifest.get("tables", {}).keys():
                entries = get_table_files(manifest, table_name)
                table_ok = True
                for e in entries:
                    rel = e.get("path")
                    size = int(e.get("size_bytes", -1))
                    if not rel:
                        table_ok = False
                        issues.append(f"Invalid manifest entry for {table_name}")
                        continue
                    fp = data_dir / rel
                    if (not fp.exists()) or fp.stat().st_size != size or size == 0:
                        table_ok = False
                        issues.append(f"Missing/empty file {rel}")
                    file_size_info[rel] = max(size, 0)
                    any_files = True
                tables_validated[table_name] = table_ok
            if not any_files:
                issues.append("No files listed in manifest")

        all_valid = (
            not missing_tables and not row_count_mismatches and all(tables_validated.values())
            if tables_validated
            else False
        )

        return DataValidationResult(
            valid=all_valid,
            tables_validated=tables_validated,
            missing_tables=missing_tables,
            row_count_mismatches=row_count_mismatches,
            file_size_info=file_size_info,
            validation_timestamp=datetime.now(),
            issues=issues,
        )

    def _write_manifest_from_scan(self, data_dir: Path) -> None:
        """Create a manifest by scanning files in the directory (best-effort)."""
        tables: dict[str, list[Path]] = {}
        # Discover data-like files
        candidates = []
        for pattern in [
            "*.tbl",
            "*.dat",
            "*.csv",
            "*.tbl.gz",
            "*.tbl.zst",
            "*.dat.gz",
            "*.dat.zst",
        ]:
            candidates.extend(list(Path(data_dir).glob(pattern)))
        # Group by table name (stem up to first dot for tbl/dat, handle chunk names)
        for fp in candidates:
            name = fp.name
            base = name
            # Remove compression suffix
            if base.endswith(".gz"):
                base = base[:-3]
            elif base.endswith(".zst"):
                base = base[:-4]
            stem = Path(base).stem  # removes .tbl/.dat/.csv
            # Normalize chunked variants by table prefix
            table = stem
            # For known patterns like table_N_M or table.tbl.N preserve table name before first underscore/dot-digit
            if "_" in stem:
                prefix, rest = stem.split("_", 1)
                if all(p.isdigit() for p in rest.split("_")):
                    table = prefix
            if "." in name:
                parts = name.split(".")
                if len(parts) >= 3 and parts[-1].isdigit():
                    table = parts[0]
            tables.setdefault(table, []).append(fp)

        manifest = {
            "benchmark": self.benchmark_name,
            "scale_factor": self.scale_factor,
            "compression": {},
            "parallel": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "generator_version": "scan-v1",
            "tables": {},
        }
        for table, paths in tables.items():
            # Sort for determinism
            paths = sorted(paths)
            # Count rows across files
            rows = self._count_rows_paths(paths)
            for p in paths:
                manifest["tables"].setdefault(table, []).append(
                    {
                        "path": p.name,
                        "size_bytes": p.stat().st_size if p.exists() else 0,
                        "row_count": rows if len(paths) == 1 else 0,  # store total on single file entries
                    }
                )
        mp = self._manifest_path(data_dir)
        with open(mp, "w") as f:
            json.dump(manifest, f, indent=2)
