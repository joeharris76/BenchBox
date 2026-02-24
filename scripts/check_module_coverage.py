#!/usr/bin/env python3
"""Check per-module coverage floors from coverage.xml."""

from __future__ import annotations

import argparse
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore[no-redef]


@dataclass(frozen=True)
class ModuleCoverage:
    module: str
    covered: int
    total: int

    @property
    def percent(self) -> float:
        if self.total == 0:
            return 0.0
        return (self.covered / self.total) * 100.0


def _normalize(path: str) -> str:
    return path.replace("\\", "/").lstrip("./")


def _relative_to_source_root(path: str, source_root: str) -> str | None:
    normalized = _normalize(path)
    root = _normalize(source_root)
    if normalized.startswith(f"{root}/"):
        return normalized[len(root) + 1 :]

    marker = f"/{root}/"
    if marker in normalized:
        return normalized.split(marker, 1)[1]

    if root == "benchbox":
        # coverage.py XML often reports paths relative to the source package root.
        # Only accept paths that actually live under benchbox/ in this repo.
        if Path(root, normalized).exists():
            return normalized

    return None


def _module_key(path: str, source_root: str) -> str | None:
    root = _normalize(source_root)
    rel = _relative_to_source_root(path, root)
    if rel is None:
        return None
    rel_parts = [part for part in rel.split("/") if part]
    if not rel_parts:
        return root
    if len(rel_parts) == 1:
        return root
    return f"{root}/{rel_parts[0]}"


def _resolve_filename(package_name: str, raw_filename: str) -> str:
    filename = _normalize(raw_filename)
    if "/" in filename or not package_name:
        return filename
    package_path = package_name.replace(".", "/")
    return _normalize(f"{package_path}/{filename}")


def _parse_coverage_xml(path: Path, source_root: str) -> list[ModuleCoverage]:
    tree = ET.parse(path)
    root = tree.getroot()
    aggregate: dict[str, tuple[int, int]] = {}

    for package_node in root.findall(".//package"):
        package_name = package_node.get("name", "")
        for class_node in package_node.findall("./classes/class"):
            raw_filename = class_node.get("filename")
            if not raw_filename:
                continue
            filename = _resolve_filename(package_name, raw_filename)
            module = _module_key(filename, source_root)
            if module is None:
                continue

            covered, total = aggregate.get(module, (0, 0))
            for line_node in class_node.findall("./lines/line"):
                hits = int(line_node.get("hits", "0"))
                total += 1
                if hits > 0:
                    covered += 1
            aggregate[module] = (covered, total)

    return [
        ModuleCoverage(module=module, covered=covered, total=total)
        for module, (covered, total) in sorted(aggregate.items())
    ]


def _load_excluded_modules(pyproject_path: Path) -> list[str]:
    if not pyproject_path.exists():
        return []
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    coverage_cfg = data.get("tool", {}).get("benchbox", {}).get("coverage", {})
    module_cfg = coverage_cfg.get("module_check", {})
    raw = module_cfg.get("exclude_modules", [])
    if not isinstance(raw, list):
        return []
    return [str(item) for item in raw]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-xml", default="coverage.xml", help="Path to coverage.py XML report")
    parser.add_argument("--threshold", type=float, default=50.0, help="Minimum per-module coverage percentage")
    parser.add_argument("--source-root", default="benchbox", help="Source root prefix to evaluate")
    parser.add_argument(
        "--exclude-module",
        action="append",
        default=[],
        help="Module key to exclude from the check (repeatable, e.g. benchbox/examples)",
    )
    parser.add_argument(
        "--exclude-from-pyproject",
        default="pyproject.toml",
        help="Load excluded modules from [tool.benchbox.coverage.module_check] in this pyproject file",
    )
    parser.add_argument(
        "--allow-empty",
        action="store_true",
        help="Allow zero matched modules (default is fail-closed for CI safety)",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    coverage_xml = Path(args.coverage_xml)
    if not coverage_xml.exists():
        print(f"warning: coverage XML not found at {coverage_xml}; skipping module coverage check")
        return 0

    rows = [row for row in _parse_coverage_xml(coverage_xml, args.source_root) if row.total > 0]
    excluded = set(args.exclude_module)
    excluded.update(_load_excluded_modules(Path(args.exclude_from_pyproject)))
    rows = [row for row in rows if row.module not in excluded]
    if not rows:
        print(f"warning: no modules matched source root '{args.source_root}' in {coverage_xml}")
        return 0 if args.allow_empty else 1

    failing = [row for row in rows if row.percent < args.threshold]
    module_width = max(len("Module"), max(len(row.module) for row in rows))
    print(f"{'Module':<{module_width}}  {'Coverage':>9}  {'Lines':>9}  Status")
    print(f"{'-' * module_width}  {'-' * 9}  {'-' * 9}  {'-' * 6}")
    for row in rows:
        status = "PASS" if row.percent >= args.threshold else "FAIL"
        print(f"{row.module:<{module_width}}  {row.percent:8.2f}%  {row.covered:4d}/{row.total:<4d}  {status}")

    if failing:
        print(f"\nmodule coverage check failed: {len(failing)} module(s) below {args.threshold:.2f}%")
        return 1

    print(f"\nmodule coverage check passed: all modules >= {args.threshold:.2f}%")
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv or sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
