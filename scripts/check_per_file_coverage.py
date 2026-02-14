#!/usr/bin/env python3
"""Enforce per-file coverage thresholds for active benchbox modules.

Reads coverage.py JSON output (coverage.json) and fails when any active-scope
module is below the configured minimum. Deferred files are excluded from the
active scope and can be loaded from pyproject.toml.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class CoverageRow:
    path: str
    percent: float
    statements: int


def _normalize_path(raw: str) -> str:
    return raw.replace("\\", "/").lstrip("./")


def _load_deferred_from_pyproject(pyproject_path: Path) -> set[str]:
    if not pyproject_path.exists() or tomllib is None:
        return set()
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    tool = data.get("tool", {})
    benchbox = tool.get("benchbox", {})
    coverage_cfg = benchbox.get("coverage", {})
    deferred = coverage_cfg.get("deferred_files", [])
    if not isinstance(deferred, list):
        return set()
    return {_normalize_path(str(item)) for item in deferred}


def _iter_coverage_rows(coverage_json: Path) -> list[CoverageRow]:
    payload = json.loads(coverage_json.read_text(encoding="utf-8"))
    rows: list[CoverageRow] = []
    for raw_path, data in payload.get("files", {}).items():
        path = _normalize_path(str(raw_path))
        summary = data.get("summary", {})
        rows.append(
            CoverageRow(
                path=path,
                percent=float(summary.get("percent_covered", 0.0)),
                statements=int(summary.get("num_statements", 0)),
            )
        )
    return rows


def _active_rows(
    rows: list[CoverageRow],
    *,
    source_prefix: str,
    deferred_files: set[str],
) -> list[CoverageRow]:
    normalized_prefix = _normalize_path(source_prefix)
    if not normalized_prefix.endswith("/"):
        normalized_prefix = f"{normalized_prefix}/"
    return [
        row
        for row in rows
        if row.path.startswith(normalized_prefix) and row.path not in deferred_files and row.path.endswith(".py")
    ]


def _format_rows(rows: list[CoverageRow]) -> list[str]:
    return [f"{row.percent:6.2f}%  {row.statements:5d}  {row.path}" for row in rows]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-json", default="coverage.json", help="Path to coverage.py JSON report")
    parser.add_argument("--min", type=float, default=50.0, dest="minimum", help="Minimum per-file coverage percent")
    parser.add_argument(
        "--source-prefix",
        default="benchbox",
        help="Source prefix to evaluate (default: benchbox)",
    )
    parser.add_argument(
        "--deferred-file",
        action="append",
        default=[],
        help="Additional deferred file path (repeatable)",
    )
    parser.add_argument(
        "--deferred-from-pyproject",
        default="pyproject.toml",
        help="Load deferred files from [tool.benchbox.coverage].deferred_files in this pyproject file",
    )
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="Report failures without exiting non-zero",
    )
    parser.add_argument(
        "--fail-on-missing-coverage",
        action="store_true",
        help="Exit non-zero when coverage JSON is missing",
    )
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    coverage_path = Path(args.coverage_json)
    if not coverage_path.exists():
        print(f"coverage file not found: {coverage_path}")
        return 1 if args.fail_on_missing_coverage else 0

    deferred = {_normalize_path(item) for item in args.deferred_file}
    pyproject_deferred = _load_deferred_from_pyproject(Path(args.deferred_from_pyproject))
    deferred |= pyproject_deferred

    rows = _iter_coverage_rows(coverage_path)
    active = _active_rows(rows, source_prefix=args.source_prefix, deferred_files=deferred)
    failing = sorted(
        (row for row in active if row.percent < args.minimum), key=lambda row: (row.percent, -row.statements)
    )

    print(
        f"Per-file coverage check: active={len(active)} deferred={len(deferred)} "
        f"threshold={args.minimum:.2f}% source={_normalize_path(args.source_prefix)}"
    )
    if failing:
        print("Files below threshold:")
        for line in _format_rows(failing):
            print(f"  {line}")
    else:
        print("No active-scope files below threshold.")

    if failing and not args.report_only:
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    return run(parse_args(argv or sys.argv[1:]))


if __name__ == "__main__":
    raise SystemExit(main())
