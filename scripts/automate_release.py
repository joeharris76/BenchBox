#!/usr/bin/env python3
"""Fully automated BenchBox release process with timestamp normalization.

This script orchestrates the complete release workflow:
1. Calculate most recent Saturday midnight timestamp
2. Run pre-flight checks (changelog, git status, version)
3. Run CI validation (lint, format, type check, tests)
4. Prepare curated public tree
5. Run size safety check
6. Build packages with SOURCE_DATE_EPOCH
7. Verify artifacts and run smoke tests
8. Finalize with git commit/tag (both with normalized timestamps)
9. Archive artifacts

All file timestamps are normalized to the most recent Saturday at midnight UTC,
ensuring no trace of actual creation/modification times in released artifacts.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Import from benchbox
sys.path.insert(0, str(Path(__file__).parent.parent))
from benchbox.release.workflow import (
    DEFAULT_MAX_FILE_SIZE_MB,
    DEFAULT_MAX_TOTAL_SIZE_MB,
    calculate_most_recent_saturday_midnight,
    check_release_size,
)


def parse_timestamp(timestamp_str: str) -> tuple[int, str, str]:
    """Parse a timestamp string into release timestamp formats.

    Args:
        timestamp_str: Timestamp in git format (e.g., "2026-01-16 13:00:00 -0500")

    Returns:
        tuple[int, str, str]: (unix_timestamp, iso_format, git_format)

    Raises:
        ValueError: If timestamp cannot be parsed
    """
    # Try git format: "YYYY-MM-DD HH:MM:SS +/-HHMM"
    try:
        dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S %z")
    except ValueError:
        # Try without timezone (assume UTC)
        try:
            dt = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            dt = dt.replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(
                f"Cannot parse timestamp '{timestamp_str}'. "
                "Expected format: 'YYYY-MM-DD HH:MM:SS -0500' or 'YYYY-MM-DD HH:MM:SS'"
            ) from e

    unix_ts = int(dt.timestamp())
    iso_fmt = dt.isoformat()
    git_fmt = dt.strftime("%Y-%m-%d %H:%M:%S %z")

    return unix_ts, iso_fmt, git_fmt


def confirm_or_exit(message: str, auto_continue: bool = False) -> None:
    """Prompt user for confirmation or exit.

    Args:
        message: Confirmation message to display
        auto_continue: If True, skip prompt and continue automatically

    Raises:
        SystemExit: If user does not confirm
    """
    if auto_continue:
        print(f"\n{message} [auto-continuing]")
        return
    response = input(f"\n{message} (y/N): ").strip().lower()
    if response not in ("y", "yes"):
        print("Aborted.")
        sys.exit(0)


def run_pre_flight_checks(source: Path, version: str, auto_continue: bool = False) -> bool:
    """Run pre-flight validation checks.

    Args:
        source: Source repository path
        version: Version string
        auto_continue: If True, skip confirmation prompts

    Returns:
        True if all checks pass, False otherwise
    """
    print("\n" + "=" * 60)
    print("Pre-Flight Checks")
    print("=" * 60)

    # Check 1: CHANGELOG.md exists and has entry for version
    changelog = source / "CHANGELOG.md"
    if not changelog.exists():
        print("❌ CHANGELOG.md not found")
        return False

    changelog_text = changelog.read_text()
    if f"## [{version}]" not in changelog_text and f"## {version}" not in changelog_text:
        print(f"⚠️  Warning: CHANGELOG.md may not have entry for v{version}")
        print("   Please ensure CHANGELOG is updated before release")
        confirm_or_exit("Continue anyway?", auto_continue)
    else:
        print(f"✓ CHANGELOG.md has entry for v{version}")

    # Check 2: Git status in main repo
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=source,
        capture_output=True,
        text=True,
    )

    if result.stdout.strip():
        print("⚠️  Warning: Main repository has uncommitted changes:")
        for line in result.stdout.strip().split("\n")[:10]:
            print(f"     {line}")
        confirm_or_exit("Continue anyway?", auto_continue)
    else:
        print("✓ Main repository is clean")

    # Check 3: Version string format
    if not version.replace(".", "").replace("-", "").replace("_", "").isalnum():
        print(f"❌ Invalid version format: {version}")
        return False
    print(f"✓ Version format valid: {version}")

    # Check 4: Version consistency between parameter and source
    try:
        # Import benchbox to get source version
        sys.path.insert(0, str(source))
        from benchbox import __version__ as source_version

        if source_version != version:
            print("⚠️  Warning: Version mismatch!")
            print(f"   Source code version: {source_version}")
            print(f"   Release version parameter: {version}")
            print(f"   Consider running: python scripts/update_version.py --version {version}")
            confirm_or_exit("Continue with mismatched versions?", auto_continue)
        else:
            print(f"✓ Version matches source: {version}")
    except Exception as e:
        print(f"⚠️  Warning: Could not verify source version: {e}")
        confirm_or_exit("Continue without version verification?", auto_continue)

    print("\n✅ Pre-flight checks passed")
    return True


def run_step(script: str, args: list[str], description: str, check: bool = True) -> bool:
    """Run a release step script.

    Args:
        script: Script name (e.g., "prepare_release.py")
        args: Command-line arguments for the script
        description: Human-readable description
        check: Whether to raise on non-zero exit

    Returns:
        True if successful, False otherwise
    """
    print("\n" + "=" * 60)
    print(description)
    print("=" * 60)
    print(f"Running: {script} {' '.join(str(a) for a in args)}\n")

    script_path = Path(__file__).parent / script
    result = subprocess.run(
        [sys.executable, str(script_path)] + args,
        check=False,
    )

    if result.returncode != 0:
        print(f"\n❌ {description} failed")
        if check:
            sys.exit(1)
        return False

    print(f"\n✓ {description} complete")
    return True


def run_size_check(target: Path) -> bool:
    """Run release size safety check.

    Args:
        target: Path to the release directory

    Returns:
        True if all checks passed, False otherwise
    """
    print("\n" + "=" * 60)
    print("Release Size Safety Check")
    print("=" * 60)

    passed, violations, total_size_mb = check_release_size(target)

    print(f"\nTotal release size: {total_size_mb:.1f} MB")
    print(f"Limits: {DEFAULT_MAX_FILE_SIZE_MB} MB per file, {DEFAULT_MAX_TOTAL_SIZE_MB} MB total")

    if passed:
        print("\n✓ All size checks passed")
        return True
    else:
        print(f"\n❌ Found {len(violations)} violation(s):\n")
        for v in violations:
            print(f"  • {v}")
        print("\nRelease aborted. Please fix the violations and try again.")
        return False


def run_ci_checks(source: Path) -> bool:
    """Run CI validation checks (lint, type check, tests).

    These are the same checks that run in GitHub Actions. Running them
    locally before release ensures the code is in a releasable state.

    Args:
        source: Source repository path

    Returns:
        True if all checks pass, False otherwise
    """
    print("\n" + "=" * 60)
    print("CI Validation Checks")
    print("=" * 60)
    print("Running the same checks as GitHub Actions...")
    print("(equivalent to: make ci-lint && make ci-test)\n")

    checks = [
        ("Lint check (ruff)", ["uv", "run", "ruff", "check", "."]),
        ("Format check (ruff)", ["uv", "run", "ruff", "format", "--check", "."]),
        ("Type check (ty)", ["uv", "run", "ty", "check"]),
        (
            "Fast tests with coverage",
            [
                "uv",
                "run",
                "--",
                "python",
                "-m",
                "pytest",
                "tests",
                "-m",
                "fast",
                "--tb=short",
                "-p",
                "pytest_cov",  # Override pytest.ini's -p no:cov
                "--cov=benchbox",
                "--cov-report=term-missing",
                "--cov-fail-under=50",
                # Exclude PySpark tests - CI runs these separately with Java
                "--ignore=tests/unit/platforms/dataframe/test_pyspark_df.py",
                "--ignore=tests/unit/platforms/pyspark",
            ],
        ),
    ]

    for description, cmd in checks:
        print(f"• {description}...", end=" ", flush=True)
        result = subprocess.run(
            cmd,
            cwd=source,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("❌ FAILED")
            print(f"\nCommand: {' '.join(cmd)}")
            if result.stdout.strip():
                # Show last 20 lines of output
                lines = result.stdout.strip().split("\n")
                if len(lines) > 20:
                    print(f"... ({len(lines) - 20} lines omitted)")
                    lines = lines[-20:]
                print("\n".join(lines))
            if result.stderr.strip():
                print(result.stderr.strip())
            print(f"\n❌ CI check '{description}' failed")
            print("Fix the issues above before releasing, or use --skip-ci to bypass.")
            return False
        print("✓")

    print("\n✅ All CI checks passed")
    return True


def squash_and_push(target: Path, version: str) -> bool:
    """Squash all commits into a single release commit and push to remote.

    Args:
        target: Path to the public repository
        version: Release version string

    Returns:
        True if successful, False otherwise
    """
    print("\n" + "=" * 60)
    print("Squashing commits and pushing to remote...")
    print("=" * 60)

    commit_message = f"""Initial release v{version}

BenchBox: SQL benchmarking framework for OLAP databases.

Features:
- TPC-H, TPC-DS, SSB, ClickBench benchmarks
- DuckDB, SQLite, PostgreSQL, Snowflake, Databricks platforms
- DataFrame support (Polars, Pandas, DuckDB, DataFusion)
- MCP server for AI agent integration
- Comprehensive validation and result analysis

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"""

    try:
        # Get commit count
        result = subprocess.run(
            ["git", "rev-list", "--count", "HEAD"],
            cwd=target,
            capture_output=True,
            text=True,
            check=True,
        )
        commit_count = int(result.stdout.strip())

        if commit_count > 1:
            # Squash all commits into one
            print(f"  Squashing {commit_count} commits...")
            subprocess.run(
                ["git", "reset", "--soft", f"HEAD~{commit_count - 1}"],
                cwd=target,
                check=True,
            )
            subprocess.run(
                ["git", "commit", "--amend", "-m", commit_message],
                cwd=target,
                check=True,
            )
        else:
            # Just amend the single commit with the proper message
            print("  Amending single commit...")
            subprocess.run(
                ["git", "commit", "--amend", "-m", commit_message],
                cwd=target,
                check=True,
            )

        # Update tag to point to new commit
        print(f"  Updating tag v{version}...")
        subprocess.run(
            ["git", "tag", "-d", f"v{version}"],
            cwd=target,
            capture_output=True,  # Suppress error if tag doesn't exist
        )
        subprocess.run(
            ["git", "tag", f"v{version}"],
            cwd=target,
            check=True,
        )

        # Push with force
        print("  Pushing to remote (force)...")
        subprocess.run(
            ["git", "push", "--force", "origin", "main"],
            cwd=target,
            check=True,
        )
        subprocess.run(
            ["git", "push", "--force", "origin", f"v{version}"],
            cwd=target,
            check=True,
        )

        print("✅ Squash and push complete!")
        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Failed: {e}")
        return False


def display_summary(
    version: str,
    timestamp_info: tuple[int, str, str],
    public_path: Path,
):
    """Display release summary.

    Args:
        version: Release version
        timestamp_info: (unix_ts, iso_fmt, git_fmt) tuple
        public_path: Path to public repository
    """
    unix_ts, iso_fmt, git_fmt = timestamp_info

    print("\n" + "=" * 60)
    print("RELEASE SUMMARY")
    print("=" * 60)
    print(f"\nVersion: {version}")
    print(f"Timestamp: {iso_fmt}")
    print(f"Unix: {unix_ts}")
    print(f"Git: {git_fmt}")
    print(f"\nPublic Repository: {public_path}")
    print(f"Distribution: {public_path / 'dist'}")

    # Show built artifacts
    dist_dir = public_path / "dist"
    if dist_dir.exists():
        print("\nArtifacts:")
        for artifact in sorted(dist_dir.glob("*")):
            if artifact.suffix in (".whl", ".gz"):
                size_mb = artifact.stat().st_size / (1024 * 1024)
                print(f"  - {artifact.name} ({size_mb:.2f} MB)")

    print("\n" + "=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        type=str,
        required=True,
        help="Release version (e.g., 0.1.0)",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=Path("../BenchBox-public"),
        help="Target directory for public release (default: ../BenchBox-public)",
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path.cwd(),
        help="Source repository (default: current directory)",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip pre-flight validation checks",
    )
    parser.add_argument(
        "--skip-ci",
        action="store_true",
        help="Skip CI validation (lint, tests, type checking)",
    )
    parser.add_argument(
        "--skip-tests",
        action="store_true",
        help="Skip smoke tests",
    )
    parser.add_argument(
        "--skip-archive",
        action="store_true",
        help="Skip artifact archiving",
    )
    parser.add_argument(
        "--force-tag",
        action="store_true",
        help="Overwrite existing git tag if it exists",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Squash commits into single release commit and push to remote",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show detailed output",
    )
    parser.add_argument(
        "--auto-continue",
        "-y",
        action="store_true",
        help="Automatically continue after each successful step (no confirmation prompts)",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        default=None,
        help=(
            "Use a specific timestamp instead of Saturday midnight. "
            "Format: 'YYYY-MM-DD HH:MM:SS -0500' (git format with timezone)"
        ),
    )

    args = parser.parse_args()

    # Resolve paths
    source = args.source.resolve()
    target = args.target.resolve()
    version = args.version

    if args.dry_run:
        print("DRY RUN MODE - No changes will be made")
        print(f"Version: {version}")
        print(f"Source: {source}")
        print(f"Target: {target}")
        return 0

    # Get timestamp: use provided or calculate Saturday midnight
    if args.timestamp:
        try:
            unix_ts, iso_fmt, git_fmt = parse_timestamp(args.timestamp)
            timestamp_source = "provided"
        except ValueError as e:
            print(f"❌ {e}")
            return 1
    else:
        unix_ts, iso_fmt, git_fmt = calculate_most_recent_saturday_midnight()
        timestamp_source = "Saturday midnight"

    print("=" * 60)
    print("BenchBox Automated Release")
    print("=" * 60)
    print(f"Version: {version}")
    print(f"Timestamp: {iso_fmt} ({timestamp_source})")
    print(f"  Unix: {unix_ts}")
    print(f"  Git: {git_fmt}")
    print(f"Source: {source}")
    print(f"Target: {target}")
    print("=" * 60)

    # Confirm before starting
    confirm_or_exit(f"Proceed with release v{version}?", args.auto_continue)

    # Step 1: Pre-flight checks
    if not args.skip_preflight:
        if not run_pre_flight_checks(source, version, args.auto_continue):
            print("\n❌ Pre-flight checks failed")
            return 1
        confirm_or_exit("Pre-flight checks passed. Continue?", args.auto_continue)

    # Step 2: CI validation (lint, type check, tests)
    if not args.skip_ci:
        if not run_ci_checks(source):
            print("\n❌ CI validation failed")
            print("Use --skip-ci to bypass (not recommended)")
            return 1
        confirm_or_exit("CI checks passed. Continue?", args.auto_continue)
    else:
        print("\n⚠️  CI validation skipped (--skip-ci)")

    # Step 3: Prepare public tree
    run_step(
        "prepare_release.py",
        [
            str(target),
            "--source",
            str(source),
            "--version",
            version,
            "--init-git",
            "--timestamp",
            str(unix_ts),
            "--no-clean",  # Preserve existing if present
            # Include TPC compilation infrastructure for users who need to build from source
            # (excludes TPC source code itself - users must obtain from tpc.org)
            "--include",
            "_sources/README.md",
            "--include",
            "_sources/compilation",
            "--include",
            "_sources/tpc-h/PATCHES.md",
            "--include",
            "_sources/tpc-h/stdout-support.patch",
            "--include",
            "_sources/tpc-ds/PATCHES.md",
            "--include",
            "_sources/tpc-ds/stdout-support.patch",
            # Include TPC answer files for validation tests
            "--include",
            "_sources/tpc-h/dbgen/answers",
            "--include",
            "_sources/tpc-ds/answer_sets",
        ],
        "Step 3/6: Preparing Public Tree",
    )

    # Step 4: Size safety check
    if not run_size_check(target):
        return 1

    # Step 5: Build packages
    run_step(
        "build_release.py",
        [str(target), "--timestamp", str(unix_ts)],
        "Step 5/6: Building Packages",
    )

    # Step 6: Verify and smoke test
    verify_args = [str(target)]
    if args.skip_tests:
        verify_args.append("--skip-smoke-tests")
    if args.verbose:
        verify_args.append("--verbose")

    run_step(
        "verify_release.py",
        verify_args,
        "Step 6/6: Verifying Artifacts & Running Smoke Tests",
    )

    # Finalize (git commit/tag)
    finalize_args = [
        str(target),
        "--version",
        version,
        "--git-timestamp",
        git_fmt,
    ]
    if args.skip_archive:
        finalize_args.append("--skip-archive")
    else:
        # Archive in main repo's release/archive directory
        archive_base = source / "release" / "archive"
        finalize_args.extend(["--archive-base", str(archive_base)])
    if args.force_tag:
        finalize_args.append("--force-tag")

    run_step(
        "finalize_release.py",
        finalize_args,
        "Finalizing Release (git commit/tag)",
    )

    # Display summary
    display_summary(version, (unix_ts, iso_fmt, git_fmt), target)

    # Squash and push if requested
    if args.push:
        confirm_or_exit("Squash commits and push to remote?", args.auto_continue)
        if not squash_and_push(target, version):
            print("\n❌ Squash and push failed")
            return 1
        print("\n✅ Release automation complete!")
        print("\nThe release has been pushed. Ready to upload to PyPI:")
        print(f"  twine upload {target}/dist/benchbox-{version}*")
    else:
        print("\n✅ Release automation complete!")
        print("\nThe release is ready to publish manually:")
        print(f"  1. Review: cd {target} && git log --format=fuller")
        print(f"  2. Push: git push origin main && git push origin v{version}")
        print(f"  3. Upload: twine upload dist/benchbox-{version}*")

    return 0


if __name__ == "__main__":
    sys.exit(main())
