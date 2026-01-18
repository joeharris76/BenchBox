"""Bidirectional sync between private (BenchBox) and public (BenchBox-public) repositories.

Commands:
    status  Show differences between repos (read-only)
    push    Push changes from private to public (creates commit)
    pull    Pull changes from public to private (no commit)

Examples:
    # Show what would sync
    benchbox-sync status

    # Push changes to public repo
    benchbox-sync push --message "Sync bug fixes"

    # Pull external contributions back
    benchbox-sync pull

    # Force push even with conflicts
    benchbox-sync push --force --message "Override public changes"
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

from benchbox.release.workflow import (
    apply_transform,
    compare_repos,
    should_transform,
)


def is_git_repo(path: Path) -> bool:
    """Check if path is a git repository."""
    return (path / ".git").exists()


def is_repo_clean(path: Path) -> bool:
    """Check if git repository has no uncommitted changes."""
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=path,
        capture_output=True,
        text=True,
    )
    return not result.stdout.strip()


def git_fetch(path: Path) -> bool:
    """Fetch latest from origin."""
    result = subprocess.run(
        ["git", "fetch", "origin"],
        cwd=path,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def git_add_files(path: Path, files: set[Path]) -> bool:
    """Stage specific files for commit.

    Args:
        path: Repository root
        files: Set of relative paths to stage (includes deleted files)

    Returns:
        True if staging succeeded
    """
    if not files:
        return True

    for rel_path in sorted(files):
        result = subprocess.run(
            ["git", "add", "--", str(rel_path)],
            cwd=path,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Error staging {rel_path}: {result.stderr}")
            return False
    return True


def git_commit(path: Path, message: str) -> bool:
    """Commit staged changes.

    Args:
        path: Repository root
        message: Commit message

    Returns:
        True if commit succeeded (or nothing to commit)
    """
    # Check if there's anything to commit
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=path,
        capture_output=True,
        text=True,
    )
    if not result.stdout.strip():
        print("No changes to commit")
        return True

    # Commit
    result = subprocess.run(
        ["git", "commit", "-m", message],
        cwd=path,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Error committing: {result.stderr}")
        return False

    return True


def cmd_status(args: argparse.Namespace) -> int:
    """Show differences between repos."""
    source = args.source.resolve()
    target = args.target.resolve()

    if not source.exists():
        print(f"Error: Source repository not found: {source}")
        return 1

    if not target.exists():
        print(f"Target repository not found: {target}")
        print("This is expected for first sync. Use 'push' to initialize.")
        return 0

    print("Comparing repositories...")
    print(f"  Private (source): {source}")
    print(f"  Public (target):  {target}")
    print()

    comparison = compare_repos(source, target, check_conflicts=True)

    # Show summary
    print(f"Summary: {comparison.summary()}")
    print()

    # Show details
    if comparison.added:
        print(f"Added ({len(comparison.added)} files):")
        for f in sorted(comparison.added)[:20]:
            print(f"  + {f}")
        if len(comparison.added) > 20:
            print(f"  ... and {len(comparison.added) - 20} more")
        print()

    if comparison.modified:
        print(f"Modified ({len(comparison.modified)} files):")
        for f in sorted(comparison.modified)[:20]:
            print(f"  M {f}")
        if len(comparison.modified) > 20:
            print(f"  ... and {len(comparison.modified) - 20} more")
        print()

    if comparison.deleted:
        print(f"Deleted ({len(comparison.deleted)} files):")
        for f in sorted(comparison.deleted)[:20]:
            print(f"  - {f}")
        if len(comparison.deleted) > 20:
            print(f"  ... and {len(comparison.deleted) - 20} more")
        print()

    if comparison.conflicts:
        print(f"⚠️  Conflicts ({len(comparison.conflicts)} files):")
        for f in sorted(comparison.conflicts):
            print(f"  ! {f}")
        print()
        print("Use --force to overwrite public changes.")
        print()

    if not comparison.has_changes and not comparison.has_conflicts:
        print("Repositories are in sync.")

    return 0


def cmd_push(args: argparse.Namespace) -> int:
    """Push changes from private to public repo."""
    source = args.source.resolve()
    target = args.target.resolve()

    if not source.exists():
        print(f"Error: Source repository not found: {source}")
        return 1

    # Validate target
    if target.exists():
        if not is_git_repo(target):
            print(f"Error: Target exists but is not a git repository: {target}")
            return 1

        if not is_repo_clean(target):
            print(f"Error: Target repository has uncommitted changes: {target}")
            print("Please commit or stash changes before syncing.")
            return 1

        # Fetch latest
        print("Fetching latest from public origin...")
        if not git_fetch(target):
            print("Warning: Could not fetch from origin")

    # Compare repos
    print("\nComparing repositories...")
    comparison = compare_repos(source, target, check_conflicts=True)

    print(f"Summary: {comparison.summary()}")
    print()

    if comparison.has_conflicts and not args.force:
        print("⚠️  Conflicts detected:")
        for f in sorted(comparison.conflicts):
            print(f"  ! {f}")
        print()
        print("Use --force to overwrite public changes.")
        return 1

    if not comparison.has_changes and not comparison.has_conflicts:
        print("No changes to push.")
        return 0

    if args.dry_run:
        print("\n[DRY RUN] Would sync the following:")
        if comparison.added:
            print(f"  Add {len(comparison.added)} files")
        if comparison.modified:
            print(f"  Modify {len(comparison.modified)} files")
        if comparison.deleted:
            print(f"  Delete {len(comparison.deleted)} files")
        if comparison.conflicts:
            print(f"  Overwrite {len(comparison.conflicts)} conflicted files")
        return 0

    # Create target if needed
    if not target.exists():
        print(f"\nInitializing target repository: {target}")
        target.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "init"], cwd=target, check=True)

    # Apply changes
    print("\nApplying changes...")

    # Copy added and modified files
    files_to_copy = comparison.added | comparison.modified
    if args.force:
        files_to_copy |= comparison.conflicts

    for rel_path in sorted(files_to_copy):
        source_file = source / rel_path
        target_file = target / rel_path

        # Create parent directories
        target_file.parent.mkdir(parents=True, exist_ok=True)

        # Apply transforms if needed
        if should_transform(rel_path):
            content = source_file.read_text(encoding="utf-8")
            transformed = apply_transform(content, "push", rel_path)
            target_file.write_text(transformed, encoding="utf-8")
        else:
            shutil.copy2(source_file, target_file)

        print(f"  {'A' if rel_path in comparison.added else 'M'} {rel_path}")

    # Delete removed files
    deleted_files: set[Path] = set()
    for rel_path in sorted(comparison.deleted):
        target_file = target / rel_path
        if target_file.exists():
            target_file.unlink()
            deleted_files.add(rel_path)
            print(f"  D {rel_path}")

    # Stage only the files we modified (not git add -A which could stage unintended files)
    all_changed_files = files_to_copy | deleted_files
    print(f"\nStaging {len(all_changed_files)} files...")
    if not git_add_files(target, all_changed_files):
        print("Error: Failed to stage changes")
        return 1

    # Commit
    message = args.message or "Sync from private repository"

    print(f"Committing: {message}")
    if not git_commit(target, message):
        print("Error: Failed to commit changes")
        return 1

    print("\n✓ Push complete")
    print("\nNext steps:")
    print(f"  1. Review: cd {target} && git log -1")
    print(f"  2. Push: cd {target} && git push origin main")
    return 0


def cmd_pull(args: argparse.Namespace) -> int:
    """Pull changes from public to private repo (no auto-commit)."""
    source = args.source.resolve()  # Private repo
    target = args.target.resolve()  # Public repo

    if not target.exists():
        print(f"Error: Public repository not found: {target}")
        return 1

    if not source.exists():
        print(f"Error: Private repository not found: {source}")
        return 1

    # For pull, we reverse the comparison: public is source, private is target
    print("\nComparing repositories (pull direction)...")
    print(f"  Public (source): {target}")
    print(f"  Private (target): {source}")
    print()

    # Get files from public that differ from private
    comparison = compare_repos(target, source, check_conflicts=True)

    print(f"Summary: {comparison.summary()}")
    print()

    if comparison.has_conflicts and not args.force:
        print("⚠️  Conflicts detected:")
        for f in sorted(comparison.conflicts):
            print(f"  ! {f}")
        print()
        print("Use --force to overwrite private changes.")
        return 1

    if not comparison.has_changes and not comparison.has_conflicts:
        print("No changes to pull.")
        return 0

    if args.dry_run:
        print("\n[DRY RUN] Would sync the following:")
        if comparison.added:
            print(f"  Add {len(comparison.added)} files")
        if comparison.modified:
            print(f"  Modify {len(comparison.modified)} files")
        if comparison.deleted:
            if args.delete:
                print(f"  Delete {len(comparison.deleted)} files")
            else:
                print(f"  Skip {len(comparison.deleted)} deletions (use --delete to remove)")
        if comparison.conflicts:
            print(f"  Overwrite {len(comparison.conflicts)} conflicted files")
        return 0

    # Apply changes
    print("\nApplying changes to private repo...")
    print("Note: Changes are NOT automatically committed. Review and commit manually.")
    print()

    # Copy added and modified files
    files_to_copy = comparison.added | comparison.modified
    if args.force:
        files_to_copy |= comparison.conflicts

    for rel_path in sorted(files_to_copy):
        public_file = target / rel_path
        private_file = source / rel_path

        # Create parent directories
        private_file.parent.mkdir(parents=True, exist_ok=True)

        # Apply reverse transforms if needed
        if should_transform(rel_path):
            content = public_file.read_text(encoding="utf-8")
            transformed = apply_transform(content, "pull", rel_path)
            private_file.write_text(transformed, encoding="utf-8")
        else:
            shutil.copy2(public_file, private_file)

        print(f"  {'A' if rel_path in comparison.added else 'M'} {rel_path}")

    # Handle deleted files
    if comparison.deleted:
        if args.delete:
            print(f"\nDeleting {len(comparison.deleted)} files not in public repo...")
            for rel_path in sorted(comparison.deleted):
                private_file = source / rel_path
                if private_file.exists():
                    private_file.unlink()
                    print(f"  D {rel_path}")
        else:
            print(f"\n⚠️  {len(comparison.deleted)} files exist in private but not in public:")
            for f in sorted(comparison.deleted)[:10]:
                print(f"    {f}")
            if len(comparison.deleted) > 10:
                print(f"    ... and {len(comparison.deleted) - 10} more")
            print("  Use --delete to remove these files.")

    print("\n✓ Pull complete")
    print("\nNext steps:")
    print("  1. Review: git status")
    print("  2. Commit: git add -p && git commit -m 'Merge from public'")
    return 0


def main() -> int:
    """Entry point for benchbox-sync command."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--source",
        type=Path,
        default=Path.cwd(),
        help="Private repository (default: current directory)",
    )
    parser.add_argument(
        "--target",
        type=Path,
        default=Path("../BenchBox-public"),
        help="Public repository (default: ../BenchBox-public)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # status command
    status_parser = subparsers.add_parser(
        "status",
        help="Show differences between repos (read-only)",
    )
    status_parser.set_defaults(func=cmd_status)

    # push command
    push_parser = subparsers.add_parser(
        "push",
        help="Push changes from private to public (creates commit)",
    )
    push_parser.add_argument(
        "--message",
        "-m",
        type=str,
        help="Commit message (default: 'Sync from private repository')",
    )
    push_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force push even with conflicts",
    )
    push_parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Show what would be done without making changes",
    )
    push_parser.set_defaults(func=cmd_push)

    # pull command
    pull_parser = subparsers.add_parser(
        "pull",
        help="Pull changes from public to private (no commit)",
    )
    pull_parser.add_argument(
        "--force",
        "-f",
        action="store_true",
        help="Force pull even with conflicts",
    )
    pull_parser.add_argument(
        "--delete",
        action="store_true",
        help="Delete files in private that don't exist in public (destructive)",
    )
    pull_parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Show what would be done without making changes",
    )
    pull_parser.set_defaults(func=cmd_pull)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
