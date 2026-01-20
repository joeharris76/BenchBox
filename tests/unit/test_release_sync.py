"""Tests for bidirectional repository sync functions.

Tests the get_syncable_files(), compare_repos(), and apply_transform()
functions in benchbox/release/workflow.py.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import subprocess
from pathlib import Path

from benchbox.release.workflow import (
    ALLOWED_ROOT_FILES,
    CLAUDE_DIR_EXCLUDES,
    CLEANUP_PATTERNS,
    DOCS_DIR_EXCLUDES,
    FORBIDDEN_PATTERNS,
    GITIGNORE_PRIVATE_LINES,
    GITIGNORE_PRIVATE_SECTIONS,
    GLOBAL_EXCLUDES,
    TESTS_DIR_EXCLUDES,
    RepoComparison,
    _cleanup_unwanted_files,
    apply_transform,
    compare_repos,
    get_syncable_files,
    should_transform,
)


class TestGetSyncableFiles:
    """Test get_syncable_files() function."""

    def test_returns_set_of_paths(self, tmp_path: Path):
        """Test that get_syncable_files returns a set of Path objects."""
        # Create minimal structure
        (tmp_path / "README.md").write_text("# Test")
        result = get_syncable_files(tmp_path)
        assert isinstance(result, set)
        if result:
            assert all(isinstance(p, Path) for p in result)

    def test_includes_allowed_root_files(self, tmp_path: Path):
        """Test that files in ALLOWED_ROOT_FILES are included."""
        # Create a subset of allowed files
        (tmp_path / "README.md").write_text("# Test")
        (tmp_path / "LICENSE").write_text("MIT License")
        (tmp_path / "pyproject.toml").write_text("[project]")

        result = get_syncable_files(tmp_path)

        assert Path("README.md") in result
        assert Path("LICENSE") in result
        assert Path("pyproject.toml") in result

    def test_excludes_global_patterns(self, tmp_path: Path):
        """Test that GLOBAL_EXCLUDES patterns are excluded."""
        # Create files that should be excluded
        (tmp_path / "benchbox").mkdir()
        (tmp_path / "benchbox" / "__pycache__").mkdir()
        (tmp_path / "benchbox" / "__pycache__" / "test.pyc").write_bytes(b"bytecode")
        (tmp_path / "benchbox" / "module.py").write_text("# module")
        (tmp_path / ".DS_Store").write_bytes(b"mac metadata")

        result = get_syncable_files(tmp_path)

        # Should include module.py but not __pycache__ contents or .DS_Store
        assert Path("benchbox/module.py") in result
        pycache_files = [p for p in result if "__pycache__" in str(p)]
        assert len(pycache_files) == 0, f"Found pycache files: {pycache_files}"
        assert Path(".DS_Store") not in result

    def test_excludes_docs_build_directory(self, tmp_path: Path):
        """Test that docs/_build is excluded."""
        (tmp_path / "docs").mkdir()
        (tmp_path / "docs" / "_build").mkdir()
        (tmp_path / "docs" / "_build" / "html").mkdir()
        (tmp_path / "docs" / "_build" / "html" / "index.html").write_text("<html>")
        (tmp_path / "docs" / "index.md").write_text("# Docs")

        result = get_syncable_files(tmp_path)

        # Should include docs/index.md but not docs/_build/*
        assert Path("docs/index.md") in result
        build_files = [p for p in result if "_build" in str(p)]
        assert len(build_files) == 0, f"Found build files: {build_files}"

    def test_excludes_claude_settings_local(self, tmp_path: Path):
        """Test that .claude/settings.local.json is excluded."""
        (tmp_path / ".claude").mkdir()
        (tmp_path / ".claude" / "settings.json").write_text("{}")
        (tmp_path / ".claude" / "settings.local.json").write_text("{}")

        result = get_syncable_files(tmp_path)

        # Should include settings.json but not settings.local.json
        assert Path(".claude/settings.json") in result
        assert Path(".claude/settings.local.json") not in result

    def test_excludes_forbidden_patterns(self, tmp_path: Path):
        """Test that FORBIDDEN_PATTERNS are excluded."""
        (tmp_path / "benchbox").mkdir()
        (tmp_path / "benchbox" / "data.dat").write_bytes(b"binary data")
        (tmp_path / "benchbox" / "table.tbl").write_bytes(b"table data")
        (tmp_path / "benchbox" / "module.py").write_text("# module")

        result = get_syncable_files(tmp_path)

        # Should include .py but not .dat or .tbl
        assert Path("benchbox/module.py") in result
        dat_files = [p for p in result if p.suffix == ".dat"]
        tbl_files = [p for p in result if p.suffix == ".tbl"]
        assert len(dat_files) == 0, f"Found .dat files: {dat_files}"
        assert len(tbl_files) == 0, f"Found .tbl files: {tbl_files}"

    def test_excludes_files_not_in_allowed_roots(self, tmp_path: Path):
        """Test that files not under ALLOWED_ROOT_FILES are excluded."""
        (tmp_path / "secret_stuff").mkdir()
        (tmp_path / "secret_stuff" / "data.py").write_text("# secret")
        (tmp_path / "benchbox").mkdir()
        (tmp_path / "benchbox" / "public.py").write_text("# public")

        result = get_syncable_files(tmp_path)

        # Should include benchbox/ but not secret_stuff/
        assert Path("benchbox/public.py") in result
        secret_files = [p for p in result if "secret" in str(p)]
        assert len(secret_files) == 0, f"Found secret files: {secret_files}"

    def test_excludes_tests_databases_directory(self, tmp_path: Path):
        """Test that tests/databases is excluded (gitignored in public repo)."""
        (tmp_path / "tests").mkdir()
        (tmp_path / "tests" / "databases").mkdir()
        (tmp_path / "tests" / "databases" / "create_test_db.py").write_text("# db script")
        (tmp_path / "tests" / "unit").mkdir()
        (tmp_path / "tests" / "unit" / "test_foo.py").write_text("# test")

        result = get_syncable_files(tmp_path)

        # Should include tests/unit but not tests/databases
        assert Path("tests/unit/test_foo.py") in result
        db_files = [p for p in result if "databases" in str(p)]
        assert len(db_files) == 0, f"Found database files: {db_files}"


class TestCompareRepos:
    """Test compare_repos() function."""

    def test_detects_added_files(self, tmp_path: Path):
        """Test detection of files added in source."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        # Use files that are in ALLOWED_ROOT_FILES
        (source / "README.md").write_text("# Source")
        (source / "CONTRIBUTING.md").write_text("# Contributing")  # In ALLOWED_ROOT_FILES
        (target / "README.md").write_text("# Source")

        result = compare_repos(source, target, check_conflicts=False)

        assert Path("CONTRIBUTING.md") in result.added
        assert Path("README.md") not in result.added

    def test_detects_modified_files(self, tmp_path: Path):
        """Test detection of modified files."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        (source / "README.md").write_text("# Modified content")
        (target / "README.md").write_text("# Original content")

        result = compare_repos(source, target, check_conflicts=False)

        assert Path("README.md") in result.modified

    def test_detects_deleted_files(self, tmp_path: Path):
        """Test detection of files deleted from source."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        # Use files that are in ALLOWED_ROOT_FILES
        (source / "README.md").write_text("# Source")
        (target / "README.md").write_text("# Source")
        (target / "CONTRIBUTING.md").write_text("# Contributing")  # In ALLOWED_ROOT_FILES

        result = compare_repos(source, target, check_conflicts=False)

        assert Path("CONTRIBUTING.md") in result.deleted

    def test_detects_unchanged_files(self, tmp_path: Path):
        """Test detection of unchanged files."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        (source / "README.md").write_text("# Same content")
        (target / "README.md").write_text("# Same content")

        result = compare_repos(source, target, check_conflicts=False)

        assert Path("README.md") in result.unchanged
        assert Path("README.md") not in result.modified

    def test_has_changes_property(self, tmp_path: Path):
        """Test has_changes property."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        (source / "README.md").write_text("# Same")
        (target / "README.md").write_text("# Same")

        result = compare_repos(source, target, check_conflicts=False)
        assert not result.has_changes

        # Add a new file (must be in ALLOWED_ROOT_FILES)
        (source / "CONTRIBUTING.md").write_text("# Contributing")
        result = compare_repos(source, target, check_conflicts=False)
        assert result.has_changes

    def test_summary_method(self, tmp_path: Path):
        """Test summary() method returns correct description."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        (source / "README.md").write_text("# Same")
        (target / "README.md").write_text("# Same")

        result = compare_repos(source, target, check_conflicts=False)
        assert "No changes" in result.summary()

        # Add changes (must be in ALLOWED_ROOT_FILES)
        (source / "CONTRIBUTING.md").write_text("# Contributing")
        result = compare_repos(source, target, check_conflicts=False)
        assert "Added: 1 files" in result.summary()

    def test_handles_missing_target(self, tmp_path: Path):
        """Test handling when target doesn't exist."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        # target is NOT created

        (source / "README.md").write_text("# Source")

        result = compare_repos(source, target, check_conflicts=False)

        # All source files should be marked as added
        assert Path("README.md") in result.added
        assert len(result.deleted) == 0
        assert len(result.modified) == 0


class TestConflictDetection:
    """Test conflict detection using git history."""

    def _init_git_repo(self, path: Path) -> None:
        """Initialize a git repo and make initial commit."""
        subprocess.run(["git", "init"], cwd=path, capture_output=True, check=True)
        subprocess.run(["git", "config", "user.email", "test@test.com"], cwd=path, capture_output=True, check=True)
        subprocess.run(["git", "config", "user.name", "Test"], cwd=path, capture_output=True, check=True)

    def _git_add_commit(self, path: Path, message: str) -> None:
        """Stage and commit all changes."""
        subprocess.run(["git", "add", "-A"], cwd=path, capture_output=True, check=True)
        subprocess.run(["git", "commit", "-m", message], cwd=path, capture_output=True, check=True)

    def test_detects_conflict_when_both_modified(self, tmp_path: Path):
        """Test conflict detection when file modified in both repos."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        # Initialize git repos with same initial content
        (source / "README.md").write_text("# Original")
        (target / "README.md").write_text("# Original")

        self._init_git_repo(source)
        self._init_git_repo(target)
        self._git_add_commit(source, "Initial")
        self._git_add_commit(target, "Initial")

        # Now modify both with different content
        (source / "README.md").write_text("# Modified in source")
        (target / "README.md").write_text("# Modified in target")

        result = compare_repos(source, target, check_conflicts=True)

        assert Path("README.md") in result.conflicts

    def test_no_conflict_when_only_source_modified(self, tmp_path: Path):
        """Test no conflict when only source is modified."""
        source = tmp_path / "source"
        target = tmp_path / "target"
        source.mkdir()
        target.mkdir()

        # Initialize git repos with same initial content
        (source / "README.md").write_text("# Original")
        (target / "README.md").write_text("# Original")

        self._init_git_repo(source)
        self._init_git_repo(target)
        self._git_add_commit(source, "Initial")
        self._git_add_commit(target, "Initial")

        # Only modify source
        (source / "README.md").write_text("# Modified in source")

        result = compare_repos(source, target, check_conflicts=True)

        assert Path("README.md") in result.modified
        assert Path("README.md") not in result.conflicts


class TestApplyTransform:
    """Test apply_transform() function."""

    def test_push_transform_applies_substitutions(self):
        """Test that push direction applies public substitutions."""
        content = 'email = "joeharris76@gmail.com"'
        result = apply_transform(content, "push")
        assert "joe@benchbox.dev" in result
        assert "joeharris76@gmail.com" not in result

    def test_pull_transform_reverses_substitutions(self):
        """Test that pull direction reverses substitutions."""
        content = 'email = "joe@benchbox.dev"'
        result = apply_transform(content, "pull")
        assert "joeharris76@gmail.com" in result
        assert "joe@benchbox.dev" not in result

    def test_round_trip_transformation(self):
        """Test that push then pull returns original content."""
        original = 'email = "joeharris76@gmail.com"'
        pushed = apply_transform(original, "push")
        pulled = apply_transform(pushed, "pull")
        assert pulled == original

    def test_no_transform_for_unrelated_content(self):
        """Test that unrelated content is unchanged."""
        content = "def hello():\n    print('hello')"
        push_result = apply_transform(content, "push")
        pull_result = apply_transform(content, "pull")
        assert push_result == content
        assert pull_result == content

    def test_invalid_direction_raises_error(self):
        """Test that invalid direction raises ValueError."""
        import pytest

        with pytest.raises(ValueError, match="Invalid direction"):
            apply_transform("content", "invalid")

        with pytest.raises(ValueError, match="must be 'push' or 'pull'"):
            apply_transform("content", "")


class TestGitignoreTransform:
    """Test gitignore transformation for sync."""

    def test_gitignore_push_removes_private_sections(self):
        """Test that push removes private-only sections from gitignore."""
        gitignore_content = """# Python
__pycache__/
*.pyc

# Exclude everything in _project/ except TODO files
_project/*
!_project/specs/
!_project/TODO/

# Virtual Environments
venv/
.venv/

# Firebolt data directories
/firebolt-data
/firebolt-core-data

# IDE specific
.idea/
"""
        result = apply_transform(gitignore_content, "push", Path(".gitignore"))

        # Should keep Python and Virtual Environments sections
        assert "__pycache__/" in result
        assert "venv/" in result
        assert ".idea/" in result

        # Should remove _project section
        assert "_project/*" not in result
        assert "!_project/specs/" not in result

        # Should remove Firebolt section
        assert "/firebolt-data" not in result
        assert "/firebolt-core-data" not in result

    def test_gitignore_push_removes_private_lines(self):
        """Test that push removes individual private-only lines."""
        gitignore_content = """# Python
__pycache__/
.mcp.json
_sources/join-order-benchmark/
*.pyc
"""
        result = apply_transform(gitignore_content, "push", Path(".gitignore"))

        # Should keep normal patterns
        assert "__pycache__/" in result
        assert "*.pyc" in result

        # Should remove private lines
        assert ".mcp.json" not in result
        assert "_sources/join-order-benchmark/" not in result

    def test_gitignore_pull_unchanged(self):
        """Test that pull returns gitignore unchanged."""
        gitignore_content = """# Public gitignore
__pycache__/
*.pyc
"""
        result = apply_transform(gitignore_content, "pull", Path(".gitignore"))
        assert result == gitignore_content

    def test_gitignore_removes_consecutive_blank_lines(self):
        """Test that consecutive blank lines are cleaned up."""
        gitignore_content = """# Python
__pycache__/

# Exclude everything in _project/
_project/*


# IDE specific
.idea/
"""
        result = apply_transform(gitignore_content, "push", Path(".gitignore"))

        # Should not have multiple consecutive blank lines
        assert "\n\n\n" not in result

    def test_should_transform_includes_gitignore(self):
        """Test that should_transform returns True for .gitignore."""
        assert should_transform(Path(".gitignore"))
        assert should_transform(Path("some/path/.gitignore"))

    def test_gitignore_private_sections_defined(self):
        """Test that GITIGNORE_PRIVATE_SECTIONS is properly defined."""
        assert len(GITIGNORE_PRIVATE_SECTIONS) > 0
        # Check for key sections
        section_texts = " ".join(GITIGNORE_PRIVATE_SECTIONS)
        assert "_project" in section_texts.lower()
        assert "firebolt" in section_texts.lower()

    def test_gitignore_private_lines_defined(self):
        """Test that GITIGNORE_PRIVATE_LINES is properly defined."""
        assert len(GITIGNORE_PRIVATE_LINES) > 0
        assert ".mcp.json" in GITIGNORE_PRIVATE_LINES


class TestShouldTransform:
    """Test should_transform() function."""

    def test_pyproject_should_transform(self):
        """Test that pyproject.toml needs transform."""
        assert should_transform(Path("pyproject.toml"))
        assert should_transform(Path("some/path/pyproject.toml"))

    def test_other_files_should_not_transform(self):
        """Test that other files don't need transform."""
        assert not should_transform(Path("README.md"))
        assert not should_transform(Path("benchbox/__init__.py"))
        assert not should_transform(Path("tests/test_foo.py"))


class TestExclusionConstants:
    """Test that exclusion constants are properly defined."""

    def test_allowed_root_files_not_empty(self):
        """Test that ALLOWED_ROOT_FILES is defined and not empty."""
        assert len(ALLOWED_ROOT_FILES) > 0
        assert "README.md" in ALLOWED_ROOT_FILES
        assert "benchbox" in ALLOWED_ROOT_FILES

    def test_global_excludes_includes_pycache(self):
        """Test that GLOBAL_EXCLUDES includes __pycache__."""
        assert "__pycache__" in GLOBAL_EXCLUDES

    def test_docs_excludes_includes_build(self):
        """Test that DOCS_DIR_EXCLUDES includes _build."""
        assert "_build" in DOCS_DIR_EXCLUDES

    def test_claude_excludes_includes_settings_local(self):
        """Test that CLAUDE_DIR_EXCLUDES includes settings.local.json."""
        assert "settings.local.json" in CLAUDE_DIR_EXCLUDES

    def test_forbidden_patterns_includes_data_files(self):
        """Test that FORBIDDEN_PATTERNS includes data file extensions."""
        assert "*.dat" in FORBIDDEN_PATTERNS
        assert "*.tbl" in FORBIDDEN_PATTERNS

    def test_tests_excludes_includes_databases(self):
        """Test that TESTS_DIR_EXCLUDES includes databases."""
        assert "databases" in TESTS_DIR_EXCLUDES


class TestSymlinkHandling:
    """Test symlink handling in sync functions."""

    def test_symlinks_are_skipped_in_syncable_files(self, tmp_path: Path):
        """Test that symlinks are not included in syncable files.

        Symlinks could point outside the repo or create cycles, so they
        should be skipped to avoid security issues and infinite loops.
        """
        import os

        # Create a regular file
        (tmp_path / "benchbox").mkdir()
        (tmp_path / "benchbox" / "real_file.py").write_text("# real")

        # Create a symlink
        symlink_path = tmp_path / "benchbox" / "link_to_file.py"
        try:
            os.symlink(tmp_path / "benchbox" / "real_file.py", symlink_path)
        except OSError:
            # Skip test on systems that don't support symlinks (e.g., some Windows configs)
            import pytest

            pytest.skip("Symlinks not supported on this system")

        result = get_syncable_files(tmp_path)

        # Real file should be included
        assert Path("benchbox/real_file.py") in result
        # Symlink should be skipped (it's not a regular file)
        assert Path("benchbox/link_to_file.py") not in result

    def test_directory_symlinks_are_not_followed(self, tmp_path: Path):
        """Test that directory symlinks are not followed (avoids cycles)."""
        import os

        # Create a directory with a file
        (tmp_path / "benchbox").mkdir()
        (tmp_path / "benchbox" / "subdir").mkdir()
        (tmp_path / "benchbox" / "subdir" / "file.py").write_text("# file")

        # Create a symlink to the parent (would create a cycle)
        symlink_dir = tmp_path / "benchbox" / "cycle_link"
        try:
            os.symlink(tmp_path / "benchbox", symlink_dir)
        except OSError:
            import pytest

            pytest.skip("Symlinks not supported on this system")

        result = get_syncable_files(tmp_path)

        # Real file should be included
        assert Path("benchbox/subdir/file.py") in result
        # Files through symlink should not be included
        cycle_files = [p for p in result if "cycle_link" in str(p)]
        assert len(cycle_files) == 0, f"Found files through symlink: {cycle_files}"


class TestRepoComparisonClass:
    """Test RepoComparison class directly."""

    def test_initialization(self):
        """Test RepoComparison can be initialized."""
        comparison = RepoComparison(
            added={Path("a.py")},
            modified={Path("b.py")},
            deleted={Path("c.py")},
            conflicts={Path("d.py")},
            unchanged={Path("e.py")},
        )

        assert Path("a.py") in comparison.added
        assert Path("b.py") in comparison.modified
        assert Path("c.py") in comparison.deleted
        assert Path("d.py") in comparison.conflicts
        assert Path("e.py") in comparison.unchanged

    def test_has_changes_with_added(self):
        """Test has_changes is True when files are added."""
        comparison = RepoComparison(
            added={Path("a.py")},
            modified=set(),
            deleted=set(),
            conflicts=set(),
            unchanged=set(),
        )
        assert comparison.has_changes

    def test_has_changes_with_deleted(self):
        """Test has_changes is True when files are deleted."""
        comparison = RepoComparison(
            added=set(),
            modified=set(),
            deleted={Path("a.py")},
            conflicts=set(),
            unchanged=set(),
        )
        assert comparison.has_changes

    def test_has_conflicts_property(self):
        """Test has_conflicts property."""
        comparison = RepoComparison(
            added=set(),
            modified=set(),
            deleted=set(),
            conflicts={Path("a.py")},
            unchanged=set(),
        )
        assert comparison.has_conflicts

        comparison_no_conflict = RepoComparison(
            added=set(),
            modified=set(),
            deleted=set(),
            conflicts=set(),
            unchanged=set(),
        )
        assert not comparison_no_conflict.has_conflicts


class TestCleanupUnwantedFiles:
    """Test _cleanup_unwanted_files() function."""

    def test_removes_venv_directory(self, tmp_path: Path):
        """Test that .venv directory is removed."""
        venv = tmp_path / ".venv"
        venv.mkdir()
        (venv / "pyvenv.cfg").write_text("test")

        _cleanup_unwanted_files(tmp_path)

        assert not venv.exists()

    def test_removes_ruff_cache(self, tmp_path: Path):
        """Test that .ruff_cache directory is removed."""
        ruff = tmp_path / ".ruff_cache"
        ruff.mkdir()
        (ruff / "cache.json").write_text("{}")

        _cleanup_unwanted_files(tmp_path)

        assert not ruff.exists()

    def test_removes_dist_directory(self, tmp_path: Path):
        """Test that dist directory is removed."""
        dist = tmp_path / "dist"
        dist.mkdir()
        (dist / "package.whl").write_text("fake wheel")

        _cleanup_unwanted_files(tmp_path)

        assert not dist.exists()

    def test_removes_egg_info(self, tmp_path: Path):
        """Test that *.egg-info directories are removed."""
        egg = tmp_path / "benchbox.egg-info"
        egg.mkdir()
        (egg / "PKG-INFO").write_text("test")

        _cleanup_unwanted_files(tmp_path)

        assert not egg.exists()

    def test_preserves_allowed_files(self, tmp_path: Path):
        """Test that allowed files are not removed."""
        # Create files that should be preserved
        readme = tmp_path / "README.md"
        readme.write_text("# Test")
        benchbox = tmp_path / "benchbox"
        benchbox.mkdir()
        (benchbox / "__init__.py").write_text("# init")

        # Create file that should be removed
        venv = tmp_path / ".venv"
        venv.mkdir()

        _cleanup_unwanted_files(tmp_path)

        # Allowed files still exist
        assert readme.exists()
        assert benchbox.exists()
        # Unwanted file removed
        assert not venv.exists()

    def test_cleanup_patterns_are_comprehensive(self):
        """Test that CLEANUP_PATTERNS includes common dev artifacts."""
        expected_patterns = {
            ".venv",
            "venv",
            ".ruff_cache",
            "*.egg-info",
            "node_modules",
            ".mypy_cache",
            ".pytest_cache",
            "__pycache__",
            ".coverage",
            "htmlcov",
            ".tox",
            ".nox",
            "dist",
            "build",
            # Note: 'landing' is NOT in cleanup patterns because it's in
            # ALLOWED_ROOT_FILES for GitHub Pages marketing site
        }
        assert set(CLEANUP_PATTERNS) == expected_patterns

    def test_handles_nonexistent_target(self, tmp_path: Path):
        """Test that cleanup handles non-existent target gracefully."""
        nonexistent = tmp_path / "does_not_exist"
        # Should not raise
        _cleanup_unwanted_files(nonexistent)
