from __future__ import annotations

from pathlib import Path

import pytest

from benchbox.release.workflow import HOLD_BACK_PATHS, compute_source_fingerprint, prepare_public_release

pytestmark = [
    pytest.mark.unit,
    pytest.mark.fast,
]


@pytest.fixture()
def temp_source(tmp_path: Path) -> Path:
    # create minimal source tree
    (tmp_path / "benchbox/platforms").mkdir(parents=True)
    (tmp_path / "benchbox/platforms/clickhouse").mkdir(parents=True)
    (tmp_path / "benchbox/platforms/bigquery").mkdir(parents=True)
    (tmp_path / "benchbox/platforms/__init__.py").write_text("# platforms\n", encoding="utf-8")
    (tmp_path / "benchbox/__init__.py").write_text("__version__='0.1.0'\n", encoding="utf-8")
    (tmp_path / "CHANGELOG.md").write_text("# Changelog\n", encoding="utf-8")
    (tmp_path / "LICENSE").write_text("MIT\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text("[project]\nname='benchbox'\n", encoding="utf-8")
    (tmp_path / "MANIFEST.in").write_text("include README.md\n", encoding="utf-8")
    (tmp_path / "README.md").write_text("Private README\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts/dummy.py").write_text("print('hi')\n", encoding="utf-8")
    for holdback in HOLD_BACK_PATHS:
        p = tmp_path / holdback
        p.mkdir(parents=True, exist_ok=True)
        (p / "placeholder.txt").write_text("holdback\n", encoding="utf-8")
    (tmp_path / "release").mkdir()
    (tmp_path / "release/README.public.md").write_text("Public README\n", encoding="utf-8")
    (tmp_path / "release/pyproject.public.toml").write_text("[project]\nname='benchbox'\n", encoding="utf-8")
    # Create .gitignore for transform testing
    (tmp_path / ".gitignore").write_text("*.pyc\n__pycache__/\n", encoding="utf-8")
    return tmp_path


def test_prepare_public_release_strips_holdbacks(temp_source: Path, tmp_path: Path) -> None:
    target = tmp_path / "public"

    prepare_public_release(
        source=temp_source,
        target=target,
        version="0.1.0",
        clean=True,
        init_git=False,
    )

    # ensure benchbox copied
    assert (target / "benchbox/__init__.py").exists()
    # ensure hold back directories removed
    for pattern in HOLD_BACK_PATHS:
        assert not (target / pattern).exists()

    # Note: README no longer sanitized - public releases use the full README
    # Only pyproject is sanitized
    assert (target / "README.md").read_text(encoding="utf-8") == "Private README\n"
    assert (target / "pyproject.toml").read_text(encoding="utf-8") == "[project]\nname='benchbox'\n"

    # gitignore written
    assert (target / ".gitignore").exists()

    # timestamps normalized (file exists implies utime run)
    assert (target / "RELEASE_VERSION").read_text(encoding="utf-8").strip() == "0.1.0"


def test_prepare_public_release_no_clean(temp_source: Path, tmp_path: Path) -> None:
    target = tmp_path / "public"
    target.mkdir()
    (target / "old.txt").write_text("keep\n", encoding="utf-8")

    prepare_public_release(
        source=temp_source,
        target=target,
        version="0.1.0",
        clean=False,
    )

    # original file should still exist alongside release files
    assert (target / "old.txt").exists()
    assert (target / "benchbox").exists()


def test_prepare_public_release_includes_extra_files(temp_source: Path, tmp_path: Path) -> None:
    extra_file = temp_source / "RELEASE_NOTES.md"
    extra_file.write_text("Notes\n", encoding="utf-8")

    target = tmp_path / "public"

    prepare_public_release(
        source=temp_source,
        target=target,
        version="0.1.0",
        extra_root_files=["RELEASE_NOTES.md"],
    )

    assert (target / "RELEASE_NOTES.md").read_text(encoding="utf-8") == "Notes\n"


def test_prepare_public_release_missing_extra_raises(temp_source: Path, tmp_path: Path) -> None:
    target = tmp_path / "public"

    with pytest.raises(FileNotFoundError):
        prepare_public_release(
            source=temp_source,
            target=target,
            version="0.1.0",
            extra_root_files=["MISSING.txt"],
        )


def test_prepare_public_release_init_git(temp_source: Path, tmp_path: Path) -> None:
    target = tmp_path / "public"

    prepare_public_release(
        source=temp_source,
        target=target,
        version="0.1.0",
        init_git=True,
    )

    assert (target / ".git").exists()
    head = (target / ".git" / "HEAD").read_text(encoding="utf-8").strip()
    assert head.startswith("ref: refs/heads/")


class TestComputeSourceFingerprint:
    """Tests for compute_source_fingerprint()."""

    def test_same_directory_produces_same_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("x = 1\n", encoding="utf-8")
        (pkg / "module.py").write_text("def f(): pass\n", encoding="utf-8")

        fp1 = compute_source_fingerprint(pkg)
        fp2 = compute_source_fingerprint(pkg)
        assert fp1 == fp2
        assert len(fp1) == 64  # SHA256 hex length

    def test_modified_file_changes_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        mod = pkg / "module.py"
        mod.write_text("x = 1\n", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        mod.write_text("x = 2\n", encoding="utf-8")
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before != fp_after

    def test_added_file_changes_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        (pkg / "new_module.py").write_text("y = 2\n", encoding="utf-8")
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before != fp_after

    def test_non_py_files_ignored(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("x = 1\n", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        (pkg / "data.txt").write_text("not python\n", encoding="utf-8")
        (pkg / "config.json").write_text("{}\n", encoding="utf-8")
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before == fp_after

    def test_empty_directory_produces_consistent_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()

        fp1 = compute_source_fingerprint(pkg)
        fp2 = compute_source_fingerprint(pkg)
        assert fp1 == fp2

    def test_subdirectory_files_included(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        sub = pkg / "sub"
        sub.mkdir(parents=True)
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        (sub / "__init__.py").write_text("", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        (sub / "deep.py").write_text("z = 3\n", encoding="utf-8")
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before != fp_after

    def test_deleted_file_changes_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        target = pkg / "to_delete.py"
        target.write_text("x = 1\n", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        target.unlink()
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before != fp_after

    def test_renamed_file_changes_hash(self, tmp_path: Path) -> None:
        pkg = tmp_path / "pkg"
        pkg.mkdir()
        (pkg / "__init__.py").write_text("", encoding="utf-8")
        original = pkg / "old_name.py"
        original.write_text("x = 1\n", encoding="utf-8")

        fp_before = compute_source_fingerprint(pkg)
        original.rename(pkg / "new_name.py")
        fp_after = compute_source_fingerprint(pkg)

        assert fp_before != fp_after
