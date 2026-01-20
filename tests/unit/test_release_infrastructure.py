"""Tests for release infrastructure.

Validates release-related configurations, workflows, and files to ensure
proper project release setup.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import sys
from pathlib import Path

import pytest
import yaml

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib  # type: ignore[import-not-found]


class TestReleaseInfrastructure:
    """Test release infrastructure configuration and files."""

    def test_changelog_exists(self):
        """Test that CHANGELOG.md exists."""
        changelog_path = Path(__file__).parent.parent.parent / "CHANGELOG.md"
        assert changelog_path.exists(), "CHANGELOG.md file must exist"

    def test_pyproject_toml_release_config(self):
        """Test that pyproject.toml has correct release configuration."""
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        # Check project metadata
        project = config["project"]
        assert "name" in project
        assert project["name"] == "benchbox"
        assert "version" in project
        assert "description" in project
        assert "readme" in project
        assert "license" in project
        assert "authors" in project

        # Check URLs point to correct repository
        urls = project["urls"]
        expected_repo = "https://github.com/joeharris76/benchbox"
        assert urls["Homepage"] == expected_repo
        assert urls["Repository"] == f"{expected_repo}.git"
        assert urls["Bug Tracker"] == f"{expected_repo}/issues"
        assert urls["Changelog"] == f"{expected_repo}/blob/main/CHANGELOG.md"

        # No incorrect repository references
        for url in urls.values():
            assert "anthropics/claude-code" not in url
            assert "anthropic" not in url

    def test_github_issue_url_fix(self):
        """Test that exceptions.py has correct GitHub issue URL."""
        exceptions_path = Path(__file__).parent.parent.parent / "benchbox" / "cli" / "exceptions.py"

        with open(exceptions_path, encoding="utf-8") as f:
            content = f.read()

        # Should have correct repository URL
        assert "https://github.com/joeharris76/benchbox/issues" in content
        # Should not have incorrect URLs
        assert "anthropics/claude-code" not in content

    def test_github_workflows_exist(self):
        """Test that GitHub workflows exist and are properly configured."""
        workflows_dir = Path(__file__).parent.parent.parent / ".github" / "workflows"
        assert workflows_dir.exists(), "GitHub workflows directory must exist"

        # Required workflows
        required_workflows = ["test.yml", "lint.yml", "release.yml"]
        for workflow in required_workflows:
            workflow_path = workflows_dir / workflow
            assert workflow_path.exists(), f"{workflow} workflow must exist"

    def test_test_workflow_configuration(self):
        """Test that test workflow is properly configured."""
        test_workflow_path = Path(__file__).parent.parent.parent / ".github" / "workflows" / "test.yml"

        with open(test_workflow_path, encoding="utf-8") as f:
            workflow = yaml.safe_load(f)

        # Check basic structure
        assert "name" in workflow
        assert True in workflow  # YAML parses "on" as boolean True
        assert "jobs" in workflow

        # Check trigger events (YAML parses 'on' as True)
        on_events = workflow[True]  # YAML parses "on" as boolean True
        assert "push" in on_events
        assert "pull_request" in on_events

        # Check jobs
        jobs = workflow["jobs"]
        assert "test" in jobs

        # Check Python versions (simplified matrix: 3.10, 3.12, 3.13)
        test_job = jobs["test"]
        matrix = test_job["strategy"]["matrix"]
        python_versions = matrix["python-version"]
        expected_versions = ["3.10", "3.12", "3.13"]
        for version in expected_versions:
            assert version in python_versions

        # Check that it uses uv
        steps = test_job["steps"]
        uv_step_found = any("uv" in str(step).lower() for step in steps)
        assert uv_step_found, "Workflow should use uv for dependency management"

    def test_release_workflow_configuration(self):
        """Test that release workflow is properly configured."""
        release_workflow_path = Path(__file__).parent.parent.parent / ".github" / "workflows" / "release.yml"

        with open(release_workflow_path, encoding="utf-8") as f:
            workflow = yaml.safe_load(f)

        # Check basic structure
        assert "name" in workflow
        assert workflow["name"] == "Release"
        assert True in workflow  # YAML parses "on" as boolean True
        assert "jobs" in workflow

        # Check trigger events (YAML parses 'on' as True)
        on_events = workflow[True]  # YAML parses "on" as boolean True
        assert "release" in on_events
        assert "workflow_dispatch" in on_events

        # Check jobs exist
        jobs = workflow["jobs"]
        # Release workflow delegates testing to CI workflows via check-ci-passed
        required_jobs = ["check-ci-passed", "build", "publish"]
        for job in required_jobs:
            assert job in jobs, f"Release workflow must have {job} job"

        # Check that publish job uses trusted publishing
        publish_job = jobs["publish"]
        assert "permissions" in publish_job
        assert "id-token" in publish_job["permissions"]
        assert publish_job["permissions"]["id-token"] == "write"

    def test_issue_templates_exist(self):
        """Test that GitHub issue templates exist."""
        templates_dir = Path(__file__).parent.parent.parent / ".github" / "ISSUE_TEMPLATE"
        assert templates_dir.exists(), "Issue templates directory must exist"

        # Required templates
        required_templates = ["bug_report.yml", "feature_request.yml", "platform_support.yml", "config.yml"]

        for template in required_templates:
            template_path = templates_dir / template
            assert template_path.exists(), f"{template} template must exist"

    def test_pr_template_exists(self):
        """Test that pull request template exists."""
        pr_template_path = Path(__file__).parent.parent.parent / ".github" / "PULL_REQUEST_TEMPLATE.md"
        assert pr_template_path.exists(), "Pull request template must exist"

        with open(pr_template_path, encoding="utf-8") as f:
            content = f.read()

        # Should have key sections
        required_sections = ["## Description", "## Type of Change", "## Testing", "## Documentation", "## Code Quality"]

        for section in required_sections:
            assert section in content, f"PR template must have {section} section"

    @pytest.mark.slow
    def test_package_build_succeeds(self):
        """Test that the package can be built successfully."""
        import subprocess
        import tempfile
        from pathlib import Path

        project_root = Path(__file__).parent.parent.parent

        # Run uv build in a temporary directory to avoid conflicts
        with tempfile.TemporaryDirectory() as tmpdir:
            result = subprocess.run(
                ["uv", "build", "--out-dir", tmpdir], cwd=project_root, capture_output=True, text=True
            )

            # Build should succeed (exit code 0)
            if result.returncode != 0:
                pytest.fail(f"Package build failed: {result.stderr}")

            # Should create both wheel and source distribution
            built_files = list(Path(tmpdir).glob("*"))
            wheel_files = [f for f in built_files if f.suffix == ".whl"]
            sdist_files = [f for f in built_files if f.suffix == ".gz"]

            assert len(wheel_files) > 0, "Build should create wheel file"
            assert len(sdist_files) > 0, "Build should create source distribution"

    def test_cli_entry_point_works(self):
        """Test that CLI entry point is properly configured."""
        import subprocess
        from pathlib import Path

        project_root = Path(__file__).parent.parent.parent

        # Test that benchbox command works
        result = subprocess.run(["uv", "run", "benchbox", "--help"], cwd=project_root, capture_output=True, text=True)

        assert result.returncode == 0, "CLI entry point should work"
        assert "BenchBox" in result.stdout
        assert "database benchmark" in result.stdout.lower()

    def test_no_incorrect_repository_references(self):
        """Test that no files contain incorrect repository references."""
        from pathlib import Path

        project_root = Path(__file__).parent.parent.parent

        # Directories to search (explicitly avoid large cache/build directories)
        search_dirs = [
            "benchbox",
            "tests",
            "docs",
            ".github",
        ]

        # Files to search in project root
        root_files = [
            "README.md",
            "CHANGELOG.md",
            "pyproject.toml",
            "setup.py",
            "setup.cfg",
        ]

        problematic_files = []

        # Search in specified directories
        for search_dir in search_dirs:
            dir_path = project_root / search_dir
            if not dir_path.exists():
                continue

            for file_path in dir_path.rglob("*"):
                if not file_path.is_file():
                    continue

                # Skip binary and cache files
                if (
                    any(skip in str(file_path) for skip in ["__pycache__", ".egg-info", ".pytest_cache", ".mypy_cache"])
                    or file_path.suffix == ".pyc"
                ):
                    continue

                # Skip this test file itself
                if file_path.name == "test_release_infrastructure.py":
                    continue

                try:
                    content = file_path.read_text(encoding="utf-8", errors="ignore")
                    if "anthropics/claude-code" in content:
                        problematic_files.append(str(file_path.relative_to(project_root)))
                except (UnicodeDecodeError, OSError):
                    # Skip files that can't be read as text
                    pass

        # Search root files
        for root_file in root_files:
            file_path = project_root / root_file
            if not file_path.exists():
                continue

            try:
                content = file_path.read_text(encoding="utf-8")
                if "anthropics/claude-code" in content:
                    problematic_files.append(root_file)
            except (UnicodeDecodeError, OSError):
                pass

        # Filter out acceptable references
        problematic_files = [
            f for f in problematic_files if not any(acceptable in f for acceptable in ["_project/PROJECT_TODO.md"])
        ]

        if problematic_files:
            pytest.fail(f"Found incorrect repository references in: {problematic_files}")


class TestVersionConsistency:
    """Test version consistency across different files."""

    def test_version_in_pyproject_toml(self):
        """Test that version is properly defined in pyproject.toml."""
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        version = config["project"]["version"]
        assert version is not None
        assert isinstance(version, str)
        assert len(version) > 0

        # Should be semantic version format
        import re

        semver_pattern = r"^\d+\.\d+\.\d+(-\w+)?$"
        assert re.match(semver_pattern, version), f"Version {version} should follow semantic versioning"

    def test_version_in_init_file(self):
        """Test that version is defined in __init__.py."""
        init_path = Path(__file__).parent.parent.parent / "benchbox" / "__init__.py"

        with open(init_path, encoding="utf-8") as f:
            content = f.read()

        assert "__version__" in content, "__init__.py should define __version__"

    def test_version_consistency(self):
        """Test that version is consistent between pyproject.toml and __init__.py."""
        # Get version from pyproject.toml
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"
        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)
        pyproject_version = config["project"]["version"]

        # Get version from __init__.py
        init_path = Path(__file__).parent.parent.parent / "benchbox" / "__init__.py"
        with open(init_path, encoding="utf-8") as f:
            content = f.read()

        # Extract version from __init__.py
        import re

        version_match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
        assert version_match, "Could not find __version__ in __init__.py"
        init_version = version_match.group(1)

        assert pyproject_version == init_version, (
            f"Version mismatch: pyproject.toml has {pyproject_version}, __init__.py has {init_version}"
        )


class TestReleaseWorkflowValidation:
    """Test that release workflows are valid YAML and properly structured."""

    def test_workflows_are_valid_yaml(self):
        """Test that all workflow files are valid YAML."""
        workflows_dir = Path(__file__).parent.parent.parent / ".github" / "workflows"

        for workflow_file in workflows_dir.glob("*.yml"):
            with open(workflow_file, encoding="utf-8") as f:
                try:
                    yaml.safe_load(f)
                except yaml.YAMLError as e:
                    pytest.fail(f"Invalid YAML in {workflow_file}: {e}")

    def test_issue_templates_are_valid_yaml(self):
        """Test that issue templates are valid YAML."""
        templates_dir = Path(__file__).parent.parent.parent / ".github" / "ISSUE_TEMPLATE"

        for template_file in templates_dir.glob("*.yml"):
            with open(template_file, encoding="utf-8") as f:
                try:
                    yaml.safe_load(f)
                except yaml.YAMLError as e:
                    pytest.fail(f"Invalid YAML in {template_file}: {e}")


class TestPackageMetadata:
    """Test package metadata and configuration."""

    def test_license_file_exists(self):
        """Test that LICENSE file exists."""
        license_path = Path(__file__).parent.parent.parent / "LICENSE"
        assert license_path.exists(), "LICENSE file must exist"

    def test_readme_file_exists(self):
        """Test that README.md exists."""
        readme_path = Path(__file__).parent.parent.parent / "README.md"
        assert readme_path.exists(), "README.md file must exist"

    def test_pyproject_toml_build_config(self):
        """Test that pyproject.toml has proper build configuration."""
        pyproject_path = Path(__file__).parent.parent.parent / "pyproject.toml"

        with open(pyproject_path, "rb") as f:
            config = tomllib.load(f)

        # Should have build system configuration
        assert "build-system" in config
        build_system = config["build-system"]
        assert "requires" in build_system
        assert "build-backend" in build_system

        # Should have entry points
        assert "project" in config
        project = config["project"]
        assert "scripts" in project
        assert "benchbox" in project["scripts"]
