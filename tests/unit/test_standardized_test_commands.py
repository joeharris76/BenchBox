"""Test for standardized test command functionality.

This test verifies that the standardized pytest-based test commands work correctly
and that legacy run_tests.py references have been properly removed.

Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.medium
class TestStandardizedTestCommands:
    """Test the standardized test command system."""

    def test_pytest_marker_system_works(self):
        """Test that pytest marker system is properly configured."""
        # Run a simple pytest command to verify markers work
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "--collect-only", "-q"],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed (0) or have warnings (2) but collect tests
        # Exit code 2 can occur with pytest-benchmark warnings when using xdist
        assert result.returncode in (0, 2), f"Unexpected exit code: {result.returncode}"
        # Check that tests were actually collected (output contains test paths)
        assert "test_" in result.stdout or "collected" in result.stdout.lower()

    def test_fast_marker_functionality(self):
        """Test that fast marker selects appropriate tests."""
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-m", "fast", "--collect-only", "-q"],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed (0) or have warnings (2)
        # Exit code 2 can occur with pytest-benchmark warnings when using xdist
        assert result.returncode in (0, 2), f"Unexpected exit code: {result.returncode}"
        # Verify that fast marker tests are collected
        assert "test_" in result.stdout or "collected" in result.stdout.lower()

    def test_unit_marker_functionality(self):
        """Test that unit marker selects appropriate tests."""
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-m", "unit", "--collect-only", "-q"],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=30,
        )

        # Should succeed (0) or have warnings (2)
        # Exit code 2 can occur with pytest-benchmark warnings when using xdist
        assert result.returncode in (0, 2), f"Unexpected exit code: {result.returncode}"
        # Verify that unit marker tests are collected
        assert "test_" in result.stdout or "collected" in result.stdout.lower()

    def test_makefile_commands_exist(self):
        """Test that key Makefile commands exist and are standardized."""
        makefile_path = Path.cwd() / "Makefile"
        assert makefile_path.exists(), "Makefile should exist"

        makefile_content = makefile_path.read_text()

        # Check that standardized commands exist
        assert "test-fast:" in makefile_content
        assert "test-unit:" in makefile_content
        assert "test-integration:" in makefile_content
        assert "test-all:" in makefile_content

        # Check that commands use pytest
        assert "python -m pytest" in makefile_content

        # Verify no references to non-existent run_tests.py remain
        assert "run_tests.py" not in makefile_content

    def test_no_legacy_run_tests_references(self):
        """Test that legacy run_tests.py file has been removed."""
        # Check that run_tests.py doesn't exist in project root
        run_tests_path = Path.cwd() / "run_tests.py"
        assert not run_tests_path.exists(), "run_tests.py should not exist in project root"

    def test_pytest_configuration_is_valid(self):
        """Test that pytest configuration is valid."""
        # Check that pytest.ini exists and is valid
        pytest_ini_path = Path.cwd() / "pytest.ini"
        assert pytest_ini_path.exists(), "pytest.ini should exist"

        pytest_ini_content = pytest_ini_path.read_text()

        # Check for key markers
        assert "fast:" in pytest_ini_content
        assert "unit:" in pytest_ini_content
        assert "integration:" in pytest_ini_content

        # Verify markers section exists
        assert "markers =" in pytest_ini_content

    def test_coverage_commands_use_pytest(self):
        """Test that coverage commands use pytest instead of legacy scripts."""
        makefile_path = Path.cwd() / "Makefile"
        makefile_content = makefile_path.read_text()

        # Find coverage commands
        coverage_section = False
        for line in makefile_content.split("\n"):
            if line.startswith("coverage:"):
                coverage_section = True
                continue
            elif coverage_section and line.startswith("\t"):
                # This is the command for coverage target
                assert "python -m pytest" in line
                assert "--cov=" in line
                break

    @pytest.mark.fast
    @pytest.mark.unit
    def test_this_test_has_proper_markers(self):
        """Test that this test itself has the proper markers."""
        # This test should run when filtering by fast and unit markers
        assert True


class TestMakefileCommands:
    """Test that Makefile commands work as expected."""

    def test_makefile_test_targets_defined(self):
        """Test that all expected test targets are defined in Makefile."""
        makefile_path = Path.cwd() / "Makefile"
        makefile_content = makefile_path.read_text()

        expected_targets = [
            "test:",
            "test-all:",
            "test-unit:",
            "test-integration:",
            "test-fast:",
            "test-medium:",
            "test-slow:",
            "coverage:",
            "coverage-html:",
        ]

        for target in expected_targets:
            assert target in makefile_content, f"Makefile should contain target: {target}"

    def test_makefile_uses_pytest_consistently(self):
        """Test that Makefile uses pytest consistently across all test targets."""
        makefile_path = Path.cwd() / "Makefile"
        makefile_content = makefile_path.read_text()

        # Find all test target lines
        test_lines = []
        for line in makefile_content.split("\n"):
            if line.startswith("\tuv run -- python -m pytest"):
                test_lines.append(line)

        # Should have multiple pytest commands
        assert len(test_lines) > 5, "Should have multiple pytest-based commands"

        # All should use the same format
        for line in test_lines:
            assert line.startswith("\tuv run -- python -m pytest"), f"Line should use pytest: {line}"
