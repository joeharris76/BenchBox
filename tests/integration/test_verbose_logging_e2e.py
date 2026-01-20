"""
Copyright 2026 Joe Harris / BenchBox Project

Licensed under the MIT License. See LICENSE file in the project root for details.
"""

import unittest

from tests.integration._cli_e2e_utils import run_cli_command


class TestVerboseLoggingE2E(unittest.TestCase):
    """End-to-end tests for the verbose logging system."""

    def _run_benchbox_command(self, args):
        """Helper to run a benchbox command and capture its output."""
        return run_cli_command(args)

    def test_e2e_no_verbosity(self):
        """Test a full run with no verbosity flags, expecting no verbose output."""
        # Using sqlite and a non-existent benchmark to run quickly and test logging
        # We expect an error, but we can still check the log output.
        args = ["run", "--platform", "sqlite", "--benchmark", "ssb", "--scale", "0.001"]
        result = self._run_benchbox_command(args)

        # Check for absence of verbose logs
        self.assertNotIn("INFO -", result.stderr)
        self.assertNotIn("DEBUG -", result.stderr)
        self.assertNotIn("Starting", result.stderr)
        self.assertNotIn("completed in", result.stderr)

    def test_e2e_level_one_verbosity(self):
        """Test a full run with -v, expecting INFO logs but not DEBUG logs."""
        args = ["run", "-v", "--platform", "sqlite", "--benchmark", "ssb", "--scale", "0.001"]
        result = self._run_benchbox_command(args)

        # Check for INFO logs and absence of DEBUG logs
        self.assertIn("INFO - Verbose logging enabled", result.stderr)
        self.assertIn("INFO - Starting direct benchmark execution", result.stderr)
        self.assertNotIn("DEBUG", result.stderr)

    def test_e2e_level_two_verbosity(self):
        """Test a full run with -vv, expecting both INFO and DEBUG logs."""
        args = ["run", "-vv", "--platform", "sqlite", "--benchmark", "ssb", "--scale", "0.001"]
        result = self._run_benchbox_command(args)

        # Check for both INFO and DEBUG logs
        self.assertIn("DEBUG - Very verbose logging enabled", result.stderr)
        self.assertIn("DEBUG - Starting BenchBox CLI run command", result.stderr)
        self.assertIn("INFO - Starting direct benchmark execution", result.stderr)


if __name__ == "__main__":
    unittest.main()
