"""Utilities for subprocess-based BenchBox CLI testing."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path

CLI_MODULE = "benchbox.cli.main"
DEFAULT_TIMEOUT = 120.0


def run_cli_command(
    args: Sequence[str],
    *,
    cwd: Path | str | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> subprocess.CompletedProcess[str]:
    """Execute the BenchBox CLI in a subprocess and capture its output."""
    command = [sys.executable, "-m", CLI_MODULE, *args]

    effective_env: MutableMapping[str, str] = os.environ.copy()
    # Force UTF-8 encoding on Windows to handle emoji characters
    effective_env["PYTHONUTF8"] = "1"
    if env:
        effective_env.update(env)

    resolved_cwd = str(cwd) if cwd is not None else None

    return subprocess.run(
        command,
        cwd=resolved_cwd,
        env=effective_env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        check=False,
    )
