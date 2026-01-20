#!/usr/bin/env python3
"""Bidirectional sync between private (BenchBox) and public (BenchBox-public) repositories.

This is a convenience wrapper script. The actual implementation is in
benchbox.release.sync. You can also use the installed entry point:

    benchbox-sync status
    benchbox-sync push --message "Sync bug fixes"
    benchbox-sync pull
"""

from __future__ import annotations

import sys


def main() -> int:
    """Run the sync CLI."""
    try:
        from benchbox.release.sync import main as sync_main

        return sync_main()
    except ImportError:
        # Fall back to sys.path manipulation if package not installed
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).parent.parent))
        from benchbox.release.sync import main as sync_main

        return sync_main()


if __name__ == "__main__":
    sys.exit(main())
