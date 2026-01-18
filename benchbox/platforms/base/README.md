# Platform Base Package Layout

The platform base package provides shared building blocks for all runtime adapters.

## Module responsibilities

- `adapter.py`: Core `PlatformAdapter` abstract class along with connection and execution orchestration logic. This module deals with runtime behaviour only and delegates passive structures and helper routines to the modules below.
- `models.py`: Dataclasses describing configuration, setup phases, execution statistics, and maintenance results used across adapters. These structures are free of side effects and can be imported without pulling in the heavy adapter logic.
- `utils.py`: Lightweight helper functions such as environment detection and future manifest helpers that do not require the full adapter context.

## Refactor guidance

When extending the platform base package:

1. Prefer adding new passive data carriers to `models.py` so they can be reused without importing the adapter implementation.
2. Add pure helpers to `utils.py` to keep `adapter.py` focused on orchestration.
3. Keep the public API stable by re-exporting key symbols in `benchbox.platforms.base.__init__`.
4. Mirror this structure in tests under `tests/unit/platforms/base/` to ensure new modules have direct coverage.

This layout results from the "Platform Adapter Core Modularization" TODO and should remain the baseline for future contributions.
