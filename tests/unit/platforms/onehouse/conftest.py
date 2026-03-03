"""Shared fixtures for Onehouse platform adapter tests."""

from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def onehouse_adapter_dependency_mocks():
    """Patch Onehouse adapter dependency gates and SDK modules for unit tests."""
    with ExitStack() as stack:
        stack.enter_context(patch("benchbox.platforms.onehouse.quanton_adapter.BOTO3_AVAILABLE", True))
        stack.enter_context(patch("benchbox.platforms.onehouse.quanton_adapter.boto3", MagicMock()))
        yield


@pytest.fixture(autouse=True)
def _auto_patch_onehouse_adapter_dependencies(onehouse_adapter_dependency_mocks):
    yield onehouse_adapter_dependency_mocks
