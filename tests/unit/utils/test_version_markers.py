import importlib

import pytest

pytestmark = pytest.mark.fast


def test_validate_version_consistency_succeeds():
    version_module = importlib.import_module("benchbox.utils.version")
    # The helper raises when documentation/version markers drift.
    version_module.validate_version_consistency()
