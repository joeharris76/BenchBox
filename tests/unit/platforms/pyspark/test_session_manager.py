"""Unit tests for SparkSessionManager behavior."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from benchbox.platforms.pyspark import session as session_module
from benchbox.platforms.pyspark.session import SparkConfigurationError, SparkSessionManager

pytestmark = pytest.mark.fast


def _patch_classmethod(monkeypatch: pytest.MonkeyPatch, attr: str, func):
    """Helper to patch classmethods on SparkSessionManager."""
    monkeypatch.setattr(SparkSessionManager, attr, classmethod(func))


@pytest.fixture(autouse=True)
def reset_manager():
    """Ensure manager state is clean between tests."""
    SparkSessionManager.close()
    yield
    SparkSessionManager.close()


def test_get_or_create_returns_singleton(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify manager creates one session per process configuration."""
    fake_session = SimpleNamespace(stop=lambda: None)

    monkeypatch.setattr(session_module, "PYSPARK_AVAILABLE", True)
    monkeypatch.setattr(session_module, "_validate_java_version", lambda: None)
    _patch_classmethod(monkeypatch, "_create_session", lambda cls, config: fake_session)

    session = SparkSessionManager.get_or_create(
        master="local[*]",
        app_name="BenchBox-Tests",
        driver_memory="2g",
        executor_memory=None,
        shuffle_partitions=8,
        enable_aqe=True,
        extra_configs={"spark.sql.warehouse.dir": "/tmp/warehouse"},
    )

    assert session is fake_session
    assert SparkSessionManager._config is not None
    assert SparkSessionManager._config.master == "local[*]"
    assert SparkSessionManager._config.shuffle_partitions == 8

    SparkSessionManager.release()
    assert SparkSessionManager._session is None


def test_configuration_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify requesting a different configuration raises SparkConfigurationError."""
    fake_session = SimpleNamespace(stop=lambda: None)

    monkeypatch.setattr(session_module, "PYSPARK_AVAILABLE", True)
    monkeypatch.setattr(session_module, "_validate_java_version", lambda: None)
    _patch_classmethod(monkeypatch, "_create_session", lambda cls, config: fake_session)

    first = SparkSessionManager.get_or_create(
        master="local[2]",
        app_name="BenchBox-Tests",
        driver_memory="1g",
        executor_memory=None,
        shuffle_partitions=4,
        enable_aqe=True,
        extra_configs={},
    )
    assert first is fake_session

    with pytest.raises(SparkConfigurationError):
        SparkSessionManager.get_or_create(
            master="local[4]",
            app_name="BenchBox-Tests",
            driver_memory="1g",
            executor_memory=None,
            shuffle_partitions=4,
            enable_aqe=True,
            extra_configs={},
        )


def test_release_stops_session_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure release stops the shared session when refcount hits zero."""
    fake_session = SimpleNamespace()
    stop_called = {"count": 0}

    def fake_stop_session(cls):
        stop_called["count"] += 1
        cls._session = None

    monkeypatch.setattr(session_module, "PYSPARK_AVAILABLE", True)
    monkeypatch.setattr(session_module, "_validate_java_version", lambda: None)
    _patch_classmethod(monkeypatch, "_create_session", lambda cls, config: fake_session)
    _patch_classmethod(monkeypatch, "_stop_session", fake_stop_session)

    SparkSessionManager.get_or_create(
        master="local[*]",
        app_name="BenchBox",
        driver_memory="1g",
        executor_memory=None,
        shuffle_partitions=2,
        enable_aqe=False,
        extra_configs={},
    )
    SparkSessionManager.release()

    assert stop_called["count"] == 1
    assert SparkSessionManager._session is None


def test_java_home_auto_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure Java validation attempts to switch JAVA_HOME when unsupported."""
    monkeypatch.setattr(session_module, "PYSPARK_AVAILABLE", True)

    # Reset Java validation state to ensure fresh validation runs
    SparkSessionManager._java_validated = False

    # Simulate: first check returns 25 (incompatible), after switch returns 17 (compatible)
    call_count = {"n": 0}

    def fake_detect(java_home: str | None = None) -> int | None:
        call_count["n"] += 1
        # If java_home is set to our fake path, return compatible version
        if java_home == "/fake/jdk17":
            return 17
        # Otherwise return incompatible version
        return 25

    # Mock find_supported_java_home to return our fake Java installation
    def fake_find_supported():
        return ("/fake/jdk17", 17)

    monkeypatch.setattr(session_module, "detect_java_version", fake_detect)
    monkeypatch.setattr(session_module, "find_supported_java_home", fake_find_supported)

    fake_session = SimpleNamespace(stop=lambda: None)
    _patch_classmethod(monkeypatch, "_create_session", lambda cls, config: fake_session)

    # Clear JAVA_HOME to trigger auto-switch
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.delenv("BENCHBOX_JAVA_HOME", raising=False)

    SparkSessionManager.get_or_create(
        master="local[*]",
        app_name="BenchBox",
        driver_memory="1g",
        executor_memory=None,
        shuffle_partitions=2,
        enable_aqe=True,
        extra_configs={},
    )

    assert os.environ.get("JAVA_HOME") == "/fake/jdk17"
