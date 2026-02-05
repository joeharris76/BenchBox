import pytest

from benchbox.utils import runtime_env
from benchbox.utils.runtime_env import DriverResolution, ensure_driver_version

pytestmark = pytest.mark.fast


def test_ensure_driver_version_matches(monkeypatch):
    monkeypatch.setattr(runtime_env, "_get_installed_version", lambda pkg: "1.2.3")
    resolution = ensure_driver_version(
        package_name="duckdb",
        requested_version="1.2.3",
        auto_install=False,
    )
    assert resolution == DriverResolution(
        package="duckdb",
        requested="1.2.3",
        resolved="1.2.3",
        auto_install_used=False,
    )


def test_ensure_driver_version_mismatch_without_auto_install(monkeypatch):
    monkeypatch.setattr(runtime_env, "_get_installed_version", lambda pkg: "1.2.3")
    with pytest.raises(RuntimeError) as excinfo:
        ensure_driver_version(
            package_name="duckdb",
            requested_version="2.0.0",
            auto_install=False,
        )
    assert "duckdb" in str(excinfo.value)
    assert "2.0.0" in str(excinfo.value)


def test_ensure_driver_version_autoinstall(monkeypatch):
    installed_version = {"value": None}

    def fake_get_version(_):
        return installed_version["value"]

    def fake_install(_command):
        installed_version["value"] = "2.0.0"

    monkeypatch.setattr(runtime_env, "_get_installed_version", fake_get_version)
    monkeypatch.setattr(runtime_env, "_run_install_command", fake_install)
    monkeypatch.setattr(runtime_env, "_should_auto_install", lambda flag: True)

    resolution = ensure_driver_version(
        package_name="duckdb",
        requested_version="2.0.0",
        auto_install=False,
    )
    assert resolution.package == "duckdb"
    assert resolution.requested == "2.0.0"
    assert resolution.resolved == "2.0.0"
    assert resolution.auto_install_used is True
