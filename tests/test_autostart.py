"""Tests for AutoStart — Windows registry-based auto-start management.

Validates creation, removal, and state queries of the HKCU\\...\\Run
registry entry used to launch Backup Manager at Windows login.
Also covers legacy VBS cleanup during migration.
"""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from src.core.scheduler import AutoStart

# ---------------------------------------------------------------------------
# Fake winreg helpers
# ---------------------------------------------------------------------------

_FAKE_STORE: dict[str, str] = {}
"""In-memory registry store keyed by value name."""


class _FakeKey:
    """Context-manager compatible fake registry key."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


def _fake_open_key(_root, _subkey, _reserved=0, _access=0):
    return _FakeKey()


def _fake_set_value_ex(_key, name, _reserved, _type, value):
    _FAKE_STORE[name] = value


def _fake_query_value_ex(_key, name):
    if name not in _FAKE_STORE:
        raise FileNotFoundError(f"Registry value not found: {name}")
    return _FAKE_STORE[name], 1  # (value, type)


def _fake_delete_value(_key, name):
    if name not in _FAKE_STORE:
        raise FileNotFoundError(f"Registry value not found: {name}")
    del _FAKE_STORE[name]


@pytest.fixture(autouse=True)
def _reset_store():
    """Clear fake registry store before each test."""
    _FAKE_STORE.clear()
    yield
    _FAKE_STORE.clear()


@pytest.fixture
def fake_winreg():
    """Patch winreg with in-memory fakes."""
    mod = SimpleNamespace(
        HKEY_CURRENT_USER=0x80000001,
        KEY_SET_VALUE=0x0002,
        KEY_READ=0x20019,
        REG_SZ=1,
        OpenKey=_fake_open_key,
        SetValueEx=_fake_set_value_ex,
        QueryValueEx=_fake_query_value_ex,
        DeleteValue=_fake_delete_value,
    )
    with patch.dict("sys.modules", {"winreg": mod}):
        yield mod


@pytest.fixture
def fake_legacy_vbs(tmp_path):
    """Create a fake legacy VBS file and point AutoStart to it."""
    vbs = tmp_path / "BackupManager.vbs"
    vbs.write_text("legacy vbs content", encoding="utf-8")
    with patch.object(AutoStart, "_LEGACY_VBS", vbs):
        yield vbs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

FAKE_EXE = Path(r"C:\Program Files\BackupManager\BackupManager.exe")


class TestAutoStart:
    """Tests for registry-based Windows auto-start management."""

    def test_is_enabled_false_when_no_key(self, fake_winreg):
        """is_enabled() returns False when registry value is absent."""
        assert AutoStart.is_enabled() is False

    def test_ensure_startup_creates_registry_key(self, fake_winreg):
        """ensure_startup() writes registry value for frozen exe."""
        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(FAKE_EXE)),
            patch.object(AutoStart, "_cleanup_legacy_vbs"),
        ):
            AutoStart.ensure_startup(show_window=True)

        assert AutoStart._REG_VALUE in _FAKE_STORE
        assert str(FAKE_EXE) in _FAKE_STORE[AutoStart._REG_VALUE]
        assert "--minimized" not in _FAKE_STORE[AutoStart._REG_VALUE]

    def test_ensure_startup_minimized_flag(self, fake_winreg):
        """ensure_startup(show_window=False) includes --minimized."""
        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(FAKE_EXE)),
            patch.object(AutoStart, "_cleanup_legacy_vbs"),
        ):
            AutoStart.ensure_startup(show_window=False)

        assert "--minimized" in _FAKE_STORE[AutoStart._REG_VALUE]

    def test_ensure_startup_skips_non_frozen(self, fake_winreg):
        """ensure_startup() does nothing when not running as frozen exe."""
        with patch.object(sys, "frozen", False, create=True):
            AutoStart.ensure_startup(show_window=True)

        assert AutoStart._REG_VALUE not in _FAKE_STORE

    def test_is_enabled_true_after_ensure(self, fake_winreg):
        """is_enabled() returns True after ensure_startup()."""
        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(FAKE_EXE)),
            patch.object(AutoStart, "_cleanup_legacy_vbs"),
        ):
            AutoStart.ensure_startup(show_window=True)

        assert AutoStart.is_enabled() is True

    def test_disable_removes_key(self, fake_winreg):
        """disable() removes the registry value."""
        _FAKE_STORE[AutoStart._REG_VALUE] = f'"{FAKE_EXE}"'

        with patch.object(AutoStart, "_cleanup_legacy_vbs"):
            ok, msg = AutoStart.disable()

        assert ok is True
        assert "disabled" in msg
        assert AutoStart._REG_VALUE not in _FAKE_STORE

    def test_disable_when_not_enabled(self, fake_winreg):
        """disable() returns success when registry value doesn't exist."""
        with patch.object(AutoStart, "_cleanup_legacy_vbs"):
            ok, msg = AutoStart.disable()

        assert ok is True
        assert "not enabled" in msg

    def test_is_show_window_true_no_minimized(self, fake_winreg):
        """is_show_window() returns True when --minimized is absent."""
        _FAKE_STORE[AutoStart._REG_VALUE] = f'"{FAKE_EXE}"'
        assert AutoStart.is_show_window() is True

    def test_is_show_window_false_with_minimized(self, fake_winreg):
        """is_show_window() returns False when --minimized is present."""
        _FAKE_STORE[AutoStart._REG_VALUE] = f'"{FAKE_EXE}" --minimized'
        assert AutoStart.is_show_window() is False

    def test_is_show_window_default_when_no_key(self, fake_winreg):
        """is_show_window() returns True as default when no key exists."""
        assert AutoStart.is_show_window() is True

    def test_ensure_startup_overwrites_existing(self, fake_winreg):
        """ensure_startup() updates existing registry value."""
        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(FAKE_EXE)),
            patch.object(AutoStart, "_cleanup_legacy_vbs"),
        ):
            AutoStart.ensure_startup(show_window=True)
            assert AutoStart.is_show_window() is True

            AutoStart.ensure_startup(show_window=False)
            assert AutoStart.is_show_window() is False

    def test_ensure_startup_cleans_old_vbs(self, fake_winreg, fake_legacy_vbs):
        """ensure_startup() removes legacy VBS if present."""
        assert fake_legacy_vbs.exists()
        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(FAKE_EXE)),
        ):
            AutoStart.ensure_startup(show_window=True)

        assert not fake_legacy_vbs.exists()

    def test_disable_cleans_old_vbs(self, fake_winreg, fake_legacy_vbs):
        """disable() removes legacy VBS if present."""
        _FAKE_STORE[AutoStart._REG_VALUE] = f'"{FAKE_EXE}"'
        assert fake_legacy_vbs.exists()

        ok, _ = AutoStart.disable()

        assert ok is True
        assert not fake_legacy_vbs.exists()

    def test_cleanup_legacy_vbs_noop_when_absent(self, tmp_path):
        """_cleanup_legacy_vbs() does nothing when VBS doesn't exist."""
        absent = tmp_path / "nonexistent.vbs"
        with patch.object(AutoStart, "_LEGACY_VBS", absent):
            AutoStart._cleanup_legacy_vbs()  # Should not raise
