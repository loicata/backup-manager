"""Tests for AutoStart — Windows startup VBS script management.

Validates creation, removal, and state queries of the VBS startup
script used to launch Backup Manager at Windows login.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from src.core.scheduler import AutoStart


@pytest.fixture
def fake_startup_dir(tmp_path):
    """Redirect AutoStart.STARTUP_DIR to a temp directory."""
    startup = tmp_path / "Startup"
    startup.mkdir()
    with patch.object(AutoStart, "STARTUP_DIR", startup):
        yield startup


class TestAutoStart:
    """Tests for VBS-based Windows auto-start management."""

    def test_is_enabled_false_when_no_vbs(self, fake_startup_dir):
        """is_enabled() returns False when no VBS script exists."""
        assert AutoStart.is_enabled() is False

    def test_ensure_startup_creates_vbs_for_frozen(self, fake_startup_dir):
        """ensure_startup() creates VBS file when running as frozen exe."""
        fake_exe = Path(r"C:\Program Files\BackupManager\BackupManager.exe")
        with patch.object(sys, "frozen", True, create=True), patch.object(
            sys, "executable", str(fake_exe)
        ):
            AutoStart.ensure_startup(show_window=True)

        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        assert vbs_path.exists()
        content = vbs_path.read_text(encoding="utf-8")
        assert str(fake_exe) in content
        assert "--minimized" not in content

    def test_ensure_startup_minimized_flag(self, fake_startup_dir):
        """ensure_startup(show_window=False) includes --minimized."""
        fake_exe = Path(r"C:\Program Files\BackupManager\BackupManager.exe")
        with patch.object(sys, "frozen", True, create=True), patch.object(
            sys, "executable", str(fake_exe)
        ):
            AutoStart.ensure_startup(show_window=False)

        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        content = vbs_path.read_text(encoding="utf-8")
        assert "--minimized" in content

    def test_ensure_startup_skips_non_frozen(self, fake_startup_dir):
        """ensure_startup() does nothing when not running as frozen exe."""
        with patch.object(sys, "frozen", False, create=True):
            AutoStart.ensure_startup(show_window=True)

        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        assert not vbs_path.exists()

    def test_is_enabled_true_after_ensure(self, fake_startup_dir):
        """is_enabled() returns True after ensure_startup()."""
        fake_exe = Path(r"C:\Program Files\BackupManager\BackupManager.exe")
        with patch.object(sys, "frozen", True, create=True), patch.object(
            sys, "executable", str(fake_exe)
        ):
            AutoStart.ensure_startup(show_window=True)

        assert AutoStart.is_enabled() is True

    def test_disable_removes_vbs(self, fake_startup_dir):
        """disable() removes the VBS startup script."""
        # Create the VBS file first
        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        vbs_path.write_text("dummy", encoding="utf-8")

        ok, msg = AutoStart.disable()
        assert ok is True
        assert not vbs_path.exists()

    def test_disable_when_not_enabled(self, fake_startup_dir):
        """disable() returns success when VBS doesn't exist."""
        ok, msg = AutoStart.disable()
        assert ok is True
        assert "not enabled" in msg

    def test_is_show_window_true_no_minimized(self, fake_startup_dir):
        """is_show_window() returns True when --minimized is absent."""
        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        vbs_path.write_text(
            'WshShell.Run """C:\\app.exe""", 1, False\n',
            encoding="utf-8",
        )
        assert AutoStart.is_show_window() is True

    def test_is_show_window_false_with_minimized(self, fake_startup_dir):
        """is_show_window() returns False when --minimized is present."""
        vbs_path = fake_startup_dir / AutoStart.VBS_FILENAME
        vbs_path.write_text(
            'WshShell.Run """C:\\app.exe"" --minimized", 0, False\n',
            encoding="utf-8",
        )
        assert AutoStart.is_show_window() is False

    def test_is_show_window_default_when_no_vbs(self, fake_startup_dir):
        """is_show_window() returns True as default when no VBS exists."""
        assert AutoStart.is_show_window() is True

    def test_ensure_startup_overwrites_existing(self, fake_startup_dir):
        """ensure_startup() updates existing VBS with new settings."""
        fake_exe = Path(r"C:\Program Files\BackupManager\BackupManager.exe")
        with patch.object(sys, "frozen", True, create=True), patch.object(
            sys, "executable", str(fake_exe)
        ):
            # First: create with show_window=True
            AutoStart.ensure_startup(show_window=True)
            assert AutoStart.is_show_window() is True

            # Update to minimized
            AutoStart.ensure_startup(show_window=False)
            assert AutoStart.is_show_window() is False
