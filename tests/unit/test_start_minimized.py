"""Tests for --minimized startup flag and AutoStart VBS generation."""

import sys
from unittest.mock import patch

from src.core.scheduler import AutoStart


class TestStartMinimizedFlag:
    """Verify that --minimized flag is detected from sys.argv."""

    def test_minimized_flag_detected(self):
        """--minimized in argv should be detected."""
        with patch.object(sys, "argv", ["backup_manager", "--minimized"]):
            assert "--minimized" in sys.argv

    def test_no_flag_means_normal_start(self):
        """No --minimized flag should default to normal window."""
        with patch.object(sys, "argv", ["backup_manager"]):
            assert "--minimized" not in sys.argv

    def test_flag_among_other_args(self):
        """--minimized should be detected even with other arguments."""
        with patch.object(sys, "argv", ["backup_manager", "--other", "--minimized", "--debug"]):
            assert "--minimized" in sys.argv


class TestAutoStartMinimizedVBS:
    """Verify that AutoStart generates correct VBS for minimized mode."""

    def test_vbs_contains_minimized_flag_when_hidden(self, tmp_path):
        """VBS script should include --minimized when show_window=False."""
        fake_exe = tmp_path / "BackupManager.exe"
        fake_exe.touch()

        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(fake_exe)),
            patch.object(AutoStart, "STARTUP_DIR", tmp_path),
        ):
            AutoStart.ensure_startup(show_window=False)

        vbs_path = tmp_path / AutoStart.VBS_FILENAME
        content = vbs_path.read_text(encoding="utf-8")
        assert "--minimized" in content
        # Window style should be 0 (hidden)
        assert ", 0, False" in content

    def test_vbs_no_minimized_flag_when_shown(self, tmp_path):
        """VBS script should NOT include --minimized when show_window=True."""
        fake_exe = tmp_path / "BackupManager.exe"
        fake_exe.touch()

        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(fake_exe)),
            patch.object(AutoStart, "STARTUP_DIR", tmp_path),
        ):
            AutoStart.ensure_startup(show_window=True)

        vbs_path = tmp_path / AutoStart.VBS_FILENAME
        content = vbs_path.read_text(encoding="utf-8")
        assert "--minimized" not in content
        # Window style should be 1 (normal)
        assert ", 1, False" in content

    def test_is_show_window_detects_minimized(self, tmp_path):
        """is_show_window() should return False when --minimized is in VBS."""
        vbs_path = tmp_path / AutoStart.VBS_FILENAME
        vbs_path.write_text(
            'WshShell.Run """C:\\app.exe"" --minimized", 0, False',
            encoding="utf-8",
        )

        with patch.object(AutoStart, "STARTUP_DIR", tmp_path):
            assert AutoStart.is_show_window() is False

    def test_is_show_window_true_without_minimized(self, tmp_path):
        """is_show_window() should return True when no --minimized in VBS."""
        vbs_path = tmp_path / AutoStart.VBS_FILENAME
        vbs_path.write_text(
            'WshShell.Run """C:\\app.exe""", 1, False',
            encoding="utf-8",
        )

        with patch.object(AutoStart, "STARTUP_DIR", tmp_path):
            assert AutoStart.is_show_window() is True

    def test_is_show_window_true_when_no_vbs(self, tmp_path):
        """is_show_window() should return True when no VBS file exists."""
        with patch.object(AutoStart, "STARTUP_DIR", tmp_path):
            assert AutoStart.is_show_window() is True
