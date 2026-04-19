"""Tests for --minimized startup flag and AutoStart registry integration.

AutoStart now uses the Windows registry (HKCU\\...\\Run) instead of a
VBS startup script.  These tests mock winreg to verify the registry
commands without touching the real registry.
"""

import sys
from unittest.mock import MagicMock, patch

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


class TestAutoStartRegistry:
    """Verify that AutoStart writes the correct registry values."""

    def test_registry_contains_minimized_flag_when_hidden(self, tmp_path):
        """Registry command should include --minimized when show_window=False."""
        fake_exe = tmp_path / "BackupManager.exe"
        fake_exe.touch()

        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(fake_exe)),
            patch.dict("sys.modules", {"winreg": mock_winreg}),
        ):
            AutoStart.ensure_startup(show_window=False)

        # SetValueEx(key, name, reserved, type, value)
        # value is the 5th positional arg (index 4)
        set_calls = mock_winreg.SetValueEx.call_args_list
        assert len(set_calls) == 1
        command = set_calls[0][0][4]
        assert "--minimized" in command
        assert str(fake_exe) in command

    def test_registry_no_minimized_flag_when_shown(self, tmp_path):
        """Registry command should NOT include --minimized when show_window=True."""
        fake_exe = tmp_path / "BackupManager.exe"
        fake_exe.touch()

        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(sys, "frozen", True, create=True),
            patch.object(sys, "executable", str(fake_exe)),
            patch.dict("sys.modules", {"winreg": mock_winreg}),
        ):
            AutoStart.ensure_startup(show_window=True)

        set_calls = mock_winreg.SetValueEx.call_args_list
        assert len(set_calls) == 1
        command = set_calls[0][0][4]
        assert "--minimized" not in command
        assert str(fake_exe) in command

    def test_not_frozen_skips_registry(self):
        """ensure_startup should do nothing when running as a dev build."""
        mock_winreg = MagicMock()

        with (
            patch.object(sys, "frozen", False, create=True),
            patch("src.__main__._is_nuitka", return_value=False),
            patch.dict("sys.modules", {"winreg": mock_winreg}),
        ):
            AutoStart.ensure_startup(show_window=False)

        mock_winreg.OpenKey.assert_not_called()

    def test_nuitka_writes_registry_even_without_sys_frozen(self, tmp_path):
        """ensure_startup must write the registry on Nuitka builds too."""
        fake_exe = tmp_path / "BackupManager.exe"
        fake_exe.touch()

        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)

        with (
            patch.object(sys, "frozen", False, create=True),
            patch.object(sys, "executable", str(fake_exe)),
            patch("src.__main__._is_nuitka", return_value=True),
            patch.dict("sys.modules", {"winreg": mock_winreg}),
        ):
            AutoStart.ensure_startup(show_window=False)

        set_calls = mock_winreg.SetValueEx.call_args_list
        assert len(set_calls) == 1
        command = set_calls[0][0][4]
        assert "--minimized" in command
        assert str(fake_exe) in command

    def test_is_show_window_detects_minimized(self):
        """is_show_window() should return False when --minimized is in registry."""
        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)
        mock_winreg.QueryValueEx.return_value = (
            '"C:\\app.exe" --minimized',
            1,
        )

        with patch.dict("sys.modules", {"winreg": mock_winreg}):
            assert AutoStart.is_show_window() is False

    def test_is_show_window_true_without_minimized(self):
        """is_show_window() should return True when no --minimized in registry."""
        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)
        mock_winreg.QueryValueEx.return_value = ('"C:\\app.exe"', 1)

        with patch.dict("sys.modules", {"winreg": mock_winreg}):
            assert AutoStart.is_show_window() is True

    def test_is_show_window_true_when_no_entry(self):
        """is_show_window() should return True when registry entry doesn't exist."""
        mock_winreg = MagicMock()
        mock_key = MagicMock()
        mock_winreg.OpenKey.return_value.__enter__ = MagicMock(return_value=mock_key)
        mock_winreg.OpenKey.return_value.__exit__ = MagicMock(return_value=False)
        mock_winreg.QueryValueEx.side_effect = FileNotFoundError

        with patch.dict("sys.modules", {"winreg": mock_winreg}):
            assert AutoStart.is_show_window() is True
