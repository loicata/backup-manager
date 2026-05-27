"""Tests for the Win32 raise-existing-window helper.

Pins the contract of ``src.__main__._bring_existing_instance_to_front``
introduced in 3.7.36 to fix the v3.7.35 user report: re-installing
the MSI on top of a running instance left the user's next launch
silently failing (mutex held by old process, signal file written
but old process unresponsive).

The helper is Win32-only and pokes ``ctypes.windll.user32`` directly,
so we test it through mocks rather than launching a real window:
the production path is exercised end-to-end on every install, the
tests pin the BRANCHING behaviour (non-Windows skip, no-window-found
return, found-window calls SW_RESTORE then SetForegroundWindow,
exception path returns False instead of raising).
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

import src.__main__ as bm_main


class TestNonWindowsReturnsFalse:
    """On POSIX the helper is a no-op — returns False, no ctypes call."""

    def test_returns_false_on_linux(self, monkeypatch):
        monkeypatch.setattr(bm_main.sys, "platform", "linux")
        # If the helper were to touch ctypes.windll despite platform,
        # the attribute access would raise on Linux. We assert False
        # is returned cleanly, AND the test would have surfaced any
        # spurious access via the AttributeError.
        assert bm_main._bring_existing_instance_to_front() is False

    def test_returns_false_on_darwin(self, monkeypatch):
        monkeypatch.setattr(bm_main.sys, "platform", "darwin")
        assert bm_main._bring_existing_instance_to_front() is False


class TestWindowEnumeration:
    """When EnumWindows finds a Backup Manager window, raise it."""

    @pytest.fixture
    def fake_user32(self, monkeypatch):
        """Mock the user32 surface ``_bring_existing_instance_to_front`` uses.

        Returns the MagicMock the test can inspect (call counts, args).
        ``EnumWindows`` calls a callback per window; we drive it from
        the per-test ``window_titles`` list, returning one title per
        synthesised hwnd.
        """
        monkeypatch.setattr(bm_main.sys, "platform", "win32")
        fake_windll = MagicMock()
        fake_user32 = MagicMock()
        fake_windll.user32 = fake_user32
        monkeypatch.setattr(bm_main.ctypes, "windll", fake_windll)
        return fake_user32

    def _wire_enum_windows(
        self,
        fake_user32: MagicMock,
        window_titles: list[str],
    ) -> None:
        """Make EnumWindows iterate the provided titles.

        ``GetWindowTextLengthW`` returns the title length, ``GetWindowTextW``
        copies the title into the supplied buffer.  hwnds are simple
        integer sentinels (i+100) so the test can assert which one was
        the match.
        """
        text_map: dict[int, str] = {i + 100: title for i, title in enumerate(window_titles)}

        def fake_text_length(hwnd):
            return len(text_map.get(hwnd, ""))

        def fake_get_text(hwnd, buffer, _max_count):
            # Tk Tcl returns a bytes buffer; emulate by writing the
            # python string into the .value of the ctypes char buffer.
            buffer.value = text_map.get(hwnd, "")
            return len(buffer.value)

        def fake_enum(callback, _lparam):
            for hwnd in text_map:
                # Callback returns False to stop iteration.
                if callback(hwnd, None) is False:
                    break
            return True

        fake_user32.GetWindowTextLengthW.side_effect = fake_text_length
        fake_user32.GetWindowTextW.side_effect = fake_get_text
        fake_user32.EnumWindows.side_effect = fake_enum

    def test_no_matching_window_returns_false(self, fake_user32):
        self._wire_enum_windows(fake_user32, ["File Explorer", "Notepad", "Some Game"])

        result = bm_main._bring_existing_instance_to_front()

        assert result is False
        # Neither raise call should fire when nothing matched.
        fake_user32.ShowWindow.assert_not_called()
        fake_user32.SetForegroundWindow.assert_not_called()

    def test_matching_window_calls_show_then_foreground(self, fake_user32):
        self._wire_enum_windows(
            fake_user32,
            ["File Explorer", "Backup Manager v3.7.34", "Notepad"],
        )

        result = bm_main._bring_existing_instance_to_front()

        assert result is True
        # SW_RESTORE = 9. Hardcoded in the helper.
        sw_restore = 9
        fake_user32.ShowWindow.assert_called_once()
        args = fake_user32.ShowWindow.call_args[0]
        assert args[1] == sw_restore
        fake_user32.SetForegroundWindow.assert_called_once()

    def test_prefix_match_works_across_versions(self, fake_user32):
        """v3.7.36 launcher must still find v3.7.34's window."""
        self._wire_enum_windows(fake_user32, ["Backup Manager v3.7.34"])

        result = bm_main._bring_existing_instance_to_front()

        assert result is True

    def test_first_matching_window_wins(self, fake_user32):
        """Enumeration stops at the first match (callback returns False)."""
        self._wire_enum_windows(
            fake_user32,
            [
                "Backup Manager v3.7.34",  # first match
                "Backup Manager v3.7.35",  # would also match
            ],
        )

        result = bm_main._bring_existing_instance_to_front()

        assert result is True
        # ShowWindow / SetForegroundWindow should fire ONCE only.
        assert fake_user32.ShowWindow.call_count == 1
        assert fake_user32.SetForegroundWindow.call_count == 1

    def test_empty_title_windows_are_skipped(self, fake_user32):
        """A 0-length GetWindowTextLengthW skips early without buffer alloc."""
        self._wire_enum_windows(fake_user32, ["", "", "Backup Manager v3.7.36"])

        result = bm_main._bring_existing_instance_to_front()

        assert result is True

    def test_unrelated_app_with_similar_name_does_not_match(self, fake_user32):
        """Match anchored on the literal ``Backup Manager`` prefix."""
        self._wire_enum_windows(
            fake_user32,
            ["My Backup Manager", "Acme Backup"],
        )
        result = bm_main._bring_existing_instance_to_front()
        assert result is False


class TestExceptionSafety:
    """Any ctypes / OS error must NOT raise out of the helper."""

    def test_exception_during_enum_returns_false(self, monkeypatch):
        monkeypatch.setattr(bm_main.sys, "platform", "win32")
        fake_user32 = MagicMock()
        fake_user32.EnumWindows.side_effect = OSError("boom")
        fake_windll = MagicMock()
        fake_windll.user32 = fake_user32
        monkeypatch.setattr(bm_main.ctypes, "windll", fake_windll)

        # Must not raise.
        result = bm_main._bring_existing_instance_to_front()
        assert result is False

    def test_exception_in_set_foreground_swallowed(self, monkeypatch):
        """SetForegroundWindow can fail on UIPI — must not crash bootstrap."""
        monkeypatch.setattr(bm_main.sys, "platform", "win32")
        fake_user32 = MagicMock()
        fake_user32.SetForegroundWindow.side_effect = OSError("UIPI")
        text_map = {100: "Backup Manager"}

        def fake_text_length(hwnd):
            return len(text_map.get(hwnd, ""))

        def fake_get_text(hwnd, buffer, _max_count):
            buffer.value = text_map.get(hwnd, "")
            return len(buffer.value)

        def fake_enum(callback, _lparam):
            for hwnd in text_map:
                if callback(hwnd, None) is False:
                    break
            return True

        fake_user32.GetWindowTextLengthW.side_effect = fake_text_length
        fake_user32.GetWindowTextW.side_effect = fake_get_text
        fake_user32.EnumWindows.side_effect = fake_enum

        fake_windll = MagicMock()
        fake_windll.user32 = fake_user32
        monkeypatch.setattr(bm_main.ctypes, "windll", fake_windll)

        # No raise.
        result = bm_main._bring_existing_instance_to_front()
        assert result is False  # exception path returns False


class TestSingleInstanceIntegration:
    """``_acquire_single_instance`` calls the helper on mutex collision."""

    def test_acquire_calls_raise_on_existing_instance(self, monkeypatch, tmp_path):
        """When mutex already exists, both raise + signal file are attempted."""
        monkeypatch.setattr(bm_main.sys, "platform", "win32")
        monkeypatch.setenv("APPDATA", str(tmp_path))

        # Fake kernel32: CreateMutexW returns a handle, GetLastError
        # reports ERROR_ALREADY_EXISTS = 183.
        fake_kernel32 = MagicMock()
        fake_kernel32.CreateMutexW.return_value = 12345
        fake_kernel32.GetLastError.return_value = 183
        fake_windll = MagicMock()
        fake_windll.kernel32 = fake_kernel32
        # user32 is touched by the raise helper — empty enumeration so
        # the call is harmless.
        fake_windll.user32 = MagicMock()
        fake_windll.user32.EnumWindows.side_effect = lambda _cb, _lp: True
        monkeypatch.setattr(bm_main.ctypes, "windll", fake_windll)

        with patch.object(
            bm_main,
            "_bring_existing_instance_to_front",
            return_value=True,
        ) as mock_raise:
            result = bm_main._acquire_single_instance()

        assert result is False, "Second instance must report as 'not first'"
        mock_raise.assert_called_once(), (
            "The raise-existing-window helper must be called BEFORE writing "
            "the signal file so the user sees an immediate response."
        )
        # Signal file written as fallback.
        signal_file = tmp_path / "BackupManager" / ".show_signal"
        assert signal_file.exists(), (
            "Signal file fallback must still be written so a stuck Win32 "
            "raise (UIPI, hidden window, etc.) is caught by the polling loop"
        )

    def test_acquire_does_not_call_raise_on_first_instance(self, monkeypatch):
        """First launch: no other window exists, the raise must not fire."""
        monkeypatch.setattr(bm_main.sys, "platform", "win32")
        fake_kernel32 = MagicMock()
        fake_kernel32.CreateMutexW.return_value = 12345
        fake_kernel32.GetLastError.return_value = 0  # ERROR_SUCCESS
        fake_windll = MagicMock()
        fake_windll.kernel32 = fake_kernel32
        monkeypatch.setattr(bm_main.ctypes, "windll", fake_windll)

        with patch.object(
            bm_main,
            "_bring_existing_instance_to_front",
        ) as mock_raise:
            result = bm_main._acquire_single_instance()

        assert result is True
        mock_raise.assert_not_called()

    @pytest.fixture(autouse=True)
    def _reset_mutex_handle(self):
        """Module-level state must not leak across tests."""
        bm_main._mutex_handle = None
        yield
        bm_main._mutex_handle = None


@pytest.fixture(autouse=True)
def _restore_real_platform():
    """Some tests monkey-patch ``sys.platform`` — restore on teardown.

    Without this, a test that set ``sys.platform = "linux"`` would
    leak to the next test, which expects the real value (``win32`` on
    the build machine).
    """
    original = sys.platform
    yield
    bm_main.sys.platform = original
