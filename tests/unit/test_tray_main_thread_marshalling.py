"""Regression tests for tray-callback marshalling onto the Tk main thread.

pystray runs its icon menu loop in a daemon thread, so menu callbacks
fire from a non-main thread.  Tk is not thread-safe — calling
``deiconify``/``destroy``/widget mutation from the tray thread causes
render races that surface as a fully-blank main window after clicking
"Show window" (the v3.3.14 blank-window bug).

Every tray-originated action must therefore go through ``root.after(0,
fn)``, which schedules the call on the Tk event loop running on the
main thread.  These tests pin that contract so a future refactor cannot
silently re-introduce direct Tk calls from the tray thread.
"""

from unittest.mock import Mock

from src.ui.app import BackupManagerApp


def test_on_tray_show_schedules_via_after():
    """``_on_tray_show`` must defer ``_show_window`` via ``root.after(0, ...)``."""
    fake_self = Mock()
    BackupManagerApp._on_tray_show(fake_self)
    fake_self.root.after.assert_called_once_with(0, fake_self._show_window)


def test_on_tray_run_schedules_via_after():
    """``_on_tray_run`` must defer ``_run_backup`` via ``root.after(0, ...)``."""
    fake_self = Mock()
    BackupManagerApp._on_tray_run(fake_self)
    fake_self.root.after.assert_called_once_with(0, fake_self._run_backup)


def test_on_tray_quit_schedules_via_after():
    """``_on_tray_quit`` must defer ``_quit_app`` via ``root.after(0, ...)``."""
    fake_self = Mock()
    BackupManagerApp._on_tray_quit(fake_self)
    fake_self.root.after.assert_called_once_with(0, fake_self._quit_app)


def test_on_tray_show_does_not_call_tk_directly():
    """Direct Tk operations from the tray thread cause render races.

    Guard against a regression where ``_show_window`` is invoked
    synchronously instead of being scheduled on the Tk loop.
    """
    fake_self = Mock()
    BackupManagerApp._on_tray_show(fake_self)
    fake_self._show_window.assert_not_called()


def test_on_tray_quit_does_not_call_tk_directly():
    """``_quit_app`` calls ``root.destroy`` — must not run on tray thread."""
    fake_self = Mock()
    BackupManagerApp._on_tray_quit(fake_self)
    fake_self._quit_app.assert_not_called()
